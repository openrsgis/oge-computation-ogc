package whu.edu.cn.geocube.application.dapa

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.{MultibandTile, Raster, Tile}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.TileUtil

import scala.collection.mutable.ArrayBuffer

object ValuePosition {
  /**
   * Return links to a single text/plain file with a single value (each with values from one or multiple bands)
   * Used bu OGC DAPA value position endpoint.
   *
   * @param sc
   * @param productName
   * @param point
   * @param startTime
   * @param endTime
   * @param aggregate
   * @param measurementsStr
   * @param outputDir
   * @return
   */
  def getDapaValuePosition(sc: SparkContext, cubeId: String, productName: String, point: String, startTime: String, endTime: String, aggregate: String, measurementsStr: String, outputDir: String): String = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val position = point.split(",").map(_.toDouble)
    val extent = Array(position(0), position(1), position(0) + 0.1, position(1) + 0.1)
    val measurements = measurementsStr.split(",")
    val queryParams: QueryParams = new QueryParams()
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductName(productName)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(measurements)
    val rasterTileLayerRdd:(RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, queryParams)
    val srcMetadata = rasterTileLayerRdd._2.tileLayerMetadata

    val rasterTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRdd._1
      .groupBy(_._1.spaceTimeKey)
      .map{x =>
        val tilePair = x._2.toArray
        val measurementArray = measurements
        val multibandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
        measurementArray.foreach{ measurement =>
          tilePair.foreach{ele =>
            if(ele._1.measurementName.equals(measurement))
              multibandTiles.append(ele._2)
          }
        }
        val multibandTile =  MultibandTile(multibandTiles)
        (x._1, multibandTile)
      }

    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,MultibandTile)])] = rasterTileRdd.groupBy(_._1.instant)
    val timeResults = temporalGroupRdd.flatMap { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)

      val tileLayerArray: Array[(SpatialKey, MultibandTile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[MultibandTile] = TileUtil.stitchMultiband(tileLayerArray, layout)
      val srcExtent = stitched.extent
      val col = math.floor((position(0) - srcExtent.xmin) / stitched.cellSize.width).toInt
      val row = stitched.rows - 1 - math.floor((position(1) - srcExtent.ymin) / stitched.cellSize.height).toInt
      val multibandPixelValue = new ArrayBuffer[(String, Float)]()
      (0 until measurements.length).foreach{i =>
        multibandPixelValue.append((measurements(i), stitched.tile.band(i).getDouble(col, row).toFloat))
      }
      multibandPixelValue
    }.collect()

    val timeGroupResuts: Map[String, Array[Float]] = timeResults.groupBy(_._1).map(x=>(x._1, x._2.map(_._2)))
    val results = aggregate match {
      case "min" => {
        timeGroupResuts.map(x => (x._1, x._2.min))
      }
      case "max" => {
        timeGroupResuts.map(x => (x._1, x._2.max))
      }
      case "avg" => {
        timeGroupResuts.map(x => (x._1, x._2.sum/x._2.length))
      }
    }

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "result.txt"
    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    results.foreach{x =>
      bw.write(x._1 + "," + x._2)
      bw.newLine()
    }
    bw.close()

    if(!sc.isStopped) sc.stop()
    outputPath
  }

  def main(args: Array[String]): Unit = {
    //parse the web request params
    val cubeId = args(0)
    val rasterProductName = args(1)
    val point = args(2)
    val startTime = args(3)
    val endTime = args(4)
    val aggregate = args(5)
    val measurements = args(6)
    val outputDir = args(7)

    println("cubeId: " + cubeId)
    println("rasterProductName: " + rasterProductName)
    println("point: " + point)
    println("time: " + (startTime, endTime))
    println("aggregate: " + aggregate)
    println("measurements:" + measurements)


    val conf = new SparkConf()
      .setAppName("DAPA value position")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getDapaValuePosition(sc, cubeId, rasterProductName, point, startTime, endTime, aggregate, measurements, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))
  }

}
