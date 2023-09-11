package whu.edu.cn.geocube.application.gdc

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.TileUtil

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import sys.process._

object RasterCube {
  /**
   * Return links to multiple raster files with one or multiple bands (one per time).
   * Used by WHU GDC cube endpoint.
   *
   * @param sc
   * @param cubeId
   * @param productName
   * @param bbox
   * @param startTime
   * @param endTime
   * @param measurementsStr
   * @param outputDir
   * @return
   */
  def getGdcRasterCube(sc: SparkContext, cubeId: String, productName: String, bbox: String, startTime: String, endTime: String, measurementsStr: String, outputDir: String): Array[String] = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    //保证主节点创建目录, 下面的map任务再次创建则保证被分配任务的节点具有目录
    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val extent = bbox.split(",").map(_.toDouble)
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
    val results = temporalGroupRdd.map { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new Date(instant)
      val instantRet = sdf.format(date)

      val tileLayerArray: Array[(SpatialKey, MultibandTile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[MultibandTile] = TileUtil.stitchMultiband(tileLayerArray, layout)

      val srcExtent = stitched.extent
      val extent_ = extent
      val requestedExtent = new Extent(extent_(0), extent_(1), extent_(2), extent_(3))
      if (requestedExtent.intersects(srcExtent)) {
        val intersection = requestedExtent.intersection(srcExtent).get
        val colMin = math.floor((intersection.xmin - srcExtent.xmin) / stitched.cellSize.width)
        val colMax = math.floor((intersection.xmax - srcExtent.xmin) / stitched.cellSize.width)
        val rowMin = math.floor((intersection.ymin - srcExtent.ymin) / stitched.cellSize.height)
        val rowMax = math.floor((intersection.ymax - srcExtent.ymin) / stitched.cellSize.height)

        val executorSessionDir = sessionDir.toString
        val executorSessionFile = new File(executorSessionDir)
        if (!executorSessionFile.exists) executorSessionFile.mkdir
        val executorOutputDir = outputDir
        val executorOutputFile = new File(executorOutputDir)
        if (!executorOutputFile.exists()) executorOutputFile.mkdir()


        val extentStr = intersection.getEnvelopeInternal.getMinX.formatted("%.2f") + "_" + intersection.getEnvelopeInternal.getMinY.formatted("%.2f") +
          "_" + intersection.getEnvelopeInternal.getMaxX.formatted("%.2f") + "_" + intersection.getEnvelopeInternal.getMaxY.formatted("%.2f")
        val outputPath = executorOutputDir + extentStr + "_" + instantRet + ".TIF"
        GeoTiff(stitched.crop(colMin.toInt, stitched.rows - rowMax.toInt - 1, colMax.toInt, stitched.rows - rowMin.toInt - 1), metadata.crs).write(outputPath)
        val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + executorOutputDir
        scpPngCommand.!
        outputPath
      } else {
        "N/A"
      }
    }

    val resultsPath = results.collect().filter(!_.equals("N/A"))
    resultsPath.foreach(println(_))
    if(!sc.isStopped) sc.stop()
    resultsPath
  }

  def main(args: Array[String]): Unit = {
    //parse the web request params
    val cubeId = args(0)
    val rasterProductName = args(1)
    val extent = args(2)
    val startTime = args(3)
    val endTime = args(4)
    val measurements = args(5)
    val outputDir = args(6)

    println("cubeId: " + cubeId)
    println("rasterProductName: " + rasterProductName)
    println("extent: " + extent)
    println("time: " + (startTime, endTime))
    println("measurements:" + measurements)


    val conf = new SparkConf()
      .setAppName("GDC cube")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getGdcRasterCube(sc, cubeId, rasterProductName, extent, startTime, endTime, measurements, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))
  }

}

