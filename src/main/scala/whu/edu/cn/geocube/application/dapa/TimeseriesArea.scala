package whu.edu.cn.geocube.application.dapa

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.TileUtil

import scala.collection.mutable.ArrayBuffer

object TimeseriesArea {
  /**
   * Return links to single text/csv file with 1d timeseries (each with values from one or multiple bands)
   * Used bu OGC DAPA timeseries area endpoint.
   *
   * @param sc
   * @param productName
   * @param bbox
   * @param startTime
   * @param endTime
   * @param measurementsStr
   * @param outputDir
   * @return
   */
  def getDapaTimeseriesArea(sc: SparkContext, cubeId: String, productName: String, bbox: String, startTime: String, endTime: String, aggregate: String, measurementsStr: String, outputDir: String): String = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

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
    val results = temporalGroupRdd.flatMap { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val sdf = new SimpleDateFormat("yyyy_MM_dd")
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
        val cropMultibandTile = stitched.crop(colMin.toInt, stitched.rows - rowMax.toInt - 1, colMax.toInt, stitched.rows - rowMin.toInt - 1).tile

        aggregate match {
          case "min" => {
            val minMultibandPixelValue = new ArrayBuffer[(String, (String, Float))]()
            (0 until measurements.length).foreach{i =>
              val tile = cropMultibandTile.band(i)
              var minPixelValue = Float.MaxValue
              tile.foreachDouble{pixelValue => if(pixelValue < minPixelValue) minPixelValue = pixelValue.toFloat}
              //minMultibandPixelValue.append((instantRet + "/" + measurements(i), minPixelValue))
              minMultibandPixelValue.append((instantRet, (measurements(i), minPixelValue)))
            }
            minMultibandPixelValue
          }
          case "max" => {
            val maxMultibandPixelValue = new ArrayBuffer[(String, (String, Float))]()
            (0 until measurements.length).foreach{i =>
              val tile = cropMultibandTile.band(i)
              var maxPixelValue = Float.MinValue
              tile.foreachDouble{pixelValue => if(pixelValue > maxPixelValue) maxPixelValue = pixelValue.toFloat}
              //maxMultibandPixelValue.append((instantRet + "/" + measurements(i), maxPixelValue))
              maxMultibandPixelValue.append((instantRet, (measurements(i), maxPixelValue)))
            }
            maxMultibandPixelValue
          }
          case "avg" => {
            val avgMultibandPixelValue = new ArrayBuffer[(String, (String, Float))]()
            (0 until measurements.length).foreach{i =>
              val tile = cropMultibandTile.band(i)
              var sumPixelValue: Float = 0.0f
              tile.foreachDouble{pixelValue => sumPixelValue += pixelValue.toFloat}
              //avgMultibandPixelValue.append((instantRet + "/" + measurements(i), sumPixelValue/(tile.rows*tile.cols)))
              avgMultibandPixelValue.append((instantRet, (measurements(i), sumPixelValue/(tile.rows*tile.cols))))
            }
            avgMultibandPixelValue
          }
        }
      } else {
        null
      }
    }.filter(_ != null).collect()

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "result.csv"

    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("," + measurementsStr)
    bw.newLine()
    results.groupBy(_._1).map{x =>
      val instant = x._1
      val values = new StringBuilder
      values.append(instant + ",")
      val measurementValues = x._2.map(_._2).toMap
      measurements.foreach{name =>
        values.append(measurementValues.get(name).get)
        values.append(",")
      }
      values.deleteCharAt(values.length - 1)
      bw.write(values.toString())
      bw.newLine()
    }
    bw.close()

    /*val bw = new BufferedWriter(new FileWriter(file))
    results.foreach{x =>
      bw.write(x._1 + "," + x._2)
      bw.newLine()
    }
    bw.close()*/

    if(!sc.isStopped) sc.stop()
    outputPath
  }

  def main(args: Array[String]): Unit = {
    //parse the web request params
    val cubeId = args(0)
    val rasterProductName = args(1)
    val extent = args(2)
    val startTime = args(3)
    val endTime = args(4)
    val aggregate = args(5)
    val measurements = args(6)
    val outputDir = args(7)

    println("cubeId: " + cubeId)
    println("rasterProductName: " + rasterProductName)
    println("extent: " + extent)
    println("time: " + (startTime, endTime))
    println("aggregate: " + aggregate)
    println("measurements:" + measurements)


    val conf = new SparkConf()
      .setAppName("DAPA timeseries area")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getDapaTimeseriesArea(sc, cubeId, rasterProductName, extent, startTime, endTime, aggregate, measurements, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))
  }

}
