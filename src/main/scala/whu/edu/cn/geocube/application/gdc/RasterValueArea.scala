package whu.edu.cn.geocube.application.gdc

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

object RasterValueArea {
  /**
   * Return links to single text/csv file with 1d timeseries (each with values from one or multiple bands)
   * Used by WHU GDC value area endpoint.
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
  def getGdcRasterValueArea(sc: SparkContext, cubeId: String, productName: String, bbox: String, startTime: String, endTime: String, aggregate: String, measurementsStr: String, outputDir: String): String = {
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

    val bandGroupRdd:RDD[((String,Long),Iterable[(SpaceTimeKey,Tile)])] = rasterTileLayerRdd._1.map(x=>((x._1._measurementName,x._1.spaceTimeKey.instant),(x._1.spaceTimeKey,x._2))).groupByKey()
    val method : RDD[String] = sc.parallelize(Array(aggregate))
    val instants = rasterTileLayerRdd._1.map(x=>x._1.spaceTimeKey.instant).distinct().map(ele=>{
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new Date(ele)
      val instantRet = sdf.format(date)
      instantRet
    }
    ).union(method).collect()
    val results = bandGroupRdd.flatMap { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val band = x._1._1
      val instant = x._1._2
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new Date(instant)
      val instantRet = sdf.format(date)

      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val srcExtent = stitched.extent
      val extent_ = extent
      val requestedExtent = new Extent(extent_(0), extent_(1), extent_(2), extent_(3))
      if (requestedExtent.intersects(srcExtent)) {
        val intersection = requestedExtent.intersection(srcExtent).get
        val colMin = math.floor((intersection.xmin - srcExtent.xmin) / stitched.cellSize.width)
        val colMax = math.floor((intersection.xmax - srcExtent.xmin) / stitched.cellSize.width)
        val rowMin = math.floor((intersection.ymin - srcExtent.ymin) / stitched.cellSize.height)
        val rowMax = math.floor((intersection.ymax - srcExtent.ymin) / stitched.cellSize.height)
        val cropTile = stitched.crop(colMin.toInt, stitched.rows - rowMax.toInt - 1, colMax.toInt, stitched.rows - rowMin.toInt - 1).tile

        aggregate match {
          case "min" => {
            val minMultiInstantPixelValue = new ArrayBuffer[(String, (String, Float))]()
            var minPixelValue = Float.MaxValue
            cropTile.foreachDouble{pixelValue => if(pixelValue < minPixelValue) minPixelValue = pixelValue.toFloat}
            minMultiInstantPixelValue.append((band, (instantRet, minPixelValue)))
            minMultiInstantPixelValue
          }
          case "max" => {
            val maxMultiInstantPixelValue = new ArrayBuffer[(String, (String, Float))]()
            var maxPixelValue = Float.MinValue
            cropTile.foreachDouble{pixelValue => if(pixelValue > maxPixelValue) maxPixelValue = pixelValue.toFloat}
            maxMultiInstantPixelValue.append((band, (instantRet, maxPixelValue)))
            maxMultiInstantPixelValue
          }
          case "avg" => {
            val avgMultiInstantPixelValue = new ArrayBuffer[(String, (String, Float))]()
            var sumPixelValue: Float = 0.0f
            cropTile.foreachDouble{pixelValue => sumPixelValue += pixelValue.toFloat}
            avgMultiInstantPixelValue.append((band, (instantRet, sumPixelValue/(cropTile.rows*cropTile.cols))))
            avgMultiInstantPixelValue
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
    bw.write("time,value,")
    bw.newLine()
    results.groupBy(_._1).map{x =>
      val band = x._1
//      val values = new StringBuilder
//      values.append(band + ",")
      bw.write(band + ",")
      bw.newLine()
      val instantValues = x._2.map(_._2).toMap
      instants.foreach{instant =>
        instant match {
          case "min" => {
            bw.write(aggregate+","+instantValues.values.min+",")
            bw.newLine()
          }
          case "max" => {
            bw.write(aggregate+","+instantValues.values.max+",")
            bw.newLine()
          }
          case "avg" => {
            bw.write(aggregate+","+instantValues.values.sum / instants.length+",")
            bw.newLine()
          }
          case _ =>{
            bw.write(instant+","+instantValues.get(instant).get+",")
            bw.newLine()
          }
        }
      }

//      values.deleteCharAt(values.length - 1)
//      bw.write(values.toString())
//      bw.newLine()
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
      .setAppName("GDC timeseries area")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getGdcRasterValueArea(sc, cubeId, rasterProductName, extent, startTime, endTime, aggregate, measurements, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))
  }

}

