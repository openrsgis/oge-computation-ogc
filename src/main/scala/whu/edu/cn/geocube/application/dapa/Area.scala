package whu.edu.cn.geocube.application.dapa

import java.io.File

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.TileUtil

import scala.collection.mutable.ArrayBuffer

object Area {
  /**
   * Return links to single raster file with one or multiple bands.
   * Used bu OGC DAPA area endpoint.
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
  def getDapaAreaRaster(sc: SparkContext, cubeId: String, productName: String, bbox: String, startTime: String, endTime: String, aggregate: String, measurementsStr: String, outputDir: String): String = {
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
    val timeSeriesStitches: Array[Raster[MultibandTile]] = temporalGroupRdd.map { x =>
      val metadata = srcMetadata
      val layout = metadata.layout

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

        val cropStitch = stitched.crop(colMin.toInt, stitched.rows - rowMax.toInt - 1, colMax.toInt, stitched.rows - rowMin.toInt - 1)
        cropStitch
      } else null
    }.filter(_ != null).collect()

    val timeLength = timeSeriesStitches.length
    val measurementNum = measurements.length
    val resultsArrayTile = new ArrayBuffer[Tile]()

    aggregate match {
      /*case "min" => {
        (0 until measurementNum).foreach{i =>
          var tile = timeSeriesStitches(0).tile.band(i)
          (1 until timeLength).foreach{j =>
            tile = tile.combineDouble(timeSeriesStitches(j).tile.band(i)){(a, b) =>
              if(!a.equals(Double.NaN) && !b.equals(Double.NaN)) math.min(a, b)
              else if (!a.equals(Double.NaN)) a
              else if (!b.equals(Double.NaN)) b
              else Double.NaN
            }

          }
          resultsArrayTile.append(tile)
        }
      }*/
      case "min" => {
        (0 until measurementNum).foreach{i =>
          var tile = timeSeriesStitches(0).tile.band(i)
          (1 until timeLength).foreach{j =>
            tile = tile.combineDouble(timeSeriesStitches(j).tile.band(i)){(a, b) => math.min(a, b)}
          }
          resultsArrayTile.append(tile)
        }
      }
      case "max" => {
        (0 until measurementNum).foreach{i =>
          var tile = timeSeriesStitches(0).tile.band(i)
          (1 until timeLength).foreach{j =>
            tile = tile.combineDouble(timeSeriesStitches(j).tile.band(i)){(a, b) => math.max(a, b)}
          }
          resultsArrayTile.append(tile)
        }
      }
      case "avg" => {
        (0 until measurementNum).foreach{i =>
          var tile = timeSeriesStitches(0).tile.band(i)
          (1 until timeLength).foreach{j =>
            tile = tile.combineDouble(timeSeriesStitches(j).tile.band(i)){(a, b) => a + b}
          }
          tile = tile / timeLength
          resultsArrayTile.append(tile)
        }
      }
      case _ => throw new RuntimeException("Not support " + aggregate)
    }

    val resultsMultibandTile = MultibandTile(resultsArrayTile)

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "result.TIF"
    GeoTiff(Raster(resultsMultibandTile, timeSeriesStitches(0).extent), srcMetadata.crs).write(outputPath)
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
      .setAppName("DAPA area")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getDapaAreaRaster(sc, cubeId, rasterProductName, extent, startTime, endTime, aggregate, measurements, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))


    //test
    /*val tile1: Tile = IntArrayTile(Array(1,1,1,1,1,1,1,1,1),3,3)
    val tile2: Tile = IntArrayTile(Array(4,4,4,4,4,4,4,4,4),3,3)
    val tile3: Tile = IntArrayTile(Array(2,2,2,2,2,2,2,2,2),3,3)


    val multibandTile1: MultibandTile = MultibandTile(Array(tile1, tile2, tile3))
    val multibandTile2: MultibandTile = MultibandTile(Array(tile1, tile2, tile3))
    val multibandTile3: MultibandTile = MultibandTile(Array(tile1, tile2, tile3))
    val arrayMultibandTile: Array[MultibandTile] = Array(multibandTile1, multibandTile2, multibandTile3)

    val arrayTile = new ArrayBuffer[Tile]()
    (0 until 3).foreach{i =>
      var tile = arrayMultibandTile(0).band(i)
      (1 until 3).foreach{j =>
        tile = tile.combine(arrayMultibandTile(j).band(i)){(a, b) => a + b}
      }
      arrayTile.append(tile)
    }
    val results = MultibandTile(arrayTile)
    (0 until results.bandCount).foreach{i =>
      results.band(i).foreach(println(_))
    }*/



  }

}
