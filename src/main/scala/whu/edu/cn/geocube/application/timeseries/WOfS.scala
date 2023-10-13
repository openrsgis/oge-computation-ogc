package whu.edu.cn.geocube.application.timeseries

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.Extent
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.{Add, LessOrEqual, Subtract}
import geotrellis.raster.render.{ColorRamp, Exact, GreaterThanOrEqualTo, LessThanOrEqualTo, RGB}

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot, localHtmlRoot}

import scala.collection.mutable.ArrayBuffer
import sys.process._
import scala.util.control.Breaks._
import whu.edu.cn.geocube.application.spetralindices._
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.core.entity.{RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.QueryRasterTiles
import whu.edu.cn.geocube.util.{RuntimeData, TileUtil}
import whu.edu.cn.geocube.view.Info
import whu.edu.cn.geocube.application.timeseries.WOfSBatchScript._

/**
 * Generate Water Observations from Space (WOfS) product,
 * which is acquired by detecting the presence of water
 * during a time period, and summarizing the results as
 * the number of times that water is detected at each pixel.
 */
object WOfS {
  /**
   * Used in Jupyter Notebook.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input.
   *
   * @param tileLayerRddWithMeta a rdd of raster tiles
   * @return result info
   */
  def wofs(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
    println("Task is running ...")
    val tranTileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)

    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata
    val results = new ArrayBuffer[Info]()

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = NDWI.ndwiTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
    val spatialGroupRdd = NDWIRdd.groupBy(_._1.spatialKey).cache()

    //calculate water presence frequency
    val wofsRdd: RDD[(SpatialKey, Tile)] = spatialGroupRdd
      .map { x =>
        val list = x._2.toList
        val timeLength = list.length
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val wofsTile = ArrayTile(Array.fill(cols * rows)(Double.NaN), cols, rows)
        for (i <- 0 until cols; j <- 0 until rows) {
          var waterCount: Double = 0.0
          var nanCount: Int = 0
          for (k <- list) {
            val value = k._2.getDouble(i, j)
            if (value.equals(Double.NaN)) nanCount += 1
            else waterCount += value / 255.0
          }
          wofsTile.setDouble(i, j, waterCount / (timeLength - nanCount))
        }
        (x._1, wofsTile)
      }

    //metadata
    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    //stitch and generate thematic product
    val wofsTileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(wofsRdd, changedSpatialMetadata)
    val stitched = wofsTileLayerRdd.stitch()

    val colorRamp = ColorRamp(
      0xF7DA22AA,
      0xECBE1DAA,
      0xE77124AA,
      0xD54927AA,
      0xCF3A27AA,
      0xA33936AA,
      0x7F182AAA,
      0x68101AAA
    )
    val uuid = UUID.randomUUID
    stitched.tile.renderPng(colorRamp).write(localHtmlRoot + uuid + "_wofs.png")

    val outputTiffPath = localHtmlRoot + uuid + "_wofs.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = localHtmlRoot + uuid + "_wofs_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Wofs", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)
    results += Info(outputThematicPngPath, 0, "Water Observations from Space")
    results.toArray
  }

  /**
   * Used in Jupyter Notebook.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta an array of raster tiles
   * @return result info
   */
  def wofs(implicit sc: SparkContext, tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- WOfS task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- WOfS task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)

    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata
    val results = new ArrayBuffer[Info]()

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = NDWI.ndwiTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
    val spatialGroupRdd = NDWIRdd.groupBy(_._1.spatialKey).cache()

    //calculate water presence frequency
    val wofsRdd: RDD[(SpatialKey, Tile)] = spatialGroupRdd //RDD[(SpatialKey, Iterable((SpaceTimeKey, Tile)))]
      .map { x =>
        val list = x._2.toList
        val timeLength = list.length
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val wofsTile = ArrayTile(Array.fill(cols * rows)(Double.NaN), cols, rows)
        val rd = new RuntimeData(0, 16, 0)
        val assignedCellNum = cols / rd.defThreadCount
        val flag = Array.fill(rd.defThreadCount)(0)
        for (threadId <- 0 until rd.defThreadCount) {
          new Thread() {
            override def run(): Unit = {
              for (i <- (threadId * assignedCellNum) until (threadId + 1) * assignedCellNum; j <- 0 until rows) {
                var waterCount: Double = 0.0
                var nanCount: Int = 0
                for (k <- list) {
                  val value = k._2.getDouble(i, j)
                  if (value.equals(Double.NaN)) nanCount += 1
                  else waterCount += value / 255.0
                }
                wofsTile.setDouble(i, j, waterCount / (timeLength - nanCount))
              }
              flag(threadId) = 1
            }
          }.start()
        }
        while (flag.contains(0)) {
          try {
            Thread.sleep(1)
          } catch {
            case ex: InterruptedException => {
              ex.printStackTrace() // 打印到标准err
              System.err.println("exception===>: ...")
            }
          }
        }
        (x._1, wofsTile)
      }

    //metadata
    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    //stitch and generate thematic product
    val wofsTileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(wofsRdd, changedSpatialMetadata)
    val stitched = wofsTileLayerRdd.stitch()

    val colorRamp = ColorRamp(
      0xF7DA22AA,
      0xECBE1DAA,
      0xE77124AA,
      0xD54927AA,
      0xCF3A27AA,
      0xA33936AA,
      0x7F182AAA,
      0x68101AAA
    )
    val uuid = UUID.randomUUID
    stitched.tile.renderPng(colorRamp).write(localHtmlRoot + uuid + "_wofs.png")

    val outputTiffPath = localHtmlRoot + uuid + "_wofs.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = localHtmlRoot + uuid + "_wofs_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Wofs", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

    results += Info(outputThematicPngPath, 0, "Water Observations from Space")
    val ret = results.toArray
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Used in web service and web platform.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input.
   *
   * @param tileLayerRddWithMeta a rdd of tiles
   * @param outputDir
   * @return
   */
  def wofs(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           outputDir: String): Unit = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for (i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tranTileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = NDWI.ndwiShortTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
    val spatialGroupRdd = NDWIRdd.groupBy(_._1.spatialKey)

    //calculate water presence frequency
    val wofsRdd: RDD[(SpatialKey, Tile)] = spatialGroupRdd
      .map { x =>
        val list = x._2.toList
        val timeLength = list.length
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val wofsTile = FloatArrayTile(Array.fill(cols * rows)(0.0f), cols, rows)
        val rd = new RuntimeData(0, 16, 0)
        val assignedCellNum = cols / rd.defThreadCount
        val flag = Array.fill(rd.defThreadCount)(0)
        for (threadId <- 0 until rd.defThreadCount) {
          new Thread() {
            override def run(): Unit = {
              for (i <- (threadId * assignedCellNum) until (threadId + 1) * assignedCellNum; j <- 0 until rows) {
                var waterCount: Double = 0.0
                var nanCount: Int = 0
                for (k <- list) {
                  val value = k._2.get(i, j)
                  if (isNoData(value)) nanCount += 1
                  else waterCount += value / 255.0
                }
                wofsTile.setDouble(i, j, waterCount / (timeLength - nanCount))
              }
              flag(threadId) = 1
            }
          }.start()
        }
        while (flag.contains(0)) {
          try {
            Thread.sleep(1)
          } catch {
            case ex: InterruptedException => {
              ex.printStackTrace()
              System.err.println("exception===>: ...")
            }
          }
        }
        (x._1, wofsTile)
      }

    //metadata
    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      FloatCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    //stitch and generate thematic product
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(wofsRdd, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()
    val extentRet = stitched.extent

    val map: Map[Double, Int] =
      Map(
        0.00 -> 0xFFFFFFFF,
        0.11 -> 0xF6EDB1FF,
        0.22 -> 0xF7DA22FF,
        0.33 -> 0xECBE1DFF,
        0.44 -> 0xE77124FF,
        0.55 -> 0xD54927FF,
        0.66 -> 0xCF3A27FF,
        0.77 -> 0xA33936FF,
        0.88 -> 0x7F182AFF,
        0.99 -> 0x68101AFF
      )

    val colorMap =
      ColorMap(
        map,
        ColorMap.Options(
          classBoundaryType = GreaterThanOrEqualTo,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = true
        )
      )

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "WOfS.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WOfS.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  /**
   * Used in web service and web platform.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta Query tiles
   * @param outputDir
   * @return
   */
  def wofs(implicit sc: SparkContext,
           tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           outputDir: String): Unit = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for (i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = NDWI.ndwiTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
    val spatialGroupRdd = NDWIRdd.groupBy(_._1.spatialKey).cache()

    val timeDimension = spatialGroupRdd.map(ele => ele._2.toList.length).reduce((x, y) => Math.max(x, y))

    //calculate water presence frequency
    val wofsRdd: RDD[(SpatialKey, Tile)] = spatialGroupRdd
      .map { x =>
        val list = x._2.toList
        val timeLength = list.length
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val wofsTile = ArrayTile(Array.fill(cols * rows)(Double.NaN), cols, rows)
        val rd = new RuntimeData(0, 16, 0)
        val assignedCellNum = cols / rd.defThreadCount
        val flag = Array.fill(rd.defThreadCount)(0)
        for (threadId <- 0 until rd.defThreadCount) {
          new Thread() {
            override def run(): Unit = {
              for (i <- (threadId * assignedCellNum) until (threadId + 1) * assignedCellNum; j <- 0 until rows) {
                var waterCount: Double = 0.0
                var nanCount: Int = 0
                for (k <- list) {
                  val value = k._2.getDouble(i, j)
                  if (value.equals(Double.NaN)) nanCount += 1
                  else waterCount += value / 255.0
                }
                wofsTile.setDouble(i, j, waterCount / (timeLength - nanCount))
              }
              flag(threadId) = 1
            }
          }.start()
        }
        while (flag.contains(0)) {
          try {
            Thread.sleep(1)
          } catch {
            case ex: InterruptedException => {
              ex.printStackTrace()
              System.err.println("exception===>: ...")
            }
          }
        }
        (x._1, wofsTile)
      }

    //metadata
    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    //stitch and generate thematic product
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(wofsRdd, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()
    val extentRet = stitched.extent
    println("<--------wofs: " + extentRet + "-------->")

    val colorArray = Array(
      0x68101AFF,
      0x7F182AFF,
      0xA33936FF,
      0xCF3A27FF,
      0xD54927FF,
      0xE77124FF,
      0xECBE1DFF,
      0xF7DA22FF,
      0xF6EDB1FF,
      0xFFFFFFFF)

    val map: Map[Double, Int] =
      if (timeDimension < 10) {
        val pixelValue = Array.fill(timeDimension + 1)(0.0)
        (0 until pixelValue.length).map { i =>
          pixelValue(i) = (pixelValue(i) + i) / timeDimension.toDouble
          (pixelValue(i), colorArray(pixelValue.length - i - 1))
        }.toMap
      } else {
        Map(
          0.0 -> 0xFFFFFFFF,
          0.11 -> 0xF6EDB1FF,
          0.22 -> 0xF7DA22FF,
          0.33 -> 0xECBE1DFF,
          0.44 -> 0xE77124FF,
          0.55 -> 0xD54927FF,
          0.66 -> 0xCF3A27FF,
          0.77 -> 0xA33936FF,
          0.88 -> 0x7F182AFF,
          0.99 -> 0x68101AFF
        )
      }

    val colorMap =
      ColorMap(
        map,
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "WOfS.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WOfS.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  /**
   * Generate WOfS product with multiple spatial batches.
   *
   * Used in web service and web platform.
   *
   * @param batches     num of batches
   * @param queryParams query parameters
   * @param outputDir
   */
  def wofsSpatialBatch(batches: Int,
                       queryParams: QueryParams,
                       outputDir: String): Unit = {
    var queryTimeCost: Long = 0
    var analysisTimeCost: Long = 0

    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for (i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val gridCodes: Array[String] = queryParams.getGridCodes.toArray
    println(gridCodes.length)
    val batchInterval = gridCodes.length / batches

    var queryTilesCount: Long = 0
    val spatialTiles = ArrayBuffer[(SpatialKey, Tile)]()
    if (batchInterval == gridCodes.length) {
      throw new RuntimeException("Grid codes length is " + gridCodes.length + ", batches is " + batches + ", batch interval is " + batchInterval + ", which is too large, please increase the batches!")
    } else if (batchInterval <= 1) {
      throw new RuntimeException("Grid codes length is " + gridCodes.length + ", batches is " + batches + ", batch interval is " + batchInterval + ", which is too small, please reduce the batches!")
    } else {
      (0 until batches).foreach { i =>
        breakable {
          val conf = new SparkConf()
            .setAppName("WOfS analysis")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
          val sc = new SparkContext(conf)

          val batchGridCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
          ((i * batchInterval) until (i + 1) * batchInterval).foreach(j => batchGridCodes.append(gridCodes(j)))
          if (i == (batches - 1) && gridCodes.length % batches != 0) {
            ((i + 1) * batchInterval until gridCodes.length).foreach(j => batchGridCodes.append(gridCodes(j)))
          }
          queryParams.setGridCodes(batchGridCodes)

          //query batch tiles
          val batchQueryBegin = System.currentTimeMillis()
          val tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
            DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
          if (tileLayerRddWithMeta == null) {
            sc.stop()
            break()
          }
          queryTilesCount += tileLayerRddWithMeta._1.count()
          val batchQueryEnd = System.currentTimeMillis()
          queryTimeCost += (batchQueryEnd - batchQueryBegin)

          //analyze batch tiles
          val batchAnalysisBegin = System.currentTimeMillis()
          val batchWOfS: Array[(SpatialKey, Tile)] = wofsSpatialBatch(tileLayerRddWithMeta)
          spatialTiles.appendAll(batchWOfS)
          val batchAnalysisEnd = System.currentTimeMillis()
          analysisTimeCost += (batchAnalysisEnd - batchAnalysisBegin)

          print("########### Query grid codes length:" + gridCodes.length + ", grid codes of spatial batch " + i + ":")
          batchGridCodes.foreach(ele => print(ele + " "));
          println(" ###########")

          sc.stop()
        }
      }
    }

    val stitchBegin = System.currentTimeMillis()

    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tileSize: Int = spatialTiles(0)._2.rows
    val tl = TileLayout(360, 180, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)
    val stitched = TileUtil.stitch(spatialTiles.toArray, ld)
    val extentRet = stitched.extent
    println("<--------wofs: " + extentRet + "-------->")

    val map: Map[Double, Int] =
      Map(
        0.00 -> 0xFFFFFFFF,
        0.11 -> 0xF6EDB1FF,
        0.22 -> 0xF7DA22FF,
        0.33 -> 0xECBE1DFF,
        0.44 -> 0xE77124FF,
        0.55 -> 0xD54927FF,
        0.66 -> 0xCF3A27FF,
        0.77 -> 0xA33936FF,
        0.88 -> 0x7F182AFF,
        0.99 -> 0x68101AFF
      )

    val colorMap =
      ColorMap(
        map,
        ColorMap.Options(
          classBoundaryType = GreaterThanOrEqualTo,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = true
        )
      )

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputPath = executorOutputDir + "WOfS.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WOfS.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)

    val stitchEnd = System.currentTimeMillis()
    analysisTimeCost += (stitchEnd - stitchBegin)

    if (batchInterval == gridCodes.length) {
      throw new RuntimeException("Batch interval is " + batchInterval + ", which is too large, please increase the batches!")
    } else if (batchInterval <= 1) {
      throw new RuntimeException("Batch interval is " + batchInterval + ", which is too small, please reduce the batches!")
    } else {
      (0 until batches).foreach { i =>
        val batchGridCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
        ((i * batchInterval) until (i + 1) * batchInterval).foreach(j => batchGridCodes.append(gridCodes(j)))
        if (i == (batches - 1) && gridCodes.length % batches != 0) {
          ((i + 1) * batchInterval until gridCodes.length).foreach(j => batchGridCodes.append(gridCodes(j)))
        }
        print("########### Query grid codes length:" + gridCodes.length + ", grid codes of spatial batch " + i + ":")
        batchGridCodes.foreach(ele => print(ele + " "));
        println(" ###########")
      }
    }

    println("Query time of " + queryTilesCount + " raster tiles: " + queryTimeCost + "ms")
    println("Analysis time: " + analysisTimeCost + "ms")
  }

  /**
   * Generate WOfS product with multiple spatial batches.
   *
   * Used in web service and web platform.
   *
   * @param tileLayerArrayWithMeta
   * @return
   */
  def wofsSpatialBatch(tileLayerArrayWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Array[(SpatialKey, Tile)] = {
    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        /*if(greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi:Tile = NDWI.ndwiShortTile(greenBandTile.get, nirBandTile.get, 0.01)*/
        val ndwi: Tile = if (greenBandTile == None || nirBandTile == None) null else NDWI.ndwiShortTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }.filter(x => x._2 != null)

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
    val spatialGroupRdd = NDWIRdd.groupBy(_._1.spatialKey)

    //calculate water presence frequency using hybrid parallel technology
    val wofsRdd: RDD[(SpatialKey, Tile)] = spatialGroupRdd
      .map { x =>
        val list = x._2.toList
        val timeLength = list.length
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val wofsTile = FloatArrayTile(Array.fill(cols * rows)(0.0f), cols, rows)
        val rd = new RuntimeData(0, 16, 0)
        val assignedCellNum = cols / rd.defThreadCount
        val flag = Array.fill(rd.defThreadCount)(0)
        for (threadId <- 0 until rd.defThreadCount) {
          new Thread() {
            override def run(): Unit = {
              for (i <- (threadId * assignedCellNum) until (threadId + 1) * assignedCellNum; j <- 0 until rows) {
                var waterCount: Double = 0.0
                var nanCount: Int = 0
                for (k <- list) {
                  val value = k._2.get(i, j)
                  if (isNoData(value)) nanCount += 1
                  else waterCount += value / 255.0
                }
                wofsTile.setDouble(i, j, waterCount / (timeLength - nanCount))
              }
              flag(threadId) = 1
            }
          }.start()
        }
        while (flag.contains(0)) {
          try {
            Thread.sleep(1)
          } catch {
            case ex: InterruptedException => {
              ex.printStackTrace()
              System.err.println("exception===>: ...")
            }
          }
        }
        (x._1, wofsTile)
      }
    wofsRdd.collect()
  }

  def main(args: Array[String]): Unit = {
    /**
     * Using raster tile array as input
     */
    /*//parse the web request params
    val rasterProductNames = args(0).split(",")
    val extent = args(1).split(",").map(_.toDouble)
    val startTime = args(2) + " 00:00:00.000"
    val endTime = args(3) + " 00:00:00.000"
    val outputDir = args(4)
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndwi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("WOfS analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    wofs(sc, tileLayerArrayWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/

    /**
     * Using raster tile rdd as input
     */
    //parse the web request params
    val cubeId = args(0)
    val rasterProductNames = args(1).split(",")
    val extent = args(2).split(",").map(_.toDouble)
    val startTime = args(3) + " 00:00:00.000"
    val endTime = args(4) + " 00:00:00.000"
    val outputDir = args(5)
    println("rasterProductName: " + rasterProductNames.foreach(x => print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    val conf = new SparkConf()
      .setAppName("WOfS analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(Array("Green", "Near-Infrared")) //该条件为NDWI分析固定条件
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //wofs
    val analysisBegin = System.currentTimeMillis()
    wofs(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))

    /**
     * Using batch raster tile rdd as input
     */
    /*//parse the web request params
    val rasterProductNames = args(0).split(",")
    val extent = args(1).split(",").map(_.toDouble)
    val startTime = args(2)
    val endTime = args(3)
    val outputDir = args(4)

    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    //Query parameters
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(Array("Green", "Near-Infrared")) //该条件为NDWI分析固定条件
    queryParams.setLevel("4000")

    //WOfS analysis
    //wofsSpatialBatch(batches = 28, queryParams, outputDir) //some bugs for this function
    wofsBatchExecutor(batches = 25, queryParams, outputDir)*/

    /**
     * API test
     */
    /*val conf = new SparkConf()
      .setAppName("WOfS analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,30.073457222586285,113.9181165740333,30.9597805438586)
    queryParams.setTime("2018-07-01 00:00:00.000", "2018-12-01 00:00:00.000")
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    queryParams.setLevel("4000")
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //wofs
    val analysisBegin = System.currentTimeMillis()
    wofs(tileLayerRddWithMeta, "/home/geocube/environment_test/geocube_core_jar/")
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))*/
  }
}

