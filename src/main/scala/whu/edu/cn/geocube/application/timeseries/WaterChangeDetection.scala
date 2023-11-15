package whu.edu.cn.geocube.application.timeseries

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.{ColorRamp, Exact, GreaterThanOrEqualTo, RGB}

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot, localHtmlRoot}

import scala.collection.mutable.ArrayBuffer
import sys.process._
import whu.edu.cn.geocube.application.spetralindices._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
import whu.edu.cn.geocube.core.raster.query.QueryRasterTiles
import whu.edu.cn.geocube.view.Info

/**
 * Detect water change between two instants
 */
object WaterChangeDetection {
  /**
   * Detect water change between two instants,
   * used in Jupyter Notebook.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input,
   * which contains Green and Near-Infrared band
   * tiles of two instants.
   *
   * @param tileLayerRddWithMeta a rdd of raster tiles
   * @return results info
   */
  def waterChangeDetection(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
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

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More or less than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }
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
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()
    val uuid = UUID.randomUUID
    val colorMap =
      ColorMap(
        Map(
          -255.0 -> RGB(255, 0, 0),
          0.0 -> RGB(255, 255, 0),
          255.0 -> RGB(0, 0, 255)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    stitched.tile.renderPng(colorMap).write(localHtmlRoot + uuid + "_water_change.png")

    val outputTiffPath = localHtmlRoot + uuid + "_water_change.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = localHtmlRoot + uuid + "_water_change_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Water_change", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

    results += Info(outputThematicPngPath, 0, "Water Change Detection")
    results.toArray
  }

  /**
   * Detect water change between two instants,
   * used in Jupyter Notebook.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input,
   * which contains Green and Near-Infrared band
   * tiles of two instants.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta an array of raster tiles
   * @return results info
   */
  def waterChangeDetection(implicit sc: SparkContext, tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
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

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More or less than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }
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
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()

    val uuid = UUID.randomUUID
    val colorMap =
      ColorMap(
        Map(
          -255.0 -> RGB(255, 0, 0),
          0.0 -> RGB(255, 255, 0),
          255.0 -> RGB(0, 0, 255)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    stitched.tile.renderPng(colorMap).write(localHtmlRoot + uuid + "_water_change.png")

    val outputTiffPath = localHtmlRoot + uuid + "_water_change.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = localHtmlRoot + uuid + "_water_change_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Water_change", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

    results += Info(outputThematicPngPath, 0, "Water Change Detection")
    val ret = results.toArray
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }


  /**
   * Detect water change between two instants,
   * used in web service and web platform.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input,
   * which contains Green and Near-Infrared band
   * tiles of two instants.
   *
   * @param tileLayerRddWithMeta a rdd of raster tiles
   * @param outputDir
   */
  def waterChangeDetection(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                           outputDir: String): Unit = {
    println("Task is running ...")
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
        val ndwi: Tile = NDWI.ndwiTile(greenBandTile.get, nirBandTile.get, 0.01)
        (spaceTimeKey, ndwi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }
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
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()
    val extentRet = stitched.extent

    val colorMap =
      ColorMap(
        Map(
          -255.0 -> RGB(255, 0, 0),
          0.0 -> RGB(255, 255, 0),
          255.0 -> RGB(0, 0, 255)
        ),
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

    val outputPath = executorOutputDir + "WaterChangeDetection.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WaterChangeDetection.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)

    //GeoTiff(stitched, resultsRdd.metadata.crs).write("/home/geocube/environment_test/geocube_core_jar/Ls8_ard_water_change.TIF")
  }


  /**
   * Detect water change between two instants,
   * used in web service and web platform.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input,
   * which contains Green and Near-Infrared band
   * tiles of two instants.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta an array of raster tiles
   * @param outputDir
   */
  def waterChangeDetection(implicit sc: SparkContext,
                           tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                           outputDir: String): Unit = {
    println("Task is running ...")
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

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }
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
    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    val stitched = resultsRdd.stitch()
    val extentRet = stitched.extent

    val colorMap =
      ColorMap(
        Map(
          -255.0 -> RGB(255, 0, 0),
          0.0 -> RGB(255, 255, 0),
          255.0 -> RGB(0, 0, 255)
        ),
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

    val outputPath = executorOutputDir + "WaterChangeDetection.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WaterChangeDetection.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()

    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  /**
   * Detect water change between two instants.
   *
   * Using RasterRDD as input, which contains ndwi tiles of two instants.
   *
   * @param ndwiRasterRdd a RasterRdd of ndwi tiles
   * @return a rdd of water change detection result
   */
  def waterChangeDetectionUsingNDWIProduct(ndwiRasterRdd: RasterRDD): TileLayerRDD[SpatialKey] = {
    //println("Task is running ...")
    val ndwiTileLayerRddWithMeta: (RDD[(SpaceTimeKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
      (ndwiRasterRdd.map(x => (x._1.spaceTimeKey, x._2)), ndwiRasterRdd.rasterTileLayerMetadata)

    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = ndwiTileLayerRddWithMeta._1
    val srcMetadata: TileLayerMetadata[SpaceTimeKey] = ndwiTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }

      }

    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    resultsRdd
  }

  /**
   * Detect water change between two instants.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input,
   * which contains ndwi tiles of two instants.
   *
   * @param ndwiTileLayerRddWithMeta a rdd of ndwi tiles
   * @return a rdd of water change detection result
   */
  def waterChangeDetectionUsingNDWIProduct(ndwiTileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): TileLayerRDD[SpatialKey] = {
    println("Task is running ...")
    val tranNdwiTileLayerRddWithMeta: (RDD[(SpaceTimeKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
      (ndwiTileLayerRddWithMeta._1.map(x => (x._1.spaceTimeKey, x._2)), ndwiTileLayerRddWithMeta._2)

    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = tranNdwiTileLayerRddWithMeta._1
    val srcMetadata: TileLayerMetadata[SpaceTimeKey] = tranNdwiTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }

      }

    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    resultsRdd
  }


  /**
   * Detect water change between two instants.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input,
   * which contains ndwi tiles of two instants.
   *
   * @param sc                         A SparkContext
   * @param ndwiTileLayerArrayWithMeta an array of raster tiles
   *
   */
  def waterChangeDetectionUsingNDWIProduct(implicit sc: SparkContext,
                                           ndwiTileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): TileLayerRDD[SpatialKey] = {
    //println("Task is running ...")
    val ndwiTileLayerRddWithMeta: (RDD[(SpaceTimeKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(ndwiTileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, x._2))), ndwiTileLayerArrayWithMeta._2)

    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = ndwiTileLayerRddWithMeta._1
    val srcMetadata: TileLayerMetadata[SpaceTimeKey] = ndwiTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDWIRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        if (list.length != 2) {
          throw new RuntimeException("More than 2 instants")
        }
        else {
          val instant1 = list(0)._1.instant
          val instant2 = list(1)._1.instant
          if (instant1 < instant2) {
            val previous: (SpaceTimeKey, Tile) = list(0)
            val rear: (SpaceTimeKey, Tile) = list(1)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          } else {
            val previous: (SpaceTimeKey, Tile) = list(1)
            val rear: (SpaceTimeKey, Tile) = list(0)
            val changedTile = rear._2 - previous._2
            (x._1, changedTile)
          }
        }

      }

    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val changedSpatialMetadata = TileLayerMetadata(
      DoubleCellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    val resultsRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRDD, changedSpatialMetadata)
    resultsRdd
  }

  def main(args: Array[String]): Unit = {
    /**
     * Using raster tile array as input
     */
    /*//parse the web request params
    val rasterProductNames = args(0).split(",")
    val extent = args(1).split(",").map(_.toDouble)
    val previousTime = args(2) + " 00:00:00.000"
    val nextTime = args(3) + " 00:00:00.000"
    val outputDir = args(4)
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (previousTime, nextTime))

    val sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val previousDate = sj.parse(previousTime)
    val calendar = Calendar.getInstance
    calendar.setTime(previousDate)
    val previousStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val previousEnd = sj.format(calendar.getTime)

    val nextDate = sj.parse(nextTime)
    calendar.setTime(nextDate)
    val nextStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val nextEnd = sj.format(calendar.getTime)

    val conf = new SparkConf()
      .setAppName("WaterChangeDetection analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))

    queryParams.setTime(previousStart, previousEnd)
    queryParams.setNextTime(nextStart, nextEnd)
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()

    //water change detection
    val analysisBegin = System.currentTimeMillis()
    waterChangeDetection(sc, tileLayerArrayWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))*/

    /**
     * Using raster tile rdd as input
     */
    //parse the web request params
    val cubeId = args(0)
    val rasterProductNames = args(1).split(",")
    val extent = args(2).split(",").map(_.toDouble)
    val previousTime = args(3) + " 00:00:00.000"
    val nextTime = args(4) + " 00:00:00.000"
    val outputDir = args(5)
    println("rasterProductName: " + rasterProductNames.foreach(x => print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (previousTime, nextTime))

    val sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val previousDate = sj.parse(previousTime)
    val calendar = Calendar.getInstance
    calendar.setTime(previousDate)
    val previousStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val previousEnd = sj.format(calendar.getTime)

    val nextDate = sj.parse(nextTime)
    calendar.setTime(nextDate)
    val nextStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val nextEnd = sj.format(calendar.getTime)

    val conf = new SparkConf()
      .setAppName("WaterChangeDetection analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))

    queryParams.setTime(previousStart, previousEnd)
    queryParams.setNextTime(nextStart, nextEnd)
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //water change detection
    val analysisBegin = System.currentTimeMillis()
    waterChangeDetection(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))

    /**
     * API test.
     */
    //parse the web request params
    /*val rasterProductNames = Array("LC08_L1TP_ARD_EO")
    val extent = Array(113.01494046724021,30.073457222586285,113.9181165740333,30.959780543858)
    val previousTime = "2018-07-29" + " 00:00:00.000"
    val nextTime = "2018-11-02" + " 00:00:00.000"
    val outputDir = "/home/geocube/environment_test/geocube_core_jar/"
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (previousTime, nextTime))

    val sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val previousDate = sj.parse(previousTime)
    val calendar = Calendar.getInstance
    calendar.setTime(previousDate)
    val previousStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val previousEnd = sj.format(calendar.getTime)

    val nextDate = sj.parse(nextTime)
    calendar.setTime(nextDate)
    val nextStart = sj.format(calendar.getTime)
    calendar.add(Calendar.DATE, 1)
    val nextEnd = sj.format(calendar.getTime)

    val conf = new SparkConf()
      .setAppName("WaterChangeDetection analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))

    queryParams.setTime(previousStart, previousEnd)
    queryParams.setNextTime(nextStart, nextEnd)
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //water change detection
    val analysisBegin = System.currentTimeMillis()
    waterChangeDetection(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))*/
  }
}

