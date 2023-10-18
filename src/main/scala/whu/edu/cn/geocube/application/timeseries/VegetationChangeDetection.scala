package whu.edu.cn.geocube.application.timeseries

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark._
import geotrellis.raster.render.{ColorRamp, Exact, RGB}
import org.apache.spark.rdd.RDD

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot, localHtmlRoot}

import scala.collection.mutable.ArrayBuffer
import sys.process._
import whu.edu.cn.geocube.core.entity.{RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.application.spetralindices.NDVI
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
import whu.edu.cn.geocube.view.Info

object VegetationChangeDetection{
  /**
   * Detect vegetation change between two instants,
   * used in Jupyter Notebook.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input,
   * which contains Red and Near-Infrared band
   * tiles of two instants.
   *
   * @param tileLayerRddWithMeta a rdd of raster tiles
   *
   * @return results info
   */
  def vegetationChangeDetection(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
    println("Task is running ...")
    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata
    val results = new ArrayBuffer[Info]()

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if(redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi:Tile = NDVI.ndviTile(redBandTile.get, nirBandTile.get, 0.2)
        (spaceTimeKey, ndvi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDVIRdd
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
          255.0 -> RGB(0, 255, 0)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    stitched.tile.renderPng(colorMap).write(localHtmlRoot + uuid + "_vegetation_change.png")

    val outputTiffPath = localHtmlRoot + uuid + "_vegetation_change.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = localHtmlRoot + uuid + "_vegetation_change_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Vegetation_change", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

    results += Info(outputThematicPngPath, 0, "Vegetation Change Detection")

    results.toArray
  }

  /**
   * Detect vegetation change between two instants,
   * used in Jupyter Notebook.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input,
   * which contains Red and Near-Infrared band
   * tiles of two instants.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of raster tiles
   *
   * @return results info
   */
  def vegetationChangeDetection(implicit sc:SparkContext, tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey])): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata
    val results = new ArrayBuffer[Info]()

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if(redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi:Tile = NDVI.ndviTile(redBandTile.get, nirBandTile.get, 0.5)
        (spaceTimeKey, ndvi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDVIRdd
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
    val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"
    val uuid = UUID.randomUUID
    val colorMap =
      ColorMap(
        Map(
          -255.0 -> RGB(255, 0, 0),
          0.0 -> RGB(255, 255, 0),
          255.0 -> RGB(0, 255, 0)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    stitched.tile.renderPng(colorMap).write(outputDir + uuid + "_vegetation_change.png")

    val outputTiffPath = outputDir + uuid + "_vegetation_change.TIF"
    GeoTiff(stitched, srcMetadata.crs).write(outputTiffPath)
    val outputThematicPngPath = outputDir + uuid + "_vegetation_change_thematic.png"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    Seq("/home/geocube/qgis/run.sh", "-t", "Vegetation_change", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

    results += Info(outputThematicPngPath, 0, "Vegetation Change Detection")

    val ret = results.toArray
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Detect vegetation change between two instants,
   * used in web service and web platform.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] as input,
   * which contains Red and Near-Infrared band
   * tiles of two instants.
   *
   * @param tileLayerRddWithMeta a rdd of raster tiles
   * @param outputDir
   */
  def vegetationChangeDetection(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
                                outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if(redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi:Tile = NDVI.ndviTile(redBandTile.get, nirBandTile.get, 0.5)
        (spaceTimeKey, ndvi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDVIRdd
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
          255.0 -> RGB(0, 255, 0)
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

    val outputPath = executorOutputDir + "VegetationChangeDetection.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "VegetationChangeDetection.json"
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  /**
   * Detect vegetation change between two instants,
   * used in web service and web platform.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] as input,
   * which contains Green and Near-Infrared band
   * tiles of two instants.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of raster tiles
   * @param outputDir
   */
  def vegetationChangeDetection(implicit sc: SparkContext,
                                tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
                                outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if(redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi:Tile = NDVI.ndviTile(redBandTile.get, nirBandTile.get, 0.5)
        (spaceTimeKey, ndvi)
      }

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRDD: RDD[(SpatialKey, Tile)] = NDVIRdd
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
          255.0 -> RGB(0, 255, 0)
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

    val outputPath = executorOutputDir + "VegetationChangeDetection.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "VegetationChangeDetection.json"
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  def main(args:Array[String]):Unit = {
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
      .setAppName("VegetationChangeDetection analysis")
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
    queryParams.setMeasurements(Array("Red", "Near-Infrared"))
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()

    //vegetation change detection
    val analysisBegin = System.currentTimeMillis()
    vegetationChangeDetection(sc, tileLayerArrayWithMeta, outputDir)
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
      .setAppName("VegetationChangeDetection analysis")
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
    queryParams.setMeasurements(Array("Red", "Near-Infrared")) //该条件为植被变化监测涉及NDVI分析的固定条件
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //vegetation change detection
    val analysisBegin = System.currentTimeMillis()
    vegetationChangeDetection(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))

    /**
     * API test.
     */
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
    queryParams.setMeasurements(Array("Red", "Near-Infrared"))
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //water change detection
    val analysisBegin = System.currentTimeMillis()
    vegetationChangeDetection(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time : " + (analysisEnd - analysisBegin))*/
  }
}

