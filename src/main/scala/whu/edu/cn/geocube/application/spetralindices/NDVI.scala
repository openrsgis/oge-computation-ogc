package whu.edu.cn.geocube.application.spetralindices

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render.ColorRamp
import geotrellis.spark._

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}

import sys.process._
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.view.Info


/**
 * Generate NDVI product.
 *
 * NDVI = (NIR – Red) / (NIR + Red)
 */
object NDVI {
  /**
   * This NDVI function is used in Jupyter Notebook, e.g. cloud free ndvi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold            NDVI threshold
   * @return results Info containing thematic ndvi product path, time, product type and vegetation-cover area.
   */
  def ndvi(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tranTileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile.
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi: Tile = ndviTile(redBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndvi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd: RDD[(Long, Iterable[(SpaceTimeKey, Tile)])] = NDVIRdd.groupBy(_._1.instant)

    val results: RDD[Info] = temporalGroupRdd.map { x =>
      //stitch extent-series tiles of each time instant to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

      val colorRamp = ColorRamp(
        0xB96230FF,
        0xDB9842FF,
        0xDFAC6CFF,
        0xE3C193FF,
        0xE6D6BEFF,
        0xE4E7C4FF,
        0xD9E2B2FF,
        0xBBCA7AFF,
        0x9EBD4DFF,
        0x569543FF
      )
      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/" //"/home/geocube/environment_test/geocube_core_jar/"
      val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_ndvi_" + instant + ".png")

      //generate ndvi thematic product
      val outputTiffPath = outputDir + uuid + "_ndvi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_ndvi_thematic" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "NDVI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      new Info(outputThematicPngPath, instant, "NDVI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This NDVI function is used in Jupyter Notebook.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold              NDVI threshold
   * @return results info containing thematic ndvi product path, time, product type and vegetation-cover area.
   */
  def ndvi(implicit sc: SparkContext,
           tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //transform tile array to tile rdd
    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val latiRad = (tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile.
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi: Tile = ndviTile(redBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndvi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd: RDD[(Long, Iterable[(SpaceTimeKey, Tile)])] = NDVIRdd.groupBy(_._1.instant)

    val results: RDD[Info] = temporalGroupRdd.map { x =>
      //stitch extent-series tiles of each time instant to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

      val colorRamp = ColorRamp(
        0xB96230FF,
        0xDB9842FF,
        0xDFAC6CFF,
        0xE3C193FF,
        0xE6D6BEFF,
        0xE4E7C4FF,
        0xD9E2B2FF,
        0xBBCA7AFF,
        0x9EBD4DFF,
        0x569543FF
      )
      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/" //"/home/geocube/environment_test/geocube_core_jar/"
      val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_ndvi_" + instant + ".png")

      //generate ndvi thematic product
      val outputTiffPath = outputDir + uuid + "_ndvi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_ndvi_thematic" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "NDVI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      new Info(outputThematicPngPath, instant, "NDVI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This NDVI function is used in web service and web platform.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold            NDVI threshold
   * @param outputDir
   * @return
   */
  def ndvi(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double,
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
    //and generate ndvi tile.
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi: Tile = ndviTile(redBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndvi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd: RDD[(Long, Iterable[(SpaceTimeKey, Tile)])] = NDVIRdd.groupBy(_._1.instant)

    //stitch extent-series tiles of each time instant to pngs
    temporalGroupRdd.foreach { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)
      val extentRet = stitched.extent
      println("<--------" + instantRet + ": " + extentRet + "-------->")
      val colorRamp = ColorRamp(
        0xB96230AA,
        0xDB9842AA,
        0xDFAC6CAA,
        0xE3C193AA,
        0xE6D6BEAA,
        0xE4E7C4AA,
        0xD9E2B2AA,
        0xBBCA7AAA,
        0x9EBD4DAA,
        0x569543AA
      )

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputPath = executorOutputDir + "NDVI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath = executorOutputDir + "NDVI_" + instantRet + ".json"
      val objectMapper = new ObjectMapper()
      val node = objectMapper.createObjectNode()
      node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
      node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
      node.put("time", instantRet)
      node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
      val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir

      scpPngCommand.!
      scpMetaCommand1.!
    }

  }

  /**
   * This NDVI function is used in web service and web platform.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc                     a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold              NDVI threshold
   * @param outputDir
   * @return
   */
  def ndvi(implicit sc: SparkContext,
           tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double,
           outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for (i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    //transform tile array to tile rdd
    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)

    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile.
    val NDVIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndvi: Tile = ndviTile(redBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndvi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd: RDD[(Long, Iterable[(SpaceTimeKey, Tile)])] = NDVIRdd.groupBy(_._1.instant)

    //stitch extent-series tiles of each time instant to pngs
    temporalGroupRdd.foreach { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)
      val extentRet = stitched.extent
      println("<--------" + instantRet + ": " + extentRet + "-------->")
      val colorRamp = ColorRamp(
        0xB96230AA,
        0xDB9842AA,
        0xDFAC6CAA,
        0xE3C193AA,
        0xE6D6BEAA,
        0xE4E7C4AA,
        0xD9E2B2AA,
        0xBBCA7AAA,
        0x9EBD4DAA,
        0x569543AA
      )

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputPath = executorOutputDir + "NDVI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath = executorOutputDir + "NDVI_" + instantRet + ".json"
      val objectMapper = new ObjectMapper()
      val node = objectMapper.createObjectNode()
      node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
      node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
      node.put("time", instantRet)
      node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
      val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir

      scpPngCommand.!
      scpMetaCommand1.!
    }

  }


  /**
   * Generate a ndvi tile of DoubleConstantNoDataCellType.
   *
   * @param redBandTile Red band Tile
   * @param nirBandTile Nir band Tile
   * @param threshold
   * @return a ndvi tile of DoubleConstantNoDataCellType
   */
  def ndviTile(redBandTile: Tile, nirBandTile: Tile, threshold: Double): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleRedBandTile = DoubleArrayTile(redBandTile.toArrayDouble(), redBandTile.cols, redBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate ndvi tile
    val ndviTile = Divide(
      Subtract(doubleNirBandTile, doubleRedBandTile),
      Add(doubleNirBandTile, doubleRedBandTile))

    ndviTile.mapDouble(pixel => {
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    })
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
    queryParams.setMeasurements(Array("Red", "Near-Infrared")) //该条件为NDVI分析固定条件
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndvi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("NDVI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    ndvi(sc, tileLayerArrayWithMeta, 0.3, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/

    /**
     * Using raster tile RDD as input
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
      .setAppName("NDVI analysis")
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
    queryParams.setMeasurements(Array("Red", "Near-Infrared")) //该条件为NDVI分析固定条件
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()
    //ndvi
    val analysisBegin = System.currentTimeMillis()
    ndvi(tileLayerRddWithMeta, 0.3, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))

    /**
     * API test
     */
    /*//query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,30.073457222586285,113.9181165740333,30.9597805438586)
    queryParams.setTime("2018-07-01 00:00:00.000", "2018-12-01 00:00:00.000")
    queryParams.setMeasurements(Array("Red", "Near-Infrared"))
    //queryParams.setLevel("4000")
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndvi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("NDVI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    ndvi(sc, tileLayerArrayWithMeta, 0.3, "/home/geocube/environment_test/geocube_core_jar/")
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/

  }

}

