package whu.edu.cn.geocube.application.spetralindices

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
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
 * Generate MNDWI product.
 *
 * mNDWI = NDSI = (Green – SWIR1) / (Green + SWIR1)
 */
object MNDWI {
  /**
   * This MNDWI function is used in Jupyter Notebook, e.g. cloud free mndwi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold MNDWI threshold
   *
   * @return results Info containing thematic mndwi product path, time, product type and water body area.
   */
  def mndwi(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
            threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- MNDWI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- MNDWI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate mndwi tile.
    val MNDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate mndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, swir1BandTile) = (bandTileMap.get("Green"), bandTileMap.get("SWIR 1"))
        if (greenBandTile == None || swir1BandTile == None)
          throw new RuntimeException("There is no Green band or SWIR 1 band")
        val mndwi: Tile = mndwiTile(greenBandTile.get, swir1BandTile.get, threshold)
        (spaceTimeKey, mndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = MNDWIRdd.groupBy(_._1.instant)

    val results:RDD[Info] = temporalGroupRdd.map{x =>
      //stitch extent-series tiles of each time instant to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele=>(ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

      val colorRamp = ColorRamp(
        0xD76B27FF,
        0xE68F2DFF,
        0xF9B737FF,
        0xF5CF7DFF,
        0xF0E7BBFF,
        0xEDECEAFF,
        0xC8E1E7FF,
        0xADD8EAFF,
        0x7FB8D4FF,
        0x4EA3C8FF,
        0x2586ABFF
      )
      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
    val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_mndwi_" + instant + ".png")

      //generate mndwi thematic product
      val outputTiffPath = outputDir + uuid + "_mndwi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_mndwi_thematic" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "MNDWI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      new Info(outputThematicPngPath, instant, "MNDWI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This MNDWI function is used in Jupyter Notebook.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold MNDWI threshold
   *
   * @return results info containing thematic mndwi product path, time, product type and water body area.
   */
  def mndwi(implicit sc:SparkContext,
            tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
            threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- MNDWI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- MNDWI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //transform tile array to tile rdd
    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val latiRad = (tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate mndwi tile.
    val MNDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate mndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, swir1BandTile) = (bandTileMap.get("Green"), bandTileMap.get("SWIR 1"))
        if (greenBandTile == None || swir1BandTile == None)
          throw new RuntimeException("There is no Green band or SWIR 1 band")
        val mndwi: Tile = mndwiTile(greenBandTile.get, swir1BandTile.get, threshold)
        (spaceTimeKey, mndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = MNDWIRdd.groupBy(_._1.instant)

    val results:RDD[Info] = temporalGroupRdd.map{x =>
      //stitch extent-series tiles of each time instant to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele=>(ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

      val colorRamp = ColorRamp(
        0xD76B27FF,
        0xE68F2DFF,
        0xF9B737FF,
        0xF5CF7DFF,
        0xF0E7BBFF,
        0xEDECEAFF,
        0xC8E1E7FF,
        0xADD8EAFF,
        0x7FB8D4FF,
        0x4EA3C8FF,
        0x2586ABFF
      )
      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
      val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_mndwi_" + instant + ".png")

      //generate mndwi thematic product
      val outputTiffPath = outputDir + uuid + "_mndwi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_mndwi_thematic" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "MNDWI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      new Info(outputThematicPngPath, instant, "MNDWI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This MNDWI function is used in web service and web platform.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold MNDWI threshold
   * @param outputDir
   *
   * @return
   */
  def mndwi(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
            threshold: Double,
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
    //and generate mndwi tile.
    val MNDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate mndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, swir1BandTile) = (bandTileMap.get("Green"), bandTileMap.get("SWIR 1"))
        if (greenBandTile == None || swir1BandTile == None)
          throw new RuntimeException("There is no Green band or SWIR 1 band")
        val mndwi: Tile = mndwiTile(greenBandTile.get, swir1BandTile.get, threshold)
        (spaceTimeKey, mndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = MNDWIRdd.groupBy(_._1.instant)

    //stitch extent-series tiles of each time instant to pngs
    temporalGroupRdd.foreach{x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele=>(ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)
      val extentRet = stitched.extent
      println("<--------" + instantRet + ": " + extentRet + "-------->")
      val colorRamp = ColorRamp(
        0xD76B27AA,
        0xE68F2DAA,
        0xF9B737AA,
        0xF5CF7DAA,
        0xF0E7BBAA,
        0xEDECEAAA,
        0xC8E1E7AA,
        0xADD8EAAA,
        0x7FB8D4AA,
        0x4EA3C8AA,
        0x2586ABAA
      )

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputPath = executorOutputDir + "MNDWI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath = executorOutputDir + "MNDWI_" + instantRet + ".json"
      val objectMapper =new ObjectMapper()
      val node: ObjectNode = objectMapper.createObjectNode()
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
   * This MNDWI function is used in web service and web platform.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc A SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold MNDWI threshold
   * @param outputDir
   *
   * @return
   */
  def mndwi(implicit sc:SparkContext,
            tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
            threshold: Double,
            outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    //transform tile array to tile rdd
    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate mndwi tile.
    val MNDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate mndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, swir1BandTile) = (bandTileMap.get("Green"), bandTileMap.get("SWIR 1"))
        if (greenBandTile == None || swir1BandTile == None)
          throw new RuntimeException("There is no Green band or SWIR 1 band")
        val mndwi: Tile = mndwiTile(greenBandTile.get, swir1BandTile.get, threshold)
        (spaceTimeKey, mndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = MNDWIRdd.groupBy(_._1.instant)

    //stitch extent-series tiles of each time instant to pngs
    temporalGroupRdd.foreach{x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele=>(ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)
      val extentRet = stitched.extent
      println("<--------" + instantRet + ": " + extentRet + "-------->")
      val colorRamp = ColorRamp(
        0xD76B27AA,
        0xE68F2DAA,
        0xF9B737AA,
        0xF5CF7DAA,
        0xF0E7BBAA,
        0xEDECEAAA,
        0xC8E1E7AA,
        0xADD8EAAA,
        0x7FB8D4AA,
        0x4EA3C8AA,
        0x2586ABAA
      )

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputPath = executorOutputDir + "MNDWI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath = executorOutputDir + "MNDWI_" + instantRet + ".json"
      val objectMapper =new ObjectMapper()
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
   * Generate a mndwi tile of DoubleConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param swir1BandTile Swir1 band Tile
   * @param threshold
   *
   * @return a mndwi tile of DoubleConstantNoDataCellType
   */
  def mndwiTile(greenBandTile: Tile, swir1BandTile: Tile, threshold: Double): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleGreenBandTile = DoubleArrayTile(greenBandTile.toArrayDouble(), greenBandTile.cols, greenBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleSwir1BandTile = DoubleArrayTile(swir1BandTile.toArrayDouble(), swir1BandTile.cols, swir1BandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate mndwi tile
    val mndwiTile = Divide(
      Subtract( doubleGreenBandTile, doubleSwir1BandTile ),
      Add( doubleGreenBandTile, doubleSwir1BandTile) )

    mndwiTile.mapDouble(pixel=>{
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    })
  }

  def main(args: Array[String]): Unit = {
    /**
     * Using raster tile array as input
     */
    //parse the web request params
    /*val rasterProductNames = args(0).split(",")
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
    queryParams.setMeasurements(Array("Green", "SWIR 1"))
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //mndwi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("MNDWI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    mndwi(sc, tileLayerArrayWithMeta, 0.1, outputDir)
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
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    val conf = new SparkConf()
      .setAppName("MNDWI analysis")
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
    queryParams.setMeasurements(Array("Green", "SWIR 1"))
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //mndwi
    val analysisBegin = System.currentTimeMillis()
    mndwi(tileLayerRddWithMeta, 0.1, outputDir)
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
    queryParams.setMeasurements(Array("Green", "SWIR 1"))
    //queryParams.setLevel("4000")
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //mndwi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("MNDWI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    mndwi(sc, tileLayerArrayWithMeta, 0.1, "/home/geocube/environment_test/geocube_core_jar/")
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/
  }

}
