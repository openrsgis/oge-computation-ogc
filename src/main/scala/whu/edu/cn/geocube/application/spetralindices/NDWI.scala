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
 * Generate NDWI product.
 *
 * NDWI = (Green – NIR) / (Green + NIR).
 */
object NDWI {
  /**
   * This NDWI function is used in Jupyter Notebook, e.g. cloud free ndwi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold NDWI threshold
   *
   * @return results info containing thematic ndwi product path, time, product type and water body area.
   */
  def ndwi(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDWI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDWI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile.
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = ndwiTile(greenBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = NDWIRdd.groupBy(_._1.instant)

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
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_ndwi_" + instant + ".png")

      //generate ndwi thematic product
      val outputTiffPath = outputDir + uuid + "_ndwi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_ndwi_thematic_" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "NDWI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)
      new Info(outputThematicPngPath, instant, "NDWI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This NDWI function is used in Jupyter Notebook.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold NDWI threshold
   *
   * @return results info containing thematic ndwi product path, time, product type and water body area.
   */
  def ndwi(implicit sc:SparkContext,
           tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDWI task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDWI task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //transform tile array to tile rdd
    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val latiRad = (tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile.
    val ndwiRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = ndwiTile(greenBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = ndwiRdd.groupBy(_._1.instant)

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
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_ndwi_" + instant + ".png")

      //generate ndwi thematic product
      val outputTiffPath = outputDir + uuid + "_ndwi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_ndwi_thematic_" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "NDWI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      //results info containing thematic ndwi product path, time, product type and water body area.
      new Info(outputThematicPngPath, instant, "NDWI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * This NDWI function is used in web service and web platform.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold NDWI threshold
   * @param outputDir
   *
   * @return
   */
  def ndwi(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
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
    //and generate ndwi tile.
    val NDWIRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        /*if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = ndwiTile(greenBandTile.get, nirBandTile.get, threshold)*/
        val ndwi: Tile = if(greenBandTile == None || nirBandTile == None) null else ndwiTile(greenBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndwi)
      }.filter(x => x._2 != null)

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = NDWIRdd.groupBy(_._1.instant)

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

      val outputPath = executorOutputDir + "NDWI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath = executorOutputDir + "NDWI_" + instantRet + ".json"
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
   * This NDWI function is used in web service and web platform.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold NDWI threshold
   * @param outputDir
   *
   * @return
   */
  def ndwi(implicit sc: SparkContext,
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
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), 32), tileLayerArrayWithMeta._2)

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile.
    val ndwiRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwi: Tile = ndwiTile(greenBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, ndwi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = ndwiRdd.groupBy(_._1.instant)

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

      val outputPath = executorOutputDir + "NDWI_" + instantRet + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)

      val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + outputDir
      val outputMetaPath = executorOutputDir + "NDWI_" + instantRet + ".json"
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
   * Generate a ndwi tile of ShortConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param nirBandTile Nir band Tile
   * @param threshold
   *
   * @return a ndwi tile of ShortConstantNoDataCellType
   */
  def ndwiShortTile(greenBandTile: Tile, nirBandTile: Tile, threshold: Double): Tile = {
    //calculate ndwi tile
    val ndwiTile = Divide(
      Subtract( greenBandTile, nirBandTile ),
      Add( greenBandTile, nirBandTile) )

    ndwiTile.mapDouble(pixel=>{
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    }).convert(ShortConstantNoDataCellType) //convert ndwi tile with constant Float.NaN to Double.NaN
  }

  /**
   * Generate a ndwi tile of DoubleConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param nirBandTile Nir band Tile
   * @param threshold
   *
   * @return a ndwi tile of DoubleConstantNoDataCellType
   */
  def ndwiTile(greenBandTile: Tile, nirBandTile: Tile, threshold: Double): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleGreenBandTile = DoubleArrayTile(greenBandTile.toArrayDouble(), greenBandTile.cols, greenBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate ndwi tile
    val ndwiTile = Divide(
      Subtract( doubleGreenBandTile, doubleNirBandTile ),
      Add( doubleGreenBandTile, doubleNirBandTile) )

    ndwiTile.mapDouble(pixel=>{
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
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    //queryParams.setLevel("4000")
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndwi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("NDWI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    ndwi(sc, tileLayerArrayWithMeta, 0.01, outputDir)
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
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    val conf = new SparkConf()
      .setAppName("NDWI analysis")
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
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()

    //ndwi calculation
    val analysisBegin = System.currentTimeMillis()
    ndwi(tileLayerRddWithMeta, 0.01, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))

    /**
     * API test
     */
    /*//spark context
    val conf = new SparkConf()
      .setAppName("NDWI analysis")
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
    //queryParams.setLevel("4000")
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()
    //ndwi
    val analysisBegin = System.currentTimeMillis()
    ndwi(tileLayerRddWithMeta, 0.01, "/home/geocube/environment_test/geocube_core_jar/")
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))*/
  }

}

