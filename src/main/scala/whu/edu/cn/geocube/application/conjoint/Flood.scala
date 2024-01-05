package whu.edu.cn.geocube.application.conjoint

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.{Exact, RGB}
import geotrellis.raster.{ColorMap, Raster, Tile}
import geotrellis.spark._

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.spark._
import org.apache.spark.rdd._
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot, localHtmlRoot}

import scala.sys.process.ProcessLogger
import sys.process._
import whu.edu.cn.geocube.application.conjoint.Overlap.overlappedGeoObjects
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.io.Output._
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
import whu.edu.cn.geocube.core.cube.vector.GeoObjectRDD
import whu.edu.cn.geocube.core.vector.query.QueryVectorObjects
import whu.edu.cn.geocube.view.Info

import scala.collection.mutable.ArrayBuffer

/**
 * This class is used to calculate impacted facilities
 * in flood disaster.
 */
object Flood{
  /**
   * Calculate covered villages and impacted population in the flood disaster,
   * used in Jupyter Notebook.
   *
   * Using RasterRDD, FeatureRDD and TabularCollection as input, RasterRDD contains
   * pre- and post-disaster ndwi tiles, TabularCollection contains village population information.
   *
   * @param rasterRdd a RasterRDD of pre- and post-disaster ndwi tiles
   * @param featureRdd a FeatureRDD of facilities
   * @param tabularCollection a tabularCollection of village population
   * @param layerType
   *
   * @return info of thematic flood product
   */
  def impactedFeatures(rasterRdd: RasterRDD,
                       featureRdd: FeatureRDD,
                       tabularCollection: Array[Map[String, String]],
                       layerType: String): Array[Info]  = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)
    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)

    val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
    val uuid = UUID.randomUUID

    //save raster tiff
    val stitched: Raster[Tile] = changedRdd.stitch()
    val outputTiffPath = outputDir + uuid + "_flooded.TIF"
    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)

    //save raster clip tiff
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val extent = changedRdd.metadata.extent

    val outputClipTiffPath = outputDir + uuid + "_clip_flooded.TIF"
    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
    //val command = "gdalwarp -overwrite -wm 20 -of GTiff -te " + extent.xmin + " " + extent.ymin + " " + extent.xmax + " " + extent.ymax + " " + outputTiffPath + " " + outputClipTiffPath
    //command.!

    //save vector
    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
    val affectedFeatures = affectedFeaturesRdd.collect()
    val outputVectorPath = outputDir + uuid + "_" + layerType + "_flooded.geojson"
    saveAsGeojson(affectedFeatures, outputVectorPath)

    //tabular statistics
    var population = 0
    val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
    affectedFeatures.foreach{ features =>
      val affectedGoName = features.getAttribute("geo_name").asInstanceOf[String]
      affectedGeoNames.append(affectedGoName)
    }
    tabularCollection.foreach{ ele =>
      val geoName = ele.get("geo_name").get
      if (affectedGeoNames.contains(geoName)){
        population += ele.get("population").get.toDouble.toInt
      }

    }
    print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " " + layerType + " are impacted including: ")
    affectedGeoNames.foreach(x => print(x + " ")); println()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + population + " people are affected")

    //save raster clip tiff and vector as thematic png
    val outputThematicPngPath = outputDir + uuid + "_flooded_thematic.png"

    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Calculate covered features by flooded region,
   * used in Jupyter Notebook.
   *
   * Using RasterRDD and FeatureRDD as input, RasterRDD contains
   * pre- and post-disaster ndwi tiles.
   *
   * @param rasterRdd a RasterRDD of pre- and post-disaster ndwi tiles
   * @param featureRdd a FeatureRDD of facilities
   * @param layerType
   *
   * @return info of thematic flood product
   */
  def impactedFeatures(rasterRdd: RasterRDD,
                       featureRdd: FeatureRDD,
                       layerType: String): Array[Info]  = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //calculate flooded regions using pre- and post-disaster ndwi tiles
    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)

    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))

    //find facilities covered by flooded region
    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)

    val uuid = UUID.randomUUID

    //save raster tiff
    val stitched: Raster[Tile] = changedRdd.stitch()
    val outputTiffPath = localHtmlRoot + uuid + "_flooded.TIF"
    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)

    //save raster clip tiff
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val extent = changedRdd.metadata.extent

    val outputClipTiffPath = localHtmlRoot + uuid + "_clip_flooded.TIF"
    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)

    //save vector
    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
    val affectedFeatures = affectedFeaturesRdd.collect()
    val outputVectorPath = localHtmlRoot + uuid + "_" + layerType + "_flooded.geojson"
    saveAsGeojson(affectedFeatures, outputVectorPath)

    //save raster clip tiff and vector as thematic png
    val outputThematicPngPath = localHtmlRoot + uuid + "_flooded_thematic.png"

    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Calculate covered features by flooded region,
   * used in Jupyter Notebook.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
   * Array[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of pre- and post-disaster ndwi tiles
   * @param gridLayerGeoObjectArray an array of facilities
   * @param layerType
   *
   * @return result info of thematic flood product
   */
  def impactedFeatures(implicit sc:SparkContext,
                       tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
                       gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
                       layerType: String): Array[Info]  = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //calculate flooded regions using pre- and post-disaster ndwi tiles
    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(sc, tileLayerArrayWithMeta)

    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))

    //find facilities covered by flooded region
    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)

    val uuid = UUID.randomUUID

    //save raster tiff
    val stitched: Raster[Tile] = changedRdd.stitch()
    val outputTiffPath = localHtmlRoot + uuid + "_flooded.TIF"
    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)

    //save raster clip tiff
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val extent = changedRdd.metadata.extent
    val outputClipTiffPath = localHtmlRoot + uuid + "_clip_flooded.TIF"
    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)


    //save vector
    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
    val affectedFeatures = affectedFeaturesRdd.collect()
    val outputVectorPath = localHtmlRoot + uuid + "_flooded.geojson"
    saveAsGeojson(affectedFeatures, outputVectorPath)

    //save raster clip tiff and vector as thematic png
    val outputThematicPngPath = localHtmlRoot + uuid + "_flooded_thematic.png"

    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Calculate covered features by flooded region,
   * used in web service and web platform.
   *
   * Using RDD[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
   * RDD[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
   *
   * @param sc A SparkContext
   * @param tileLayerRddWithMeta a rdd of raster tiles
   * @param gridLayerGeoObjectArray an array of facilities
   * @param outputDir
   */
  def impactedFeaturesService1(implicit sc:SparkContext,
                               tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
                               gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
                               outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputVectorPath = executorOutputDir + "affectedObjects.geojson"
    val outputRasterPath = executorOutputDir + "flooded.png"
    val outputMetaPath = executorOutputDir + "Metadata.json"

    //calculate flooded regions using pre- and post-disaster ndwi tiles
    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(tileLayerRddWithMeta)

    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))

    //find facilities covered by flooded region
    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)

    //save vector
    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
    val affectedFeatures = affectedFeaturesRdd.collect()
    saveAsGeojson(affectedFeatures, outputVectorPath)
    //saveAsShapefile(affectedFeatures, executorOutputFile + "")

    //save raster
    val colorMap =
      ColorMap(
        Map(
          -255.0 -> 0x00000000,
          0.0 -> 0x00000000,
          255.0 -> RGB(255, 0, 0)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    val stitched = changedRdd.stitch()
    val extentRet = stitched.extent
    stitched.tile.renderPng(colorMap).write(outputRasterPath)

    //save meta
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("vectorPath", outputVectorPath.replace(localDataRoot, httpDataRoot))
    node.put("rasterPath", outputRasterPath.replace(localDataRoot, httpDataRoot))
    node.put("metaPath", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("rasterExtent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }

  /**
   * Calculate covered features by flooded region,
   * used in web service and web platform.
   *
   * Using Array[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
   * Array[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
   *
   * @param sc A SparkContext
   * @param tileLayerArrayWithMeta a rdd of raster tiles
   * @param gridLayerGeoObjectArray an array of facilities
   * @param outputDir
   */
  def impactedFeaturesService(implicit sc:SparkContext,
                              tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
                              gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
                              outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val outputVectorPath = executorOutputDir + "affectedObjects.geojson"
    val outputRasterPngPath = executorOutputDir + "flooded.png"
    val outputRasterPath = executorOutputDir + "flooded.tiff"
    val outputMetaPath = executorOutputDir + "Metadata.json"

    //calculate flooded regions using pre- and post-disaster ndwi tiles
    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(sc, tileLayerArrayWithMeta)

    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))

    //find facilities covered by flooded region
    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)

    //save vector
    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
    val affectedFeatures = affectedFeaturesRdd.collect()
    saveAsGeojson(affectedFeatures, outputVectorPath)
    //saveAsShapefile(affectedFeatures, executorOutputFile + "")

    //save raster
    val colorMap =
      ColorMap(
        Map(
          -255.0 -> 0x00000000,
          0.0 -> 0x00000000,
          255.0 -> RGB(255, 0, 0)
        ),
        ColorMap.Options(
          classBoundaryType = Exact,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = false
        )
      )
    val stitched = changedRdd.stitch()
    val extentRet = stitched.extent

    stitched.tile.renderPng().write(outputRasterPngPath)
    GeoTiff(stitched, CRS.fromName("EPSG:4326")).write(outputRasterPath)

    //save meta
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()

    node.put("vectorPath", outputVectorPath.replace(localDataRoot, httpDataRoot))
    node.put("rasterPath", outputRasterPath.replace(localDataRoot, httpDataRoot))
    node.put("metaPath", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("rasterExtent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
  }


  def main(args: Array[String]): Unit = {
    /**
     * Using raster tile array and feature array as input
     */
    //parse the web request params
    val cubeId = args(0)
    val rasterProductNames = args(1).split(",")
    val vectorProductNames = args(2).split(",")
    val extent = args(3).split(",").map(_.toDouble)
    val startTime = args(4) + " 00:00:00.000"
    val endTime = args(5) + " 00:00:00.000"
    val outputDir = args(6)
    println("rasterProductNames: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("vectorProductNames: " + vectorProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setVectorProductNames(vectorProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    var geoObjectsNum = 0
    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
    println("tiles sum: " + tileLayerArrayWithMeta._1.length)
    println("geoObjects sum: " + geoObjectsNum)

    //flood
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("Flooded region analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    impactedFeaturesService(sc, tileLayerArrayWithMeta, gridLayerGeoObjectArray, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles and " + geoObjectsNum + " vector geoObjects: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))

    /**
     * Using raster tile rdd and feature array as input
     */
    /*//parse the web request params
    val rasterProductNames = args(0).split(",")
    val vectorProductNames = args(1).split(",")
    val extent = args(2).split(",").map(_.toDouble)
    val startTime = args(3) + " 00:00:00.000"
    val endTime = args(4) + " 00:00:00.000"
    val outputDir = args(5)
    println("rasterProductNames: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("vectorProductNames: " + vectorProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("Flooded region analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setVectorProductNames(vectorProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    //queryParams.setLevel("4000")
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    var geoObjectsNum = 0
    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)

    //flood
    impactedFeaturesService1(sc, tileLayerRddWithMeta, gridLayerGeoObjectArray, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))*/

    /**
     * API test.
     */
    /*//parse the web request params
    val rasterProductNames = Array("GF_Hainan_Daguangba_NDWI_EO")
    val vectorProductNames = Array("Hainan_Daguangba_Bridge_Vector")
    val extent = Array(108.46494046724021,18.073457222586285,111.02181165740333,20.2597805438586)
    val startTime = "2016-06-01" + " 00:00:00.000"
    val endTime = "2016-09-01" + " 00:00:00.000"
    val outputDir = "/home/geocube/environment_test/geocube_core_jar/"
    println("rasterProductNames: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("vectorProductNames: " + vectorProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setVectorProductNames(vectorProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    var geoObjectsNum = 0
    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
    println("tiles sum: " + tileLayerArrayWithMeta._1.length)
    println("geoObjects sum: " + geoObjectsNum)

    //flood
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("Flooded region analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    impactedFeaturesService(sc, tileLayerArrayWithMeta, gridLayerGeoObjectArray, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles and " + geoObjectsNum + " vector geoObjects: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))*/
  }
}

