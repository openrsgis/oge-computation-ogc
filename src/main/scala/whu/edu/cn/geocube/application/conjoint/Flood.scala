//package whu.edu.cn.geocube.application.conjoint
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import geotrellis.layer._
//import geotrellis.raster.io.geotiff.GeoTiff
//import geotrellis.raster.render.{Exact, RGB}
//import geotrellis.raster.{ColorMap, Raster, Tile}
//import geotrellis.spark._
//import java.io.{File, FileOutputStream}
//import java.text.SimpleDateFormat
//import java.util.{Date, UUID}
//
//import org.apache.spark._
//import org.apache.spark.rdd._
//import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
//
//import scala.sys.process.ProcessLogger
//import sys.process._
//import whu.edu.cn.geocube.application.conjoint.Overlap.overlappedGeoObjects
//import whu.edu.cn.geocube.application.conjoint.Overlap.overlappedTabularRecords
//import whu.edu.cn.geocube.core.cube.raster.RasterRDD
//import whu.edu.cn.geocube.core.cube.tabular.{TabularCollection, TabularRDD, TabularRecord, TabularRecordRDD}
//import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
//import whu.edu.cn.geocube.core.entity.{BiQueryParams, QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
//import whu.edu.cn.geocube.core.io.Output._
//import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, QueryRasterTiles}
//import whu.edu.cn.geocube.core.cube.vector.GeoObjectRDD
//import whu.edu.cn.geocube.core.vector.query.QueryVectorObjects
//import whu.edu.cn.geocube.util.{GcConstant, PostgresqlService}
//import whu.edu.cn.geocube.view.{Info, RasterView}
//import whu.edu.cn.geocube.core.cube.CubeRDD.{getData, _}
//import whu.edu.cn.geocube.core.entity.BiDimensionKey.LocationTimeGenderKey
//import whu.edu.cn.geocube.core.tabular.query.DistributedQueryTabularRecords.getBITabulars
//
//import scala.collection.mutable.ArrayBuffer
//
///**
// * This class is used to calculate impacted facilities
// * in flood disaster.
// */
//object Flood{
//  def impactedFeaturesWithBITabular(rasterRdd: RasterRDD,
//                                    featureRdd: FeatureRDD,
//                                    tabularRdd: RDD[(LocationTimeGenderKey,Int)],
//                                    layerType: String): Array[Info] = {
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
//    val analysisBegin = System.currentTimeMillis()
//
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
//
//    val affectedGeoObjectRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd).cache()
//    val affectedGeoObjectArray: Array[GeoObject] = affectedGeoObjectRdd.collect()
//
//    val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
//    val uuid = UUID.randomUUID
//
//    //save raster tiff
//    val stitched: Raster[Tile] = changedRdd.stitch()
//    val outputTiffPath = outputDir + uuid + "_flooded.TIF"
//    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)
//
//    //save raster clip tiff
//    val stdout = new StringBuilder
//    val stderr = new StringBuilder
//    val extent = changedRdd.metadata.extent
//
//    val outputClipTiffPath = outputDir + uuid + "_clip_flooded.TIF"
//    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
//    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
//
//    //save vector
//    val affectedFeatures = affectedGeoObjectRdd.map(x => x.feature).collect()
//    val outputVectorPath = outputDir + uuid + "_" + layerType + "_flooded.geojson"
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//
//    //find location_keys by GeoObjectKeys(tile_data_id))
//    val affectedGeoObjectKeys: Array[String] = affectedGeoObjectArray.map(_.id)
//    val locationKeys: Array[Int] = new PostgresqlService().getLocationKeysByFeatureIds(affectedGeoObjectKeys)
//    val affectedTabularArray: Array[(LocationTimeGenderKey,Int)] = tabularRdd.filter(x => locationKeys.contains(x._1.locationKey)).collect()
//    val affectedPopulations = affectedTabularArray.map(_._2).sum
//
//    //print result
//    val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
//    affectedFeatures.foreach{ features =>
//      val affectedGeoName = features.getAttribute("geo_name").asInstanceOf[String]
//      affectedGeoNames.append(affectedGeoName)
//    }
//
//    print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " " + layerType + " are impacted including: ")
//    affectedGeoNames.foreach(x => print(x + " ")); println()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedPopulations + " people are affected")
//
//    //save raster clip tiff and vector as thematic png
//    val outputThematicPngPath = outputDir + uuid + "_flooded_thematic.png"
//
//    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
//    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
//    val analysisEnd = System.currentTimeMillis()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
//    ret
//  }
//  /**
//   * Calculate covered villages and impacted population in the flood disaster,
//   * used in Jupyter Notebook.
//   *
//   * Using RasterRDD, FeatureRDD and TabularCollection as input, RasterRDD contains
//   * pre- and post-disaster ndwi tiles, TabularCollection contains village population information.
//   *
//   * @param rasterRdd a RasterRDD of pre- and post-disaster ndwi tiles
//   * @param featureRdd a FeatureRDD of facilities
//   * @param tabularRdd a tabularRDD of village population
//   * @param layerType
//   * @param distance determining the buffer size for the intersection between vector and tabular
//   *
//   * @return info of thematic flood product
//   */
//  def impactedFeatures(rasterRdd: RasterRDD,
//                       featureRdd: FeatureRDD,
//                       tabularRdd: TabularRDD,
//                       layerType: String,
//                       distance: Double): Array[Info] = {
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
//    val analysisBegin = System.currentTimeMillis()
//
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
//    val gridLayerTabularRdd:RDD[(SpatialKey, Iterable[TabularRecord])] = tabularRdd.map(x=>(x._1.spatialKey, x._2))
//
//    val affectedGeoObjectRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//    val affectedTabularRecordRdd: TabularRecordRDD = overlappedTabularRecords(changedRdd, gridLayerTabularRdd)
//
//    val affectedGeoObjectArray: Array[GeoObject] = affectedGeoObjectRdd.collect()
//    val affectedTabularRecordArray: Array[TabularRecord] = affectedTabularRecordRdd.collect()
//
//    val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
//    val uuid = UUID.randomUUID
//
//    //save raster tiff
//    val stitched: Raster[Tile] = changedRdd.stitch()
//    val outputTiffPath = outputDir + uuid + "_flooded.TIF"
//    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)
//
//    //save raster clip tiff
//    val stdout = new StringBuilder
//    val stderr = new StringBuilder
//    val extent = changedRdd.metadata.extent
//
//    val outputClipTiffPath = outputDir + uuid + "_clip_flooded.TIF"
//    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
//    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
//
//    //save vector
//    val affectedFeatures = affectedGeoObjectRdd.map(x => x.feature).collect()
//    val outputVectorPath = outputDir + uuid + "_" + layerType + "_flooded.geojson"
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//
//    //tabular statistics,通过buffer intersects来匹配
//    var affectedPopulations = 0
//    affectedGeoObjectArray.foreach{geoObject =>
//      val vectorGeom = geoObject.getFeature.getDefaultGeometry.asInstanceOf[Geometry]
//      affectedTabularRecordArray.foreach{tabularRecord =>
//        val attributes = tabularRecord.getAttributes
//        val x = attributes.get("longitude").get.toDouble
//        val y = attributes.get("latitude").get.toDouble
//        val coord = new Coordinate(x, y)
//        val tabularGeom = new GeometryFactory().createPoint(coord)
//        if (tabularGeom.buffer(distance).intersects(vectorGeom)){
//          affectedPopulations += attributes.get("population").get.toDouble.toInt
//        }
//      }
//    }
//    val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
//    affectedFeatures.foreach{ features =>
//      val affectedGeoName = features.getAttribute("geo_name").asInstanceOf[String]
//      affectedGeoNames.append(affectedGeoName)
//    }
//
//    print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " " + layerType + " are impacted including: ")
//    affectedGeoNames.foreach(x => print(x + " ")); println()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedPopulations + " people are affected")
//
//    //save raster clip tiff and vector as thematic png
//    val outputThematicPngPath = outputDir + uuid + "_flooded_thematic.png"
//
//    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
//    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
//    val analysisEnd = System.currentTimeMillis()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
//    ret
//
//  }
//  /**
//   * Calculate covered villages and impacted population in the flood disaster,
//   * used in Jupyter Notebook.
//   *
//   * Using RasterRDD, FeatureRDD and TabularCollection as input, RasterRDD contains
//   * pre- and post-disaster ndwi tiles, TabularCollection contains village population information.
//   *
//   * @param rasterRdd a RasterRDD of pre- and post-disaster ndwi tiles
//   * @param featureRdd a FeatureRDD of facilities
//   * @param tabularData a tabularCollection of village population
//   * @param layerType
//   *
//   * @return info of thematic flood product
//   */
//  def impactedFeatures(rasterRdd: RasterRDD,
//                       featureRdd: FeatureRDD,
//                       tabularData: TabularCollection,
//                       layerType: String): Array[Info]  = {
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
//    val analysisBegin = System.currentTimeMillis()
//
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
//    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//    val tabularCollection = tabularData.tabularCollection
//
//    val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
//    val uuid = UUID.randomUUID
//
//    //save raster tiff
//    val stitched: Raster[Tile] = changedRdd.stitch()
//    val outputTiffPath = outputDir + uuid + "_flooded.TIF"
//    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)
//
//    //save raster clip tiff
//    val stdout = new StringBuilder
//    val stderr = new StringBuilder
//    val extent = changedRdd.metadata.extent
//
//    val outputClipTiffPath = outputDir + uuid + "_clip_flooded.TIF"
//    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
//    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
//    //val command = "gdalwarp -overwrite -wm 20 -of GTiff -te " + extent.xmin + " " + extent.ymin + " " + extent.xmax + " " + extent.ymax + " " + outputTiffPath + " " + outputClipTiffPath
//    //command.!
//
//    //save vector
//    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
//    val affectedFeatures = affectedFeaturesRdd.collect()
//    val outputVectorPath = outputDir + uuid + "_" + layerType + "_flooded.geojson"
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//
//    //tabular statistics,通过geo_name属性来匹配
//    var population = 0
//    val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
//    affectedFeatures.foreach{ features =>
//      val affectedGeoName = features.getAttribute("geo_name").asInstanceOf[String]
//      affectedGeoNames.append(affectedGeoName)
//    }
//    tabularCollection.foreach{ ele =>
//      val geoName = ele.get("geo_name").get
//      if (affectedGeoNames.contains(geoName)){
//        population += ele.get("population").get.toDouble.toInt
//      }
//
//    }
//    print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " " + layerType + " are impacted including: ")
//    affectedGeoNames.foreach(x => print(x + " ")); println()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + population + " people are affected")
//
//    //save raster clip tiff and vector as thematic png
//    val outputThematicPngPath = outputDir + uuid + "_flooded_thematic.png"
//
//    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
//    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
//    val analysisEnd = System.currentTimeMillis()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
//    ret
//  }
//
//  /**
//   * Calculate covered features by flooded region,
//   * used in Jupyter Notebook.
//   *
//   * Using RasterRDD and FeatureRDD as input, RasterRDD contains
//   * pre- and post-disaster ndwi tiles.
//   *
//   * @param rasterRdd a RasterRDD of pre- and post-disaster ndwi tiles
//   * @param featureRdd a FeatureRDD of facilities
//   * @param layerType
//   *
//   * @return info of thematic flood product
//   */
//  def impactedFeatures(rasterRdd: RasterRDD,
//                       featureRdd: FeatureRDD,
//                       layerType: String): Array[Info]  = {
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
//    val analysisBegin = System.currentTimeMillis()
//
//    //calculate flooded regions using pre- and post-disaster ndwi tiles
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(rasterRdd)
//
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
//
//    //find facilities covered by flooded region
//    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//
//    val uuid = UUID.randomUUID
//
//    //save raster tiff
//    val stitched: Raster[Tile] = changedRdd.stitch()
//    val outputTiffPath = GcConstant.localHtmlRoot + uuid + "_flooded.TIF"
//    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)
//
//    //save raster clip tiff
//    val stdout = new StringBuilder
//    val stderr = new StringBuilder
//    val extent = changedRdd.metadata.extent
//
//    val outputClipTiffPath = GcConstant.localHtmlRoot + uuid + "_clip_flooded.TIF"
//    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
//    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
//
//    //save vector
//    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
//    val affectedFeatures = affectedFeaturesRdd.collect()
//    val outputVectorPath = GcConstant.localHtmlRoot + uuid + "_" + layerType + "_flooded.geojson"
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//
//    //save raster clip tiff and vector as thematic png
//    val outputThematicPngPath = GcConstant.localHtmlRoot + uuid + "_flooded_thematic.png"
//
//    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
//    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
//    val analysisEnd = System.currentTimeMillis()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
//    ret
//  }
//
//  /**
//   * Calculate covered features by flooded region,
//   * used in Jupyter Notebook.
//   *
//   * Using Array[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
//   * Array[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
//   *
//   * @param sc a SparkContext
//   * @param tileLayerArrayWithMeta an array of pre- and post-disaster ndwi tiles
//   * @param gridLayerGeoObjectArray an array of facilities
//   * @param layerType
//   *
//   * @return result info of thematic flood product
//   */
//  def impactedFeatures(implicit sc:SparkContext,
//                       tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
//                       gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
//                       layerType: String): Array[Info]  = {
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")
//    val analysisBegin = System.currentTimeMillis()
//
//    //calculate flooded regions using pre- and post-disaster ndwi tiles
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(sc, tileLayerArrayWithMeta)
//
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))
//
//    //find facilities covered by flooded region
//    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//
//    val uuid = UUID.randomUUID
//
//    //save raster tiff
//    val stitched: Raster[Tile] = changedRdd.stitch()
//    val outputTiffPath = GcConstant.localHtmlRoot + uuid + "_flooded.TIF"
//    GeoTiff(stitched, changedRdd.metadata.crs).write(outputTiffPath)
//
//    //save raster clip tiff
//    val stdout = new StringBuilder
//    val stderr = new StringBuilder
//    val extent = changedRdd.metadata.extent
//    val outputClipTiffPath = GcConstant.localHtmlRoot + uuid + "_clip_flooded.TIF"
//    val (xmin, ymin, xmax, ymax) = (extent.xmin, extent.ymin, extent.xmax, extent.ymax)
//    Seq("gdalwarp", "-overwrite", "-wm", "20", "-of", "GTiff", "-te", s"$xmin", s"$ymin", s"$xmax", s"$ymax", s"$outputTiffPath", s"$outputClipTiffPath") ! ProcessLogger(stdout append _, stderr append _)
//
//
//    //save vector
//    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
//    val affectedFeatures = affectedFeaturesRdd.collect()
//    val outputVectorPath = GcConstant.localHtmlRoot + uuid + "_flooded.geojson"
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//
//    //save raster clip tiff and vector as thematic png
//    val outputThematicPngPath = GcConstant.localHtmlRoot + uuid + "_flooded_thematic.png"
//
//    Seq("/home/geocube/qgis/run.sh", "-r", s"$outputClipTiffPath", "-v", s"$outputVectorPath", "-o", s"$outputThematicPngPath", "-t", "Flood", "-n", s"$layerType") ! ProcessLogger(stdout append _, stderr append _)
//    val ret = Array(Info(outputThematicPngPath, 0, "Flooded Region"))
//    val analysisEnd = System.currentTimeMillis()
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
//    ret
//  }
//
//  /**
//   * Calculate covered features by flooded region,
//   * used in web service and web platform.
//   *
//   * Using RDD[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
//   * RDD[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
//   *
//   * @param sc A SparkContext
//   * @param tileLayerRddWithMeta a rdd of raster tiles
//   * @param gridLayerGeoObjectArray an array of facilities
//   * @param outputDir
//   */
//  def impactedFeaturesService1(implicit sc:SparkContext,
//                               tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
//                               gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
//                               outputDir: String): Unit = {
//    println("Task is running ...")
//    val outputDirArray = outputDir.split("/")
//    val sessionDir = new StringBuffer()
//    for(i <- 0 until outputDirArray.length - 1)
//      sessionDir.append(outputDirArray(i) + "/")
//
//    val executorSessionDir = sessionDir.toString
//    val executorSessionFile = new File(executorSessionDir)
//    if (!executorSessionFile.exists) executorSessionFile.mkdir
//    val executorOutputDir = outputDir
//    val executorOutputFile = new File(executorOutputDir)
//    if (!executorOutputFile.exists()) executorOutputFile.mkdir()
//
//    val sourceVectorPath = executorOutputDir + "sourceObjects.geojson"
//    val outputVectorPath = executorOutputDir + "affectedObjects.geojson"
//    val outputRasterPath = executorOutputDir + "flooded.png"
//    val outputMetaPath = executorOutputDir + "Metadata.json"
//
//
//    //calculate flooded regions using pre- and post-disaster ndwi tiles
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(tileLayerRddWithMeta)
//
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))
//
//    //find facilities covered by flooded region
//    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//
//    //save vector
//    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
//    val affectedFeatures = affectedFeaturesRdd.collect()
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//    //saveAsShapefile(affectedFeatures, executorOutputFile + "")
//
//    val sourceFeatures = gridLayerGeoObjectArray.flatMap(x => x._2.toArray.map(_.feature))
//    saveAsGeojson(sourceFeatures, sourceVectorPath)
//
//    //save raster
//    val colorMap =
//      ColorMap(
//        Map(
//          -255.0 -> 0x00000000,
//          0.0 -> 0x00000000,
//          255.0 -> RGB(255, 0, 0)
//        ),
//        ColorMap.Options(
//          classBoundaryType = Exact,
//          noDataColor = 0x00000000, // transparent
//          fallbackColor = 0x00000000, // transparent
//          strict = false
//        )
//      )
//    val stitched = changedRdd.stitch()
//    val extentRet = stitched.extent
//    stitched.tile.renderPng(colorMap).write(outputRasterPath)
//
//    //save meta
//    val objectMapper =new ObjectMapper()
//    val node = objectMapper.createObjectNode()
//    val localDataRoot = GcConstant.localDataRoot
//    val httpDataRoot = GcConstant.httpDataRoot
//    node.put("sourceVectorPath", sourceVectorPath.replace(localDataRoot, httpDataRoot))
//    node.put("vectorPath", outputVectorPath.replace(localDataRoot, httpDataRoot))
//    node.put("rasterPath", outputRasterPath.replace(localDataRoot, httpDataRoot))
//    node.put("metaPath", outputMetaPath.replace(localDataRoot, httpDataRoot))
//    node.put("rasterExtent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
//    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
//  }
//
//  /**
//   * Calculate covered features by flooded region,
//   * used in web service and web platform.
//   *
//   * Using Array[(SpaceTimeBandKey,Tile)] and Array[(SpaceTimeKey, Iterable[GeoObject])] as input,
//   * Array[(SpaceTimeBandKey,Tile)] contains pre- and post-disaster ndwi tiles.
//   *
//   * @param sc A SparkContext
//   * @param tileLayerArrayWithMeta a rdd of raster tiles
//   * @param gridLayerGeoObjectArray an array of facilities
//   * @param outputDir
//   */
//  def impactedFeaturesService(implicit sc:SparkContext,
//                              tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
//                              gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])],
//                              outputDir: String): Unit = {
//    println("Task is running ...")
//    val outputDirArray = outputDir.split("/")
//    val sessionDir = new StringBuffer()
//    for(i <- 0 until outputDirArray.length - 1)
//      sessionDir.append(outputDirArray(i) + "/")
//
//    val executorSessionDir = sessionDir.toString
//    val executorSessionFile = new File(executorSessionDir)
//    if (!executorSessionFile.exists) executorSessionFile.mkdir
//    val executorOutputDir = outputDir
//    val executorOutputFile = new File(executorOutputDir)
//    if (!executorOutputFile.exists()) executorOutputFile.mkdir()
//
//    val sourceVectorPath = executorOutputDir + "sourceObjects.geojson"
//    val outputVectorPath = executorOutputDir + "affectedObjects.geojson"
//    val outputRasterPath = executorOutputDir + "flooded.png"
//    val outputMetaPath = executorOutputDir + "Metadata.json"
//
//    //calculate flooded regions using pre- and post-disaster ndwi tiles
//    val changedRdd:TileLayerRDD[SpatialKey] = whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct(sc, tileLayerArrayWithMeta)
//
//    val gridLayerGeoObjectRdd:RDD[(SpatialKey, Iterable[GeoObject])] = sc.parallelize(gridLayerGeoObjectArray.map(x=>(x._1.spatialKey, x._2)))
//
//    //find facilities covered by flooded region
//    val affectedRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd)
//
//    //save vector
//    val affectedFeaturesRdd = affectedRdd.map(x => x.feature)
//    val affectedFeatures = affectedFeaturesRdd.collect()
//    saveAsGeojson(affectedFeatures, outputVectorPath)
//    //saveAsShapefile(affectedFeatures, executorOutputFile + "")
//
//    val sourceFeatures = gridLayerGeoObjectArray.flatMap(x => x._2.toArray.map(_.feature))
//    saveAsGeojson(sourceFeatures, sourceVectorPath)
//
//    //save raster
//    val colorMap =
//      ColorMap(
//        Map(
//          -255.0 -> 0x00000000,
//          0.0 -> 0x00000000,
//          255.0 -> RGB(255, 0, 0)
//        ),
//        ColorMap.Options(
//          classBoundaryType = Exact,
//          noDataColor = 0x00000000, // transparent
//          fallbackColor = 0x00000000, // transparent
//          strict = false
//        )
//      )
//    val stitched = changedRdd.stitch()
//    val extentRet = stitched.extent
//    stitched.tile.renderPng(colorMap).write(outputRasterPath)
//
//
//    //save meta
//    val objectMapper =new ObjectMapper()
//    val node = objectMapper.createObjectNode()
//    val localDataRoot = GcConstant.localDataRoot
//    val httpDataRoot = GcConstant.httpDataRoot
//    node.put("sourceVectorPath", sourceVectorPath.replace(localDataRoot, httpDataRoot))
//    node.put("vectorPath", outputVectorPath.replace(localDataRoot, httpDataRoot))
//    node.put("rasterPath", outputRasterPath.replace(localDataRoot, httpDataRoot))
//    node.put("metaPath", outputMetaPath.replace(localDataRoot, httpDataRoot))
//    node.put("rasterExtent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
//    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    /**
//     * Using raster tile array and feature array as input
//     */
//    //parse the web request params
////    val rasterProductNames = args(0).split(",")
////    val vectorProductNames = args(1).split(",")
////    val extent = args(2).split(",").map(_.toDouble)
////    val startTime = args(3) + " 00:00:00.000"
////    val endTime = args(4) + " 00:00:00.000"
////    val outputDir = args(5)
////    println("rasterProductNames: " + rasterProductNames.foreach(x=>print(x + "|")))
////    println("vectorProductNames: " + vectorProductNames.foreach(x=>print(x + "|")))
////    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
////    println("time: " + (startTime, endTime))
////
////    //query and access
////    val queryBegin = System.currentTimeMillis()
////    val queryParams = new QueryParams
////    queryParams.setRasterProductNames(rasterProductNames)
////    queryParams.setVectorProductNames(vectorProductNames)
////    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
////    queryParams.setTime(startTime, endTime)
////    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
////    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
////    val queryEnd = System.currentTimeMillis()
////    var geoObjectsNum = 0
////    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
////    println("tiles sum: " + tileLayerArrayWithMeta._1.length)
////    println("geoObjects sum: " + geoObjectsNum)
////
////    //flood
////    val analysisBegin = System.currentTimeMillis()
////    val conf = new SparkConf()
////      .setAppName("Flooded region analysis")
////      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
////
////    val sc = new SparkContext(conf)
////    impactedFeaturesService(sc, tileLayerArrayWithMeta, gridLayerGeoObjectArray, outputDir)
////    val analysisEnd = System.currentTimeMillis()
////
////    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles and " + geoObjectsNum + " vector geoObjects: " + (queryEnd - queryBegin))
////    println("Analysis time: " + (analysisEnd - analysisBegin))
//
//    /**
//     * Using raster tile rdd and feature array as input
//     */
//    /*//parse the web request params
//    val rasterProductNames = args(0).split(",")
//    val vectorProductNames = args(1).split(",")
//    val extent = args(2).split(",").map(_.toDouble)
//    val startTime = args(3) + " 00:00:00.000"
//    val endTime = args(4) + " 00:00:00.000"
//    val outputDir = args(5)
//    println("rasterProductNames: " + rasterProductNames.foreach(x=>print(x + "|")))
//    println("vectorProductNames: " + vectorProductNames.foreach(x=>print(x + "|")))
//    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
//    println("time: " + (startTime, endTime))
//
//    val analysisBegin = System.currentTimeMillis()
//    val conf = new SparkConf()
//      .setAppName("Flooded region analysis")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//
//    val sc = new SparkContext(conf)
//
//    //query and access
//    val queryBegin = System.currentTimeMillis()
//    val queryParams = new QueryParams
//    queryParams.setRasterProductNames(rasterProductNames)
//    queryParams.setVectorProductNames(vectorProductNames)
//    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
//    queryParams.setTime(startTime, endTime)
//    //queryParams.setLevel("4000")
//    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
//    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
//    val queryEnd = System.currentTimeMillis()
//    var geoObjectsNum = 0
//    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
//
//    //flood
//    impactedFeaturesService1(sc, tileLayerRddWithMeta, gridLayerGeoObjectArray, outputDir)
//    val analysisEnd = System.currentTimeMillis()
//
//    println("Query time: " + (queryEnd - queryBegin))
//    println("Analysis time: " + (analysisEnd - analysisBegin))*/
//
//    /**
//     * API test.
//     */
//    //raster vector EO, tabular BI
//   val conf = new SparkConf()
//      .setAppName("GeoCube-Dianmu Hurrican Flood Analysis")
//      .setMaster("local[8]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//      .set("spark.kryoserializer.buffer.max", "512m")
//      .set("spark.rpc.message.maxSize", "1024")
//    val sc = new SparkContext(conf)
//
//    val queryParams = new QueryParams
//    //queryParams.setCubeId("Hainan_Daguangba")
//    queryParams.setRasterProductName("GF_Hainan_Daguangba_NDWI_EO")
//    queryParams.setVectorProductName("Hainan_Daguangba_Village_Vector")
//    queryParams.setExtent(108.90494046724021,18.753457222586285,109.18763565740333,19.0497805438586)
//    queryParams.setTime("2016-06-01 00:00:00.000", "2016-09-01 00:00:00.000")
//    val rasterRdd: RasterRDD = getData(sc, queryParams)
//    val featureRdd: FeatureRDD = getData(sc, queryParams)
//    val tabularRdd: RDD[(LocationTimeGenderKey,Int)] = getBITabulars(sc, new BiQueryParams)
//
//    val results:Array[Info] = impactedFeaturesWithBITabular(rasterRdd, featureRdd, tabularRdd, "villages")
//    RasterView.display(results)
//
//    //raster, vector and tabular EO cube
//    /*val conf = new SparkConf()
//      .setAppName("GeoCube-Dianmu Hurrican Flood Analysis")
//      //.setMaster("local[8]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//      .set("spark.kryoserializer.buffer.max", "512m")
//      .set("spark.rpc.message.maxSize", "1024")
//    val sc = new SparkContext(conf)
//
//    val queryParams = new QueryParams
//    //queryParams.setCubeId("Hainan_Daguangba")
//    queryParams.setRasterProductName("GF_Hainan_Daguangba_NDWI_EO")
//    queryParams.setVectorProductName("Hainan_Daguangba_Village_Vector")
//    queryParams.setTabularProductName("Hainan_Daguangba_Village_Tabular")
//    queryParams.setExtent(108.90494046724021,18.753457222586285,109.18763565740333,19.0497805438586)
//    queryParams.setTime("2016-06-01 00:00:00.000", "2016-09-01 00:00:00.000")
//    val rasterRdd: RasterRDD = getData(sc, queryParams)
//    val featureRdd: FeatureRDD = getData(sc, queryParams)
//    val tabularRdd: TabularRDD = getData(sc, queryParams)
//
//    val results:Array[Info] = impactedFeatures(rasterRdd, featureRdd, tabularRdd, "villages", 0.0001)
//    RasterView.display(results)*/
//  }
//}
//
