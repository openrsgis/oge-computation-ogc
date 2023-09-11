package whu.edu.cn.geocube.core.raster.query

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date
import com.google.gson.{JsonObject, JsonParser}
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.Mean
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{CellType, ColorMap, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.scalap.scalasig.ScalaSigEntryParsers.index
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity
import whu.edu.cn.geocube.core.entity._
import whu.edu.cn.geocube.core.entity.GcProduct._
import whu.edu.cn.geocube.core.entity.GcMeasurement._
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomGridInfo
import whu.edu.cn.geocube.util.HbaseUtil.{getTileCell, getTileMeta}
import whu.edu.cn.geocube.util.{PostgresqlService, TileUtil}
import whu.edu.cn.geocube.util.TileSerializer.deserializeTileData
import whu.edu.cn.geocube.util.TileUtil.convertNodataValue
import whu.edu.cn.util.PostgresqlUtil


/**
 * Query raster tiles in a distributed way.
 * Return tiles with a RDD.
 *
 */
object DistributedQueryRasterTiles {
  /**
   * Query raster tiles and return a RasterRDD.
   *
   * @param sc Spark context
   * @param p  Query parameter
   * @return A RasterRDD
   */
  def getRasterTiles(implicit sc: SparkContext, p: QueryParams, olap: Boolean = false): RasterRDD = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of raster tiles is being processed...")
    val queryBegin = System.currentTimeMillis()
    val results: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, p, olap)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + results.count + " raster tiles: " + (queryEnd - queryBegin) + " ms")
    new RasterRDD(results._1, results._2)
  }

  /**
   * Query raster tiles and return (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]).
   *
   * @param sc Spark context
   * @param p  Query parameter
   * @return A RDD tile with metadata
   */
  def getRasterTileRDD(implicit sc: SparkContext, p: QueryParams, olap: Boolean = false): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    if (p.getRasterProductNames.length == 0 || p.getRasterProductNames.length == 1) { //single product
      println("Single Product")
      if (p.getRasterProductNames.length == 1) p.setRasterProductName(p.getRasterProductNames(0))
      if (!olap) getRasterTileRDDWithMeta(sc, p)
      else getRasterTileRDDWithMetaOLAP(sc, p)
    } else { //multiple product
      println("multiple Product")
      val multiProductTileLayerRdd = ArrayBuffer[RDD[(SpaceTimeBandKey, Tile)]]()
      val multiProductTileLayerMeta = ArrayBuffer[RasterTileLayerMetadata[SpaceTimeKey]]()
      for (rasterProductName <- p.getRasterProductNames) {
        p.setRasterProductName(rasterProductName)
        val results: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
          if (!olap) getRasterTileRDDWithMeta(sc, p)
          else getRasterTileRDDWithMetaOLAP(sc, p)
        if (results != null) {
          multiProductTileLayerRdd.append(results._1)
          multiProductTileLayerMeta.append(results._2)
        }
      }
      if (multiProductTileLayerMeta.length < 1) return null
      var destTileLayerMetaData = multiProductTileLayerMeta(0).getTileLayerMetadata
      val destRasterProductNames = ArrayBuffer[String]()
      for (i <- multiProductTileLayerMeta) {
        destTileLayerMetaData = destTileLayerMetaData.merge(i.getTileLayerMetadata)
        destRasterProductNames.append(i.getProductName)
      }
      val destRasterTileLayerMetaData = entity.RasterTileLayerMetadata[SpaceTimeKey](destTileLayerMetaData, _productNames = destRasterProductNames)
      (sc.union(multiProductTileLayerRdd), destRasterTileLayerMetaData)
    }
  }

  /**
   * Query raster tiles parallel (olap on the fly) and return a RDD of which.
   *
   * @param sc Spark context
   * @param p  Query parameter
   * @return A RDD of queried raster tiles.
   */
  def getRasterTileRDDWithMetaOLAP(implicit sc: SparkContext, p: QueryParams): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val queryTileIDBegin = System.currentTimeMillis()
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    val cubeId = p.getCubeId

    //month and year params
    val month = p.getMonth
    val year = p.getYear

    //city and province params
    val cityName = p.getCityName
    val provinceName = p.getProvinceName
    val envelope = new PostgresqlService().getCityGeometry(cityName, "/home/geocube/data/vector/gadm36_CHN_shp/gadm36_CHN_2.shp").getEnvelopeInternal
    val leftBottomLong = envelope.getMinX
    val LeftBottomLat = envelope.getMinY
    val rightUpperLong = envelope.getMaxX
    val rightUpperLat = envelope.getMaxY
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(leftBottomLong, LeftBottomLat),
      new Coordinate(leftBottomLong, rightUpperLat),
      new Coordinate(rightUpperLong, rightUpperLat),
      new Coordinate(rightUpperLong, LeftBottomLat),
      new Coordinate(leftBottomLong, LeftBottomLat)
    ))
    p.polygon = queryGeometry

    //high-level measurement params
    val highLevelMeasurement = p.getHighLevelMeasurement

    //CRS, tile size, resolution and level params
    val level = p.getLevel

    //Cloud params
    val cloud = p.getCloudMax
    val cloudShadow = p.getCloudShadowMax

    //Product params
    val rasterProductName = p.getRasterProductName
    val platform = p.getPlatform
    val instrument = p.getInstruments

    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent_" + cubeId + "\" where 1=1 "
        if (cityName != "") {
          extentsql ++= "AND city_name like "
          extentsql ++= "\'%"
          extentsql ++= cityName
          extentsql ++= "%\'"
        }
        if (provinceName != "") {
          extentsql ++= "AND province_name like "
          extentsql ++= "\'%"
          extentsql ++= provinceName
          extentsql ++= "%\'"
        }
        if (level != "") {
          extentsql ++= "AND level ="
          extentsql ++= "\'"
          extentsql ++= level
          extentsql ++= "\'"
        }
        println(extentsql)
        val extentResults = statement.executeQuery(extentsql.toString())
        val extentKeys = new StringBuilder;
        if (extentResults.first()) {
          extentResults.previous()
          extentKeys ++= "("
          while (extentResults.next) {
            extentKeys ++= "\'"
            extentKeys ++= extentResults.getString(1)
            extentKeys ++= "\',"
          }
          extentKeys.deleteCharAt(extentKeys.length - 1)
          extentKeys ++= ")"
        } else {
          return null
        }
        println("extent key: " + extentKeys)

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct_" + cubeId + "\" where 1=1 "
        if (instrument.length != 0) {
          productsql ++= "AND sensor_name IN ("
          for (ins <- instrument) {
            productsql ++= "\'"
            productsql ++= ins
            productsql ++= "\',"
          }
          productsql.deleteCharAt(productsql.length - 1)
          productsql ++= ")"
        }
        if (rasterProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= rasterProductName
          productsql ++= "\'"
        }
        if (platform != "") {
          productsql ++= "AND platform_name ="
          productsql ++= "\'"
          productsql ++= platform
          productsql ++= "\'"
        }
        if (level != "") {
          productsql ++= "AND level ="
          productsql ++= "\'"
          productsql ++= level
          productsql ++= "\'"
        }
        if (month != -1) {
          productsql ++= "AND phenomenon_time_month = "
          productsql ++= month.toString
        }
        if (year != -1) {
          productsql ++= "AND phenomenon_time_year = "
          productsql ++= year.toString
        }
        println(productsql)
        val productResults = statement.executeQuery(productsql.toString());
        val productKeys = new StringBuilder;
        if (productResults.first()) {
          productResults.previous()
          productKeys ++= "("
          while (productResults.next) {
            productKeys ++= "\'"
            productKeys ++= productResults.getString(1)
            productKeys ++= "\',"
          }
          productKeys.deleteCharAt(productKeys.length - 1)
          productKeys ++= ")"
        } else {
          return null
        }
        println("product key:" + productKeys)

        //Quality dimension
        val qualitysql = new StringBuilder
        qualitysql ++= "Select tile_quality_key from gc_tile_quality where 1=1 "
        if (cloud != "") {
          qualitysql ++= "AND cloud<"
          qualitysql ++= cloud
        }
        if (cloudShadow != "") {
          qualitysql ++= "AND cloudShadow<"
          qualitysql ++= cloudShadow
        }
        println(qualitysql)
        val qualityResults = statement.executeQuery(qualitysql.toString());
        val qualityKeys = new StringBuilder;
        if (qualityResults.first()) {
          qualityResults.previous()
          qualityKeys ++= "("
          while (qualityResults.next) {
            qualityKeys ++= "\'"
            qualityKeys ++= qualityResults.getString(1)
            qualityKeys ++= "\',"
          }
          qualityKeys.deleteCharAt(qualityKeys.length - 1)
          qualityKeys ++= ")"
        } else {
          return null
        }
        println("quality key: " + qualityKeys)

        val measurementsql = new StringBuilder
        measurementsql ++= "Select measurement_key from gc_measurement where 1=1 "
        if (highLevelMeasurement != "") {
          measurementsql ++= "AND product_level like "
          measurementsql ++= "\'%"
          measurementsql ++= highLevelMeasurement
          measurementsql ++= "%\'"
        }
        println(measurementsql)
        val measurementResults = statement.executeQuery(measurementsql.toString());
        val measurementKeys = new StringBuilder;
        if (measurementResults.first()) {
          measurementResults.previous()
          measurementKeys ++= "("
          while (measurementResults.next) {
            measurementKeys ++= "\'"
            measurementKeys ++= measurementResults.getString(1)
            measurementKeys ++= "\',"
          }
          measurementKeys.deleteCharAt(measurementKeys.length - 1)
          measurementKeys ++= ")"
        } else {
          return null
        }
        println("measurement key: " + measurementKeys)

        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact_" + cubeId + " where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"
        println("Raster Tile Fact Query SQL:" + command)

        val tileIDResults = statement.executeQuery(command)
        val tileAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (tileIDResults.first()) {
          tileIDResults.previous()
          while (tileIDResults.next()) {
            val keyArray = new Array[String](5)
            keyArray(0) = tileIDResults.getString(1)
            keyArray(1) = tileIDResults.getString(2)
            keyArray(2) = tileIDResults.getString(3)
            keyArray(3) = tileIDResults.getString(4)
            keyArray(4) = tileIDResults.getString(5)
            tileAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No tiles of " + rasterProductName + " acquired!")
          return null //for batch execution
        }

        val queryTileIDEnd = System.currentTimeMillis()
        var partitions = tileAndDimensionKeys.length
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        if (partitions > 96) partitions = 96
        val postgresqlService = new PostgresqlService()
        val hbaseTableName = postgresqlService.getHbaseTableName(cubeId)
        val queriedRasterTilesRdd: RDD[RasterTile] = sc.parallelize(tileAndDimensionKeys, partitions)
          .map(keys => initRasterTile(cubeId, keys(0), keys(1), keys(2), keys(3), keys(4), hbaseTableName))
          .filter(_ != null) //过滤掉库内为null的瓦片
          .cache()
        val tileCount = queriedRasterTilesRdd.count()
        println("###### Returned " + tileCount + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms ######")
        println("###### Returned " + tileCount + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms ######")
        println("###### Returned " + tileCount + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms ######")
        println("###### Returned " + tileCount + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms ######")
        initLayerMeta(queriedRasterTilesRdd, p)
        //        val layer = initLayerMeta(queriedRasterTilesRdd, p)
        //        (layer._1.map(value => (value._1._1, value._2)), layer._2)
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
  }

  /**
   * Query raster tiles and return (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]).
   *
   * @param sc Spark context
   * @param p  Query parameter
   * @return A RDD with metadata
   */
  def getRasterTileRDDWithMeta(implicit sc: SparkContext, p: QueryParams): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val queryTileIDBegin = System.currentTimeMillis()
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection

    //Cube ID
    val cubeId = p.getCubeId

    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime
    val nextStartTime = p.getNextStartTime
    val nextEndTime = p.getNextEndTime

    //Spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS, tile size, resolution and level params
    val crs = p.getCRS
    val tileSize = p.getTileSize
    val cellRes = p.getCellRes
    val level = p.getLevel

    //Cloud params
    val cloud = p.getCloudMax
    val cloudShadow = p.getCloudShadowMax

    //Product params
    val rasterProductName = p.getRasterProductName
    val platform = p.getPlatform
    val instrument = p.getInstruments

    //Measurement params
    val measurements = p.getMeasurements

    val message = new StringBuilder
    if (conn != null) {
      try {
        // Configure to be read only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent_" + cubeId + "\" where 1=1 "

        if (gridCodes.length != 0) {
          extentsql ++= "AND grid_code IN ("
          for (grid <- gridCodes) {
            extentsql ++= "\'"
            extentsql ++= grid
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (cityCodes.length != 0) {
          extentsql ++= "AND city_code IN ("
          for (city <- cityCodes) {
            extentsql ++= "\'"
            extentsql ++= city
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (cityNames.length != 0) {
          extentsql ++= "AND city_name IN ("
          for (cityname <- cityNames) {
            extentsql ++= "\'"
            extentsql ++= cityname
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (provinceName != "") {
          extentsql ++= "AND province_name ="
          extentsql ++= "\'"
          extentsql ++= provinceName
          extentsql ++= "\'"
        }
        if (districtName != "") {
          extentsql ++= "AND district_name ="
          extentsql ++= "\'"
          extentsql ++= districtName
          extentsql ++= "\'"
        }
        if (tileSize != "") {
          extentsql ++= "AND tile_size ="
          extentsql ++= "\'"
          extentsql ++= tileSize
          extentsql ++= "\'"
        }
        if (cellRes != "") {
          extentsql ++= "AND cell_res ="
          extentsql ++= "\'"
          extentsql ++= cellRes
          extentsql ++= "\'"
        }
        if (level != "") {
          extentsql ++= "AND level ="
          extentsql ++= "\'"
          extentsql ++= level
          extentsql ++= "\'"
        }
        println(extentsql)
        val extentResults = statement.executeQuery(extentsql.toString());
        val extentKeys = new StringBuilder;
        if (extentResults.first()) {
          extentResults.previous()
          extentKeys ++= "("
          while (extentResults.next) {
            extentKeys ++= "\'"
            extentKeys ++= extentResults.getString(1)
            extentKeys ++= "\',"
          }
          extentKeys.deleteCharAt(extentKeys.length - 1)
          extentKeys ++= ")"
        } else {
          return null
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        //Product dimension query
        var productsql = new StringBuilder;
        //        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct_" + cubeId + "\" where 1=1 AND preasure = 1000 "
        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct_" + cubeId + "\" where 1=1 "
        productsql = addAdditionalSubset2ProductSQL(productsql, p.getSubsetQuery)
        if (instrument.length != 0) {
          productsql ++= "AND sensor_name IN ("
          for (ins <- instrument) {
            productsql ++= "\'"
            productsql ++= ins
            productsql ++= "\',"
          }
          productsql.deleteCharAt(productsql.length - 1)
          productsql ++= ")"
        }
        if (rasterProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= rasterProductName
          productsql ++= "\'"
        }
        if (platform != "") {
          productsql ++= "AND platform_name ="
          productsql ++= "\'"
          productsql ++= platform
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if (tileSize != "") {
          productsql ++= "AND tile_size ="
          productsql ++= "\'"
          productsql ++= tileSize
          productsql ++= "\'"
        }
        if (cellRes != "") {
          productsql ++= "AND cell_res ="
          productsql ++= "\'"
          productsql ++= cellRes
          productsql ++= "\'"
        }
        if (level != "") {
          productsql ++= "AND level ="
          productsql ++= "\'"
          productsql ++= level
          productsql ++= "\'"
        }
        if (startTime != "" && endTime != "") {
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        if (nextStartTime != "" && nextEndTime != "") {
          productsql ++= " Or phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= nextStartTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= nextEndTime
          productsql ++= "\')"
        } else {
          productsql ++= ")"
        }

        println(productsql.toString())
        val productResults = statement.executeQuery(productsql.toString());
        val productKeys = new StringBuilder;
        if (productResults.first()) {
          productResults.previous()
          productKeys ++= "("
          while (productResults.next) {
            productKeys ++= "\'"
            productKeys ++= productResults.getString(1)
            productKeys ++= "\',"
          }
          productKeys.deleteCharAt(productKeys.length - 1)
          productKeys ++= ")"
        } else {
          return null
        }
        println("Product Query SQL: " + productsql)
        println("Product Keys :" + productKeys)

        //Quality dimension query
        val qualitysql = new StringBuilder
        qualitysql ++= "Select tile_quality_key from gc_tile_quality where 1=1 "
        if (cloud != "") {
          qualitysql ++= "AND cloud<"
          qualitysql ++= cloud
        }
        if (cloudShadow != "") {
          qualitysql ++= "AND cloudShadow<"
          qualitysql ++= cloudShadow
        }

        val qualityResults = statement.executeQuery(qualitysql.toString());
        val qualityKeys = new StringBuilder;
        if (qualityResults.first()) {
          qualityResults.previous()
          qualityKeys ++= "("
          while (qualityResults.next) {
            qualityKeys ++= "\'"
            qualityKeys ++= qualityResults.getString(1)
            qualityKeys ++= "\',"
          }
          qualityKeys.deleteCharAt(qualityKeys.length - 1)
          qualityKeys ++= ")"
        } else {
          return null
        }
        println("Quality Query SQL: " + qualitysql)
        println("Quality Keys :" + qualityKeys)

        //Measurement dimension query
        val measurementsql = new StringBuilder
        measurementsql ++= "Select measurement_key from gc_measurement where 1=1 "
        if (measurements.length != 0) {
          measurementsql ++= "AND measurement_name IN ("
          for (measure <- measurements) {
            measurementsql ++= "\'"
            measurementsql ++= measure
            measurementsql ++= "\',"
          }
          measurementsql.deleteCharAt(measurementsql.length - 1)
          measurementsql ++= ")"
        }

        val measurementResults = statement.executeQuery(measurementsql.toString());
        val measurementKeys = new StringBuilder;
        if (measurementResults.first()) {
          measurementResults.previous()
          measurementKeys ++= "("
          while (measurementResults.next) {
            measurementKeys ++= "\'"
            measurementKeys ++= measurementResults.getString(1)
            measurementKeys ++= "\',"
          }
          measurementKeys.deleteCharAt(measurementKeys.length - 1)
          measurementKeys ++= ")"
        } else {
          return null
        }
        println("Measurement Query SQL: " + measurementsql)
        println("Measurement Keys :" + measurementKeys)

        //Tile_ID query
        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact_" + cubeId + " where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"
        println("Raster Tile Fact Query SQL:" + command)

        val tileIDResults = statement.executeQuery(command)
        val tileAndDimensionKeys = new ArrayBuffer[Array[String]]()
        val productKeyArray = new ArrayBuffer[String]()
        if (tileIDResults.first()) {
          tileIDResults.previous()
          while (tileIDResults.next()) {
            val keyArray = new Array[String](5)
            keyArray(0) = tileIDResults.getString(1)
            keyArray(1) = tileIDResults.getString(2)
            keyArray(2) = tileIDResults.getString(3)
            keyArray(3) = tileIDResults.getString(4)
            keyArray(4) = tileIDResults.getString(5)
            tileAndDimensionKeys.append(keyArray)
            productKeyArray.append(keyArray(1))
          }
        } else {
          println("No tiles of " + rasterProductName + " acquired!")
          return null //for batch execution
        }

        val queryTileIDEnd = System.currentTimeMillis()
        println("-------- Returned " + tileAndDimensionKeys.length + " raster tiles, time cost is " + (queryTileIDEnd - queryTileIDBegin) + "ms --------")
        //        val gcDimensionArray = getOtherDimensions(cubeId, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password, productKeys.toString())
        val postgresqlService = new PostgresqlService()
        val gcDimensionArray = postgresqlService.getOtherDimensions(cubeId, productKeys.toString())
        val hbaseTableName = postgresqlService.getHbaseTableName(cubeId)
        val minMaxTime: (String, String) = postgresqlService.getMaxMinTime(cubeId, productKeyArray)
        //Distributed access tile data and return a RDD[RasterTile]
        var partitions = tileAndDimensionKeys.length
        val firstRasterTile: RasterTile = initRasterTile(cubeId, tileAndDimensionKeys(0)(0), tileAndDimensionKeys(0)(1),
          tileAndDimensionKeys(0)(2), tileAndDimensionKeys(0)(3), tileAndDimensionKeys(0)(4), hbaseTableName, gcDimensionArray)
        if (partitions > 96) partitions = 96
        var queriedRasterTilesRdd: RDD[RasterTile] = sc.parallelize(tileAndDimensionKeys, partitions)
          .map(
            keys => initRasterTile(cubeId, keys(0), keys(1), keys(2), keys(3), keys(4), hbaseTableName, gcDimensionArray)
          )
          .filter(_ != null) //filter null tile
          .cache()
        initLayerMeta(queriedRasterTilesRdd, p, firstRasterTile, minMaxTime)
        //        val tileRDD:(RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = initLayerMeta(queriedRasterTilesRdd, p)
        //        queriedRasterTilesRdd.unpersist()
        //        tileRDD
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * add the additional subset into the product sql
   *
   * @param initSQL          the input sql
   * @param subsetQueryArray the subset query array
   * @return the modified sql
   */
  def addAdditionalSubset2ProductSQL(initSQL: StringBuilder, subsetQueryArray: ArrayBuffer[SubsetQuery]): StringBuilder = {
    for (subsetQuery <- subsetQueryArray) {
      val axisName = subsetQuery.axisName.get
      // if interval
      if (subsetQuery.getInterval.isDefined && subsetQuery.getInterval.get) {
        val lowPoint = subsetQuery.getLowPoint
        val highPoint = subsetQuery.getHighPoint
        if (lowPoint.isDefined && subsetQuery.getIsNumber.isDefined && subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " > " + lowPoint.get + " ")
        } else if (lowPoint.isDefined && subsetQuery.getIsNumber.isDefined && !subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " > '" + lowPoint.get + "' ")
        }
        if (highPoint.isDefined && subsetQuery.getIsNumber.isDefined && subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " < " + highPoint.get + " ")
        } else if (highPoint.isDefined && subsetQuery.getIsNumber.isDefined && !subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " < '" + highPoint.get + "' ")
        }
      }
      // if not interval
      else if (subsetQuery.getIsNumber.isDefined && !subsetQuery.getInterval.get) {
        val point = subsetQuery.getPoint
        if (point.isDefined && subsetQuery.getIsNumber.isDefined && subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " = " + point.get + " ")
        } else if (point.isDefined && subsetQuery.getIsNumber.isDefined && !subsetQuery.getIsNumber.get) {
          initSQL ++= (" AND " + axisName + " = '" + point.get + "' ")
        }
      }
    }
    initSQL
  }

  /**
   * Initiate a RasterTile object using dimension keys.
   *
   * @param id Tile data id
   * @param productKey
   * @param measurementKey
   * @param extentKey
   * @param qualityKey
   * @return A RasterTile object
   */
  def initRasterTile(cubeId: String, id: String, productKey: String, measurementKey: String, extentKey: String, qualityKey: String, hbaseTableName: String, gcDimensionArray: ArrayBuffer[GcDimension] = null): RasterTile = {
    val rasterTile = RasterTile(id)

    //Access product and measurement dimensional info in postgresql
    val productMeta = getProductByKey(cubeId, productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password, gcDimensionArray)
    val measurementMeta = getMeasurementByMeasureAndProdKey(cubeId, measurementKey, productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)

    rasterTile.setProductMeta(productMeta)
    rasterTile.setMeasurementMeta(measurementMeta)

    //Access tile data and meta in HBase
    /*val tileMeta = getTileMeta("hbase_raster_regions", id, "rasterData", "metaData")
    val tileBytes = getTileCell("hbase_raster_regions", id, "rasterData", "tile")*/
    /*val tileMeta = getTileMeta("hbase_raster", id, "rasterData", "metaData")
    val tileBytes = getTileCell("hbase_raster", id, "rasterData", "tile")*/
    val tileMeta = getTileMeta(hbaseTableName, id, "rasterData", "metaData")
    val tileBytes = getTileCell(hbaseTableName, id, "rasterData", "tile")
    //    val tileMeta =
    //      if (PostgresqlUtil.url.contains("whugeocube"))
    //        getTileMeta("hbase_raster", id, "rasterData", "metaData")
    //      else if (PostgresqlUtil.url.contains("ypgeocube"))
    //        getTileMeta("hbase_raster_fivehundred", id, "rasterData", "metaData")
    //      else if (PostgresqlUtil.url.contains("multigeocube"))
    //        getTileMeta("hbase_raster_regions_" + cubeId, id, "rasterData", "metaData")
    //      else
    //        getTileMeta("hbase_raster_regions", id, "rasterData", "metaData")
    //
    //    val tileBytes =
    //      if (PostgresqlUtil.url.contains("whugeocube"))
    //        getTileCell("hbase_raster", id, "rasterData", "tile")
    //      else if (PostgresqlUtil.url.contains("ypgeocube"))
    //        getTileCell("hbase_raster_fivehundred", id, "rasterData", "tile")
    //      else if (PostgresqlUtil.url.contains("multigeocube"))
    //        getTileCell("hbase_raster_regions_" + cubeId, id, "rasterData", "tile")
    //      else
    //        getTileCell("hbase_raster_regions", id, "rasterData", "tile")

    if (tileMeta == null || tileBytes == null) return null //filter null tile

    val json = new JsonParser()
    val obj = json.parse(tileMeta).asInstanceOf[JsonObject]
    //Add hbase attributes to a RasterTile object
    rasterTile.setCRS(obj.get("cRS").toString)
    rasterTile.setColNum(obj.get("column").toString)
    rasterTile.setRowNum(obj.get("row").toString)
    val trueDataWKT = obj.get("trueDataWKT").toString.replace("\"", "")
    val reader = new WKTReader
    val polygon = reader.read(trueDataWKT)
    val envelope = polygon.getEnvelopeInternal
    rasterTile.setLeftBottomLong(envelope.getMinX.toString)
    rasterTile.setLeftBottomLat(envelope.getMinY.toString)
    rasterTile.setRightUpperLong(envelope.getMaxX.toString)
    rasterTile.setRightUpperLat(envelope.getMaxY.toString)
    //    val dType = obj.get("cellType").toString.replace("\"", "")
    val postgresqlService = new PostgresqlService
    val dType = postgresqlService.getCellType(cubeId, productKey)
    //Transform tile data to a Tile Object
    var tileData: Tile = deserializeTileData(rasterTile.getProductMeta.getPlatform, tileBytes, rasterTile.getProductMeta.tileSize.toInt, dType)
    rasterTile.setData(tileData)
    rasterTile
  }

  /**
   * Extract metadata of RDD[RasterTile] and transform to a TileLayerRDD[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   *
   * @param rasterTilesRdd
   * @return a TileLayerRDD[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   */
  def initLayerMeta(rasterTilesRdd: RDD[RasterTile], p: QueryParams, firstTile: RasterTile, minMaxTime: (String, String)):
  (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    //Translate RDD[RasterTile] to a TileLayerRDD[(SpaceTimeBandKey, Tile)]
    var layerRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTilesRdd.map { tile =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val phenomenonTime = sdf.parse(tile.getProductMeta.getPhenomenonTime).getTime
      val measurement = tile.getMeasurementMeta.getMeasurementName
      val colNum = Integer.parseInt(tile.getColNum)
      val rowNum = Integer.parseInt(tile.getRowNum)
      var Tile = tile.getData
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum, rowNum, phenomenonTime), measurement, tile.getProductMeta.otherDimensions.toArray)
      Tile = convertNodataValue(Tile)
      (k, Tile)
    }.cache()
    layerRdd = layerRdd.groupBy(_._1).mapValues(values => Mean(values.map(_._2)))
    rasterTilesRdd.unpersist()
    // The extent info of the cube
    val SRE = p.getSizeResAndExtentByCubeId(p.getCubeId, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password).toList.head

    //Extract metadata of rasterTilesRdd
    val productName: String = firstTile.getProductMeta.productName
    // The extent of the whole cube
    val extent = geotrellis.vector.Extent(SRE._2._1, SRE._2._2, SRE._2._3, SRE._2._4)
    val tileSize: Int = firstTile.getProductMeta.tileSize.toInt
    // The layout of the cube
    val tl = TileLayout(((SRE._2._3 - SRE._2._1) / SRE._1._1).toInt, ((SRE._2._4 - SRE._2._2) / SRE._1._1).toInt, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)

    //maybe unaccurate actual extent calculated by the query polygon, but faster, get the col raw and long lat of the grids
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(p.getPolygon, ld.layoutCols, ld.layoutRows, ld.extent, colRow, longLati, SRE._1._1)
    val minCol = colRow(0);
    val minRow = colRow(1);
    val maxCol = colRow(2);
    val maxRow = colRow(3)
    val minLong = longLati(0);
    val minLat = longLati(1);
    val maxLong = longLati(2);
    val maxLat = longLati(3)
    /*val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(p.getStartTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(p.getEndTime).getTime*/
    //    val minInstant =
    //      if (p.getStartTime != "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getStartTime).getTime
    //      else if (p.getYear != -1 && p.getMonth == -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-01-01 00:00:000").getTime
    //      else if (p.getYear != -1 && p.getMonth != -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-" + p.getMonth.toString + "-01 00:00:000").getTime
    //      else throw new RuntimeException("Error date")
    //    val maxInstant =
    //      if (p.getEndTime != "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getEndTime).getTime
    //      else if (p.getYear != -1 && p.getMonth == -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-12-31 00:00:000").getTime
    //      else if (p.getYear != -1 && p.getMonth != -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-" + p.getMonth.toString + "-29 00:00:000").getTime
    //      else throw new RuntimeException("Error date")
    // if product doesn't have the time, we will use "1978-01-01 00:00:00" which is equal to long 252432000000
    val minInstant =
      if (minMaxTime._1 == "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1978-01-01 00:00:00").getTime
      else new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(minMaxTime._1).getTime
    val maxInstant =
      if (minMaxTime._2 == "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1978-01-01 00:00:00").getTime
      else new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(minMaxTime._2).getTime

    val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
    val actualExtent = Extent(minLong, minLat, maxLong, maxLat)
    //    val dtype = firstRasterTile.getMeasurementMeta.getMeasurementDType
    val dtype = firstTile.getMeasurementMeta.getMeasurementDType
    val celltype = CellType.fromName(dtype)
    //    val crsStr = firstRasterTile.getCRS.toString.replace("\"", "")
    val crsStr = firstTile.getCRS.toString.replace("\"", "")
    val crs: CRS =
      if (crsStr == "WGS84") CRS.fromEpsgCode(4326)
      else throw new RuntimeException("Not support " + crsStr)

    (layerRdd, RasterTileLayerMetadata(TileLayerMetadata(celltype, ld, actualExtent, crs, bounds), productName))
  }

  /**
   * Extract metadata of RDD[RasterTile] and transform to a TileLayerRDD[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   *
   * @param rasterTilesRdd
   * @return a TileLayerRDD[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   */
  def initLayerMeta(rasterTilesRdd: RDD[RasterTile], p: QueryParams): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val firstRasterTile = rasterTilesRdd.take(1)(0)

    //Translate RDD[RasterTile] to a TileLayerRDD[(SpaceTimeBandKey, Tile)]
    var layerRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTilesRdd.map { tile =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val phenomenonTime = sdf.parse(tile.getProductMeta.getPhenomenonTime).getTime
      val measurement = tile.getMeasurementMeta.getMeasurementName
      val colNum = Integer.parseInt(tile.getColNum)
      val rowNum = Integer.parseInt(tile.getRowNum)
      var Tile = tile.getData
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum, rowNum, phenomenonTime), measurement, tile.getProductMeta.otherDimensions.toArray)
      Tile = convertNodataValue(Tile)
      (k, Tile)
    }.cache()

    //    val a = layerRdd.count()
    layerRdd = layerRdd.groupBy(_._1).mapValues(values => Mean(values.map(_._2)))
    rasterTilesRdd.unpersist()
    // The extent info of the cube
    val SRE = p.getSizeResAndExtentByCubeId(p.getCubeId, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password).toList.head

    //Extract metadata of rasterTilesRdd
    val productName: String = firstRasterTile.getProductMeta.productName

    // The extent of the whole cube
    val extent = geotrellis.vector.Extent(SRE._2._1, SRE._2._2, SRE._2._3, SRE._2._4)
    val tileSize: Int = firstRasterTile.getProductMeta.tileSize.toInt
    // The layout of the cube
    val tl = TileLayout(((SRE._2._3 - SRE._2._1) / SRE._1._1).toInt, ((SRE._2._4 - SRE._2._2) / SRE._1._1).toInt, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)

    //maybe unaccurate actual extent calculated by the query polygon, but faster, get the col raw and long lat of the grids
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(p.getPolygon, ld.layoutCols, ld.layoutRows, ld.extent, colRow, longLati, SRE._1._1)
    val minCol = colRow(0);
    val minRow = colRow(1);
    val maxCol = colRow(2);
    val maxRow = colRow(3)
    val minLong = longLati(0);
    val minLat = longLati(1);
    val maxLong = longLati(2);
    val maxLat = longLati(3)
    /*val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(p.getStartTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(p.getEndTime).getTime*/
    val minInstant =
      if (p.getStartTime != "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getStartTime).getTime
      else if (p.getYear != -1 && p.getMonth == -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-01-01 00:00:000").getTime
      else if (p.getYear != -1 && p.getMonth != -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-" + p.getMonth.toString + "-01 00:00:000").getTime
      else throw new RuntimeException("Error date")
    val maxInstant =
      if (p.getEndTime != "") new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getEndTime).getTime
      else if (p.getYear != -1 && p.getMonth == -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-12-31 00:00:000").getTime
      else if (p.getYear != -1 && p.getMonth != -1) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getYear.toString + "-" + p.getMonth.toString + "-29 00:00:000").getTime
      else throw new RuntimeException("Error date")

    //accurate actual extent calculation, but may costs too much time
    /*//layerRdd.count()
    //rasterTilesRdd.unpersist()
    val extentArray = rasterTilesRdd
      .map(tile => Array(tile.getLeftBottomLong, tile.getLeftBottomLat, tile.getRightUpperLong, tile.getRightUpperLat))
      .collect()
    rasterTilesRdd.unpersist()
    val minLongT = extentArray.map(x => x(0)).min.toDouble
    val minLatT = extentArray.map(x => x(1)).min.toDouble
    val maxLongT = extentArray.map(x => x(2)).max.toDouble
    val maxLatT = extentArray.map(x => x(3)).max.toDouble*/

    val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
    val actualExtent = Extent(minLong, minLat, maxLong, maxLat)
    val dtype = firstRasterTile.getMeasurementMeta.getMeasurementDType
    val celltype = CellType.fromName(dtype)
    val crsStr = firstRasterTile.getCRS.toString.replace("\"", "")
    val crs: CRS =
      if (crsStr == "WGS84") CRS.fromEpsgCode(4326)
      else throw new RuntimeException("Not support " + crsStr)

    (layerRdd, RasterTileLayerMetadata(TileLayerMetadata(celltype, ld, actualExtent, crs, bounds), productName))
  }

  /**
   * Get coverage with png format.
   * Used by OGC API - Coverages.
   *
   * @param p      query parameters
   * @param bbox   bounding box represented by a string
   * @param subset bounding box represented by an array
   * @return
   */
  def getCoveragePng(p: QueryParams, bbox: String, subset: Array[String]): Array[Byte] = {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    val rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, p)
    val rasterTileRdd = rasterTileLayerRdd.map(x => (x._1.spaceTimeKey.spatialKey, x._2))
    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)

    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(rasterTileRdd, spatialMetadata)
    val stitched: Raster[Tile] = tileLayerRdd.stitch()
    val srcExtent = stitched.extent

    if (bbox != null) {
      val coordinates = bbox.split(",")
      val requestedExtent = new Extent(coordinates(0).toDouble, coordinates(1).toDouble, coordinates(2).toDouble, coordinates(3).toDouble)
      if (requestedExtent.intersects(srcExtent)) {
        val intersection = requestedExtent.intersection(srcExtent).get
        val colMin = (intersection.xmin - srcExtent.xmin) / stitched.cellSize.width
        val colMax = (intersection.xmax - srcExtent.xmin) / stitched.cellSize.width
        val rowMin = (intersection.ymin - srcExtent.ymin) / stitched.cellSize.height
        val rowMax = (intersection.ymax - srcExtent.ymin) / stitched.cellSize.height
        val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
          .stops(100)
          .setAlphaGradient(0xFF, 0xAA)
        val png = stitched.tile.crop(colMin.toInt, rowMin.toInt, colMax.toInt, rowMax.toInt).renderPng(colorRamp) //Array[Byte]
        return png
      } else
        return null
    }

    if (subset != null) {
      if (subset.length != 2) throw new RuntimeException("Wrong subset paramters")
      val prefix = subset(0).substring(0, subset(0).indexOf("("))
      val coordinates: Array[String] = prefix match {
        case "Lat" => {
          val yCoords = subset(0).substring(subset(0).indexOf("(") + 1, subset(0).indexOf(")")).split(",")
          val xCoords = subset(1).substring(subset(1).indexOf("(") + 1, subset(1).indexOf(")")).split(",")
          Array(xCoords(0), yCoords(0), xCoords(1), yCoords(1))
        }
        case "Long" => {
          val xCoords = subset(0).substring(subset(0).indexOf("(") + 1, subset(0).indexOf(")")).split(",")
          val yCoords = subset(1).substring(subset(1).indexOf("(") + 1, subset(1).indexOf(")")).split(",")
          Array(xCoords(0), yCoords(0), xCoords(1), yCoords(1))
        }
      }

      val requestedExtent = new Extent(coordinates(0).toDouble, coordinates(1).toDouble, coordinates(2).toDouble, coordinates(3).toDouble)

      if (requestedExtent.intersects(srcExtent)) {
        val intersection = requestedExtent.intersection(srcExtent).get
        println(intersection.toString())
        val colMin = (intersection.xmin - srcExtent.xmin) / stitched.cellSize.width
        val colMax = (intersection.xmax - srcExtent.xmin) / stitched.cellSize.width
        val rowMin = (intersection.ymin - srcExtent.ymin) / stitched.cellSize.height
        val rowMax = (intersection.ymax - srcExtent.ymin) / stitched.cellSize.height
        println(stitched.cellSize.toString)
        println(colMin, colMax, rowMin, rowMax)
        val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
          .stops(100)
          .setAlphaGradient(0xFF, 0xAA)
        val png = stitched.tile.crop(colMin.toInt, rowMin.toInt, colMax.toInt, rowMax.toInt).renderPng(colorRamp) //Array[Byte]
        return png
      } else
        return null
    }

    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val png = stitched.tile.renderPng(colorRamp) //Array[Byte]
    png
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
//    val a = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1978-01-01 00:00:00").getTime
    val timebegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    val queryParams = new QueryParams
    queryParams.setCubeId("10")
    queryParams.setRasterProductNames(Array("NRCAN_DEM_ARD"))
    queryParams.setExtent(-77.01, 81.11, -77.99, 81.91)
    queryParams.setTime("1978-01-01 00:00:00.000", "1978-01-02 00:00:00.000")
    //queryParams.setMeasurements(Array("DEM"))
    val rasterRdd: RasterRDD = getRasterTiles(sc, queryParams)

    val timeend = System.currentTimeMillis()
    println("query time cost" + (timeend - timebegin))

    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    rasterRdd.collect().foreach { x =>
      val path = "/home/geocube/environment_test/geocube_core_jar/" + x._1.spaceTimeKey.col + "_" + x._1.spaceTimeKey.row + "_dem.png"
      x._2.renderPng(colorRamp).write(path)
    }

    /*val resultsRdd = rasterRdd.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
    val spatialKeyBounds = rasterRdd.rasterTileLayerMetadata.tileLayerMetadata.bounds.get.toSpatial
    val spatialMetadata = TileLayerMetadata(
      rasterRdd.rasterTileLayerMetadata.tileLayerMetadata.cellType,
      rasterRdd.rasterTileLayerMetadata.tileLayerMetadata.layout,
      rasterRdd.rasterTileLayerMetadata.tileLayerMetadata.extent,
      rasterRdd.rasterTileLayerMetadata.tileLayerMetadata.crs,
      spatialKeyBounds)
    val stitchedRdd:TileLayerRDD[SpatialKey] = ContextRDD(resultsRdd, spatialMetadata)

    val stitch: Raster[Tile] = stitchedRdd.stitch()
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    stitch.tile.renderPng(colorRamp).write("/home/geocube/environment_test/geocube_core_jar/test.png")*/
  }
}
