package whu.edu.cn.geocube.core.raster.query

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import com.google.gson.{JsonObject, JsonParser}
import org.locationtech.jts.io.WKTReader
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{CellType, Raster, Tile, TileLayout}
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.geocube.core.entity
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.entity.GcProduct._
import whu.edu.cn.geocube.core.entity.GcMeasurement._
import whu.edu.cn.geocube.util.HbaseUtil.{getTileCell, getTileMeta}
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.geocube.util.TileSerializer.deserializeTileData
import whu.edu.cn.geocube.util.TileUtil

/**
 * Query raster tiles in a serial way.
 * Return tiles with an Array.
 *
 */
object QueryRasterTiles {
  /**
   * Query raster tiles and return (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]).
   *
   * @param p query parameter
   * @return an array tile with metadata
   */
  def getRasterTileArray(p: QueryParams):(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of raster tiles is being processed...")

    val queryBegin = System.currentTimeMillis()
    if (p.getRasterProductNames.length == 0 || p.getRasterProductNames.length == 1) { //single product
      if(p.getRasterProductNames.length == 1) p.setRasterProductName(p.getRasterProductNames(0))
      val tileLayerArrayWithMeta = getRasterTileArrayWithMeta(p)
      val queryEnd = System.currentTimeMillis()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying" + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin) + " ms")
      tileLayerArrayWithMeta
    } else { //multiple product
      val multiProductTileLayerArray= ArrayBuffer[Array[(SpaceTimeBandKey, Tile)]]()
      val multiProductTileLayerMeta = ArrayBuffer[RasterTileLayerMetadata[SpaceTimeKey]]()
      for(rasterProductName <- p.getRasterProductNames){
        p.setRasterProductName(rasterProductName)
        val results = getRasterTileArrayWithMeta(p)
        if(results != null){
          multiProductTileLayerArray.append(results._1)
          multiProductTileLayerMeta.append(results._2)
        }
      }
      if(multiProductTileLayerMeta.length < 1) throw new RuntimeException("No tiles with the query condition!")
      var destTileLayerMetaData = multiProductTileLayerMeta(0).getTileLayerMetadata
      val destRasterProductNames = ArrayBuffer[String]()
      for(i <- multiProductTileLayerMeta) {
        destTileLayerMetaData = destTileLayerMetaData.merge(i.getTileLayerMetadata)
        destRasterProductNames.append(i.getProductName)
      }
      val destRasterTileLayerMetaData = entity.RasterTileLayerMetadata[SpaceTimeKey](destTileLayerMetaData, _productNames = destRasterProductNames)
      val tileLayerArrayWithMeta = (multiProductTileLayerArray.toArray.flatten, destRasterTileLayerMetaData)
      val queryEnd = System.currentTimeMillis()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin) + " ms")
      tileLayerArrayWithMeta
    }
  }

  /**
   * Query raster tiles and return (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]).
   *
   * @param p query parameter
   * @return an array with metadata
   */
  def getRasterTileArrayWithMeta(p: QueryParams): (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
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

    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent_"+cubeId+"\" where 1=1 "

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

        val extentResults = statement.executeQuery(extentsql.toString())
        val extentKeys = new StringBuilder
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

        //Product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct_"+cubeId+"\" where 1=1 "
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
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        if(nextStartTime != "" && nextEndTime != ""){
          productsql ++= " Or phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= nextStartTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= nextEndTime
          productsql ++= "\')"
        } else{
          productsql ++= ")"
        }
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

        //Tile_ID query
        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact_"+cubeId+" where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"

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
        }

        //Access tile data and return a Array[RasterTile]
        val queriedRasterTiles = ArrayBuffer[RasterTile]()
        tileAndDimensionKeys.foreach(keys => queriedRasterTiles.append(initRasterTile(cubeId,keys(0), keys(1), keys(2), keys(3), keys(4))))

        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Get " + queriedRasterTiles.length + " tiles of " + rasterProductName + " product successfully")

        //Extract metadata of Array[RasterTile]
        if(queriedRasterTiles.length != 0) initLayerMeta(queriedRasterTiles)
        else null

      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Initiate a RasterTile object using dimension keys.
   *
   * @param id tile data id
   * @param productKey
   * @param measurementKey
   * @param extentKey
   * @param qualityKey
   * @return a RasterTile object
   */
  def initRasterTile(cubeId:String, id: String, productKey: String, measurementKey: String, extentKey: String, qualityKey: String): RasterTile = {
    val rasterTile = RasterTile(id)

    //Access product and measurement dimensional info in postgresql
    val productMeta = getProductByKey(cubeId,productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val measurementMeta = getMeasurementByMeasureAndProdKey(cubeId,measurementKey, productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)

    rasterTile.setProductMeta(productMeta)
    rasterTile.setMeasurementMeta(measurementMeta)

    //Access tile data and meta in HBase
    val tileMeta =
      if (PostgresqlUtil.url.contains("whugeocube"))
        getTileMeta("hbase_raster", id, "rasterData", "metaData")
      else if (PostgresqlUtil.url.contains("ypgeocube"))
        getTileMeta("hbase_raster_fivehundred", id, "rasterData", "metaData")
      else
        getTileMeta("hbase_raster_regions_"+cubeId, id, "rasterData", "metaData")

    val tileBytes =
      if (PostgresqlUtil.url.contains("whugeocube"))
        getTileCell("hbase_raster", id, "rasterData", "tile")
      else if (PostgresqlUtil.url.contains("ypgeocube"))
        getTileCell("hbase_raster_fivehundred", id, "rasterData", "tile")
      else
        getTileCell("hbase_raster_regions_"+cubeId, id, "rasterData", "tile")

    /*val tileMeta = getTileMeta("hbase_raster_regions", id, "rasterData", "metaData")
    val tileBytes = getTileCell("hbase_raster_regions", id, "rasterData", "tile")*/

    //Add hbase attributes to a RasterTile object
    val json = new JsonParser()
    val obj = json.parse(tileMeta).asInstanceOf[JsonObject]

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

    val dType = obj.get("cellType").toString.replace("\"", "")

    //Transform tile data to a Tile Object
    val tileData = deserializeTileData(rasterTile.getProductMeta.getPlatform, tileBytes, rasterTile.getProductMeta.tileSize.toInt, dType)
    rasterTile.setData(tileData)
    rasterTile
  }

  /**
   * Extract metadata of Array[RasterTile] and transform to a TileLayerRDD[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   *
   * @param rasterTiles
   * @return an Array[(SpaceTimeBandKey, Tile)] with RasterTileLayerMetadata[SpaceTimeKey]
   */
  def initLayerMeta(rasterTiles:ArrayBuffer[RasterTile]):(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val layer:ArrayBuffer[(SpaceTimeBandKey,Tile)] = rasterTiles.map{ tile =>
      val phenomenonTime = sdf.parse(tile.getProductMeta.getPhenomenonTime).getTime
      val measurement = tile.getMeasurementMeta.getMeasurementName
      val colNum = Integer.parseInt(tile.getColNum)
      val rowNum = Integer.parseInt(tile.getRowNum)
      val Tile = tile.getData
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum, rowNum, phenomenonTime), measurement)
      val v = Tile
      (k, v)
    }
    val productName: String =rasterTiles(0).getProductMeta.productName
    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tileSize: Int = rasterTiles(0).getProductMeta.tileSize.toInt

    val tl = TileLayout(360, 180, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)

    val colArray = new ArrayBuffer[Int]()
    val rowArray = new ArrayBuffer[Int]()
    val longArray = new ArrayBuffer[Double]()
    val latArray =new ArrayBuffer[Double]()
    val instantArray = new ArrayBuffer[Long]()

    for(tile <- rasterTiles){
      colArray.append(Integer.parseInt(tile.getColNum))
      rowArray.append(Integer.parseInt(tile.getRowNum))
      longArray.append(tile.getLeftBottomLong.toDouble)
      longArray.append(tile.getRightUpperLong.toDouble)
      latArray.append(tile.getLeftBottomLat.toDouble)
      latArray.append(tile.getRightUpperLat.toDouble)
      instantArray.append(new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(tile.getProductMeta.getPhenomenonTime).getTime)
    }

    val (minCol,maxCol)  = (colArray.min, colArray.max)
    val (minRow,maxRow)  = (rowArray.min, rowArray.max)
    val (minLong,maxLong)  = (longArray.min, longArray.max)
    val (minLat,maxLat)  = (latArray.min, latArray.max)
    val (minInstant,maxInstant)  = (instantArray.min, instantArray.max)

    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))
    val actualExtent = geotrellis.vector.Extent(minLong, minLat, maxLong, maxLat)
    val dtype = rasterTiles.last.getMeasurementMeta.getMeasurementDType
    val celltype = CellType.fromName(dtype)
    val crsStr = rasterTiles.last.getCRS.toString.replace("\"", "")
    var crs:CRS = CRS.fromEpsgCode(4326)
    if(crsStr == "WGS84"){
      crs = CRS.fromEpsgCode(4326)
    }

    val targetArray = (layer.toArray,entity.RasterTileLayerMetadata(TileLayerMetadata(celltype,ld,actualExtent,crs,bounds), productName))
    targetArray
  }


  /**
   * Get TMS tile in Array[Byte].
   * Used in geocube-boot project for tile visualization.
   *
   * @param p query parameters
   * @return TMS tile in Array[Byte]
   */
  def getPyramidTile(p: QueryParams): Array[Byte] = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
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

        // Extent dimension
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent\" where 1=1 "

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

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct_"+cubeId+"\" where 1=1 "
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
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        if(nextStartTime != "" && nextEndTime != ""){
          productsql ++= " Or phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= nextStartTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= nextEndTime
          productsql ++= "\')"
        } else{
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

        //Measurement dimension
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

        //tile id query
        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact_"+cubeId+ " where extent_key IN" +
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
        }
        val tileDataID = tileAndDimensionKeys(0)(0)

        //Access tile byts
        val tilePngBytes = getTileCell("hbase_raster_regions_"+cubeId, tileDataID, "rasterData", "tile")

        println("Returned " + tileAndDimensionKeys.length + " tiles of " + rasterProductName + " product")
        tilePngBytes
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }


  /**
   * Get coverage range set.
   * Used by OGC API - Coverages.
   *
   * @param p query parameters
   * @return an array of pixel value
   */
  def getCoverageRangeSet(p: QueryParams): Array[Double] = {
    val tileArray:(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileArray(p)
    val tileLayerArray: Array[(SpatialKey, Tile)] = tileArray._1.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
    val metadata = tileArray._2.tileLayerMetadata
    val layout = metadata.layout
    val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)
    stitched.tile.toArrayDouble()
  }

  /**
   * Get coverage with png format.
   * Used by OGC API - Coverages.
   *
   * @param p query parameters
   * @param bbox bounding box represented by a string
   * @param subset bounding box represented by an array
   * @return
   */
  def getCoveragePng(p: QueryParams, bbox: String, subset: Array[String]): Array[Byte] = {
    val tileArray:(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileArray(p)
    val tileLayerArray: Array[(SpatialKey, Tile)] = tileArray._1.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
    val metadata = tileArray._2.tileLayerMetadata
    val layout = metadata.layout
    val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)
    val srcExtent = stitched.extent

    if(bbox != null){
      val coordinates = bbox.split(",")
      val requestedExtent = new Extent(coordinates(0).toDouble, coordinates(1).toDouble, coordinates(2).toDouble, coordinates(3).toDouble)
      if(requestedExtent.intersects(srcExtent)){
        val intersection = requestedExtent.intersection(srcExtent).get
        val colMin = (intersection.xmin - srcExtent.xmin)/stitched.cellSize.width
        val colMax = (intersection.xmax - srcExtent.xmin)/stitched.cellSize.width
        val rowMin = (intersection.ymin - srcExtent.ymin)/stitched.cellSize.height
        val rowMax = (intersection.ymax - srcExtent.ymin)/stitched.cellSize.height
        val colorRamp = ColorRamp(RGB(0,0,0), RGB(255,255,255))
          .stops(100)
          .setAlphaGradient(0xFF, 0xAA)
        val png = stitched.tile.crop(colMin.toInt, rowMin.toInt, colMax.toInt, rowMax.toInt).renderPng(colorRamp)  //Array[Byte]
        return png
      }else
        return null
    }

    if(subset != null){
      if(subset.length != 2) throw new RuntimeException("Wrong subset paramters")
      val prefix = subset(0).substring(0, subset(0).indexOf("("))
      val coordinates:Array[String] = prefix match{
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

      if(requestedExtent.intersects(srcExtent)){
        val intersection = requestedExtent.intersection(srcExtent).get
        println(intersection.toString())
        val colMin = (intersection.xmin - srcExtent.xmin)/stitched.cellSize.width
        val colMax = (intersection.xmax - srcExtent.xmin)/stitched.cellSize.width
        val rowMin = (intersection.ymin - srcExtent.ymin)/stitched.cellSize.height
        val rowMax = (intersection.ymax - srcExtent.ymin)/stitched.cellSize.height
        println(stitched.cellSize.toString)
        println(colMin, colMax, rowMin, rowMax)
        val colorRamp = ColorRamp(RGB(0,0,0), RGB(255,255,255))
          .stops(100)
          .setAlphaGradient(0xFF, 0xAA)
        val png = stitched.tile.crop(colMin.toInt, rowMin.toInt, colMax.toInt, rowMax.toInt).renderPng(colorRamp)  //Array[Byte]
        return png
      }else
        return null
    }

    val colorRamp = ColorRamp(RGB(0,0,0), RGB(255,255,255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val png = stitched.tile.renderPng(colorRamp)  //Array[Byte]
    png
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
    val queryParams = new QueryParams
    queryParams.setCubeId("2")
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(116.44735,32.10642,118.93871,34.22733)
    queryParams.setTime("2014-05-17 02:42:39", "2014-05-17 02:42:41")
    queryParams.setMeasurements(Array("Red"))
    //queryParams.setLevel("999") //default 999
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileArray(queryParams)
    val layout = tileLayerArrayWithMeta._2.tileLayerMetadata.layout
    val tileLayerArray = tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey.spatialKey, x._2))
    val stitch = TileUtil.stitch(tileLayerArray, layout)
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    stitch.tile.renderPng(colorRamp).write("/home/geocube/environment_test/geocube_core_jar/test.png")
  }
}
