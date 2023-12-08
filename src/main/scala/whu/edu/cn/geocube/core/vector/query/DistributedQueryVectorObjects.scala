package whu.edu.cn.geocube.core.vector.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.JsonParser
import geotrellis.layer.{Bounds, SpaceTimeKey, SpatialKey}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.entity.GcProduct.getProductByKey
import whu.edu.cn.geocube.core.entity.{QueryParams, VectorGridLayerMetadata}
import whu.edu.cn.geocube.core.vector.grid.GridConf
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomGridInfo
import whu.edu.cn.geocube.util.HbaseUtil.getVectorMeta
import whu.edu.cn.util.PostgresqlUtil

/**
 * Query vector data in a distributed way.
 * Return logical vector tiles with a RDD.
 *
 * note: logical vector tile means vector data are grouped into a grid if intersected, maintaining whose topology structure.
 */
object DistributedQueryVectorObjects {
  /**
   * Query vector data and return a FeatureRDD.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a FeatureRDD
   * */
  def getFeatures(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean = true): FeatureRDD = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of features is being processed...")
    val queryBegin = System.currentTimeMillis()
    val results = getGeoObjectsRDD(sc, p, duplicated)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + results._1.map(_._2.size).sum.toInt + " features: " + (queryEnd - queryBegin) + " ms")
    new FeatureRDD(results._1, results._2)
  }

  /**
   * Query vector data and return (RDD[(SpaceTimeKey, Iterable[GeoObject])]).
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpaceTimeKey, Iterable[GeoObject])]
   * */
  def getGeoObjectsRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean = true): (RDD[(SpaceTimeKey, Iterable[GeoObject])], VectorGridLayerMetadata[SpaceTimeKey]) = {
    val gridLayerGeoObjectRDD = getGridLayerGeoObjectsRDD(sc, p, duplicated)
    gridLayerGeoObjectRDD
  }

  /**
   * Query vector data and return (RDD[(SpaceTimeKey, Iterable[GeoObject])]).
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpaceTimeKey, Iterable[GeoObject])]
   * */
  def getGridLayerGeoObjectsRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean): (RDD[(SpaceTimeKey, Iterable[GeoObject])], VectorGridLayerMetadata[SpaceTimeKey]) = {
    val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val cubeId = p.getCubeId
    //product params
    val vectorProductName = p.getVectorProductName

    //temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from gc_extent_"+cubeId+" where 1=1 "
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

        //product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product_"+cubeId+"\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
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
          productsql ++= "\')"
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

        //query vector data ids contained in each grid
        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact_"+cubeId+" where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        var geoObjectCount = 0
        geoObjectsAndDimensionKeys.foreach { keys =>
          val geoObjectKeyListString = keys(0)
          val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")
          geoObjectCount += geoObjectKeys.length
        }

        //discarded duplicated vectors if $duplicated is false
        val geoObjectsAndDimensionKeysDuplication: ArrayBuffer[Array[String]] = if (duplicated) geoObjectsAndDimensionKeys else {
          val mutableSet = scala.collection.mutable.Set("")
          val geoObjectsAndDimensionKeysResults = new ArrayBuffer[Array[String]]()
          geoObjectsAndDimensionKeys.foreach{ keys =>
            val geoObjectKeysNoDuplicated = new StringBuilder
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")
            geoObjectKeys.foreach{ geoObjectKey =>
              if(!mutableSet.contains(geoObjectKey)){
                mutableSet.add(geoObjectKey)
                geoObjectKeysNoDuplicated.append(geoObjectKey + ", ")
              }
            }
            if(geoObjectKeysNoDuplicated.length()!=0){
              val wrapGeoObjectKeysNoDuplicated = geoObjectKeysNoDuplicated.deleteCharAt(geoObjectKeysNoDuplicated.length()-1)
              geoObjectsAndDimensionKeysResults.append(Array("(" + wrapGeoObjectKeysNoDuplicated.deleteCharAt(wrapGeoObjectKeysNoDuplicated.length()-1).toString() + ")", keys(1), keys(2)))
            }

          }
          geoObjectsAndDimensionKeysResults
        }

        geoObjectCount = 0
        geoObjectsAndDimensionKeysDuplication.foreach { keys =>
          val geoObjectKeyListString = keys(0)
          val geoObjectKeys: Array[String] = geoObjectKeyListString.split(",")
          geoObjectCount += geoObjectKeys.length
        }

        //access vectors in HBase in a distributed way
        var partitions = geoObjectsAndDimensionKeysDuplication.length
        if(partitions > 48) partitions = 48
        val geoObjectsAndDimensionKeysRdd = sc.parallelize(geoObjectsAndDimensionKeysDuplication, partitions)

        val gridLayerGeoObjectRdd: RDD[(SpaceTimeKey, Iterable[GeoObject])] = geoObjectsAndDimensionKeysRdd.mapPartitions{ partition =>
          val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val results = new ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])]()
          partition.foreach{ keys =>
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")

            //product key is used to get time of the grid
            val productKey = keys(1)
            val productSql = "select phenomenon_time from gc_product_"+cubeId+" where product_key=" + productKey + ";"
            val productRs = statement.executeQuery(productSql)
            var time: Long = 0L
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            if (productRs.first()) {
              productRs.previous()
              while (productRs.next()) {
                time = sdf.parse(productRs.getString(1)).getTime
              }
            } else {
              throw new RuntimeException("No vector time acquired!")
            }

            //extent key is used to get (col, row) of the grid
            val extentKey = keys(2)
            val extentSql = "select extent from gc_extent_"+cubeId+" where extent_key=" + extentKey + ";"
            val extentRs = statement.executeQuery(extentSql)
            var (col, row) = (-1, - 1)
            if (extentRs.first()) {
              extentRs.previous()
              while (extentRs.next()) {
                val json = new JsonParser()
                val obj = json.parse(extentRs.getString(1)).getAsJsonObject
                col = obj.get("column").toString.toInt
                row = obj.get("row").toString.toInt
              }
            } else {
              throw new RuntimeException("No column and row acquired!")
            }

            //access vectors in the grid
            val fjson = new FeatureJSON()
            val geoObjects:Array[GeoObject] = geoObjectKeys.map { geoObjectkey =>
              val featureStr = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "metaData")
              val feature: SimpleFeature = fjson.readFeature(featureStr)
              new GeoObject(geoObjectkey, feature)
            }.filter(geoObject => geoObject.feature.getDefaultGeometry.asInstanceOf[Geometry].isValid)

            results.append((SpaceTimeKey(col, row, time), geoObjects))
          }
          conn.close()
          results.iterator
        }.cache()

        val layerMeta: VectorGridLayerMetadata[SpaceTimeKey] = initLayerMeta(geoObjectsAndDimensionKeysDuplication(0)(1), p, gridLayerGeoObjectRdd)

        (gridLayerGeoObjectRdd, layerMeta)
      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Extract vector layer metadata.
   *
   * @param productKey
   * @param queryParams
   * @param gridLayerGeoObjectsRdd
   *
   * @return VectorGridLayerMetadata[SpaceTimeKey]
   */
  def initLayerMeta(productKey: String,
                    queryParams: QueryParams,
                    gridLayerGeoObjectsRdd: RDD[(SpaceTimeKey, Iterable[GeoObject])]): VectorGridLayerMetadata[SpaceTimeKey] = {
    val crsStr = queryParams.getCRS
    val crs: CRS =
      if (crsStr == "WGS84") CRS.fromEpsgCode(4326)
      else throw new RuntimeException("Not support " + crsStr)

    val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))

    //maybe unaccurate actual extent calculated by the query polygon, but faster
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(queryParams.getPolygon, gridConf.gridDimX, gridConf.gridDimX, gridConf.extent, colRow, longLati,1)
    val minCol = colRow(0); val minRow = colRow(1); val maxCol = colRow(2); val maxRow = colRow(3)
    val minLong = longLati(0); val minLat = longLati(1) ; val maxLong = longLati(2); val maxLat = longLati(3)
    val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(queryParams.getStartTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(queryParams.getEndTime).getTime
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))
    val extent = Extent(minLong, minLat, maxLong, maxLat)

    //accurate actual extent calculation, but cause more time cost
    /*val productMeta = getProductByKey(productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val instant = sdf.parse(productMeta.getPhenomenonTime).getTime
    val colRow = gridLayerGeoObjectsRdd.map(x=>(x._1.spatialKey.col, x._1.spatialKey.row)).collect()
    val minCol = colRow.map(_._1).min
    val maxCol = colRow.map(_._1).max
    val minRow = colRow.map(_._2).min
    val maxRow = colRow.map(_._2).max
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,instant),SpaceTimeKey(maxCol,maxRow,instant))

    val bboxes = gridLayerGeoObjectsRdd
      .flatMap(x=>(x._2.map(_.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)))
      .map(x=>(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
      .collect()
    val extentMinX = bboxes.map(_._1).min
    val extentMinY = bboxes.map(_._2).min
    val extentMaxX = bboxes.map(_._3).max
    val extentMaxY = bboxes.map(_._4).max
    val extent = new Extent(extentMinX, extentMinY, extentMaxX, extentMaxY)*/

    val productName = queryParams.getVectorProductName
    VectorGridLayerMetadata[SpaceTimeKey](gridConf, extent, bounds, crs, productName)
  }

  /**
   * Query geom IDs and return (RDD[(SpatialKey, Array[GeomId])]).
   *
   * This function is developed for AI-based method.
   *
   * There is a repartition(shuffle) process based on computational
   * intensity in AI-based method. It can take too much time if access
   * and shuffle GeoObject directly, so the function only accesses
   * GeomID for repartition(shuffle) to decrease the time cost.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpatialKey, Array[GeomId])]
   * */
  def getGeoObjectKeysRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean = true): RDD[(SpatialKey, Array[String])] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of vectors is being processed...")
    val queryBegin = System.currentTimeMillis()
    val gridLayerGeoObjecKeysRdd: RDD[(SpatialKey, Array[String])] = getGridLayerGeoObjectKeysRDD(sc, p, duplicated)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying: " + (queryEnd - queryBegin) + " ms")
    gridLayerGeoObjecKeysRdd
  }

  /**
   * Query geom IDs and return (RDD[(SpatialKey, Array[GeomId])]).
   *
   * This function is developed for AI-based method.
   *
   * There is a repartition(shuffle) process based on computational
   * intensity in AI-based method. It can take too much time if access
   * and shuffle GeoObject directly, so the function only accesses
   * GeomID for repartition(shuffle) to decrease the time cost.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpatialKey, Array[GeomId])]
   * */
  def getGridLayerGeoObjectKeysRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean): RDD[(SpatialKey, Array[String])] = {
    val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val cubeId = p.getCubeId
    //product params
    val vectorProductName = p.getVectorProductName

    //temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        //extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from gc_extent_"+cubeId+" where 1=1 "
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

        //product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product_aigis_"+cubeId+"\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
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
          productsql ++= "\')"
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

        //query vector data ids contained in each grid
        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact_aigis_"+cubeId+" where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        //discarded duplicated vectors if $duplicated is false
        val geoObjectsAndDimensionKeysDuplication: ArrayBuffer[Array[String]] = if (duplicated) geoObjectsAndDimensionKeys else {
          val mutableSet = scala.collection.mutable.Set("")
          val geoObjectsAndDimensionKeysResults = new ArrayBuffer[Array[String]]()
          geoObjectsAndDimensionKeys.foreach{ keys =>
            val geoObjectKeysNoDuplicated = new StringBuilder
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")
            geoObjectKeys.foreach{ geoObjectKey =>
              if(!mutableSet.contains(geoObjectKey)){
                mutableSet.add(geoObjectKey)
                geoObjectKeysNoDuplicated.append(geoObjectKey + ", ")
              }
            }
            if(geoObjectKeysNoDuplicated.length()!=0){
              val wrapGeoObjectKeysNoDuplicated = geoObjectKeysNoDuplicated.deleteCharAt(geoObjectKeysNoDuplicated.length()-1)
              geoObjectsAndDimensionKeysResults.append(Array("(" + wrapGeoObjectKeysNoDuplicated.deleteCharAt(wrapGeoObjectKeysNoDuplicated.length()-1).toString() + ")", keys(1), keys(2)))
            }

          }
          geoObjectsAndDimensionKeysResults
        }

        var partitions = geoObjectsAndDimensionKeysDuplication.length
        if(partitions > 8) partitions = 8
        val geoObjectsAndDimensionKeysRdd = sc.parallelize(geoObjectsAndDimensionKeysDuplication, partitions)

        val gridLayerGeoObjectKeysRdd: RDD[(SpatialKey, Array[String])] = geoObjectsAndDimensionKeysRdd.mapPartitions{ partition =>
          val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val results = new ArrayBuffer[(SpatialKey, Array[String])]()
          partition.foreach{ keys =>
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")

            val productKey = keys(1)
            val productSql = "select phenomenon_time from gc_product_aigis_"+cubeId+" where product_key=" + productKey + ";"
            val productRs = statement.executeQuery(productSql)

            //product key is used to get time of the grid
            var time: Long = 0L
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            if (productRs.first()) {
              productRs.previous()
              while (productRs.next()) {
                time = sdf.parse(productRs.getString(1)).getTime
              }
            } else {
              throw new RuntimeException("No vector time acquired!")
            }

            //extent key is used to get (col, row) of the grid
            val extentKey = keys(2)
            val extentSql = "select extent from gc_extent_"+cubeId+" where extent_key=" + extentKey + ";"
            val extentRs = statement.executeQuery(extentSql)
            var (col, row) = (-1, - 1)
            if (extentRs.first()) {
              extentRs.previous()
              while (extentRs.next()) {
                val json = new JsonParser()
                val obj = json.parse(extentRs.getString(1)).getAsJsonObject
                col = obj.get("column").toString.toInt
                row = obj.get("row").toString.toInt
              }
            } else {
              throw new RuntimeException("No column and row acquired!")
            }

            results.append((SpatialKey(col, row), geoObjectKeys))
          }
          conn.close()
          results.iterator
        }.cache()

        gridLayerGeoObjectKeysRdd
      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Query productKey, factKey and geom IDs, return a
   * (RDD[(SpatialKey, ((productKey, factKey), Array[GeomId])])).
   *
   * This function is developed for AI-based method.
   *
   * The returned results is used to ingest computational intensity
   * for double-layer analysis, e.g. spatial intersection.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpatialKey, ((productKey, factKey), Array[GeomId]))]
   * */
  def getGeoObjectAndProductFactKeysRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean = true): RDD[(SpatialKey, ((String, String), Array[String]))] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of vectors is being processed...")
    val queryBegin = System.currentTimeMillis()
    val gridLayerGeoObjecAndFactKeysRdd: RDD[(SpatialKey, ((String, String), Array[String]))] = getGridLayerGeoObjectAndProductFactKeysRDD(sc, p, duplicated)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying: " + (queryEnd - queryBegin) + " ms")
    gridLayerGeoObjecAndFactKeysRdd
  }

  /**
   * Query productKey, factKey and geom IDs, return a
   * (RDD[(SpatialKey, ((productKey, factKey), Array[GeomId])])).
   *
   * This function is developed for AI-based method.
   *
   * The returned results is used to ingest computational intensity
   * for double-layer analysis, e.g. spatial intersection.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @return a RDD[(SpatialKey, ((productKey, factKey), Array[GeomId]))]
   * */
  def getGridLayerGeoObjectAndProductFactKeysRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean): RDD[(SpatialKey, ((String, String), Array[String]))] = {
    val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val cubeId = p.getCubeId
    //product params
    val vectorProductName = p.getVectorProductName

    //temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        //extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key_"+cubeId+" from gc_extent where 1=1 "
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

        //product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product_aigis_"+cubeId+"\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
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
          productsql ++= "\')"
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

        //query vector data ids contained in each grid
        val command = "Select tile_data_id,product_key,extent_key,fact_key from gc_vector_tile_fact_aigis_"+cubeId+" where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](4)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            keyArray(3) = geoObjectsIDResults.getString(4)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        //discarded duplicated vectors if $duplicated is false
        val geoObjectsAndDimensionKeysDuplication: ArrayBuffer[Array[String]] = if (duplicated) geoObjectsAndDimensionKeys else {
          val mutableSet = scala.collection.mutable.Set("")
          val geoObjectsAndDimensionKeysResults = new ArrayBuffer[Array[String]]()
          geoObjectsAndDimensionKeys.foreach{ keys =>
            val geoObjectKeysNoDuplicated = new StringBuilder
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")
            geoObjectKeys.foreach{ geoObjectKey =>
              if(!mutableSet.contains(geoObjectKey)){
                mutableSet.add(geoObjectKey)
                geoObjectKeysNoDuplicated.append(geoObjectKey + ", ")
              }
            }
            if(geoObjectKeysNoDuplicated.length()!=0){
              val wrapGeoObjectKeysNoDuplicated = geoObjectKeysNoDuplicated.deleteCharAt(geoObjectKeysNoDuplicated.length()-1)
              geoObjectsAndDimensionKeysResults.append(Array("(" + wrapGeoObjectKeysNoDuplicated.deleteCharAt(wrapGeoObjectKeysNoDuplicated.length()-1).toString() + ")", keys(1), keys(2), keys(3)))
            }

          }
          geoObjectsAndDimensionKeysResults
        }

        var partitions = geoObjectsAndDimensionKeysDuplication.length
        if(partitions > 8) partitions = 8
        val geoObjectsAndDimensionKeysRdd = sc.parallelize(geoObjectsAndDimensionKeysDuplication, partitions)

        val gridLayerGeoObjectKeysRdd: RDD[(SpatialKey, ((String, String), Array[String]))] = geoObjectsAndDimensionKeysRdd.mapPartitions{ partition =>
          val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val results = new ArrayBuffer[(SpatialKey, ((String, String), Array[String]))]()
          partition.foreach{ keys =>
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")

            //product key is used to get time of the grid
            val productKey = keys(1)
            val productSql = "select phenomenon_time from gc_product_aigis_"+cubeId+" where product_key=" + productKey + ";"
            val productRs = statement.executeQuery(productSql)
            var time: Long = 0L
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            if (productRs.first()) {
              productRs.previous()
              while (productRs.next()) {
                time = sdf.parse(productRs.getString(1)).getTime
              }
            } else {
              throw new RuntimeException("No vector time acquired!")
            }

            //extent key is used to get (col, row) of the grid
            val extentKey = keys(2)
            val extentSql = "select extent from gc_extent_"+cubeId+" where extent_key=" + extentKey + ";"
            val extentRs = statement.executeQuery(extentSql)
            var (col, row) = (-1, - 1)
            if (extentRs.first()) {
              extentRs.previous()
              while (extentRs.next()) {
                val json = new JsonParser()
                val obj = json.parse(extentRs.getString(1)).getAsJsonObject
                col = obj.get("column").toString.toInt
                row = obj.get("row").toString.toInt
              }
            } else {
              throw new RuntimeException("No column and row acquired!")
            }

            val factKey = keys(3)
            results.append((SpatialKey(col, row), ((productKey, factKey), geoObjectKeys)))
          }
          conn.close()
          results.iterator
        }.cache()

        gridLayerGeoObjectKeysRdd
      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Query geom IDs and computational intensity(CIT) of each grid/tile,
   * return a RDD[(SpatialKey, (Array[GeomID], CIT))]
   *
   * This function is developed for AI-based method.
   *
   * The returned results can be repartitioned based on CIT,
   * then achieve better load balance performance in data analysis.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @param processName identify computational intensity info in which analysis process
   * @param otherP another query params for double-layers analysis, used to
   *               identify computational intensity info
   *
   * @return a RDD[(SpatialKey, (Array[GeomID], CIT))]
   * */
  def getGeoObjectKeysAndCompuIntensityRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean = true, processName: String, otherP: QueryParams = null): RDD[(SpatialKey, (Array[String], Long))] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of vectors is being processed...")
    val queryBegin = System.currentTimeMillis()
    val gridLayerGeoObjecKeysAndCompuIntensityRdd: RDD[(SpatialKey, (Array[String], Long))] = getGridLayerGeoObjectKeysAndCompuIntensityRDD(sc, p, duplicated, processName, otherP)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying: " + (queryEnd - queryBegin) + " ms")
    gridLayerGeoObjecKeysAndCompuIntensityRdd
  }

  /**
   * Query geom IDs and computational intensity(CIT) of each grid/tile,
   * return a RDD[(SpatialKey, (Array[GeomID], CIT))]
   *
   * This function is developed for AI-based method.
   *
   * The returned results can be repartitioned based on CIT,
   * then achieve better load balance performance in data analysis.
   *
   * @param sc SparkContext
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded randomly.
   * @param processName identify computational intensity info in which analysis process
   * @param otherP another query params for double-layers analysis, used to
   *               identify computational intensity info
   *
   * @return a RDD[(SpatialKey, (Array[GeomID], CIT))]
   * */
  def getGridLayerGeoObjectKeysAndCompuIntensityRDD(implicit sc: SparkContext, p: QueryParams, duplicated: Boolean, processName: String, otherP: QueryParams): RDD[(SpatialKey, (Array[String], Long))] = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    val cubeId = p.getCubeId
    //product params
    val vectorProductName = p.getVectorProductName

    //temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        //extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from gc_extent_"+cubeId+" where 1=1 "
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

        //product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product_aigis_"+cubeId+"\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
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
          productsql ++= "\')"
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

        //query vector data ids contained in each grid
        val command = "Select tile_data_id,product_key,extent_key,compu_intensity from gc_vector_tile_fact_aigis_"+cubeId+" where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](4)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            keyArray(3) = geoObjectsIDResults.getString(4)
            if(keyArray(3) != null) geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        //discarded duplicated vectors if $duplicated is false
        val geoObjectsAndDimensionKeysDuplication: ArrayBuffer[Array[String]] = if (duplicated) geoObjectsAndDimensionKeys else {
          val mutableSet = scala.collection.mutable.Set("")
          val geoObjectsAndDimensionKeysResults = new ArrayBuffer[Array[String]]()
          geoObjectsAndDimensionKeys.foreach{ keys =>
            val geoObjectKeysNoDuplicated = new StringBuilder
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")
            geoObjectKeys.foreach{ geoObjectKey =>
              if(!mutableSet.contains(geoObjectKey)){
                mutableSet.add(geoObjectKey)
                geoObjectKeysNoDuplicated.append(geoObjectKey + ", ")
              }
            }
            if(geoObjectKeysNoDuplicated.length()!=0){
              val wrapGeoObjectKeysNoDuplicated = geoObjectKeysNoDuplicated.deleteCharAt(geoObjectKeysNoDuplicated.length()-1)
              geoObjectsAndDimensionKeysResults.append(Array("(" + wrapGeoObjectKeysNoDuplicated.deleteCharAt(wrapGeoObjectKeysNoDuplicated.length()-1).toString() + ")", keys(1), keys(2), keys(3)))
              //geoObjectsAndDimensionKeysResults.append(Array(geoObjectKeysNoDuplicated.deleteCharAt(geoObjectKeysNoDuplicated.length()-1).toString(), keys(1), keys(2)))
            }

          }
          geoObjectsAndDimensionKeysResults
        }

        var partitions = geoObjectsAndDimensionKeysDuplication.length
        if(partitions > 8) partitions = 8
        val geoObjectsAndDimensionKeysRdd = sc.parallelize(geoObjectsAndDimensionKeysDuplication, partitions)

        val gridLayerGeoObjectKeysAndCompuIntensityRdd: RDD[(SpatialKey, (Array[String], Long))] = geoObjectsAndDimensionKeysRdd.mapPartitions{ partition =>
          val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val results = new ArrayBuffer[(SpatialKey, (Array[String], Long))]()
          val processID = processName
          val otherQueryPara = otherP
          partition.foreach{ keys =>
            val geoObjectKeyListString = keys(0)
            val geoObjectKeys: Array[String] = geoObjectKeyListString.substring(geoObjectKeyListString.indexOf("(") + 1, geoObjectKeyListString.indexOf(")")).split(", ")

            //product key is used to get time of the grid
            val productKey = keys(1)
            val productSql = "select phenomenon_time from gc_product_aigis_"+cubeId+" where product_key=" + productKey + ";"
            val productRs = statement.executeQuery(productSql)
            var time: Long = 0L
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            if (productRs.first()) {
              productRs.previous()
              while (productRs.next()) {
                time = sdf.parse(productRs.getString(1)).getTime
              }
            } else {
              throw new RuntimeException("No vector time acquired!")
            }

            //extent key is used to get (col, row) of the grid
            val extentKey = keys(2)
            val extentSql = "select extent from gc_extent_"+cubeId+" where extent_key=" + extentKey + ";"
            val extentRs = statement.executeQuery(extentSql)
            var (col, row) = (-1, - 1)
            if (extentRs.first()) {
              extentRs.previous()
              while (extentRs.next()) {
                val json = new JsonParser()
                val obj = json.parse(extentRs.getString(1)).getAsJsonObject
                col = obj.get("column").toString.toInt
                row = obj.get("row").toString.toInt
              }
            } else {
              throw new RuntimeException("No column and row acquired!")
            }

            //read computational intensity json
            val compuIntensityJsonStr:String = keys(3)
            val objectMapper = new ObjectMapper
            val compuIntensityNode = objectMapper.readTree(compuIntensityJsonStr.getBytes)

            //query another product key
            var compuIntensity: Long = 0L
            if(otherQueryPara == null) compuIntensity = compuIntensityNode.get(processID).asLong()
            else{
              val processNode = compuIntensityNode.get(processID)
              val otherProductsql = new StringBuilder;
              otherProductsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product_aigis_"+cubeId+"\" where 1=1 "
              if (otherP.getVectorProductName != "") {
                otherProductsql ++= "AND product_name ="
                otherProductsql ++= "\'"
                otherProductsql ++= otherP.getVectorProductName
                otherProductsql ++= "\'"
              }
              if (otherP.getCRS != "") {
                otherProductsql ++= "AND crs ="
                otherProductsql ++= "\'"
                otherProductsql ++= otherP.getCRS
                otherProductsql ++= "\'"
              }
              if (otherP.getStartTime != "" && otherP.getEndTime != "") {
                otherProductsql ++= " AND (phenomenon_time BETWEEN "
                otherProductsql ++= "\'"
                otherProductsql ++= otherP.getStartTime
                otherProductsql ++= "\'"
                otherProductsql ++= " AND"
                otherProductsql ++= "\'"
                otherProductsql ++= otherP.getEndTime
                otherProductsql ++= "\')"
              }

              val otherProductResults = statement.executeQuery(otherProductsql.toString())
              val otherProductKeys = new ArrayBuffer[String]()
              if (otherProductResults.first()) {
                otherProductResults.previous()
                while (otherProductResults.next) {
                  otherProductKeys.append(otherProductResults.getString(1))
                }
                if(otherProductKeys.length > 1) throw new RuntimeException("Can only exists one another product")
              } else {
                throw new RuntimeException("Another product doesn't exists with $otherP query parameters!")
              }

              //get computational intensity by another product key
              val productKey2 = otherProductKeys(0)
              compuIntensity = processNode.get(productKey2).asLong()
            }

            results.append((SpatialKey(col, row), (geoObjectKeys, compuIntensity)))
          }
          conn.close()
          results.iterator
        }.cache()

        gridLayerGeoObjectKeysAndCompuIntensityRdd
      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("query")
      .setMaster("local[8]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    val queryParams = new QueryParams
    queryParams.setVectorProductNames(Array("Hainan_Daguangba_ST_Vector"))
    queryParams.setExtent(107.4497916000, 17.6720266000, 111.5907298000, 21.3226934000)
    queryParams.setTime("2016-07-01 02:30:59.415", "2016-07-03 02:30:59.41")

    val featureRDD: FeatureRDD = getFeatures(sc, queryParams, duplicated = false)
  }

}
