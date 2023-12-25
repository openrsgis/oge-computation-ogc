package whu.edu.cn.geocube.core.vector.query

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{JsonObject, JsonParser}
import geotrellis.layer.SpaceTimeKey
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.opengis.feature.simple.SimpleFeature
import whu.edu.cn.geocube.core.cube.vector.GeoObject
import whu.edu.cn.geocube.core.entity.QueryParams
import whu.edu.cn.geocube.util.HbaseUtil.getVectorMeta
import whu.edu.cn.util.PostgresqlUtil

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ArrayBuffer

/**
 * Query vector data in a serial way.
 * Return logical vector tiles with an Array.
 *
 */
object QueryVectorObjects {
  /**
   * Query vector data and return (Array[(SpaceTimeKey, Iterable[GeoObject])]).
   *
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded based on {@param principle}.
   * @param principle "simple": duplicated simply,
   *                  "area": duplicated based on the area of intersection.
   *                  Default is simple.
   * @return an Array[(SpaceTimeKey, Iterable[GeoObject])]
   * */
  def getGeoObjectsArray(p: QueryParams, duplicated:Boolean = true, principle: String = "simple"): Array[(SpaceTimeKey, Iterable[GeoObject])] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of vectors is being processed...")

    val queryBegin = System.currentTimeMillis()
    if (p.getVectorProductNames.length == 0 || p.getVectorProductNames.length == 1) {
      if(p.getVectorProductNames.length == 1) p.setVectorProductName(p.getVectorProductNames(0))
      val gridLayerGeoObjectArray = getGridLayerGeoObjectsArray(p, duplicated, principle)
      val queryEnd = System.currentTimeMillis()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + gridLayerGeoObjectArray.map(_._2.size).sum + " vectors: " + (queryEnd - queryBegin) + " ms")
      gridLayerGeoObjectArray
    } else {
      val multiProductGridLayerGeoObjectArray = ArrayBuffer[Array[(SpaceTimeKey, Iterable[GeoObject])]]()
      for (vectorProductName <- p.getVectorProductNames) {
        p.setVectorProductName(vectorProductName)
        val results = getGridLayerGeoObjectsArray(p, duplicated, principle)
        if (results != null)
          multiProductGridLayerGeoObjectArray.append(results)
      }
      if (multiProductGridLayerGeoObjectArray.length < 1) throw new RuntimeException("No vectors with the query condition!")
      val gridLayerGeoObjectArray = multiProductGridLayerGeoObjectArray.flatten.groupBy(_._1).toArray.map { x =>
        val geoObjects: ArrayBuffer[GeoObject] = x._2.map(_._2).flatten
        (x._1, geoObjects.toIterable)
      }
      val queryEnd = System.currentTimeMillis()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + gridLayerGeoObjectArray.map(_._2.size).sum + " vectors: " + (queryEnd - queryBegin) + " ms")
      gridLayerGeoObjectArray
    }
  }

  /**
   * Query vector data and return (Array[(SpaceTimeKey, Iterable[GeoObject])]).
   *
   * @param p query params
   * @param duplicated whether allow duplicated objects, default is true,
   *                   if false, duplicated objects will be discarded based on {@param principle}.
   * @param principle "simple": duplicated simply,
   *                  "area": duplicated based on the area of intersection.
   *                  Default is simple.
   * @return an Array[(SpaceTimeKey, Iterable[GeoObject])]
   * */
  def getGridLayerGeoObjectsArray(p: QueryParams, duplicated: Boolean, principle: String = "simple"): Array[(SpaceTimeKey, Iterable[GeoObject])] = {
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

        //access vectors in HBase in a serial way
        val gridLayerGeoObjectArray: ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])] = new ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])]()
        val fjson = new FeatureJSON()
        val mutableSet = scala.collection.mutable.Set("")
        geoObjectsAndDimensionKeys.foreach { keys =>
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

          //discarded duplicated vectors based on $principle if $duplicated is false
          val geoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
          if(duplicated){
            geoObjectKeys.foreach { geoObjectkey =>
              var featureStr = ""
              if (cubeId == "27") {
                featureStr = getVectorMeta("hbase_vector", geoObjectkey, "vectorData", "metaData")
              } else {
                featureStr = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "metaData")
              }
              val feature: SimpleFeature = fjson.readFeature(featureStr)
              val geoObject = new GeoObject(geoObjectkey, feature) //一个geoObject和多个网格相交，则该geoObject在多个网格中的geoObjectkey相等,但具有不同的(SpaceTimeKey(col, row, time)
              geoObjects.append(geoObject)
            }
          }else if(!duplicated && principle.equals("simple")){
            geoObjectKeys.foreach { geoObjectkey =>
              if(!mutableSet.contains(geoObjectkey)){
                mutableSet.add(geoObjectkey)
                val featureStr = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "metaData")
                val feature: SimpleFeature = fjson.readFeature(featureStr)
                val geoObject = new GeoObject(geoObjectkey, feature) //一个geoObject和多个网格相交，则该geoObject在多个网格中的geoObjectkey相等,但具有不同的(SpaceTimeKey(col, row, time)
                geoObjects.append(geoObject)
              }
            }
          }else if(!duplicated && principle.equals("area")){
            geoObjectKeys.foreach { geoObjectkey =>
              if(!mutableSet.contains(geoObjectkey)){
                val featureStr = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "metaData")
                val feature: SimpleFeature = fjson.readFeature(featureStr)
                val geometry: Geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
                if(geometry.isValid){
                  val tilesMetaData = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "tilesMetaData")
                  val json = new JsonParser()
                  val objArray = json.parse(tilesMetaData).getAsJsonArray
                  var (currentCol, currentRow) = (-1, -1)
                  var intersectedArea = Double.MinValue
                  for(i <- 0 until objArray.size()){
                    val extent = json.parse(objArray.get(i).getAsJsonObject.get("extent").toString).getAsJsonObject
                    val leftBottomLong = extent.get("leftBottomLong").toString.toDouble
                    val leftBottomLat = extent.get("leftBottomLat").toString.toDouble
                    val rightUpperLong = extent.get("rightUpperLong").toString.toDouble
                    val rightUpperLat = extent.get("rightUpperLat").toString.toDouble
                    import org.locationtech.jts.geom.Coordinate
                    val grid = new GeometryFactory().createPolygon(Array[Coordinate](
                      new Coordinate(leftBottomLong, leftBottomLat),
                      new Coordinate(leftBottomLong, rightUpperLat),
                      new Coordinate(rightUpperLong, rightUpperLat),
                      new Coordinate(rightUpperLong, leftBottomLat),
                      new Coordinate(leftBottomLong, leftBottomLat)))
                    if(geometry.getEnvelopeInternal.intersects(grid.getEnvelopeInternal)){
                      val currentIntersectedArea = geometry.intersection(grid.asInstanceOf[Geometry]).getArea
                      if(currentIntersectedArea > intersectedArea) {
                        intersectedArea = currentIntersectedArea
                        currentCol = extent.get("column").toString.toInt
                        currentRow = extent.get("row").toString.toInt
                      }
                    }
                  }
                  if(currentCol == col && currentRow == row){
                    val geoObject = new GeoObject(geoObjectkey, feature) //一个geoObject和多个网格相交，则该geoObject在多个网格中的geoObjectkey相等,但具有不同的(SpaceTimeKey(col, row, time)
                    geoObjects.append(geoObject)
                    mutableSet.add(geoObjectkey)
                  }
                }
              }
            }
          }

          gridLayerGeoObjectArray.append((SpaceTimeKey(col, row, time), geoObjects))
        }
        var geoObjectsNum = 0
        gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Get " + geoObjectsNum + " geoObjects of " + vectorProductName + " product successfully")
        gridLayerGeoObjectArray.toArray
      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }


  /**
   * Under developing.
   *
   * Query vector data and return java.util.List[GeoJson].
   *
   * @param p query params
   *
   * @return a java.util.List[GeoJson]
   * */
  def getGeoJsonsArray(p:QueryParams):java.util.List[String]={
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
      try{
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        //extent dimension
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
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\')"
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

        //access vector data in HBase
        val VectorGeoJsonArray: ArrayBuffer[String] = new ArrayBuffer[String]()
        val fjson = new FeatureJSON()
        geoObjectsAndDimensionKeys.foreach{ keys =>
          println(keys(0))
          val listString = keys(0)
          val geoObjectKeys = listString.substring(listString.indexOf("(")+1,listString.indexOf(")")).split(", ")

          val productKey = keys(1)
          val sql = "select phenomenon_time from gc_product_"+cubeId+" where product_key=" + productKey + ";"
          val rs = statement.executeQuery(sql)

          var time:Long ="20200101".toLong
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          if (rs.first()) {
            rs.previous()
            while (rs.next()) {
              time = sdf.parse(rs.getString(1)).getTime
            }
          } else {
            println("No vector time acquired!")
          }
          val col_row =getVectorMeta("hbase_vector_"+cubeId, geoObjectKeys(0), "vectorData", "tilesMetaData").dropRight(1).substring(1)
          val json = new JsonParser()
          val obj = json.parse(col_row).asInstanceOf[JsonObject]
          val extent = json.parse(obj.get("extent").toString).asInstanceOf[JsonObject]
          val (col, row) = (extent.get("column").toString.toInt, extent.get("row").toString.toInt)
          geoObjectKeys.foreach{geoObjectkey =>
            println(geoObjectkey)
            val metaJson = getVectorMeta("hbase_vector_"+cubeId, geoObjectkey, "vectorData", "metaData")
            val Json = json.parse(metaJson).asInstanceOf[JsonObject]
            VectorGeoJsonArray.append(metaJson)
          }
        }
        bufferAsJavaList(VectorGeoJsonArray)

      }finally
        conn.close()
    }else
      throw new RuntimeException("connection failed")
  }

  /**
   * Under developing.
   * Get attributes list of queried vector.
   *
   * @param p
   * @return a combination string of attributes
   */
  def getVectorAttributes(p: QueryParams): String = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    val cubeId = p.getCubeId
    //Product params
    val vectorProductName = p.getVectorProductName

    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //Spatial params
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


        //Product dimension
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

        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact_"+cubeId+" where  product_key IN" + productKeys.toString() + ";"
        println("Vector Fact Query SQL:" + command)

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

        val Buffer:StringBuilder=new StringBuilder
        val keys=geoObjectsAndDimensionKeys.head
        println(keys(0))
        val listString = keys(0)
        val geoObjectKeys = listString.substring(listString.indexOf("(") + 1, listString.indexOf(")")).split(", ")


        val meta = getVectorMeta("hbase_vector_"+cubeId, geoObjectKeys(0), "vectorData", "metaData")
        val json = new JsonParser()
        val obj = json.parse(meta).asInstanceOf[JsonObject]

        val properties = json.parse(obj.get("properties").toString).asInstanceOf[JsonObject]
        val iter = properties.entrySet().iterator()
        while (iter.hasNext){
          Buffer.append(iter.next().getKey).append("|")
        }

        Buffer.toString()

      } finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
    val queryParams = new QueryParams
    queryParams.setVectorProductNames(Array("Hainan_Daguangba_ST_Vector"))
    queryParams.setExtent(107.4497916000, 17.6720266000, 111.5907298000, 21.3226934000)
    queryParams.setTime("2016-07-01 02:30:59.415", "2016-07-03 02:30:59.41")
    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = getGeoObjectsArray(queryParams, duplicated = false, principle = "simple")
  }
}
