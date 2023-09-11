package whu.edu.cn.geocube.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import geotrellis.layer.LayoutDefinition
import geotrellis.raster.TileLayout
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureIterator
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.geocube.core.entity.{GcDimension, VectorGridFact}
import whu.edu.cn.geocube.core.vector.grid.GridTransformer
import whu.edu.cn.util.PostgresqlUtil

import java.io.File
import java.nio.charset.Charset
import java.sql.{DriverManager, ResultSet, SQLException, Statement}
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PostgresqlService {
  /**
   * Get cell size and resolution info from SensorLevelAndProduct view, and return a HashMap object.
   *
   * @param cubeId
   * @param connAddr
   * @param user
   * @param password
   * @return a HashMap object.
   */
  def getSizeResAndExtentByCubeId(cubeId: String, connAddr: String, user: String, password: String): mutable.HashMap[(Double, Double), (Double, Double, Double, Double)] = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select distinct cell_size,cell_res,slice_minx,slice_miny,slice_maxx,slice_maxy " +
          "from gc_cube where id = " + cubeId + ";"
        println(sql)
        val rs = statement.executeQuery(sql)
        val resHashMap = new mutable.HashMap[(Double, Double), (Double, Double, Double, Double)]
        //add every measurementName
        while (rs.next) {
          resHashMap.put((rs.getString(1).toDouble, rs.getDouble(2)), (rs.getDouble(3), rs.getDouble(4), rs.getDouble(5), rs.getDouble(6)))
        }
        resHashMap
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }

  /**
   * Get the hbase table name by the cube id
   *
   * @param cubeId :String the cube id
   * @return the hbase table name
   */
  def getHbaseTableName(cubeId: String): String = {
    val sql = "SELECT hbase_table_name from gc_cube where id = " + cubeId + ";"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var hbaseTableName: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        hbaseTableName = resultSet.getString("hbase_table_name")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    hbaseTableName
  }

  /**
   * get the max and min time of the selected tiles
   *
   * @param cubeId          the cube id
   * @param productKeyArray productKeys
   * @return (minTime, maxTime)
   */
  def getMaxMinTime(cubeId: String, productKeyArray: ArrayBuffer[String]): (String, String) = {
    val sql = "select MIN(phenomenon_time) AS min_time, MAX(phenomenon_time) AS max_time " +
      "from gc_product_" + cubeId + " where product_key IN ( " + productKeyArray.mkString(",") + " )"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var minTime: String = ""
    var maxTime: String = ""
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        if (resultSet.getString("min_time") != null){
          minTime = resultSet.getString("min_time")
        }
        if (resultSet.getString("max_time") != null){
          maxTime = resultSet.getString("max_time")
        }
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    (minTime, maxTime)
  }

  def getOtherDimensions(cubeId: String, productKeys: String = null): ArrayBuffer[GcDimension] = {
    val sql = "select id,dimension_name,dimension_table_name,member_type,step,description,unit,dimension_table_column_name " +
      "from gc_dimension_" + cubeId + " where dimension_name NOT IN ('extent', 'product', 'phenomenonTime', 'measurement')"
    val postgresqlUtil = new PostgresqlUtil(sql)
    val gcDimensionArray: ArrayBuffer[GcDimension] = new ArrayBuffer[GcDimension]()
    val rsArray = new Array[String](8);
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      val columnCount = resultSet.getMetaData.getColumnCount;
      while (resultSet.next()) {
        for (i <- 1 to columnCount) {
          rsArray(i - 1) = resultSet.getString(i)
        }
        val gcDimension: GcDimension = new GcDimension()
        gcDimension.setId(rsArray(0).toInt)
        gcDimension.setDimensionName(rsArray(1))
        gcDimension.setDimensionTableName(rsArray(2))
        gcDimension.setMemberType(rsArray(3))
        if (rsArray(4) != null) {
          gcDimension.setStep(rsArray(4).toDouble)
        }
        gcDimension.setDescription(rsArray(5))
        if (rsArray(6) != null) {
          gcDimension.setUnit(rsArray(6))
        }
        gcDimension.setDimensionTableColumnName(rsArray(7))
        gcDimensionArray.append(gcDimension)
        val coordinates: ArrayBuffer[String] = getDimensionCoordinates(gcDimension, cubeId, productKeys)
        gcDimension.coordinates = coordinates
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    gcDimensionArray
  }


  def getDimensionCoordinates(gcDimension: GcDimension, cubeId: String, productKeys: String = null): ArrayBuffer[String] = {

    val sql = "select distinct " + gcDimension.getDimensionName +
      " from \"SensorLevelAndProduct_" + cubeId + "\" where product_key IN " + productKeys + "ORDER BY " + gcDimension.getDimensionName
    val postgresqlUtil = new PostgresqlUtil(sql)
    val rsArray = new ArrayBuffer[String]()
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      val columnCount = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        for (i <- 1 to columnCount) {
          rsArray.append(resultSet.getString(i))
        }
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    rsArray
  }

  /**
   * get the cube id by the cube name
   *
   * @param cubeName the cube name
   * @return cube id
   */
  def getCubeIdByCubeName(cubeName: String): String = {
    val sql = "SELECT id from gc_cube where cube_name = '" + cubeName + "';"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var cubeId: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        cubeId = resultSet.getString("id")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    cubeId
  }

  /**
   * get the measurements_product_view_name by the cube name
   *
   * @param cubeName the cube name
   * @return measurements_product_view_name
   */
  def getMeasurementsProductViewName(cubeName: String): String = {
    val sql = "SELECT measurements_product_view_name from gc_cube where cube_name = \'" + cubeName + "\';"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var measurementsProductViewName: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        measurementsProductViewName = resultSet.getString("measurements_product_view_name")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    measurementsProductViewName
  }

  /**
   * get the product name by the cube name
   *
   * @param measurementsProductViewName the measurementProductViewName
   * @return the product name (only one product name)
   */
  def getProductNameByCubeName(measurementsProductViewName: String): String = {
    val sql = "Select DISTINCT product_name from \"" + measurementsProductViewName + "\""
    val postgresqlUtil = new PostgresqlUtil(sql)
    var productName: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        productName = resultSet.getString("product_name")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    productName
  }


  /**
   * get the data type of the cell
   *
   * @param cubeId :string cubeId
   * @return the data type
   */
  def getCellType(cubeId: String, productKey: String): String = {
    val sql = "SELECT dtype from gc_product_measurement_" + cubeId + " where product_key = " + productKey + ";"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var dtype: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        dtype = resultSet.getString("dtype")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    dtype
  }

  def getMinTime(cubeId: String, startTime: String, endTime: String): String = {
    var sql = "SELECT MIN(phenomenon_time) as minTime from \"SensorLevelAndProduct_" + cubeId + "\"" + " where 1 = 1 "
    if (startTime != "" && endTime != "") {
      sql ++= " AND (phenomenon_time BETWEEN "
      sql ++= "\'"
      sql ++= startTime
      sql ++= "\'"
      sql ++= " AND"
      sql ++= "\'"
      sql ++= endTime
      sql ++= "\')"
    }
    sql ++= ";"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var minTime: String = null
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery()
      while (resultSet.next()) {
        minTime = resultSet.getString("minTime")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    minTime
  }

  /**
   * Get maximum value of a column in a table.
   *
   * @param columnName
   * @param tableName
   * @return
   */
  def getMaxValue(columnName: String, tableName: String): Integer = {
    val sql = "SELECT max(" + columnName + ") from " + tableName + ";"
    var maxValue = 0
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next)
        maxValue = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxValue
  }


  /**
   * Get maximum key in product dimension table.
   * */
  def getMaxProductKey: Integer = {
    val sql = "SELECT max(product_key) from gc_product_aigis;"
    var maxProductKey = 0
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next)
        maxProductKey = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxProductKey
  }

  /**
   * Get maximum id in product dimension table.
   * */
  def getMaxProductId: Integer = {
    val sql = "SELECT max(id) from gc_product_aigis;"
    var maxProductId = 0
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next) maxProductId = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxProductId
  }

  /**
   * Get maximum id in product dimension table.
   * */
  def getMaxRasterProductId: Integer = {
    val sql = "SELECT max(id) from gc_product;"
    var maxProductId = 0
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next) maxProductId = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxProductId
  }

  /**
   * Get maximum key in vector fact table.
   * */
  def getMaxVectorFactKey: Integer = {
    val sql = "SELECT max(fact_key) from gc_vector_tile_fact_aigis;"
    val postgresqlUtil = new PostgresqlUtil(sql)
    var maxVectorFactKey = 0
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next) maxVectorFactKey = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxVectorFactKey
  }

  /**
   * Get maximum id in vector fact table.
   * */
  def getMaxVectorFactId: Integer = {
    val sql = "SELECT max(id) from gc_vector_tile_fact_aigis;"
    var maxVectorFactId = 0
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val resultSet = postgresqlUtil.getStatement.executeQuery
      while (resultSet.next) maxVectorFactId = resultSet.getInt("max")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    maxVectorFactId
  }

  /**
   * Insert record to product dimension table.
   *
   * @param productId             Attribute[id] in product dimension table
   * @param productKey            Attribute[product_key] in product dimension table
   * @param productName           Attribute[product_name] in product dimension table
   * @param productIdentification Attribute[product_identification] in product dimension table
   * @param phenomenonTime        Attribute[phenomenon_time] in product dimension table
   * @param resultTime            Attribute[result_time] in product dimension table
   * @param geom                  Attribute[geom] in product dimension table
   * @param maxx                  Attribute[maxx] in product dimension table
   * @param minx                  Attribute[minx] in product dimension table
   * @param maxy                  Attribute[maxy] in product dimension table
   * @param miny                  Attribute[miny] in product dimension table
   * @return True if insert successfully, false otherwise
   */
  def insertProduct(productId: Integer, productKey: Integer, productName: String, productIdentification: String, phenomenonTime: String, resultTime: String, geom: String, maxx: Double, minx: Double, maxy: Double, miny: Double): Boolean = {
    val sql = "INSERT INTO gc_product_aigis(id,product_key, " +
      "product_name, product_identification,product_type,  resolution_key, " +
      "crs,phenomenon_time,result_time,geom,upper_left_lat, upper_left_long, upper_right_lat, upper_right_long, " +
      "lower_left_lat, lower_left_long, lower_right_lat, lower_right_long," +
      "create_by, create_time, update_by, update_time) VALUES (" +
      productId + "," + productKey + ",'" + productName + "','" + productIdentification + "','Vector',999,'WGS84'," +
      "'" + phenomenonTime + "','" + resultTime + "'," + geom + "," + maxy + "," + minx + "," + maxy + "," + maxx + "," + miny + "," + minx + "," + miny + "," + maxx + "," +
      "'admin',current_timestamp,null,null);"
    var insert = false
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val result = postgresqlUtil.getStatement.executeUpdate
      if (result > 0) insert = true
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    insert
  }

  /**
   * Insert record to vector fact table.
   *
   * @param vectorGridFacts A list of VectorGridFact objects
   * @return True if insert successfully, false otherwise
   */
  def insertFact(vectorGridFacts: util.List[VectorGridFact]): Boolean = {
    var sql = "INSERT INTO gc_vector_tile_fact_aigis(id,fact_key, product_key, extent_key, tile_data_id, create_by, create_time, update_by, update_time) VALUES  "
    for (i <- 0 until vectorGridFacts.size) {
      val vectorGridFact = vectorGridFacts.get(i)
      val fact_id = vectorGridFact.getFactId()
      val fact_key = vectorGridFact.getFactKey()
      val product_key = vectorGridFact.getProductKey()
      val extent_key = vectorGridFact.getExtentKey()
      val uuids = vectorGridFact.getUUIDs
      var subString = "  (" + fact_id + "," + fact_key + "," + product_key + "," + extent_key + ",'" + uuids + "','admin',current_timestamp,null,null)"
      if (i < vectorGridFacts.size - 1) subString = subString + ","
      sql = sql + subString
    }
    var insert = false
    val postgresqlUtil = new PostgresqlUtil(sql)
    try {
      val result = postgresqlUtil.getStatement.executeUpdate
      if (result > 0) insert = true
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    postgresqlUtil.close()
    insert
  }

  /**
   * Insert record to vector fact table.
   *
   * @param statement       PostgreSQL statement
   * @param vectorGridFacts A list of VectorGridFact objects
   * @return True if insert successfully, false otherwise
   */
  def insertFact(statement: Statement, vectorGridFacts: util.List[VectorGridFact]): Boolean = {
    var sql = "INSERT INTO gc_vector_tile_fact_aigis(id,fact_key, product_key, extent_key, tile_data_id, create_by, create_time, update_by, update_time) VALUES  "
    for (i <- 0 until vectorGridFacts.size) {
      val vectorGridFact = vectorGridFacts.get(i)
      val fact_id = vectorGridFact.getFactId()
      val fact_key = vectorGridFact.getFactKey()
      val product_key = vectorGridFact.getProductKey()
      val extent_key = vectorGridFact.getExtentKey()
      val uuids = vectorGridFact.getUUIDs
      var subString = "  (" + fact_id + "," + fact_key + "," + product_key + "," + extent_key + ",'" + uuids + "','admin',current_timestamp,null,null)"
      if (i < vectorGridFacts.size - 1) subString = subString + ","
      sql = sql + subString
    }
    var insert = false
    try {
      val result = statement.executeUpdate(sql)
      if (result > 0) insert = true
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    insert
  }

  /**
   * Insert computational intensity info to vector fact table.
   *
   * @param statement          PostgreSQL statement
   * @param factKey            Key in fact table
   * @param compuIntensityJson Computational intensity info of Json format
   * @return True if insert successfully, false otherwise
   */
  def insertCompuIntensity(statement: Statement, factKey: String, compuIntensityJson: ObjectNode): Boolean = {
    //val sql = "UPDATE gc_json_test SET json_test='" + compuIntensityJson.toString + "' WHERE fact_key=" + factKey
    val sql = "UPDATE gc_vector_tile_fact_aigis SET compu_intensity='" + compuIntensityJson.toString + "' WHERE fact_key=" + factKey
    var insert = false
    try {
      val result = statement.executeUpdate(sql)
      if (result > 0) insert = true
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    insert
  }

  /**
   * deprecated
   * */
  @deprecated
  def getCompuIntensity(statement: Statement, factKey1: String, factKey2: String): Long = {
    //val sql = "select json_test from gc_json_test where fact_key=" + factKey1 + ";"
    val sql = "select compu_intensity from gc_vector_tile_fact_aigis where fact_key=" + factKey1 + ";"
    var compuIntensity = -1L
    try {
      val resultSet = statement.executeQuery(sql)
      if (resultSet.first()) {
        resultSet.previous()
        while (resultSet.next()) {
          val jsonString = resultSet.getString(1)
          val objectMapper = new ObjectMapper
          val node = objectMapper.readTree(jsonString.getBytes)
          val processNode = node.get("intersection")
          compuIntensity = processNode.get(factKey2).longValue()
        }
      } else {
        throw new RuntimeException("Nothing returned!")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    compuIntensity
  }

  /**
   * Insert city into gc_extent table
   *
   * @param cityName
   * @param boundaryFile
   */
  def insertCityName(cityName: String, boundaryFile: String): Unit = {
    //get grid covered by the city
    val cityGeometry: Geometry = getCityGeometry(cityName, boundaryFile)
    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tl = TileLayout(360, 180, 4000, 4000)
    val ld = LayoutDefinition(extent, tl)
    val gridCodes = GridTransformer.getGeomZcodes(cityGeometry.getEnvelope, ld.layoutCols, ld.layoutRows, ld.extent, 1).toArray
    gridCodes.foreach(x => print(x + " "));
    println()

    //get extentKey corresponding to the grid
    val connStr = "jdbc:postgresql://125.220.153.26:5432/whugeocube"
    val conn = DriverManager.getConnection(connStr, "geocube", "ypfamily608")
    val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    val filteredExtents = getExtentByGridCodes(statement, gridCodes, "4000") //return extentKey and extent string
    filteredExtents.foreach(x => println(x._1 + "," + x._2 + "," + x._3))

    //update these extentKey with cityName
    filteredExtents.foreach { x =>
      val extentKey = x._1
      val sql = "UPDATE gc_extent SET city_name='" + cityName + "' WHERE extent_key=" + extentKey
      try {
        val result = statement.executeUpdate(sql)
        if (!(result > 0)) throw new RuntimeException(extentKey + " inserted to gc_extent failed")
      } catch {
        case e: SQLException =>
          e.printStackTrace()
      }
    }
    println("exit")
  }

  /**
   * Get city geometry
   *
   * @param cityName
   * @param boundaryFile
   * @return
   */
  def getCityGeometry(cityName: String, boundaryFile: String): Geometry = {
    val dataStoreFactory = new ShapefileDataStoreFactory()
    val sds = dataStoreFactory.createDataStore(new File(boundaryFile).toURI.toURL)
      .asInstanceOf[ShapefileDataStore]
    sds.setCharset(Charset.forName("GBK"))
    val featureSource = sds.getFeatureSource()
    val iterator: SimpleFeatureIterator = featureSource.getFeatures().features()
    while (iterator.hasNext) {
      val feature = iterator.next()
      if (feature.getAttribute("NAME_2").equals(cityName)) {
        val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
        sds.dispose()
        return geometry
      }
    }
    sds.dispose()
    throw new RuntimeException(cityName + " was not found in the boundary file")
  }

  /**
   *
   * @param statement
   * @param gridCodes
   * @param level
   * @return
   */
  def getExtentByGridCodes(statement: Statement, gridCodes: Array[String], level: String): Array[(String, String, String)] = {
    val extentsql = new StringBuilder
    extentsql ++= "Select extent_key,extent,grid_code from \"LevelAndExtent\" where 1=1 "
    if (gridCodes.length != 0) {
      extentsql ++= "AND grid_code IN ("
      for (grid <- gridCodes) {
        extentsql ++= "\'"
        extentsql ++= grid
        extentsql ++= "\',"
      }
      extentsql.deleteCharAt(extentsql.length - 1)
      extentsql ++= ")"
      extentsql ++= "AND level ="
      extentsql ++= "\'"
      extentsql ++= level
      extentsql ++= "\'"
    }
    val extentResults = statement.executeQuery(extentsql.toString())
    val extentKeys = new ArrayBuffer[(String, String, String)]()
    if (extentResults.first()) {
      extentResults.previous()
      while (extentResults.next) {
        extentKeys.append((extentResults.getString(1), extentResults.getString(2), extentResults.getString(3)))
      }
      extentKeys.toArray
    } else null
  }

  /**
   * Insert month and year to gc_product table
   */
  def insertMonthYear(): Unit = {
    val connStr = "jdbc:postgresql://125.220.153.26:5432/whugeocube"
    val conn = DriverManager.getConnection(connStr, "geocube", "ypfamily608")
    val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    val sql = new StringBuilder
    sql ++= "Select product_key,phenomenon_time from gc_product;"
    val results = statement.executeQuery(sql.toString())
    if (results.first()) {
      results.previous()
      while (results.next) {
        val productKey = results.getString(1)
        val phenomenonTime = results.getString(2).split(" ")(0)
        val month = phenomenonTime.split("-")(1).toInt
        val year = phenomenonTime.split("-")(0).toInt
        println(year + "-" + month)
        val insertSql = "UPDATE gc_product SET phenomenon_time_month=" + month + ", phenomenon_time_year=" + year + " WHERE product_key=" + productKey
        println(insertSql)
        try {
          val insertStatement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val result = insertStatement.executeUpdate(insertSql)
          if (!(result > 0)) throw new RuntimeException(productKey + " inserted to gc_product failed")
        } catch {
          case e: SQLException =>
            e.printStackTrace()
        }
      }
    }

  }

  def updatePath(url: String, user: String, passwd: String): Unit = {
    val conn = DriverManager.getConnection(url, user, passwd)
    val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    val sql = new StringBuilder
    sql ++= "Select id,data_path,meta_path from gc_pointer;"
    val results = statement.executeQuery(sql.toString())
    if (results.first()) {
      results.previous()
      while (results.next) {
        val id = results.getString(1).toInt
        val dataPath = results.getString(2)
        val metaPath = results.getString(3)
        val updateDataPath = dataPath.replace("/home/geocube/environment_test/geocube_core_jar", "/home/geocube/kernel/geocube-core/v2")
        val updateMetaPath = metaPath.replace("/home/geocube/environment_test/geocube_core_jar", "/home/geocube/kernel/geocube-core/v2")
        val insertSql = "UPDATE gc_pointer SET data_path='" + updateDataPath + "', meta_path='" + updateMetaPath + "' WHERE id=" + id
        println(insertSql)
        try {
          val insertStatement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val result = insertStatement.executeUpdate(insertSql)
          if (!(result > 0)) throw new RuntimeException(id + " updated to gc_product failed")
        } catch {
          case e: SQLException =>
            e.printStackTrace()
        }

      }
    }
  }


}
