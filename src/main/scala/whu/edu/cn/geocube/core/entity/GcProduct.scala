package whu.edu.cn.geocube.core.entity

import whu.edu.cn.config.GlobalConfig.PostgreSqlConf.POSTGRESQL_DRIVER
import whu.edu.cn.util.PostgresqlUtil

import java.sql.{Connection, DriverManager, ResultSet}
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer


/**
 * Dimension class - product.
 *
 */
case class GcProduct() {
  @BeanProperty
  var productKey: String = ""
  @BeanProperty
  var productName: String = ""
  @BeanProperty
  var platform: String = ""
  @BeanProperty
  var instrument: String = ""
  @BeanProperty
  var CRS: String = ""
  @BeanProperty
  var tileSize: String = ""
  @BeanProperty
  var cellRes: String = ""
  @BeanProperty
  var level: String = ""
  @BeanProperty
  var phenomenonTime: String = ""
  @BeanProperty
  var height: String = ""
  @BeanProperty
  var width: String = ""
  @BeanProperty
  var measurements: ArrayBuffer[GcMeasurement] = new ArrayBuffer[GcMeasurement]()
  @BeanProperty
  var upperLeftLat: String = ""
  @BeanProperty
  var upperLeftLong: String = ""
  @BeanProperty
  var upperRightLat: String = ""
  @BeanProperty
  var upperRightLong: String = ""
  @BeanProperty
  var lowerLeftLat: String = ""
  @BeanProperty
  var lowerLeftLong: String = ""
  @BeanProperty
  var lowerRightLat: String = ""
  @BeanProperty
  var lowerRightLong: String = ""
  @BeanProperty
  var otherDimensions: ArrayBuffer[GcDimension] = new ArrayBuffer[GcDimension]()

//  def setOtherDimensions(otherDimensions: ArrayBuffer[GcDimension]): Unit = {
//    this.otherDimensions = otherDimensions
//  }
//
//  def getOtherDimensions: ArrayBuffer[GcDimension] = {
//    this.otherDimensions
//  }

  def this(_productKey: String = "", _productName: String = "", _platform: String = "",
           _instrument: String = "", _CRS: String = "", _tileSize: String = "",
           _cellRes: String = "", _level: String = "", _phenomenonTime: String = "",
           _height: String = "", _width: String = "") {
    this()
    productKey = _productKey
    productName = _productName
    platform = _platform
    instrument = _instrument
    CRS = _CRS
    tileSize = _tileSize
    cellRes = _cellRes
    level = _level
    phenomenonTime = _phenomenonTime
    height = _height
    width = _width
  }

  def getMeasurementsArray: Array[GcMeasurement] = measurements.toArray
}

object GcProduct {

  /**
   *
   * @param gcDimension GcDimension instance
   * @param cubeId      the cube id
   * @param connAddr    the connect address
   * @param user        the user id
   * @param password    the password
   * @return the all coordinates of the dimension
   */
  def getDimensionCoordinates(gcDimension: GcDimension, cubeId: String, connAddr: String, user: String, password: String, productKeys: String = null): ArrayBuffer[String] = {
//    PostgresqlUtil.get()
//    Class.forName(PostgresqlUtil.driver)
//    val conn = DriverManager.getConnection(connAddr, user, password)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    val rsArray = new ArrayBuffer[String]()
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select distinct " + gcDimension.getDimensionName +
          " from \"SensorLevelAndProduct_" + cubeId + "\" where product_key IN " + productKeys + "ORDER BY " + gcDimension.getDimensionName
        val rs = statement.executeQuery(sql)
        val columnCount = rs.getMetaData().getColumnCount();
        while (rs.next) {
          for (i <- 1 to columnCount) {
            rsArray.append(rs.getString(i))
          }
        }
      } finally {
        conn.close
      }
    }
    rsArray
  }

  /**
   *
   * @param cubeId   the cube id
   * @param connAddr the connect address
   * @param user     the user id
   * @param password the password
   * @return get all dimensions
   */
  def getOtherDimensions(cubeId: String, connAddr: String, user: String, password: String, productKeys: String = null): ArrayBuffer[GcDimension] = {
//    PostgresqlUtil.get()
//    Class.forName(PostgresqlUtil.driver)
//    val conn = DriverManager.getConnection(connAddr, user, password)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    val gcDimensionArray: ArrayBuffer[GcDimension] = new ArrayBuffer[GcDimension]()
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select id,dimension_name,dimension_table_name,member_type,step,description,unit,dimension_table_column_name " +
          "from gc_dimension_" + cubeId + " where dimension_name NOT IN ('extent', 'product', 'phenomenonTime', 'measurement')"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](8);
        val columnCount = rs.getMetaData().getColumnCount();
        while (rs.next) {
          for (i <- 1 to columnCount) {
            rsArray(i - 1) = rs.getString(i)
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
          val coordinates: ArrayBuffer[String] = getDimensionCoordinates(gcDimension, cubeId, connAddr, user, password, productKeys)
          gcDimension.coordinates = coordinates
        }

      } finally {
        conn.close
      }
    }
    gcDimensionArray
  }

  /**
   * Get product info using productKey, and return a GcProduct object.
   *
   * @param cubeId
   * @param productKey
   * @param connAddr
   * @param user
   * @param password
   * @return a GcProduct object
   */
  def getProductByKey(cubeId: String, productKey: String, connAddr: String, user: String, password: String,
                      gcDimensionArray: ArrayBuffer[GcDimension] = new ArrayBuffer[GcDimension]()): GcProduct = {
//    Class.forName("org.postgresql.Driver")
//    val conn = DriverManager.getConnection(connAddr, user, password)
    Class.forName(POSTGRESQL_DRIVER)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        //        val gcDimensionArray = getOtherDimensions(cubeId, connAddr, user, password)
        var sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width"

        if (gcDimensionArray.nonEmpty) {
          for (gcDimension <- gcDimensionArray) {
            sql = sql + "," + gcDimension.getDimensionName
          }
        }
        sql = sql + " from \"SensorLevelAndProduct_" + cubeId + "\" where product_key=" + productKey + ";"
        //        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width " +
        //          "from \"SensorLevelAndProduct_" + cubeId + "\" where product_key=" + productKey + ";"
//        println(sql.toString)
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](11 + gcDimensionArray.size);
        val columnCount = rs.getMetaData().getColumnCount();
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
        }

        val productID = rsArray(0)
        val productName = rsArray(1)
        val productPlatform = rsArray(2)
        val productInstrument = rsArray(3)
        val productCRS = rsArray(6)
        val productTileSize = rsArray(4)
        val productRes = rsArray(5)
        val productLevel = rsArray(7)
        val productPhenomenonTime = rsArray(8)
        val productHeight = rsArray(9)
        val productWidth = rsArray(10)
        val product = new GcProduct(productID, productName, productPlatform, productInstrument, productCRS,
          productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)
        val otherDimension: ArrayBuffer[GcDimension] = new ArrayBuffer[GcDimension]()
        if (gcDimensionArray.nonEmpty) {
          var index = 1;
          for (gcDimension <- gcDimensionArray) {
            if (rsArray(10 + index) != null) {
              gcDimension.setValue(rsArray(10 + index))
              gcDimension.convertDataType
              otherDimension.append(gcDimension)
              index = index + 1
            }
          }
        }
        product.setOtherDimensions(otherDimension)
        val sql2 = "select measurement_key, measurement_name, dtype " +
          "from \"MeasurementsAndProduct_" + cubeId + "\" where product_key=" + productKey + ";"

        val rs2 = statement.executeQuery(sql2)
        val measurementlist = new ArrayBuffer[GcMeasurement]()
        while (rs2.next) {
          val measurement = new GcMeasurement()
          measurement.setMeasurementKey(rs2.getString(1))
          measurement.setMeasurementName(rs2.getString(2))
          measurement.setMeasurementDType(rs2.getString(3))
          measurementlist += measurement
        }
        product.setMeasurements(measurementlist)
        product
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  /**
   * Get product info using productName, may return multiple GcProduct objects.
   *
   * @param cubeId
   * @param productName
   * @param connAddr
   * @param user
   * @param password
   * @return multiple GcProduct objects
   */
  def getProductByName(cubeId: String, productName: String, connAddr: String, user: String, password: String): ArrayBuffer[GcProduct] = {
    val productArray = new ArrayBuffer[GcProduct]()
    Class.forName(POSTGRESQL_DRIVER)
//    Class.forName("org.postgresql.Driver")
//    val conn = DriverManager.getConnection(connAddr, user, password)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width " +
          "from \"SensorLevelAndProduct_" + cubeId + "\" where product_name=\'" + productName + "\';"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](11);
        val columnCount = rs.getMetaData().getColumnCount();

        println("Queried products:")
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
          val productID = rsArray(0)
          val productPlatform = rsArray(2)
          val productInstrument = rsArray(3)
          val productCRS = rsArray(6)
          val productTileSize = rsArray(4)
          val productRes = rsArray(5)
          val productLevel = rsArray(7)
          val productPhenomenonTime = rsArray(8)
          val productHeight = rsArray(9)
          val productWidth = rsArray(10)

          println("productID/Key = " + productID,
            "productName = " + productName,
            "productPlatform = " + productPlatform,
            "productInstrument = " + productInstrument,
            "productCRS = " + productCRS,
            "productTileSize = " + productTileSize,
            "productRes = " + productRes,
            "productLevel = " + productLevel,
            "productPhenomenonTime = " + productPhenomenonTime,
            "productHeight = " + productHeight,
            "productWidth = " + productWidth)

          val product = new GcProduct(productID, productName, productPlatform, productInstrument, productCRS,
            productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)

          val sql2 = "select measurement_key, measurement_name, dtype " +
            "from \"MeasurementsAndProduct_" + cubeId + "\" where product_key=" + productID + ";"
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val rs2 = statement.executeQuery(sql2)
          val measurementlist = new ArrayBuffer[GcMeasurement]()
          while (rs2.next) {
            val measurement = new GcMeasurement()
            measurement.setMeasurementKey(rs2.getString(1))
            measurement.setMeasurementName(rs2.getString(2))
            measurement.setMeasurementDType(rs2.getString(3))
            measurementlist += measurement
          }
          product.setMeasurements(measurementlist)
          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  /**
   * Get all products, return multiple GcProduct objects
   *
   * @param cubeId
   * @param connAddr
   * @param user
   * @param password
   * @return multiple GcProduct objects
   */
  def getAllProducts(cubeId: String, connAddr: String, user: String, password: String): ArrayBuffer[GcProduct] = {
    val productArray = new ArrayBuffer[GcProduct]()
//    PostgresqlUtil.get()
//    Class.forName(PostgresqlUtil.driver)
//    val conn = DriverManager.getConnection(connAddr, user, password)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width," +
          "upper_left_lat,upper_left_long,upper_right_lat,upper_right_long,lower_left_lat,lower_left_long,lower_right_lat,lower_right_long " +
          "from \"SensorLevelAndProduct_" + cubeId + "\" " + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](19);
        val columnCount = rs.getMetaData().getColumnCount();

        println("Queried products:")
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
          val productID = rsArray(0)
          val productName = rsArray(1)
          val productPlatform = rsArray(2)
          val productInstrument = rsArray(3)
          val productCRS = rsArray(6)
          val productTileSize = rsArray(4)
          val productRes = rsArray(5)
          val productLevel = rsArray(7)
          val productPhenomenonTime = rsArray(8)
          val productHeight = rsArray(9)
          val productWidth = rsArray(10)
          val upperLeftLat = rsArray(11)
          val upperLeftLong = rsArray(12)
          val upperRightLat = rsArray(13)
          val upperRightLong = rsArray(14)
          val lowerLeftLat = rsArray(15)
          val lowerLeftLong = rsArray(16)
          val lowerRightLat = rsArray(17)
          val lowerRightLong = rsArray(18)

          println("productID/Key = " + productID,
            "productName = " + productName,
            "productPlatform = " + productPlatform,
            "productInstrument = " + productInstrument,
            "productCRS = " + productCRS,
            "productTileSize = " + productTileSize,
            "productRes = " + productRes,
            "productLevel = " + productLevel,
            "productPhenomenonTime = " + productPhenomenonTime,
            "productHeight = " + productHeight,
            "productWidth = " + productWidth,
            "upperLeftLat/Long = " + (upperLeftLat, upperLeftLong),
            "upperRightLat/Long = " + (upperRightLat, upperRightLong),
            "lowerLeftLat/Long = " + (lowerLeftLat, lowerLeftLong),
            "lowerRightLat/Long = " + (lowerRightLat, lowerRightLong))

          val product = new GcProduct(productID, productName, productPlatform, productInstrument, productCRS,
            productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)
          product.setLowerLeftLat(lowerLeftLat)
          product.setLowerLeftLong(lowerLeftLong)
          product.setLowerRightLat(lowerRightLat)
          product.setLowerRightLong(lowerRightLong)
          product.setUpperLeftLat(upperLeftLat)
          product.setUpperLeftLong(upperLeftLong)
          product.setUpperRightLat(upperRightLat)
          product.setUpperRightLong(upperRightLong)

          val sql2 = "select measurement_key, measurement_name, dtype " +
            "from \"MeasurementsAndProduct_" + cubeId + "\" where product_key=" + productID + ";"
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val rs2 = statement.executeQuery(sql2)
          val measurementlist = new ArrayBuffer[GcMeasurement]()
          while (rs2.next) {
            val measurement = new GcMeasurement()
            measurement.setMeasurementKey(rs2.getString(1))
            measurement.setMeasurementName(rs2.getString(2))
            measurement.setMeasurementDType(rs2.getString(3))
            measurementlist += measurement
          }
          product.setMeasurements(measurementlist)

          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  /**
   * Get product using QueryParams, may return multiple GcProduct objects.
   *
   * @param p QueryParams
   * @param connAddr
   * @param user
   * @param password
   * @return multiple GcProduct objects.
   */
  def getProducts(p: QueryParams, connAddr: String, user: String, password: String): ArrayBuffer[GcProduct] = {
//    Class.forName(POSTGRESQL_DRIVER)
//    val conn = DriverManager.getConnection(connAddr, user, password)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    val cubeId = p.getCubeId
    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

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
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder;
        extentsql ++= "Select extent_key from gc_extent_" + cubeId + " where 1=1 "
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
          message ++= "No products in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

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
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if (tileSize != "") {
          productsql ++= "AND tilesize ="
          productsql ++= "\'"
          productsql ++= tileSize
          productsql ++= "\'"
        }
        if (cellRes != "") {
          productsql ++= "AND cellres ="
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
        if (startTime != "") {
          productsql ++= "AND phenomenon_time>"
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
        }
        if (endTime != "") {
          productsql ++= "AND phenomenon_time<"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
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
          message ++= "No products in the query product: " + rasterProductName
          //println(message)
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
          message ++= "No products in the query cloud conditions！"
          //println(message)
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
          message ++= "No products in the query measurements！"
          println(message)
        }
        println("Measurement Query SQL: " + measurementsql)
        println("Measurement Keys :" + measurementKeys)

        val command = "Select distinct product_key from gc_raster_tile_fact_" + cubeId + " where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"

        println("Product Query SQL:" + command)

        val productKeysResults = statement.executeQuery(command)
        val productKeysArray = new ArrayBuffer[String]()
        val productArray = new ArrayBuffer[GcProduct]()
        if (productKeysResults.first()) {
          productKeysResults.previous()
          while (productKeysResults.next()) {
            productKeysArray.append(productKeysResults.getString(1))
          }
        } else {
          message ++= "product not found！"
        }
        for (key <- productKeysArray) {
          val product = getProductByKey(cubeId, key, connAddr, user, password)
          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Get ARD products, return multiple GcProduct objects
   *
   * @param cubeId
   * @param connAddr
   * @param user
   * @param password
   * @return multiple GcProduct objects
   */
  def getArdProducts(cubeId: String, connAddr: String, user: String, password: String): Array[GcProduct] = {
    val allProducts: ArrayBuffer[GcProduct] = getAllProducts(cubeId, connAddr, user, password)
    val ardProducts: ArrayBuffer[GcProduct] = allProducts.filter(_.productName.contains("ARD"))
    ardProducts.toArray
  }

}

