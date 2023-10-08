package whu.edu.cn.geocube.core.entity

import whu.edu.cn.util.PostgresqlUtilDev

import java.sql.{DriverManager, ResultSet, SQLException, Statement}
import java.util
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * Dimension class - extent.
 *
 */
case class GcExtent() {
  @BeanProperty
  var id: Int = -1
  @BeanProperty
  var extentKey: Int = -1
  @BeanProperty
  var gridCode: String = ""
  @BeanProperty
  var cityCode: String = ""
  @BeanProperty
  var cityName: String = ""
  @BeanProperty
  var provinceName: String = ""
  @BeanProperty
  var provinceCode: String = ""
  @BeanProperty
  var districtName: String = ""
  @BeanProperty
  var districtCode: String = ""
  @BeanProperty
  var extent: java.lang.Object = null
  @BeanProperty
  var resolutionKey: Int = -1

  @BeanProperty
  var tileSize: String = ""
  @BeanProperty
  var cellRes: String = ""
  @BeanProperty
  var level: String = ""

  def this(_id: Int, _extentKey: Int, _gridCode: String,
           _cityCode: String, _cityName: String,
           _provinceName: String, _provinceCode: String,
           _districtName: String, _districtCode: String,
           _extent: java.lang.Object, _resolutionKey: Int) {
    this()
    id = _id
    extentKey = _extentKey
    gridCode = _gridCode
    cityCode = _cityCode
    cityName = _cityName
    provinceName = _provinceName
    provinceCode = _provinceCode
    districtName = _districtName
    districtCode = _districtCode
    extent = _extent
    resolutionKey = _resolutionKey
  }

  def this(_id: Int, _extentKey: Int, _gridCode: String,
           _cityCode: String, _cityName: String,
           _provinceName: String, _provinceCode: String,
           _districtName: String, _districtCode: String,
           _extent: java.lang.Object, _resolutionKey: Int,
           _tilesize: String, _cellres: String, _level: String) {
    this()
    id = _id
    extentKey = _extentKey
    gridCode = _gridCode
    cityCode = _cityCode
    cityName = _cityName
    provinceName = _provinceName
    provinceCode = _provinceCode
    districtName = _districtName
    districtCode = _districtCode
    extent = _extent
    resolutionKey = _resolutionKey
    tileSize = _tilesize
    cellRes = _cellres
    level = _level
  }

  def this(_extentKey: Int, _gridCode: String,
           _cityCode: String, _cityName: String,
           _provinceName: String, _districtName: String,
           _extent: java.lang.Object) {
    this()
    extentKey = _extentKey
    gridCode = _gridCode
    cityCode = _cityCode
    cityName = _cityName
    provinceName = _provinceName
    districtName = _districtName
    extent = _extent
  }

  def this(_id: Int, _extentKey: Int, _gridCode: String,
           _cityCode: String, _cityName: String,
           _provinceName: String, _districtName: String,
           _extent: java.lang.Object, _resolutionKey: Int) {
    this()
    id = _id
    extentKey = _extentKey
    gridCode = _gridCode
    cityCode = _cityCode
    cityName = _cityName
    provinceName = _provinceName
    districtName = _districtName
    extent = _extent
    resolutionKey = _resolutionKey
  }

  def transToString: String = "{" + "\"id\":" + this.id + "," + "\"extentKey\":" + this.extentKey + "," + "\"gridCode\":\"" + this.gridCode + "\"," + "\"cityCode\":\"" + this.cityCode + "\"," + "\"cityName\":\"" + this.cityName + "\"," + "\"provinceName\":\"" + this.provinceName + "\"," + "\"extent\":" + this.extent + "," + "\"resolutionKey\":" + this.resolutionKey + "}"
}

object GcExtent {

  /**
   * Get extent key in extent dimension table using grid zorder code and key in level table.
   *
   * @param gridCode      grid zorder code
   * @param resolutionKey key in level table
   * @return unique extent key
   */
  def getExtentKey(gridCode: String, resolutionKey: String): Integer = {
    // forDece: done
    getExtentKey(null, gridCode, resolutionKey)
  }

  /**
   * Get extent key in extent dimension table using grid zorder code and key in level table.
   *
   * @param statement     postgreSQL statement
   * @param gridCode      grid zorder code
   * @param resolutionKey key in level table
   * @return unique extent key
   */
  def getExtentKey(statement: Statement, gridCode: String, resolutionKey: String): Integer = {

    // forDece TODO: 是否抛弃处理传入的 statement 或修改外部调用方式？
    // 暂时保留原本的逻辑，如需优化请联系本人
    var maxExtentKey = 0
    try {
      PostgresqlUtilDev.simpleSelect(
        resultNames = Array("*"),
        tableName = "gc_extent",
        rangeLimit = Array(("grid_code", "=", gridCode), ("resolution_key", "=", resolutionKey)),
        connection = statement.getConnection,
        func = resultSet => {
          while (resultSet.next) maxExtentKey = resultSet.getInt("extent_key")

          resultSet.close()
        }
      )

    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    maxExtentKey
  }

  /**
   * Get extent info using grid zorder code and key in level table, and return a GcExtent object.
   *
   * @param gridCode      grid zorder code
   * @param resolutionKey key in level table
   * @return a GcExtent object
   */
  def getExtent(gridCode: String, resolutionKey: String): GcExtent = {
    // forDece: done
    getExtent(null, gridCode, resolutionKey)
  }

  /**
   * Get extent info using grid zorder code and key in level table, and return a GcExtent object.
   *
   * @param statement     postgresql statement
   * @param gridCode      grid zorder code
   * @param resolutionKey key in level table
   * @return a GcExtent object
   */
  def getExtent(statement: Statement, gridCode: String, resolutionKey: String): GcExtent = {

    //forDece TODO: 同上
    val gcExtent = new GcExtent
    try {
      // forDece: done
      PostgresqlUtilDev.simpleSelect(
        resultNames = Array("*"), tableName = "gc_extent",
        rangeLimit = Array(("resolution_key", "=", resolutionKey),
          ("grid_code", "=", gridCode)),
        connection = statement.getConnection,
        func = resultSet => {
          while (resultSet.next) {
            val extentKey: Int = resultSet.getInt("extent_key")
            val id: Int = resultSet.getInt("id")
            val gridCode: String = resultSet.getString("grid_code")
            val cityCode: String = resultSet.getString("city_code")
            val cityName: String = resultSet.getString("city_name")
            val provinceName: String = resultSet.getString("province_name")
            val districtName: String = resultSet.getString("district_name")
            val extent: AnyRef = resultSet.getObject("extent")
            val resolutionKey: Int = resultSet.getInt("resolution_key")
            gcExtent.setId(id)
            gcExtent.setExtentKey(extentKey)
            gcExtent.setGridCode(gridCode)
            gcExtent.setCityCode(cityCode)
            gcExtent.setCityName(cityName)
            gcExtent.setProvinceName(provinceName)
            gcExtent.setDistrictName(districtName)
            gcExtent.setExtent(extent)
            gcExtent.setResolutionKey(resolutionKey)
          }
        }
      )

    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    gcExtent
  }

  /**
   * Get extent info using extent key, and return a GcExtent object.
   *
   * @param extentKey
   * @param connAddr
   * @param user
   * @param password
   * @return
   */
  @deprecated("forDece: 该方法未被使用，个人分析认为其应当被弃用")
  def getExtent(extentKey: String, connAddr: String, user: String, password: String): GcExtent = {
    var extent: GcExtent = null
    try {
      PostgresqlUtilDev.simpleSelect(
        resultNames = Array(
          "extent_key", "grid_code", "city_code",
          "city_name", "province_name", "district_name",
          "extent"),
        tableName = "gc_extent",
        rangeLimit = Array(("extent_key", "=", extentKey)),
        func = resultSet => {
          val resultSetArray = new Array[String](7)
          val columnCount: Int = resultSet.getMetaData.getColumnCount
          //each tile has unique extent object
          while (resultSet.next()) {
            for (i <- 1 to columnCount)
              resultSetArray(i - 1) = resultSet.getString(i)
          }
          extent = new GcExtent(
            resultSetArray(0).toInt,
            resultSetArray(1), resultSetArray(2),
            resultSetArray(3), resultSetArray(4),
            resultSetArray(5), resultSetArray(6)
          )
        })
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    extent
  }

  /**
   * Get extent info using grid zorder codes and key in level table, and return multiple GcExtent object strings.
   *
   * @param statement     postgresql statement
   * @param gridCodes     grid zorder code
   * @param resolutionKey key in level table
   * @return multiple GcExtent object strings
   */
  @deprecated("forDece: 该方法未被使用，个人分析认为其应当被弃用")
  def getExtents(statement: Statement, gridCodes: Array[String], resolutionKey: String): Array[String] = {
    val sql = new StringBuilder
    sql ++= "SELECT * from gc_extent where resolution_key=" + resolutionKey + " AND grid_code IN ("
    for (grid <- gridCodes) {
      sql ++= "\'"
      sql ++= grid
      sql ++= "\',"
    }
    sql.deleteCharAt(sql.length - 1)
    sql ++= ")"
    val gcExtentStrings = new ArrayBuffer[String]()

    try {
      val resultSet = statement.executeQuery(sql.toString())
      while (resultSet.next) {
        val gcExtent = new GcExtent
        val extent_key = resultSet.getInt("extent_key")
        val id = resultSet.getInt("id")
        val grid_code = resultSet.getString("grid_code")
        val city_code = resultSet.getString("city_code")
        val city_name = resultSet.getString("city_name")
        val province_name = resultSet.getString("province_name")
        val district_name = resultSet.getString("district_name")
        val extent = resultSet.getObject("extent")
        val resolution_key = resultSet.getInt("resolution_key")
        gcExtent.setId(id)
        gcExtent.setExtentKey(extent_key)
        gcExtent.setGridCode(grid_code)
        gcExtent.setCityCode(city_code)
        gcExtent.setCityName(city_name)
        gcExtent.setProvinceName(province_name)
        gcExtent.setDistrictName(district_name)
        gcExtent.setExtent(extent)
        gcExtent.setResolutionKey(resolution_key)
        gcExtentStrings.append(gcExtent.transToString)
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    gcExtentStrings.toArray
  }

  /**
   * Get an extent info pair by a given resolution key.
   * In the returned pair, key is a grid zorder code,
   * value is a GcExtent represented by string.
   *
   * @param resolutionKey key in level table
   * @return a extent-dimensional Map[GridZorderCode, GcExtent String]
   */
  def getAllExtent(resolutionKey: String): util.HashMap[String, String] = {
    val gcExtentHashMap = new util.HashMap[String, String]
    try {
      //forDece: done
      PostgresqlUtilDev.simpleSelect(resultNames = Array("*"),
        tableName = "gc_extent",
        rangeLimit = Array(("resolution_key", "=", resolutionKey)),
        func = resultSet => {
          while (resultSet.next) {
            val gcExtent = new GcExtent
            val extent_key: Int = resultSet.getInt("extent_key")
            val id: Int = resultSet.getInt("id")
            val grid_code: String = resultSet.getString("grid_code")
            val city_code: String = resultSet.getString("city_code")
            val city_name: String = resultSet.getString("city_name")
            val province_name: String = resultSet.getString("province_name")
            val district_name: String = resultSet.getString("district_name")
            val extent: AnyRef = resultSet.getObject("extent")
            val resolution_key: Int = resultSet.getInt("resolution_key")
            gcExtent.setId(id)
            gcExtent.setExtentKey(extent_key)
            gcExtent.setGridCode(grid_code)
            gcExtent.setCityCode(city_code)
            gcExtent.setCityName(city_name)
            gcExtent.setProvinceName(province_name)
            gcExtent.setDistrictName(district_name)
            gcExtent.setExtent(extent)
            gcExtent.setResolutionKey(resolution_key)
            val gcExtentStr: String = gcExtent.transToString
            gcExtentHashMap.put(grid_code, gcExtentStr)
          }
        })

    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    gcExtentHashMap
  }

}
