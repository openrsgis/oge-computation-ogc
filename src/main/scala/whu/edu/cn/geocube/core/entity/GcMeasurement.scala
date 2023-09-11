package whu.edu.cn.geocube.core.entity

import java.sql.{Connection, DriverManager, ResultSet}

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * Dimension class - measurement.
 *
 */
case class GcMeasurement(){
  @BeanProperty
  var measurementKey: String = ""
  @BeanProperty
  var measurementName: String = ""
  @BeanProperty
  var measurementDType: String = ""

  def this(_measurementKey: String = "", _measurementName: String = "", _measurementDType: String = ""){
    this()
    measurementKey = _measurementKey
    measurementName = _measurementName
    measurementDType = _measurementDType
  }
}

object GcMeasurement{
  /**
   * Get measurement info from MeasurementsAndProduct view, and return a GcMeasurement object.
    * @param cubeId
   * @param measurementKey
   * @param productKey
   * @param connAddr
   * @param user
   * @param password
   *
   * @return a GcMeasurement object.
   */
  def getMeasurementByMeasureAndProdKey(cubeId:String, measurementKey: String, productKey: String, connAddr: String, user: String, password: String): GcMeasurement = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select measurement_key, measurement_name, dtype " +
          "from \"MeasurementsAndProduct_"+cubeId+"\" where product_key=" + productKey + " And measurement_key=" + measurementKey + ";"
        val rs = statement.executeQuery(sql)
        val measurement = new GcMeasurement()
        //each tile has unique measurement
        while (rs.next) {
          measurement.setMeasurementKey(rs.getString(1))
          measurement.setMeasurementName(rs.getString(2))
          measurement.setMeasurementDType(rs.getString(3))
        }
        measurement
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }
  /**
    * Get measurement info from MeasurementsAndProduct view, and return a GcMeasurement object.
    * @param cubeId
    * @param productName
    * @param connAddr
    * @param user
    * @param password
    *
    * @return a GcMeasurement object.
    */
  def getMeasurementByProdName(cubeId:String,  productName: String, connAddr: String, user: String, password: String): Array[String] = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select distinct measurement_name " +
          "from \"MeasurementsAndProduct_"+cubeId+"\" where product_name =" + productName + ";"
        val rs = statement.executeQuery(sql)
        val measurementNamelist = new ArrayBuffer[String]()
        //add every measurementName
        while (rs.next) {
          measurementNamelist+=rs.getString(0)
        }
        measurementNamelist.toArray
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }

}
