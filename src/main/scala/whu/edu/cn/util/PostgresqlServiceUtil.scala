package whu.edu.cn.util

import geotrellis.proj4.CRS
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata}

import java.sql.{Connection, ResultSet, Statement}
import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object PostgresqlServiceUtil {
  def main(args: Array[String]): Unit = {
    val md = new CoverageCollectionMetadata
    md.setProductName("LC08_L1TP_C01_T1")
    md.setSensorName("")
    md.setMeasurementName("[B4,B5]")
    md.setStartTime("2018/01/01")
    md.setEndTime("2018/12/31")
    md.setExtent("[113,29,120,34]")
    md.setCrs("4326")
    queryCoverageCollection(md.getProductName, null, md.getMeasurementName, md.getStartTime, md.getEndTime, md.getExtent, md.getCrs)
  }

  def queryCoverageCollection(productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: LocalDateTime = null, endTime: LocalDateTime = null, extent: Geometry = null, crs: CRS = null): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        val sql = new mutable.StringBuilder
        sql ++= "select oge_image.product_key, oge_image.image_identification, oge_image.crs, oge_image.path, oge_image.phenomenon_time, oge_data_resource_product.name, oge_data_resource_product.dtype"
        sql ++= ", oge_product_measurement.band_num, oge_product_measurement.band_rank, oge_product_measurement.band_train, oge_product_measurement.resolution_m"
        sql ++= " from oge_image "
        sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        if (sensorName != "" && sensorName != null) {
          sql ++= "join oge_sensor on oge_data_resource_product.sensor_Key=oge_sensor.sensor_Key "
        }
        sql ++= "join oge_product_measurement on oge_product_measurement.product_key=oge_data_resource_product.id join oge_measurement on oge_product_measurement.measurement_key=oge_measurement.measurement_key "
        // productName is not null
        var t = "where"
        sql ++= t
        sql ++= " name="
        sql ++= "\'"
        sql ++= productName
        sql ++= "\'"
        t = " AND"
        if (sensorName != "" && sensorName != null) {
          sql ++= t
          sql ++= " sensor_name="
          sql ++= "\'"
          sql ++= sensorName
          sql ++= "\'"
          t = " AND"
        }
        if (measurementName.nonEmpty) {
          sql ++= t
          sql ++= " band_num in ("
          if (measurementName.size >= 2) {
            for (i <- 0 until measurementName.size - 1) {
              sql ++= "\'"
              sql ++= measurementName(i)
              sql ++= "\', "
            }
          }
          sql ++= "\'"
          sql ++= measurementName.last
          sql ++= "\'"
          sql ++= ")"
          t = " AND"
        }
        if (startTime != null) {
          sql ++= t
          sql ++= " phenomenon_time >= "
          sql ++= "\'"
          sql ++= startTime.toString
          sql ++= "\'"
          t = " AND"
        }
        if (endTime != null) {
          sql ++= t
          sql ++= "\' "
          sql ++= endTime.toString
          sql ++= "\'"
          sql ++= " >= phenomenon_time"
          t = " AND"
        }
        if (crs != null) {
          sql ++= t
          sql ++= " crs="
          sql ++= "\'"
          sql ++= crs.toString()
          sql ++= "\'"
          t = " AND"
        }
        if (extent != null) {
          sql ++= t
          sql ++= " ST_Intersects(geom,'SRID=4326;"
          sql ++= geotrellis.vector.io.wkt.WKT.write(extent)
          sql ++= "')"
        }
        println(sql)
        val extentResults: ResultSet = statement.executeQuery(sql.toString())

        // 排除BQA波段
        while (extentResults.next()) {
          if (extentResults.getInt("band_train") != 0) {
            val coverageMetadata = new CoverageMetadata
            coverageMetadata.setCoverageID(extentResults.getString("image_identification"))
            coverageMetadata.setMeasurement(extentResults.getString("band_num"))
            coverageMetadata.setMeasurementRank(extentResults.getInt("band_rank"))
            coverageMetadata.setPath(extentResults.getString("path") + "/" + coverageMetadata.getCoverageID + "_" + coverageMetadata.getMeasurement + ".tif")
            coverageMetadata.setTime(extentResults.getString("phenomenon_time"))
            coverageMetadata.setCrs(extentResults.getString("crs"))
            coverageMetadata.setDataType(extentResults.getString("dtype"))
            coverageMetadata.setResolution(extentResults.getDouble("resolution_m"))
            metaData.append(coverageMetadata)
          }
        }
      }
      finally {
        conn.close()
      }
    } else throw new RuntimeException("connection failed")

    metaData
  }

  def queryCoverage(coverageId: String,productKey :String): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection

    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        val sql = new mutable.StringBuilder
        sql ++= "select oge_image.product_key, oge_image.image_identification, oge_image.crs, oge_image.path, st_astext(oge_image.geom) as geom, oge_image.phenomenon_time, oge_data_resource_product.name, oge_data_resource_product.dtype"
        sql ++= ", oge_product_measurement.band_num, oge_product_measurement.band_rank, oge_product_measurement.band_train, oge_product_measurement.resolution_m"
        sql ++= " from oge_image "
        sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        sql ++= "join oge_product_measurement on oge_product_measurement.product_key=oge_data_resource_product.id join oge_measurement on oge_product_measurement.measurement_key=oge_measurement.measurement_key "
        // productName is not null
        var t = "where"
        sql ++= t
        sql ++= " image_identification="
        sql ++= "\'"
        sql ++= coverageId
        sql ++= "\' AND name = \'"
        sql ++= productKey
        sql ++= "\'"
        println(sql)
        val extentResults: ResultSet = statement.executeQuery(sql.toString())

        // 排除BQA波段
        while (extentResults.next()) {
          if (extentResults.getInt("band_train") != 0) {
            val coverageMetadata = new CoverageMetadata
            coverageMetadata.setCoverageID(extentResults.getString("image_identification"))
            coverageMetadata.setMeasurement(extentResults.getString("band_num"))
            coverageMetadata.setMeasurementRank(extentResults.getInt("band_rank"))
            coverageMetadata.setPath(extentResults.getString("path") + "/" + coverageMetadata.getCoverageID + "_" + coverageMetadata.getMeasurement + ".tif")
            coverageMetadata.setGeom(extentResults.getString("geom"))
            coverageMetadata.setTime(extentResults.getString("phenomenon_time"))
            coverageMetadata.setCrs(extentResults.getString("crs"))
            coverageMetadata.setDataType(extentResults.getString("dtype"))
            coverageMetadata.setResolution(extentResults.getDouble("resolution_m"))
            metaData.append(coverageMetadata)
          }
        }
      }
      finally {
        conn.close()
      }
    } else throw new RuntimeException("connection failed")

    metaData
  }
}
