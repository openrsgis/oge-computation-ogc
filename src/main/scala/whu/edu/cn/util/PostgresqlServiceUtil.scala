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

    val res: ListBuffer[CoverageMetadata] = queryCoverage("ASTGTM_N28E056", "ASTER_GDEM_DEM30")

    res.foreach(println)


    //    val md = new CoverageCollectionMetadata
    //    md.setProductName("LC08_L1TP_C01_T1")
    //    md.setSensorName("")
    //    md.setMeasurementName("[B4,B5]")
    //    md.setStartTime("2018/01/01")
    //    md.setEndTime("2018/12/31")
    //    md.setExtent("[113,29,120,34]")
    //    md.setCrs("4326")
    //    queryCoverageCollection(md.getProductName, null, md.getMeasurementName, md.getStartTime, md.getEndTime, md.getExtent, md.getCrs)
  }

  def queryCoverageCollection(productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: String = null, endTime: String = null, extent: Geometry = null, crs: CRS = null, cloudCoverMin: Float = 0, cloudCoverMax: Float = 100): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn: Connection = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        val sql = new mutable.StringBuilder
        sql ++= "select oge_image.product_key, oge_image.image_identification, oge_image.crs, oge_image.path, st_astext(oge_image.geom) AS geom,oge_image.phenomenon_time, oge_data_resource_product.name, oge_data_resource_product.product_type, oge_data_resource_product.description, oge_data_resource_product.dtype, oge_sensor.platform_name"
        sql ++= ", oge_product_measurement.band_num, oge_product_measurement.band_rank, oge_product_measurement.band_train, oge_product_measurement.resolution_m"
        sql ++= " from oge_image "
        sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        sql ++= "join oge_sensor on oge_data_resource_product.sensor_key=oge_sensor.sensor_key "
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
          sql ++= startTime
          sql ++= "\'"
          t = " AND"
        }
        if (endTime != null) {
          sql ++= t
          sql ++= "\' "
          sql ++= endTime
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
        if (cloudCoverMin != null && cloudCoverMax != null) {
          sql ++= t
          sql ++= " cover_cloud >="
          sql ++= "\'"
          sql ++= cloudCoverMin.toString()
          sql ++= "\'"
          t = " AND"
          sql ++= t
          sql ++= " cover_cloud <= "
          sql ++= "\'"
          sql ++= cloudCoverMax.toString()
          sql ++= "\'"
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
            coverageMetadata.setGeom(extentResults.getString("geom"))
            coverageMetadata.setDataType(extentResults.getString("dtype"))
            coverageMetadata.setProduct(extentResults.getString("name"))
            coverageMetadata.setProductType(extentResults.getString("product_type"))
            coverageMetadata.setProductDescription(extentResults.getString("description"))
            coverageMetadata.setPlatformName(extentResults.getString("platform_name"))
            coverageMetadata.setResolution(extentResults.getDouble("resolution_m"))
            metaData.append(coverageMetadata)
          }
        }
      }
      finally {
        conn.close()
      }
    } else throw new RuntimeException("connection failed")
    if (metaData.isEmpty){
      throw new RuntimeException("There is no data in the query range.")
    }
    metaData
  }

  def queryCoverage(coverageId: String, productKey: String): ListBuffer[CoverageMetadata] = {
    val metaData = new ListBuffer[CoverageMetadata]

    // 查询数据并处理
    PostgresqlUtilDev.simpleSelect(
      resultNames = Array(
        "oge_image.product_key", "oge_image.image_identification",
        "oge_image.crs", "oge_image.path", "st_astext(oge_image.geom)"
      ),
      tableName = "oge_image",
      rangeLimit = Array(
        ("image_identification", "=", coverageId),
        ("name", "=", productKey)
      ),
      aliases = Array(
        "geom",
        "oge_image.phenomenon_time",
        "oge_data_resource_product.name",
        "oge_data_resource_product.dtype",
        "oge_product_measurement.band_num",
        "oge_product_measurement.band_rank",
        "oge_product_measurement.band_train",
        " oge_product_measurement.resolution_m"
      ),
      jointLimit = Array(
        ("oge_data_resource_product",
          "oge_image.product_key= oge_data_resource_product.id"),
        ("oge_product_measurement",
          "oge_product_measurement.product_key=oge_data_resource_product.id"),
        ("oge_measurement",
          "oge_product_measurement.measurement_key=oge_measurement.measurement_key")
      ),
      func = extentResults => {
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
            println(coverageMetadata.getPath)
          }
        }
      }
    )

    metaData
  }
}
