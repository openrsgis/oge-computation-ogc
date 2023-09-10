package whu.edu.cn.geocube.core.raster.ingest

import java.io.File
import java.sql.{ResultSet, SQLException}
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.geocube.util.PostgresqlService


/**
 * Save the results of on-the-fly computation to database for precomputation.
 */
object OlapIngestor {
  /**
   * Save city-month-highlevelmeasurement level data.
   *
   * @param rasterProductName
   * @param cityName
   * @param month
   * @param highlevelMeasurement
   * @param preComputeDir
   */
  def cityMonthHighlevelmeasurement(rasterProductName: String, cityName: String, month: Int, highlevelMeasurement: String, preComputeDir: String): Unit = {

  }

  /**
   * Save city-year-highlevelmeasurement level data.
   *
   * @param rasterProductName
   * @param cityName
   * @param year
   * @param highlevelMeasurement
   * @param preComputeDir
   */
  def cityYearHighlevelmeasurement(rasterProductName: String, cityName: String, year: Int, highlevelMeasurement: String, preComputeDir: String): Unit = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        //extent
        val extentSQL = "Select extent_key from gc_extent_city where city_name = \'" + cityName + "\'"
        val extentResults = statement.executeQuery(extentSQL.toString())
        if (!extentResults.first()){
          val postgresqlService = new PostgresqlService
          var extentId = postgresqlService.getMaxValue("id","gc_extent_city")
          if (extentId == null) extentId = 0 else extentId += 1
          var extentKey = postgresqlService.getMaxValue("extent_key","gc_extent_city")
          if (extentKey == null) extentKey = 0 else extentKey += 1
          val extentWKT = postgresqlService.getCityGeometry(cityName, "/home/geocube/data/vector/gadm36_CHN_shp/gadm36_CHN_2.shp").toString
          //val extentWKT = postgresqlService.getCityGeometry(cityName,"E:\\VectorData\\Administry District\\gadm36_CHN_shp\\gadm36_CHN_2.shp").getEnvelope.toString
          val extentInsertSQL = "INSERT INTO gc_extent_city(id, extent_key, city_name, extent, create_by, create_time, update_by, update_time) VALUES (" +
            extentId + "," + extentKey + ",'" + cityName + "','" + extentWKT + "'," + "'admin',current_timestamp,null,null);"
          try {
            val result = statement.executeUpdate(extentInsertSQL)
            if (!(result > 0)) throw new RuntimeException("Insert to gc_extent_city failed")
          } catch {
            case e: SQLException =>
              e.printStackTrace()
          }
        }

        //product
        val productSQL = "Select product_key from gc_product_year where product_name = \'" + rasterProductName + "\' AND " + "phenomenon_time_year = " + year
        val productResults = statement.executeQuery(productSQL.toString())
        if (!productResults.first()){
          val postgresqlService = new PostgresqlService
          var productId = postgresqlService.getMaxValue("id","gc_product_year")
          if (productId == null) productId = 0 else productId += 1
          var productKey = postgresqlService.getMaxValue("product_key","gc_product_year")
          if (productKey == null) productKey = 0 else productKey += 1

          val productInsertSQL = "INSERT INTO gc_product_year(id, product_key, product_name, product_type, phenomenon_time_year, create_by, create_time, update_by, update_time) VALUES (" +
            productId + "," + productKey + ",'" + rasterProductName + "','EO'," + year + ",'admin',current_timestamp,null,null);"
          try {
            val result = statement.executeUpdate(productInsertSQL)
            if (!(result > 0)) throw new RuntimeException("Insert to gc_product_year failed")
          } catch {
            case e: SQLException =>
              e.printStackTrace()
          }
        }

        //measurement
        val measurementSQL = "Select measurement_key from gc_measurement_highlevel where product_level = \'" + highlevelMeasurement + "\'"
        val measurementResults = statement.executeQuery(measurementSQL.toString())
        if (!measurementResults.first()){
          val postgresqlService = new PostgresqlService
          var measurementId = postgresqlService.getMaxValue("id","gc_measurement_highlevel")
          if (measurementId == null) measurementId = 0 else measurementId += 1
          var measurementKey = postgresqlService.getMaxValue("measurement_key","gc_measurement_highlevel")
          if (measurementKey == null) measurementKey = 0 else measurementKey += 1

          val measurementInsertSQL = "INSERT INTO gc_measurement_highlevel(id, measurement_key, product_level, create_by, create_time, update_by, update_time) VALUES (" +
            measurementId + "," + measurementKey + ",'" + highlevelMeasurement + "','admin',current_timestamp,null,null);"
          try {
            val result = statement.executeUpdate(measurementInsertSQL)
            if (!(result > 0)) throw new RuntimeException("Insert to gc_measurement_highlevel failed")
          } catch {
            case e: SQLException =>
              e.printStackTrace()
          }
        }

        //fact and pointer
        val extentUpdateResults = statement.executeQuery(extentSQL.toString())
        var extentUpdateKey = -1
        var count = 0
        while(extentUpdateResults.next){
          extentUpdateKey = extentUpdateResults.getString(1).toInt
          count += 1
        }
        if(count != 1) throw new RuntimeException("extent_key insert to gc_raster_tile_fact_city_year_highlevel failed")

        val productUpdateResults = statement.executeQuery(productSQL.toString())
        var productUpdateKey = -1
        count = 0
        while(productUpdateResults.next){
          productUpdateKey = productUpdateResults.getString(1).toInt
          count += 1
        }
        if(count != 1) throw new RuntimeException("product_key insert to gc_raster_tile_fact_city_year_highlevel failed")


        val measurementUpdateResults = statement.executeQuery(measurementSQL.toString())
        var measurementUpdateKey = -1
        count = 0
        while(measurementUpdateResults.next){
          measurementUpdateKey = measurementUpdateResults.getString(1).toInt
          count += 1
        }
        if(count != 1) throw new RuntimeException("measurement_key insert to gc_raster_tile_fact_city_year_highlevel failed")


        val factSQL = "Select fact_key from gc_raster_tile_fact_city_year_highlevel where product_key = "+
          productUpdateKey + " AND extent_key = " + extentUpdateKey + " AND measurement_key = " + measurementUpdateKey

        val factResults = statement.executeQuery(factSQL.toString())
        if (!factResults.first()){
          val postgresqlService = new PostgresqlService
          var factId = postgresqlService.getMaxValue("id","gc_raster_tile_fact_city_year_highlevel")
          if (factId == null) factId = 0 else factId += 1
          var factKey = postgresqlService.getMaxValue("fact_key","gc_raster_tile_fact_city_year_highlevel")
          if (factKey == null) factKey = 0 else factKey += 1

          //calculate tile data id and insert to gc_pointer
          val tileDataID = new StringBuilder
          var pointerId = postgresqlService.getMaxValue("id","gc_pointer")
          if (pointerId == null) pointerId = 0
          val file = new File(preComputeDir)
          val pathArray = file.listFiles().filter(x => !x.getName.contains("job_meta")).map(_.getAbsolutePath)
          assert(pathArray.length % 2 == 0)
          val pathSet = pathArray.map(x=> x.replace(".png", "").replace(".json", "")).toSet
          pathSet.foreach{x =>
            val pngDataPath = x + ".png"
            val jsonMetaPath = x + ".json"
            val pointerSQL = "Select id from gc_pointer where data_path = '" + pngDataPath + "' AND meta_path = '" + jsonMetaPath + "'"
            val pointerResults = statement.executeQuery(pointerSQL.toString())
            if (!pointerResults.first()){
              pointerId += 1
              val pointerInsertSQL = "INSERT INTO gc_pointer(id, data_path, meta_path) VALUES (" + pointerId + ",'" + pngDataPath + "','" + jsonMetaPath + "');"
              try {
                val result = statement.executeUpdate(pointerInsertSQL)
                if (!(result > 0)) throw new RuntimeException("Insert to gc_pointer failed")
              } catch {
                case e: SQLException =>
                  e.printStackTrace()
              }
            }else{
              var count = 0
              pointerResults.previous()
              while(pointerResults.next()){
                pointerId = pointerResults.getString(1).toInt
                count += 1
              }
              if(count != 1)throw new RuntimeException("multiple pointerId but same data returned!")
            }
            tileDataID ++= (pointerId.toString + ",")
          }
          tileDataID.deleteCharAt(tileDataID.lastIndexOf(","))

          //insert to fact
          val factInsertSQL = "INSERT INTO gc_raster_tile_fact_city_year_highlevel(id, fact_key, product_key, extent_key, measurement_key, tile_data_id, create_by, create_time, update_by, update_time) VALUES (" +
            factId + "," + factKey + "," + productUpdateKey + ","+ extentUpdateKey + ","+ measurementUpdateKey + ",'"+ tileDataID.toString() + "','admin',current_timestamp,null,null);"
          try {
            val result = statement.executeUpdate(factInsertSQL)
            if (!(result > 0)) throw new RuntimeException("Insert to gc_raster_tile_fact_city_year_highlevel failed")
          } catch {
            case e: SQLException =>
              e.printStackTrace()
          }
        }

      }finally
        conn.close()
    }else
      throw new RuntimeException("connection failed")
  }

  /**
   * Save province-month-highlevelmeasurement level data.
   *
   * @param rasterProductName
   * @param provinceName
   * @param month
   * @param highlevelMeasurement
   * @param preComputeDir
   */
  def provinceMonthHighlevelmeasurement(rasterProductName: String, provinceName: String, month: Int, highlevelMeasurement: String, preComputeDir: String): Unit = {

  }

  /**
   * Save province-year-highlevelmeasurement level data.
   *
   * @param rasterProductName
   * @param provinceName
   * @param year
   * @param highlevelMeasurement
   * @param preComputeDir
   */
  def provinceYearHighlevelmeasurement(rasterProductName: String, provinceName: String, year: Int, highlevelMeasurement: String, preComputeDir: String): Unit = {

  }
}
