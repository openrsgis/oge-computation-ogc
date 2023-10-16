package whu.edu.cn.geocube.core.raster.query

import java.io.{File, FileOutputStream}
import java.sql.ResultSet
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}

import scala.collection.mutable.ArrayBuffer
import sys.process._
import whu.edu.cn.util.PostgresqlUtil

/**
 * Query precomputed results in the database.
 */
object OlapQueryPrecomputeRasterTiles {
  /**
   * Query city-month-highlevelmeasurement level data.
   *
   * @param rasterProductNames
   * @param cityName
   * @param month
   * @param highlevelMeasurement
   * @param outputDir
   * @return
   */
  def cityMonthHighlevelmeasurement(rasterProductNames: Array[String], cityName: String, month: Int, highlevelMeasurement: String, outputDir: String): Array[(String, String)] = {
    null
  }

  /**
   * Query city-year-highlevelmeasurement level data.
   *
   * @param rasterProductNames
   * @param cityName
   * @param year
   * @param highlevelMeasurement
   * @param outputDir
   * @return
   */
  def cityYearHighlevelmeasurement(rasterProductNames: Array[String], cityName: String, year: Int, highlevelMeasurement: String, outputDir: String): Array[(String, String)] = {
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        val extentSQL = "Select extent_key from gc_extent_city where city_name = \'" + cityName + "\'"
        println(extentSQL)
        val extentResults = statement.executeQuery(extentSQL.toString())
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
          println("extent key: " + extentKeys)
        } else {
          return null
        }

        val productSQL = new StringBuilder
        productSQL ++= "Select product_key from gc_product_year where 1=1 "
        if (rasterProductNames.length != 0) {
          productSQL ++= "AND product_name IN ("
          for (rasterProductName <- rasterProductNames) {
            productSQL ++= "\'"
            productSQL ++= rasterProductName
            productSQL ++= "\',"
          }
          productSQL.deleteCharAt(productSQL.length - 1)
          productSQL ++= ")"
        }
        productSQL ++= "AND phenomenon_time_year = "
        productSQL ++= year.toString
        println(productSQL)
        val productResults = statement.executeQuery(productSQL.toString());
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
        println("product key:" + productKeys)

        val measurementSQL = new StringBuilder
        measurementSQL ++= "Select measurement_key from gc_measurement_highlevel where 1=1 "
        measurementSQL ++= "AND product_level = "
        measurementSQL ++= "\'"
        measurementSQL ++= highlevelMeasurement
        measurementSQL ++= "\'"
        println(measurementSQL)
        val measurementResults = statement.executeQuery(measurementSQL.toString());
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
        println("measurement key: " + measurementKeys)

        val command = "Select tile_data_id from gc_raster_tile_fact_city_year_highlevel where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"
        println("Raster Tile Fact Query SQL:" + command)

        val results = statement.executeQuery(command)
        val resultIDs = new ArrayBuffer[String]()
        if (results.first()) {
          results.previous()
          while (results.next()) {
            resultIDs.append(results.getString(1))
          }
        } else {
          println("No tiles acquired!")
          return null
        }

        val paths = new ArrayBuffer[(String, String)]()
        resultIDs.foreach{x =>
          val ids = x.split(",")
          val pointerSQL = new StringBuilder
          pointerSQL ++= "Select data_path,meta_path from gc_pointer where 1=1 "
          if(ids.length != 0) {
            pointerSQL ++= "AND id IN ("
            for(id <- ids) {
              pointerSQL ++= id
              pointerSQL ++= ","
            }
            pointerSQL.deleteCharAt(pointerSQL.length - 1)
            pointerSQL ++= ")"
          }
          println(pointerSQL)
          val pathResults = statement.executeQuery(pointerSQL.toString())
          if (pathResults.first()) {
            pathResults.previous()
            while (pathResults.next) {
              paths.append((pathResults.getString(1), pathResults.getString(2)))
            }
          } else throw new RuntimeException("no results with " + pointerSQL)
        }

        val outputDirArray = outputDir.split("/")
        val sessionDir = new StringBuffer()
        for(i <- 0 until outputDirArray.length - 1)
          sessionDir.append(outputDirArray(i) + "/")
        val executorSessionDir = sessionDir.toString
        val executorSessionFile = new File(executorSessionDir)
        if (!executorSessionFile.exists) executorSessionFile.mkdir
        val executorOutputDir = outputDir
        val executorOutputFile = new File(executorOutputDir)
        if (!executorOutputFile.exists()) executorOutputFile.mkdir()

        paths.foreach{x =>
          val dataPath = x._1
          val metaPath = x._2
          val cpPathCommand = "cp " + dataPath + " " + outputDir
          val cpMetaCommand = "cp " + metaPath + " " + outputDir
          cpPathCommand.!
          cpMetaCommand.!

          val dataName = new File(dataPath).getName
          val metaName = new File(metaPath).getName
          val objMap = new ObjectMapper
          val root = objMap.readTree(new File(outputDir + metaName)).asInstanceOf[ObjectNode]
          root.put("path", outputDir.replace(localDataRoot, httpDataRoot) + dataName)
          root.put("meta", outputDir.replace(localDataRoot, httpDataRoot) + metaName)
          objMap.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputDir + metaName), root)
        }
        paths.toArray
      }finally {
        conn.close()
      }
    }else
      throw new RuntimeException("connection failed")
  }

  /**
   * Query province-month-highlevelmeasurement level data.
   *
   * @param rasterProductNames
   * @param provinceName
   * @param month
   * @param highlevelMeasurement
   * @param outputDir
   * @return
   */
  def provinceMonthHighlevelmeasurement(rasterProductNames: Array[String], provinceName: String, month: Int, highlevelMeasurement: String, outputDir: String): Array[(String, String)] = {
    null
  }

  /**
   * Query province-year-highlevelmeasurement level data.
   *
   * @param rasterProductNames
   * @param provinceName
   * @param year
   * @param highlevelMeasurement
   * @param outputDir
   * @return
   */
  def provinceYearHighlevelmeasurement(rasterProductNames: Array[String], provinceName: String, year: Int, highlevelMeasurement: String, outputDir: String): Array[(String, String)] = {
    null
  }
}
