package whu.edu.cn.util.cube

import com.alibaba.fastjson.JSONObject
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.entity.cube._
import whu.edu.cn.util.{MinIOUtil, PostgresqlUtilDev}

import java.sql.{Connection, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

object CubePostgresqlUtil {
  private final val conn: Connection = PostgresqlUtilDev.getConnection

  /**
   * 创建表
   *
   * @param tableName 表名
   * @param fields    字段
   */
  def createTable(tableName: String, fields: Array[String]): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "CREATE TABLE IF NOT EXISTS "
      sql ++= tableName
      sql ++= "("
      for (i <- fields.indices) {
        sql ++= fields(i)
        if (i < fields.length - 1) {
          sql ++= ","
        }
      }
      sql ++= ")"
      println(sql)
      statement.execute(sql.toString())
    }
  }

  /**
   * 删除表
   *
   * @param tableName 表名
   */
  def dropTable(tableName: String): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "DROP TABLE "
      sql ++= tableName
      println(sql)
      statement.execute(sql.toString())
    }
  }

  /**
   * 清空表
   *
   * @param tableName 表名
   */
  def truncateTable(tableName: String): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "TRUNCATE TABLE "
      sql ++= tableName
      println(sql)
      statement.execute(sql.toString())
    }
  }

  /**
   * 重置KeySeq
   *
   * @param tableName 表名
   * @param keyName   Key名
   */
  def resetKeySeq(tableName: String, keyName: String): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "alter sequence if exists "
      sql ++= tableName
      sql ++= "_"
      sql ++= keyName
      sql ++= "_seq restart with 1 "
      sql ++= "cache 1"
      println(sql)
      statement.execute(sql.toString())
    }
  }

  /**
   * 插入数据
   *
   * @param tableName 表名
   * @param colNames  列名
   * @param values    值
   */
  def insertDataToTable(tableName: String, colNames: Array[String], values: Array[Any]): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "INSERT INTO "
      sql ++= tableName
      sql ++= "("
      for (i <- colNames.indices) {
        sql ++= colNames(i)
        if (i < colNames.length - 1) {
          sql ++= ","
        }
      }
      sql ++= ") VALUES ("
      for (i <- values.indices) {
        // 检查值是否为 null，如果为 null 则跳过该列
        if (values(i) != null) {
          // 如果是 st_geomfromtext() 列，则不添加引号
          if (colNames(i).equalsIgnoreCase("geom")) {
            sql ++= values(i).toString
          } else {
            sql ++= "'"
            sql ++= values(i).toString
            sql ++= "'"
          }
        } else {
          sql ++= "NULL"
        }

        if (i < values.length - 1) {
          sql ++= ","
        }
      }
      sql ++= ")"
      // println(sql)
      statement.execute(sql.toString())
    }
  }


  /**
   * 删除数据
   *
   * @param tableName 表名
   * @param colNames  列名
   * @param values    值
   */
  def deleteDataInTable(tableName: String, colNames: Array[String], values: Array[Any]): Unit = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "DELETE FROM "
      sql ++= tableName
      sql ++= " WHERE "
      for (i <- colNames.indices) {
        sql ++= colNames(i)
        sql ++= "="
        sql ++= "'"
        sql ++= values(i).toString
        sql ++= "'"
        if (i < colNames.length - 1) {
          sql ++= " AND "
        }
      }
      println(sql)
      statement.execute(sql.toString())
    }
  }

  /**
   * 查询数据
   *
   * @param tableName 表名
   * @param colNames  列名
   * @param values    值
   * @return ResultSet 结果集
   */
  def selectDataFromTable(tableName: String, colNames: Array[String], values: Array[Any]): ResultSet = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "SELECT * FROM "
      sql ++= tableName
      sql ++= " WHERE "
      for (i <- colNames.indices) {
        sql ++= colNames(i)
        sql ++= "="
        sql ++= "'"
        sql ++= values(i).toString
        sql ++= "'"
        if (i < colNames.length - 1) {
          sql ++= " AND "
        }
      }
      // println(sql)
      statement.executeQuery(sql.toString())
    } else {
      null
    }
  }

  /**
   * 查询数据
   *
   * @param tableName 表名
   * @param colNames  列名
   * @param values    值
   * @param where     条件
   * @param orderBy   排序
   * @return ResultSet 结果集
   */
  def selectDataFromTableBySql(sql: String): ResultSet = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      println(sql)
      statement.executeQuery(sql)
    } else {
      null
    }
  }

  /**
   * 查询数据是否存在
   *
   * @param tableName 表名
   * @param colNames  列名
   * @param values    值
   * @return Boolean 是否存在
   */
  def isDataInTable(tableName: String, colNames: Array[String], values: Array[Any]): Boolean = {
    if (conn != null) {
      val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val sql = new StringBuilder
      sql ++= "SELECT * FROM "
      sql ++= tableName
      sql ++= " WHERE "
      for (i <- colNames.indices) {
        sql ++= colNames(i)
        sql ++= "="
        sql ++= "'"
        sql ++= values(i).toString
        sql ++= "'"
        if (i < colNames.length - 1) {
          sql ++= " AND "
        }
      }
      // println(sql)
      val resultSet: ResultSet = statement.executeQuery(sql.toString())
      if (resultSet.next()) {
        true
      } else {
        false
      }
    }
    else {
      false
    }
  }

  /**
   * 创建Cube的元数据表，并获取唯一的CubeId
   *
   * @param cubeName        Cube名称
   * @param tms             TileMatrixSet名称
   * @param cubeDescription Cube描述
   * @return CubeId Cube的唯一标识
   */
  def createCubeMetaTableGroup(cubeName: String, tms: String, cubeDescription: String): Int = {
    // 凡是查询维度，都不能作为共享表
    // 创建oc_cube表 —— 共享表 —— 假设已经有了
    // createTable("oc_cube", Array("cube_key SERIAL PRIMARY KEY", "cube_name TEXT", "tms TEXT", "cube_description TEXT"))
    // 插入oc_cube表
    insertDataToTable("oc_cube", Array("cube_name", "tms", "cube_description"), Array(cubeName, tms, cubeDescription))
    val conn: Connection = PostgresqlUtilDev.getConnection
    if (conn != null) {
      try {
        val statement: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val resultSet: ResultSet = statement.executeQuery("SELECT max(cube_key) FROM oc_cube")
        resultSet.next()
        val cubeId: Int = resultSet.getInt(1)
        println("新建的Cube的ID是" + cubeId)
        // 创建oc_extent_level表 —— 共享表 —— 假设已经有了
        // createTable("oc_extent_level", Array("extent_level_key SERIAL PRIMARY KEY", "extent_level INT", "resolution FLOAT", "tms TEXT", "extent" TEXT))
        // 创建oc_extent表
        createTable("oc_extent_" + cubeId, Array("extent_key SERIAL PRIMARY KEY", "extent_level_key INT", "min_x FLOAT", "min_y FLOAT", "max_x FLOAT", "max_y FLOAT", "row INT", "col INT"))
        // 创建oc_product表
        createTable("oc_product_" + cubeId, Array("product_key SERIAL PRIMARY KEY", "product_name TEXT", "product_type TEXT", "product_description TEXT"))
        // 创建oc_band总表 —— 共享表 —— 假设已经有了
        // createTable("oc_band", Array("band_key SERIAL PRIMARY KEY", "band_name TEXT", "band_unit TEXT", "band_min FLOAT", "band_max FLOAT", "band_scale FLOAT", "band_offset FLOAT", "band_description TEXT", "band_resolution FLOAT", "band_wavelength TEXT", "band_platform TEXT"))
        // 创建oc_band表
        createTable("oc_band_" + cubeId, Array("band_key SERIAL PRIMARY KEY", "band_name TEXT", "band_unit TEXT", "band_min FLOAT", "band_max FLOAT", "band_scale FLOAT", "band_offset FLOAT", "band_description TEXT", "band_resolution FLOAT", "band_wavelength TEXT", "band_platform TEXT"))
        // 创建oc_time_level表 —— 共享表 —— 假设已经有了
        // createTable("oc_time_level", Array("time_level_key SERIAL PRIMARY KEY", "time_level TEXT", "resolution INT"))
        // 创建oc_time表
        createTable("oc_time_" + cubeId, Array("time_key SERIAL PRIMARY KEY", "time_level_key INT", "time TIMESTAMP"))
        // 时间的具体值下沉到了oc_tile_fact表
        // 创建oc_tile_fact表
        createTable("oc_tile_fact_" + cubeId, Array("tile_key SERIAL PRIMARY KEY", "tile_offset INT", "tile_byte_count INT", "compression INT", "product_key INT", "band_key INT", "extent_key INT", "time_key INT", "data_type TEXT", "path TEXT"))
        cubeId
      } finally {
        conn.close()
      }
    }
    else {
      -1
    }
  }

  /**
   * 管理员创建共享表
   */
  def adminCreateSharingTables(): Unit = {
    // 创建oc_cube表
    createTable("oc_cube", Array("cube_key SERIAL PRIMARY KEY", "cube_name TEXT", "tms TEXT", "cube_description TEXT"))
    // 创建oc_extent_level表
    createTable("oc_extent_level", Array("extent_level_key SERIAL PRIMARY KEY", "extent_level INT", "resolution FLOAT", "tms TEXT", "extent TEXT"))
    // 创建oc_time_level表
    createTable("oc_time_level", Array("time_level_key SERIAL PRIMARY KEY", "time_level TEXT", "resolution INT"))
    // 创建oc_band总表
    // createTable("oc_band", Array("band_key SERIAL PRIMARY KEY", "band_name TEXT", "band_unit TEXT", "band_min FLOAT", "band_max FLOAT", "band_scale FLOAT", "band_offset FLOAT", "band_description TEXT", "band_resolution FLOAT", "band_wavelength TEXT", "band_platform TEXT"))
  }

  /**
   * 管理员删除共享表
   */
  def adminDropSharingTables(): Unit = {
    dropTable("oc_cube")
    dropTable("oc_extent_level")
    dropTable("oc_time_level")
  }

  /**
   * 管理员插入共享表数据
   */
  def adminInsertDataToSharingTables(): Unit = {
    // 插入oc_extent_level表
    // 首先清空表格
    truncateTable("oc_extent_level")
    // 然后重置KeySeq
    resetKeySeq("oc_extent_level", "extent_level_key")
    // WGS1984Quad的范围
    val extentWGS1984Quad: JSONObject = new JSONObject
    extentWGS1984Quad.put("min_x", -180.0)
    extentWGS1984Quad.put("min_y", -90.0)
    extentWGS1984Quad.put("max_x", 180.0)
    extentWGS1984Quad.put("max_y", 90.0)
    // WebMercatorQuad的范围
    val extentWebMercatorQuad: JSONObject = new JSONObject
    extentWebMercatorQuad.put("min_x", -20037508.342789244)
    extentWebMercatorQuad.put("min_y", -20037508.342789244)
    extentWebMercatorQuad.put("max_x", 20037508.342789244)
    extentWebMercatorQuad.put("max_y", 20037508.342789244)
    // rHEALPixCustom的范围
    val extentRHEALPixCustom: JSONObject = new JSONObject
    extentRHEALPixCustom.put("min_x", -20015625.0)
    extentRHEALPixCustom.put("min_y", -15011718.75)
    extentRHEALPixCustom.put("max_x", 20015625.0)
    extentRHEALPixCustom.put("max_y", 15011718.75)
    // 先插入tms是WGS1984Quad的
    for (i <- 0 to 23) {
      insertDataToTable("oc_extent_level", Array("extent_level", "resolution", "tms", "extent"), Array(i, 180.0 / 256.0 / Math.pow(2, i), "WGS1984Quad", extentWGS1984Quad.toJSONString))
    }
    // 再插入tms是WebMercatorQuad的
    for (i <- 0 to 24) {
      insertDataToTable("oc_extent_level", Array("extent_level", "resolution", "tms", "extent"), Array(i, 20037508.342789244 * 2 / 256.0 / Math.pow(2, i), "WebMercatorQuad", extentWebMercatorQuad.toJSONString))
    }
    // 再插入tms是rHEALPixCustom的
    for (i <- 0 to 22) {
      insertDataToTable("oc_extent_level", Array("extent_level", "resolution", "tms", "extent"), Array(i, 20015625.0 / 2.0 / 256.0 / Math.pow(2, i), "rHEALPixCustom", extentRHEALPixCustom.toJSONString))
    }
    // 插入oc_time_level表
    // 首先清空表格
    truncateTable("oc_time_level")
    // 然后重置KeySeq
    resetKeySeq("oc_time_level", "time_level_key")
    // 插入time_level_type是average的
    // Y表示年，M表示月，D表示日，H表示小时，M表示分钟，S表示秒
    insertDataToTable("oc_time_level", Array("time_level", "resolution"), Array("D", 1))

  }

  /**
   * 删除Cube的元数据表
   *
   * @param cubeId Cube的唯一标识
   */
  def dropCubeMetaTableGroup(cubeId: Int): Unit = {
    // 首先清空表格
    truncateTable("oc_cube")
    // 然后重置KeySeq
    resetKeySeq("oc_cube", "cube_key")
    dropTable("oc_extent_" + cubeId)
    dropTable("oc_time_" + cubeId)
    dropTable("oc_product_" + cubeId)
    dropTable("oc_band_" + cubeId)
    dropTable("oc_tile_fact_" + cubeId)
  }

  def loadCubeSubsetAlone(cubeId: String, product: Array[String], band: Array[String], time: Array[String], minX: Double, minY: Double, maxX: Double, maxY: Double, tms: String, resolution: Double): Unit = {
    // 1. 首先通过波段找到对应的波段分辨率的最小值
    val bandResolutionList: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    for (i <- band.indices) {
      val bandResolutionAll: ResultSet = selectDataFromTableBySql("SELECT band_resolution FROM oc_band_" + cubeId + " WHERE band_name='" + band(i) + "'")
      bandResolutionAll.next()
      bandResolutionList.append(bandResolutionAll.getDouble("band_resolution"))
    }
    var bandResolutionMin: Double = bandResolutionList.min
    var resolutionWeb: Double = resolution
    if (tms == "WGS1984Quad") {
      bandResolutionMin = bandResolutionMin * 180 / math.Pi / 6378137
      resolutionWeb = resolutionWeb * 180 / math.Pi / 6378137
    }

    // 2. 找到extent_level_key
    val extentLevelAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_extent_level WHERE tms='" + tms + "' AND resolution > " + math.max(resolutionWeb, bandResolutionMin) + " ORDER BY resolution LIMIT 1")
    extentLevelAll.next()
    val extentLevelKey: Int = extentLevelAll.getInt("extent_level_key")
    // 3. 找到所有的extent_key
    var minXQuery: Double = 0.0
    var minYQuery: Double = 0.0
    var maxXQuery: Double = 0.0
    var maxYQuery: Double = 0.0
    // 3.1 然后前端4326的空间范围转为对应的坐标系
    if (tms == "WGS1984Quad") {
      minXQuery = minX
      minYQuery = minY
      maxXQuery = maxX
      maxYQuery = maxY
    }
    else if (tms == "WebMercatorQuad") {
      val extentWeb: Extent = Extent(minX, minY, maxX, maxY)
      val extentWebMercator: Extent = extentWeb.reproject(CRS.fromEpsgCode(4326), CRS.fromEpsgCode(3857))
      minXQuery = extentWebMercator.xmin
      minYQuery = extentWebMercator.ymin
      maxXQuery = extentWebMercator.xmax
      maxYQuery = extentWebMercator.ymax
    }
    // TODO rHEALPix的查询
    else if (tms == "rHEALPixCustom") {
      val extentWeb: Extent = Extent(minX, minY, maxX, maxY)
      val extentWebMercator: Extent = extentWeb.reproject(CRS.fromEpsgCode(4326), CRS.fromEpsgCode(3857))
      minXQuery = extentWebMercator.xmin
      minYQuery = extentWebMercator.ymin
      maxXQuery = extentWebMercator.xmax
      maxYQuery = extentWebMercator.ymax
    }
    val extentKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val extentAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_extent_" + cubeId + " WHERE extent_level_key=" + extentLevelKey + " AND min_x<" + maxXQuery + " AND max_x>" + minXQuery + " AND min_y<" + maxYQuery + " AND max_y>" + minYQuery)
    while (extentAll.next()) {
      extentKeyList.append(extentAll.getInt("extent_key"))
    }
    // 4. 找到所有的product_key
    val productKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (i <- product.indices) {
      val productAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_product_" + cubeId + " WHERE product_name='" + product(i) + "'")
      productAll.next()
      productKeyList.append(productAll.getInt("product_key"))
    }
    // 5. 找到所有的band_key
    val bandKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (i <- band.indices) {
      val bandAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_band_" + cubeId + " WHERE band_name='" + band(i) + "'")
      bandAll.next()
      bandKeyList.append(bandAll.getInt("band_key"))
    }
    // 6. 找到time_level_key
    val timeLevelAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_time_level WHERE resolution = " + 1 + " AND time_level = 'D'")
    timeLevelAll.next()
    val timeLevelKey: Int = timeLevelAll.getInt("time_level_key")
    // 7. 找到所有的time_key
    val timeKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val timeAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_time_" + cubeId + " WHERE time_level_key=" + timeLevelKey + " AND time>='" + time(0) + "' AND time<='" + time(1) + "'")
    while (timeAll.next()) {
      timeKeyList.append(timeAll.getInt("time_key"))
    }
    // 8. 找到所有的tile_key
    val tileKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val tileAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_tile_fact_" + cubeId + " WHERE extent_key in (" + extentKeyList.mkString(",") + ") AND product_key in (" + productKeyList.mkString(",") + ") AND band_key in (" + bandKeyList.mkString(",") + ") AND time_key in (" + timeKeyList.mkString(",") + ")")
    while (tileAll.next()) {
      tileKeyList.append(tileAll.getInt("tile_key"))
    }
    tileKeyList.foreach(println)
  }

  def loadCubeSubsetJoint(implicit sc: SparkContext, cubeId: String, product: Array[String], band: Array[String], time: Array[String], minX: Double, minY: Double, maxX: Double, maxY: Double, tms: String, resolution: Double): RDD[(CubeTileKey, Tile)] = {
    // 1. 首先通过波段找到对应的波段分辨率的最小值
    val bandResolutionList: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    for (i <- band.indices) {
      val bandResolutionAll: ResultSet = selectDataFromTableBySql("SELECT band_resolution FROM oc_band_" + cubeId + " WHERE band_name='" + band(i) + "'")
      bandResolutionAll.next()
      bandResolutionList.append(bandResolutionAll.getDouble("band_resolution"))
    }
    var bandResolutionMin: Double = bandResolutionList.min
    var resolutionWeb: Double = resolution
    if (tms == "WGS1984Quad") {
      bandResolutionMin = bandResolutionMin * 180 / math.Pi / 6378137
      resolutionWeb = resolutionWeb * 180 / math.Pi / 6378137
    }

    // 2. 找到extent_level_key
    val extentLevelAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_extent_level WHERE tms='" + tms + "' AND resolution > " + math.max(resolutionWeb, bandResolutionMin) + " ORDER BY resolution LIMIT 1")
    extentLevelAll.next()
    val extentLevelKey: Int = extentLevelAll.getInt("extent_level_key")
    // 3. 找到所有的extent_key
    var minXQuery: Double = 0.0
    var minYQuery: Double = 0.0
    var maxXQuery: Double = 0.0
    var maxYQuery: Double = 0.0
    // 3.1 然后前端4326的空间范围转为对应的坐标系
    if (tms == "WGS1984Quad") {
      minXQuery = minX
      minYQuery = minY
      maxXQuery = maxX
      maxYQuery = maxY
    }
    else if (tms == "WebMercatorQuad") {
      val extentWeb: Extent = Extent(minX, minY, maxX, maxY)
      val extentWebMercator: Extent = extentWeb.reproject(CRS.fromEpsgCode(4326), CRS.fromEpsgCode(3857))
      minXQuery = extentWebMercator.xmin
      minYQuery = extentWebMercator.ymin
      maxXQuery = extentWebMercator.xmax
      maxYQuery = extentWebMercator.ymax
    }
    // TODO rHEALPix的查询
    else if (tms == "rHEALPixCustom") {
      val extentWeb: Extent = Extent(minX, minY, maxX, maxY)
      val extentWebMercator: Extent = extentWeb.reproject(CRS.fromEpsgCode(4326), CRS.fromEpsgCode(3857))
      minXQuery = extentWebMercator.xmin
      minYQuery = extentWebMercator.ymin
      maxXQuery = extentWebMercator.xmax
      maxYQuery = extentWebMercator.ymax
    }
    // 4. 找到time_level_key
    val timeLevelAll: ResultSet = selectDataFromTableBySql("SELECT * FROM oc_time_level WHERE resolution = " + 1 + " AND time_level = 'D'")
    timeLevelAll.next()
    val timeLevelKey: Int = timeLevelAll.getInt("time_level_key")
    // 8. 找到所有的tile_key
    val tileKeyList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val tileAll: ResultSet = selectDataFromTableBySql("select oc_tile_fact_" + cubeId + ".tile_offset,oc_tile_fact_" + cubeId + ".tile_byte_count, oc_tile_fact_" + cubeId + ".compression, oc_tile_fact_" + cubeId + ".path, oc_tile_fact_" + cubeId + ".data_type, oc_time_" + cubeId + ".time, oc_extent_" + cubeId + ".min_x, oc_extent_" + cubeId + ".min_y, oc_extent_" + cubeId + ".max_x, oc_extent_" + cubeId + ".max_y, oc_extent_" + cubeId + ".row, oc_extent_" + cubeId + ".col,oc_product_" + cubeId + ".product_name,oc_product_" + cubeId + ".product_type,oc_band_" + cubeId + ".band_name, oc_band_" + cubeId + ".band_platform from oc_tile_fact_" + cubeId + " join oc_band_" + cubeId + " on oc_tile_fact_" + cubeId + ".band_key=oc_band_" + cubeId + ".band_key join oc_time_" + cubeId + " on oc_tile_fact_" + cubeId + ".time_key=oc_time_" + cubeId + ".time_key join oc_extent_" + cubeId + " on oc_tile_fact_" + cubeId + ".extent_key=oc_extent_" + cubeId + ".extent_key join oc_product_" + cubeId + " on oc_tile_fact_" + cubeId + ".product_key=oc_product_" + cubeId + ".product_key where oc_time_" + cubeId + ".time>'" + time(0) + "' and oc_time_" + cubeId + ".time<'" + time(1) + "' and oc_extent_" + cubeId + ".min_x<" + maxXQuery + " and oc_extent_" + cubeId + ".max_x>" + minXQuery + " and oc_extent_" + cubeId + ".min_y<" + maxYQuery + " and oc_extent_" + cubeId + ".max_y>" + minYQuery + " and oc_band_" + cubeId + ".band_name in ('" + band.mkString("','") + "')" + " and oc_product_" + cubeId + ".product_name in ('" + product.mkString("','") + "')" + " and oc_extent_" + cubeId + ".extent_level_key=" + extentLevelKey)
    val cubeTileMetaList: ArrayBuffer[CubeTileMeta] = new ArrayBuffer[CubeTileMeta]()
    while (tileAll.next()) {
      val cubeTileKey: CubeTileKey = new CubeTileKey(new SpaceKey(tileAll.getInt("row"), tileAll.getInt("col"), tileAll.getDouble("min_x"), tileAll.getDouble("max_x"), tileAll.getDouble("min_y"), tileAll.getDouble("max_y")), new TimeKey(tileAll.getTimestamp("time").getTime), new ProductKey(tileAll.getString("product_name"), tileAll.getString("product_type")), new BandKey(tileAll.getString("band_name"), tileAll.getString("band_platform")))
      val tileOffset: Int = tileAll.getInt("tile_offset")
      val tileByteCount: Int = tileAll.getInt("tile_byte_count")
      val compression: Int = tileAll.getInt("compression")
      val dataType: String = tileAll.getString("data_type")
      val path: String = tileAll.getString("path")
      cubeTileMetaList.append(new CubeTileMeta(cubeTileKey, path, tileOffset, tileByteCount, dataType, compression))
    }
    val cubeTileMetaRDD: RDD[CubeTileMeta] = sc.parallelize(cubeTileMetaList)
    val cubeRDD: RDD[(CubeTileKey, Tile)] = cubeTileMetaRDD.map(cubeTileMeta => {
      val cubeTileKey: CubeTileKey = cubeTileMeta.cubeTileKey
      val tileCompressed: Array[Byte] = MinIOUtil.getMinioObject("oge-cube", cubeTileMeta.tilePath, cubeTileMeta.tileOffset, cubeTileMeta.tileByteCount)
      val tileDecompressed: Array[Byte] = CubeUtil.decompress(tileCompressed)
      val dataType: String = cubeTileMeta.dataType
      val cubeTile: Tile = CubeTileSerializerUtil.deserializeTileData(tileDecompressed, 256, dataType)
      (cubeTileKey, cubeTile)
    })
    cubeRDD
  }

  def main(args: Array[String]): Unit = {
    // val interpreter = new PythonInterpreter
    // interpreter.exec("import pyproj")
  }

}
