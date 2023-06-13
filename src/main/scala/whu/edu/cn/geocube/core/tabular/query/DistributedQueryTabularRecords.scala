package whu.edu.cn.geocube.core.tabular.query

import java.sql.{DriverManager, ResultSet, SQLException}
import java.text.SimpleDateFormat
import java.util.Date
import com.google.gson.JsonParser
import geotrellis.layer.{Bounds, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.cube.tabular.{TabularRDD, TabularRecord, TabularRecordRDD}
import whu.edu.cn.geocube.core.entity.BiDimensionKey.LocationTimeGenderKey
import whu.edu.cn.geocube.core.entity.{BiQueryParams, QueryParams, TabularGridLayerMetadata}
import whu.edu.cn.geocube.core.vector.grid.GridConf
import whu.edu.cn.geocube.util.{GISUtil, HbaseUtil, PostgresqlService}
import whu.edu.cn.geocube.core.tabular.grid.GridTransformer.getGeomGridInfo
import whu.edu.cn.util.GlobalConstantUtil.{POSTGRESQL_PWD, POSTGRESQL_URL, POSTGRESQL_USER}
import whu.edu.cn.util.PostgresqlUtil

import scala.collection.mutable.ArrayBuffer

object DistributedQueryTabularRecords {
  /**
   * Support EO query
   * @param sc
   * @param p
   * @return
   */
  def getTabulars(implicit sc: SparkContext, p: QueryParams): TabularRDD = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of tabulars is being processed...")
    val queryBegin = System.currentTimeMillis()
    val results = getGridLayerTabularRecordsRDD(sc, p)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + results._1.map(_._2.size).sum.toInt + " tabular records: " + (queryEnd - queryBegin) + " ms")
    new TabularRDD(results._1, results._2)
  }

  /**
   * EO query
   * @param sc
   * @param p
   * @return
   */
  def getGridLayerTabularRecordsRDD(implicit sc: SparkContext, p: QueryParams): (RDD[(SpaceTimeKey, Iterable[TabularRecord])], TabularGridLayerMetadata[SpaceTimeKey]) = {
    val conn = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD)
    //product params
    val tabularProductName = p.getTabularProductName

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

        // extent dimension query
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from gc_extent where 1=1 "
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
        println(extentKeys.toString())

        //product dimension query
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product\" where 1=1 "
        if (tabularProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= tabularProductName
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
        println(productKeys.toString())

        //query tabular data ids contained in each grid
        val command = "Select tile_data_id,product_key,extent_key from gc_tabular_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"

        val queryResults = statement.executeQuery(command)
        val tileAndDimensionKeys = new ArrayBuffer[Array[String]]()

        if (queryResults.first()) {
          queryResults.previous()
          while (queryResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = queryResults.getString(1)
            keyArray(1) = queryResults.getString(2)
            keyArray(2) = queryResults.getString(3)
            tileAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No tabular records of " + tabularProductName + " acquired!")
        }

        var tabularRecordCount = 0
        tileAndDimensionKeys.foreach { keys =>
          val tabularRecordKeyListString = keys(0)
          val tabularRecordKeys: Array[String] = tabularRecordKeyListString.substring(tabularRecordKeyListString.indexOf("(") + 1, tabularRecordKeyListString.indexOf(")")).split(", ")
          tabularRecordCount += tabularRecordKeys.length
        }

        var partitions = tileAndDimensionKeys.length
        if(partitions > 48) partitions = 48
        val tileAndDimensionKeysRdd = sc.parallelize(tileAndDimensionKeys, partitions)

        val gridLayerTabularRecordRdd: RDD[(SpaceTimeKey, Iterable[TabularRecord])] = tileAndDimensionKeysRdd.mapPartitions { partition =>
          val conn = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD)
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val results = new ArrayBuffer[(SpaceTimeKey, Iterable[TabularRecord])]()
          partition.foreach { keys =>
            val tabularRecordKeyListString = keys(0)
            val tabularRecordKeys: Array[String] = tabularRecordKeyListString.substring(tabularRecordKeyListString.indexOf("(") + 1, tabularRecordKeyListString.indexOf(")")).split(", ")

            //product key is used to get time of the grid
            val productKey = keys(1)
            val productSql = "select phenomenon_time from gc_product where product_key=" + productKey + ";"
            val productRs = statement.executeQuery(productSql)
            var time: Long = 0L
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            if (productRs.first()) {
              productRs.previous()
              while (productRs.next()) {
                time = sdf.parse(productRs.getString(1)).getTime
              }
            } else {
              throw new RuntimeException("No tabular time acquired!")
            }

            //extent key is used to get (col, row) of the grid
            val extentKey = keys(2)
            val extentSql = "select extent from gc_extent where extent_key=" + extentKey + ";"
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

            //access vectors in the grid
            val tabularRecords:Array[TabularRecord] = tabularRecordKeys.map { key =>
              val jsonStr = HbaseUtil.getTabularCell("hbase_tabular", key, "tabularData", "tile")
              //println(jsonStr)
              //println(GISUtil.toMap[String](jsonStr).toString())
              new TabularRecord(key, GISUtil.toMap[String](jsonStr))

            }
            results.append((SpaceTimeKey(col, row, time), tabularRecords))
          }
          conn.close()
          results.iterator
        }.cache()

        val layerMeta: TabularGridLayerMetadata[SpaceTimeKey] = initLayerMeta(tileAndDimensionKeys(0)(1), p, gridLayerTabularRecordRdd)
        (gridLayerTabularRecordRdd, layerMeta)

      }finally
        conn.close()
    } else
      throw new RuntimeException("connection failed")
  }

  /**
   * Extract tabular layer metadata.
   * @param productKey
   * @param queryParams
   * @param gridLayerTabularRecordRdd
   * @return
   */
  def initLayerMeta(productKey: String,
                    queryParams: QueryParams,
                    gridLayerTabularRecordRdd: RDD[(SpaceTimeKey, Iterable[TabularRecord])]): TabularGridLayerMetadata[SpaceTimeKey] = {
    val crsStr = queryParams.getCRS
    val crs: CRS =
      if (crsStr == "WGS84") CRS.fromEpsgCode(4326)
      else throw new RuntimeException("Not support " + crsStr)

    val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
    //maybe unaccurate actual extent calculated by the query polygon, but faster
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(queryParams.getPolygon, gridConf.gridDimX, gridConf.gridDimY, gridConf.extent, colRow, longLati)
    val minCol = colRow(0); val minRow = colRow(1); val maxCol = colRow(2); val maxRow = colRow(3)
    val minLong = longLati(0); val minLat = longLati(1) ; val maxLong = longLati(2); val maxLat = longLati(3)
    val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(queryParams.getStartTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(queryParams.getEndTime).getTime
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))
    val extent = Extent(minLong, minLat, maxLong, maxLat)

    //accurate actual extent calculation, but cause more time cost
    /*val productMeta = getProductByKey(productKey, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val instant = sdf.parse(productMeta.getPhenomenonTime).getTime
    val colRow = gridLayerTabularRecordRdd.map(x=>(x._1.spatialKey.col, x._1.spatialKey.row)).collect()
    val minCol = colRow.map(_._1).min
    val maxCol = colRow.map(_._1).max
    val minRow = colRow.map(_._2).min
    val maxRow = colRow.map(_._2).max
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,instant),SpaceTimeKey(maxCol,maxRow,instant))

    val bboxes = gridLayerTabularRecordRdd
      .flatMap(x=>(x._2.map(_.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)))
      .map(x=>(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
      .collect()
    val extentMinX = bboxes.map(_._1).min
    val extentMinY = bboxes.map(_._2).min
    val extentMaxX = bboxes.map(_._3).max
    val extentMaxY = bboxes.map(_._4).max
    val extent = new Extent(extentMinX, extentMinY, extentMaxX, extentMaxY)*/

    val productName = queryParams.getVectorProductName
    TabularGridLayerMetadata[SpaceTimeKey](gridConf, extent, bounds, crs, productName)
  }

  /**
   * 获取 BI cube 中的 fact/cell 数据
   * 目前只是用于测试 BI cube和 EO cube 的联合分析, 该方法是hard-coded, 不是通用接口, 请慎用
   * @param sparkContext
   * @param biQueryParams
   * @return
   */
  def getBITabulars(implicit sparkContext: SparkContext, biQueryParams: BiQueryParams): (RDD[(LocationTimeGenderKey, Int)]) = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of tabulars is being processed...")
    val queryBegin = System.currentTimeMillis()
    val results = getBITabularRecordsRDD(sparkContext, biQueryParams)
    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying " + results.count().toInt + " tabular records: " + (queryEnd - queryBegin) + " ms")
    results
  }

  /**
   * 获取 BI cube 中的 fact/cell 数据
   * 目前只是用于测试 BI cube和 EO cube 的联合分析, 该方法是hard-coded, 不是通用接口, 请慎用
   * @param sparkContext
   * @param biQueryParams
   * @return
   */
  def getBITabularRecordsRDD(sparkContext: SparkContext, biQueryParams: BiQueryParams): (RDD[(LocationTimeGenderKey,Int)]) = {
    val locationNames = biQueryParams.getLocationNames
    val genders = biQueryParams.getGenders
    val times = biQueryParams.getTimes
    val timeType = biQueryParams.getTimeType

    val postgreService = new PostgresqlService

    val locationKeys = postgreService.getDimKeys("bi_location", "location_key", "location_name", locationNames.toArray, "discrete")
    val timeKeys = postgreService.getDimKeys("bi_time", "time_key", "time_value", times.toArray, timeType)
    val genderKeys = postgreService.getDimKeys("bi_gender", "gender_key", "gender_value", genders.toArray,"discrete")

    val populationsWithLTGKey: Array[Array[Int]] = postgreService.getFactValues("bi_village_fact", "population", Array("location_key", "time_key", "gender_key"), Array(locationKeys, timeKeys, genderKeys))
    //val householdsWithLTGKey: Array[Array[Int]]= postgreService.getFactValues("bi_village_fact", "households", Array("location_key", "time_key", "gender_key"), Array(locationKeys, timeKeys, genderKeys))

    val populationsWithLTGKeyRdd = sparkContext.parallelize(populationsWithLTGKey.map(x => (new LocationTimeGenderKey(x(1), x(2), x(3)), x(0))))
    //val householdsWithLTGKeyRdd = sparkContext.parallelize(householdsWithLTGKey.map(x => (new LocationTimeGenderKey(x(1), x(2), x(3)), x(0))))

    populationsWithLTGKeyRdd
  }
  def getFactValues(table: String, factItem: String, items: Array[String], itemValues: Array[Array[Int]]): Array[Array[Int]] = {
    val connStr = POSTGRESQL_URL
    val conn = DriverManager.getConnection(connStr, POSTGRESQL_USER, POSTGRESQL_PWD)
    val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    val sql = new StringBuilder
    sql ++= "SELECT " + factItem
    for(i <- 0 until items.size){
      sql ++= "," + items(i)
    }
    sql ++= " from " + table + " WHERE 1=1 "
    for(i <- 0 until items.size){
      sql ++= "AND " + items(i) + " IN ("
      for (value <- itemValues(i)){
        sql ++= "\'" + value + "\',"
      }
      sql.deleteCharAt(sql.length - 1)
      sql ++= ") "
    }

    val resultKeys = new ArrayBuffer[Array[Int]]()
    try {
      val resultSet = statement.executeQuery(sql.toString())
      if(resultSet.first()){
        resultSet.previous()
        while(resultSet.next()){
          val resultKey = new ArrayBuffer[Int]()
          for(i <- 0 until (items.size + 1)){
            resultKey.append(resultSet.getInt(i + 1))
          }
          resultKeys.append(resultKey.toArray)
        }
      }else
        throw new RuntimeException("No record matching the query condition: " + factItem)
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    conn.close()
    resultKeys.toArray
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val queryParams = new BiQueryParams()
    //queryParams.setLocationNames(Array("美桐村委会", "兰兴村村委会"))
    //queryParams.setTimes(Array("2016-07-02"))
    //queryParams.setGenders(Array("male", "female"))
    //queryParams.setTimeType("discrete")

    val biTabularRDD = getBITabulars(sc, queryParams)
    biTabularRDD.collect().foreach{x =>
      println("-----------------------")
      x._2
    }
    /*val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    val queryParams = new QueryParams
    queryParams.setTabularProductName("Hainan_Daguangba_Village_Tabular")
    queryParams.setExtent(108.90494046724021,18.753457222586285,109.18763565740333,19.0497805438586)
    queryParams.setTime("2016-06-01 00:00:00.000", "2016-09-01 00:00:00.000")

    val tabularRDD: TabularRDD = getTabulars(sc, queryParams)
    tabularRDD.collect().foreach{x =>
      println("-----------------------")
      val iter = x._2
      iter.foreach(ele => println(ele.attributes.toString()))
    }*/

  }

}
