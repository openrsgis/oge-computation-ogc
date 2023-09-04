//package whu.edu.cn.geocube.core.tabular.ingest
//
//import java.io.File
//import java.sql.{DriverManager, ResultSet}
//
//import geotrellis.vector.Extent
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.json4s.NoTypeHints
//import org.json4s.jackson.Serialization
//import whu.edu.cn.geocube.core.cube.tabular.{TabularRecord, TabularRecordRDD}
//import whu.edu.cn.geocube.core.entity.{GcExtent, TabularGridFact}
//import whu.edu.cn.geocube.core.vector.grid.GridConf
//import whu.edu.cn.geocube.core.tabular.grid.GridTransformer
//import whu.edu.cn.geocube.util.{GISUtil, HbaseUtil, PostgresqlService, PostgresqlUtil}
//
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//import scala.collection.JavaConversions._
//
//object Ingestor {
//  implicit val formats = Serialization.formats(NoTypeHints)
//
//  def ingestTabular(inputPath: String, productName: String, cubeType: String): Unit ={
//    cubeType match {
//      case "EO" => ingestTabularEOCube(inputPath, productName)
//      case "BI" => ingestTabularBICube(inputPath, productName)
//      case _ => throw new RuntimeException("Cube type options: BI and EO, please select one of them!")
//    }
//  }
//
//  def ingestTabularEOCube(inputPath: String, productName: String): Unit = {
//    val conf = new SparkConf()
//      .setAppName("Vector Ingestion Using Spark")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer.max", "1024m")
//      .set("spark.rpc.message.maxSize", "512")
//    val sc = new SparkContext(conf)
//
//    try {
//      val tabularRecordRDD: TabularRecordRDD = TabularRecordRDD.createTabularRecordRDD(sc, inputPath, 16).cache()
//
//      //initialize grid layout and get RDD[(gridZCode, TabularIds)]
//      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
//      val gridWithTabularIdsRdd: RDD[(Long, Iterable[String])] = tabularRecordRDD.flatMap(x => GridTransformer.groupTabularId2ZCgrid(x, gridConf)).groupByKey()
//
//      //calculate layer extent and transform to wkt format
//      val file = Source.fromFile(inputPath)
//      val productIdentity = new File(inputPath).getName.substring(0, new File(inputPath).getName.indexOf("."))
//      val attributeArr = file.getLines().next().split(" ")
//      var (minX, minY) = (Double.MaxValue, Double.MaxValue)
//      var (maxX, maxY) = (Double.MinValue, Double.MinValue)
//      var phenomenonTime = ""
//      var resultTime = ""
//      for(line <- file.getLines) {
//        val valueArr = line.split(" ")
//        var result: Map[String, String] = Map()
//        (0 until attributeArr.length).foreach{ i =>
//          result += (attributeArr(i)->valueArr(i))
//        }
//        //using GeoNames web service
//        /*val geo_name = result.getAttributes.get("geo_name").get
//        val geo_addr = result.getAttributes.get("geo_address").get
//        val (x, y) = getGeogpraphicCoordinate(geo_name)*/
//        val x = result.get("longitude").get.toDouble
//        val y = result.get("latitude").get.toDouble
//        if(x < minX) minX = x
//        if(x > maxX) maxX = x
//        if(y < minY) minY = y
//        if(y > maxY) maxY = y
//
//        if (result.get("time").isEmpty) throw new Exception("There is no attribute[time]")
//        phenomenonTime = result.get("time").get
//        resultTime = result.get("time").get
//      }
//      val gisUtil = new GISUtil
//      val wkt = gisUtil.DoubleToWKT(minX, minY, maxX, maxY)
//      val geom = "ST_GeomFromText('" + wkt + "', 4326)"
//      println(minX, maxX, minY, maxY)
//
//      //ingest to dimensional table in postgresql
//      val postgresqlService = new PostgresqlService
//
//      //get maxProductId and maxProductKey
//      var productKey = postgresqlService.getMaxProductKey
//      if (productKey == null) productKey = 0
//      var productId = postgresqlService.getMaxProductId
//      if (productId == null) productId = 0
//
//      //get maxFactkey and maxTileId
//      var factId = postgresqlService.getMaxFactId("gc_tabular_tile_fact")
//      if (factId == null) factId = 0
//      var factKey = postgresqlService.getMaxFactKey("gc_tabular_tile_fact")
//      if (factKey == null) factKey = 0
//
//      productId = productId + 1
//      productKey = productKey + 1
//
//      //insert to product table
//      val insertProduct = postgresqlService.insertProduct(productId, productKey, productName, productIdentity, "Tabular", phenomenonTime, resultTime, geom, maxX, minX, maxY, minY)
//      if (insertProduct)
//        println("Insert to product table successfully!")
//
//      //insert to fact table
//      gridWithTabularIdsRdd.zipWithIndex().foreachPartition{partition =>
//        val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
//        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
//        val postgresqlService = new PostgresqlService
//        val gcTabularTileFacts = new ArrayBuffer[TabularGridFact]()
//        val _productName = productName
//        val _productKey = productKey
//        partition.foreach{record =>
//          val gridZorderCode = record._1._1.toString
//          val rowkeys = record._1._2.map(x => _productName + "_" + x)
//          val _extentKey = GcExtent.getExtentKey(statement, gridZorderCode, "999")
//          val _factId: Int = (factId + 1 + record._2).toInt
//          val _factKey: Int = (factKey + 1 + record._2).toInt
//          gcTabularTileFacts.append(new TabularGridFact(_factId, _factKey, _productKey, _extentKey, rowkeys.toList.toString()))
//        }
//
//        println(gcTabularTileFacts.size)
//        if(gcTabularTileFacts.size() > 0){
//          val insertTabularFact = postgresqlService.insertFact(statement, gcTabularTileFacts.toList, tableName = "gc_tabular_tile_fact")
//          if (!insertTabularFact) {
//            throw new RuntimeException("fact insert failed!")
//          }
//        }
//        conn.close()
//      }
//      println("Insert to fact table successfully!")
//
//      //ingest to data table in hbase
//      tabularRecordRDD.foreachPartition{partition =>
//        partition.foreach{ele =>
//          val uuid = ele.id
//          val rowKey = productName + "_" + uuid
//
//          val attributesMap = ele.getAttributes
//          val attributesJson = GISUtil.toJson(attributesMap)
//
//          //val attributesMap = GISUtil.toMap[String](json)
//          //val mutableSymbolMap = GISUtil.fromJson[collection.mutable.Map[Symbol,Seq[Int]]](json)
//
//          HbaseUtil.insertData("hbase_tabular", rowKey, "tabularData", "tile", attributesJson.getBytes("utf-8"))
//          //HbaseUtil.insertData("hbase_tabular", rowKey, "tabularData", "tileMetaData", tileMetaData.getBytes("utf-8"))
//        }
//      }
//
//    }finally {
//      sc.stop()
//    }
//  }
//
//  def ingestTabularBICube(inputPath: String, productName: String): Unit = {
//    val file = Source.fromFile(inputPath)
//    val attributeArr = file.getLines().next().split(" ")
//
//    val tabularRecords = new ArrayBuffer[Map[String, String]]()
//    for(line <- file.getLines) {
//      val valueArr = line.split(" ")
//      var result: Map[String, String] = Map()
//      (0 until attributeArr.length).foreach { i =>
//        result += (attributeArr(i) -> valueArr(i))
//      }
//      tabularRecords.append(result)
//    }
//
//    val postgresqlService = new PostgresqlService
//    for(tabularRecord <- tabularRecords){
//      val locationName = tabularRecord.get("geo_name").get
//      val timeValue = tabularRecord.get("time").get
//      val malePopulation = tabularRecord.get("male").get.toDouble.toInt
//      val femalePopulation = tabularRecord.get("female").get.toDouble.toInt
//      val households = tabularRecord.get("households").get.toDouble.toInt
//
//      val locationKey = postgresqlService.insertLocationBI("bi_location", locationName)
//      val timeKey = postgresqlService.insertTimeBI("bi_time", timeValue)
//      val genderMaleKey = postgresqlService.insertGenderBI("bi_gender", "male")
//      val genderFemaleKey = postgresqlService.insertGenderBI("bi_gender", "female")
//
//      if(!postgresqlService.insertFactBI("bi_village_fact", locationKey, timeKey, genderMaleKey, malePopulation, households))
//        throw new RuntimeException("BI fact insert failed: locationKey=" + locationKey + ", timeKey=" + timeKey + ", genderKey=" + genderMaleKey)
//      if(!postgresqlService.insertFactBI("bi_village_fact", locationKey, timeKey, genderFemaleKey, femalePopulation, households))
//        throw new RuntimeException("BI fact insert failed: locationKey=" + locationKey + ", timeKey=" + timeKey + ", genderKey=" + genderFemaleKey)
//    }
//
//  }
//
//  def main(args: Array[String]): Unit = {
//    //ingestTabular("/home/geocube/data/tabular/mz_Village.txt", "Hainan_Daguangba_Village_Tabular", "EO")
//    //ingestTabularEOCube("E:/TabularData/Hainan_Daguangba/mz_Village.txt", "Hainan_Daguangba_Village_Tabular")
//    ingestTabularBICube("E:/TabularData/Hainan_Daguangba/mz_Village.txt", "Hainan_Daguangba_Village_Tabular")
//  }
//}
