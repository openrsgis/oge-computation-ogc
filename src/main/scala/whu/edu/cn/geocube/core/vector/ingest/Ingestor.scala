package whu.edu.cn.geocube.core.vector.ingest

import geotrellis.vector.Extent
import java.io.File
import java.nio.charset.Charset
import java.sql.{DriverManager, ResultSet}
import java.util
import java.util.List

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom._
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.geojson.feature.FeatureJSON
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import whu.edu.cn.geocube.core.cube.vector.{GeoObjectRDD, SpatialRDD}
import whu.edu.cn.geocube.core.entity.{GcExtent, VectorGridFact}
import whu.edu.cn.geocube.core.vector.grid.{GridConf, GridTransformer}
import whu.edu.cn.geocube.util.{GISUtil, HbaseUtil, PostgresqlService}
import whu.edu.cn.util.PostgresqlUtil

/**
 * In the GeoCube, vector data is segmented into tiles logically based on a global grid tessellation,
 * which means vector data are grouped into a grid if intersected, maintaining whose topology structure.
 *
 * This class is used to generate logical vector tile for analysis only, not supporting vector tile for visualization yet.
 *
 * The metadata of tile is stored in PostgreSQL based on a fact constellation schema,
 * while vector data in each tile are stored in HBase.
 *
 * */
object Ingestor {
  implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Ingest vector feature data, containing geometry and attribute information.
   * Support shp and json format.
   *
   * @param inputPath Vector file path
   * @param productName Product name of the vector data
   * @param phenomenonTime Acquired time
   * @param resultTime Processing time
   * @return
   */
  def ingestFeature(inputPath: String, productName: String, phenomenonTime: String, resultTime: String): Unit ={
    val suffix = inputPath.substring(inputPath.lastIndexOf("."))
    if (suffix.equals("shp")) //support shapefile format
      ingestFeatureShp(inputPath, productName, phenomenonTime, resultTime)
    else if (suffix.contains("json")) //support fjson format
      ingestFeatureJson(inputPath, productName, phenomenonTime, resultTime)
    else
      throw new RuntimeException("Not support " + suffix)
  }

  /**
   * Ingest vector feature data, containing geometry and attribute information.
   * Support shp format.
   *
   * @param inputPath Shapefile path
   * @param productName Product name of the vector data
   * @param phenomenonTime Acquired time
   * @param resultTime Processing time
   * @return
   */
  def ingestFeatureShp(inputPath: String, productName: String, phenomenonTime: String, resultTime: String): Unit = {
    val conf = new SparkConf()
      .setAppName("Vector Ingestion Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.rpc.message.maxSize", "512")
    val sc = new SparkContext(conf)

    try {
      val file: File = new File(inputPath)
      val fileName = file.getName
      val pureFileName = fileName.substring(0, fileName.indexOf("."))

      //read as GeoObjectRDD
      val geoObjectRdd: GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromShp(sc, inputPath, 16).cache()

      //initialize grid layout and get RDD[(gridZCode, GeomIds)]
      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdsRdd: RDD[(Long, Iterable[String])] = geoObjectRdd.flatMap(x => GridTransformer.groupGeomId2ZCgrid(x, gridConf)).groupByKey()

      //calculate layer extent and transform to wkt format
      val dataStoreFactory = new ShapefileDataStoreFactory()
      val sds = dataStoreFactory.createDataStore(new File(inputPath).toURI.toURL)
        .asInstanceOf[ShapefileDataStore]
      sds.setCharset(Charset.forName("GBK"))
      val featureSource = sds.getFeatureSource()
      val layerExtent = featureSource.getBounds
      val maxx = layerExtent.getMaxX
      val minx = layerExtent.getMinX
      val maxy = layerExtent.getMaxY
      val miny = layerExtent.getMinY
      val gisUtil = new GISUtil
      val wkt = gisUtil.DoubleToWKT(minx, miny, maxx, maxy)
      val geom = "ST_GeomFromText('" + wkt + "', 4326)"
      println(minx, maxx, miny, maxy)
      sds.dispose()

      //ingest to dimensional table in postgresql
      val postgresqlService = new PostgresqlService

      //get maxProductId and maxProductKey
      var productKey = postgresqlService.getMaxProductKey
      if (productKey == null) productKey = 0
      var productId = postgresqlService.getMaxProductId
      if (productId == null) productId = 0

      //get maxFactkey and maxTileId
      var factId = postgresqlService.getMaxVectorFactId
      if (factId == null) factId = 0
      var factKey = postgresqlService.getMaxVectorFactKey
      if (factKey == null) factKey = 0
      productId = productId + 1
      productKey = productKey + 1

      //insert to product table
      val insertProduct = postgresqlService.insertProduct(productId, productKey, productName, pureFileName, phenomenonTime, resultTime, geom, maxx, minx, maxy, miny)
      if (insertProduct)
        println("Insert to product table successfully!")

      //insert to fact table
      gridWithGeomIdsRdd.zipWithIndex().foreachPartition{partition =>
        val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val postgresqlService = new PostgresqlService
        val gcVectorTileFacts = new ArrayBuffer[VectorGridFact]()
        val _productName = productName
        val _productKey = productKey
        partition.foreach{record =>
          val gridZorderCode = record._1._1.toString
          val rowkeys = record._1._2.map(x => _productName + "_" + x)
          val _extentKey = GcExtent.getExtentKey(statement, gridZorderCode, "999")
          val _factId: Int = (factId + 1 + record._2).toInt
          val _factKey: Int = (factKey + 1 + record._2).toInt
          gcVectorTileFacts.append(new VectorGridFact(_factId, _factKey, _productKey, _extentKey, rowkeys.toList.toString()))
        }
        val insertVectorFact = postgresqlService.insertFact(statement, gcVectorTileFacts.toList)
        if (!insertVectorFact) {
          throw new RuntimeException("fact insert failed!")
        }
        conn.close()
      }
      println("Insert to fact table successfully!")

      //ingest to data table in hbase
      val _allExtent = GcExtent.getAllExtent("999")
      geoObjectRdd.foreachPartition{partition =>
        val allExtent = _allExtent
        partition.foreach{ele =>
          val uuid = ele.id
          val rowKey = productName + "_" + uuid

          val feature = ele.feature
          val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
          val wktValue = geom.toString
          val feajson = new FeatureJSON
          val metaData = feajson.toString(feature)

          val zorderCodes = GridTransformer.getGeomZcodes(ele, gridConf)
          val tilesMetaData = zorderCodes.map(x => allExtent.get(x.toString)).mkString("[", ", ", "]")

          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "tile", wktValue.getBytes("utf-8"))
          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "metaData", metaData.getBytes("utf-8"))
          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "tilesMetaData", tilesMetaData.getBytes("utf-8"))
        }
      }
    }finally {
      sc.stop()
    }

  }

  /**
   * Under developing.
   *
   * @param inputPathGeojson
   * @param productName
   * @param phenomenon_time
   * @param result_time
   */
  def ingestFeatureJson(inputPathGeojson: String, productName: String, phenomenon_time: String, result_time: String): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Vector Divide Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      val file: File = new File(inputPathGeojson)
      val fileName = file.getName
      val pureFileName = fileName.substring(0, fileName.indexOf("."))
      val rdd: GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromFeatureJson(sc, inputPathGeojson, 8)

      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdListRDD = rdd.flatMap(x => GridTransformer.groupGeom2ZCgrid(x, gridConf)).groupByKey()
      val mbr = rdd.map(x => {("key", x.feature.getBounds)}).groupByKey().map(x => GISUtil.getVectorExtent(x)).collect()
      val simpleMbr = mbr(0)
      val maxx = simpleMbr._1
      val minx = simpleMbr._2
      val maxy = simpleMbr._3
      val miny = simpleMbr._4

      val gisUtil = new GISUtil
      val wkt = gisUtil.DoubleToWKT(minx, miny, maxx, maxy)
      val geom = "ST_GeomFromText('" + wkt + "', 4326)"
      val gridWithGeomIdListScala = gridWithGeomIdListRDD.collect()
      val postgresqlService = new PostgresqlService
      var product_key = postgresqlService.getMaxProductKey
      if (product_key == null) {
        product_key = 0
      }
      var product_id = postgresqlService.getMaxProductId
      if (product_id == null) {
        product_id = 0
      }
      var factId = postgresqlService.getMaxVectorFactId
      if (factId == null) {
        factId = 0
      }
      var factKey = postgresqlService.getMaxVectorFactKey
      if (factKey == null) {
        factKey = 0
      }
      product_id = product_id + 1
      product_key = product_key + 1
      val insertProduct = postgresqlService.insertProduct(product_id, product_key, productName, pureFileName, phenomenon_time, result_time, geom, maxx, minx, maxy, miny)
      if (insertProduct) {
        println("Insert to product table successfully!")
      }
      var geomIdExtentsHashMap = new scala.collection.mutable.HashMap[String, ArrayBuffer[String]]
      var gcVectorTileFacts: List[VectorGridFact] = new util.LinkedList[VectorGridFact]()
      for (i <- 0 until gridWithGeomIdListScala.length) {
        val grid = gridWithGeomIdListScala(i)._1
        val geomList = gridWithGeomIdListScala(i)._2
        val gcExtent: GcExtent = GcExtent.getExtent(grid.toString, "999")
        val extentKey = gcExtent.getExtentKey

        val gcExtentStr = gcExtent.transToString
        var rowkeys = new ArrayBuffer[String]
        geomList.iterator.foreach(geom => {
          val uuid = geom.id
          val rowkey = productName + "_" + uuid
          rowkeys.add(rowkey)
          if (geomIdExtentsHashMap.contains(uuid)) {
            var extentArray = geomIdExtentsHashMap.get(uuid).get
            extentArray += gcExtentStr
            geomIdExtentsHashMap += (uuid -> extentArray)
          } else {
            var extentArray = ArrayBuffer[String]()
            extentArray += gcExtentStr
            geomIdExtentsHashMap += (uuid -> extentArray)
          }
        })
        factId = factId + 1
        factKey = factKey + 1
        val gcVectorTileFact = new VectorGridFact(factId, factKey, product_key, extentKey, rowkeys.toList.toString())
        gcVectorTileFacts.add(gcVectorTileFact)
      }
      val insertVectorFact = postgresqlService.insertFact(gcVectorTileFacts)
      if (insertVectorFact) {
        println("Insert to fact table successfully!")
      }
      rdd.foreach(element => {
        val uuid = element.id
        val feature = element.feature
        val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val wktValue = geom.toString
        val rowKey = productName + "_" + uuid
        val feajson = new FeatureJSON
        val metaData = feajson.toString(feature)
        val tilesMetaData = geomIdExtentsHashMap.get(uuid).get.toArray.mkString("[", ", ", "]")
        HbaseUtil.insertData("hbase_vector", rowKey, "vectorData", "tile", wktValue.getBytes("utf-8"))
        HbaseUtil.insertData("hbase_vector", rowKey, "vectorData", "metaData", metaData.getBytes("utf-8"))
        HbaseUtil.insertData("hbase_vector", rowKey, "vectorData", "tilesMetaData", tilesMetaData.getBytes("utf-8"))
      })
    } finally {
      sc.stop()
    }
  }

  /**
   * Ingest vector geometry data, containing geometry information only, not include attribute.
   * Support shp format.
   *
   * @param inputPath Shapefile path
   * @param productName Product name of the vector data
   * @param phenomenonTime Acquired time
   * @param resultTime Processing time
   * @return
   */
  def ingestGeometryShp(inputPath: String, productName: String, phenomenonTime: String, resultTime: String): Unit = {
    val conf = new SparkConf()
      .setAppName("Vector Ingestion Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.rpc.message.maxSize", "512")
    val sc = new SparkContext(conf)

    try {
      val file: File = new File(inputPath)
      val fileName = file.getName
      val pureFileName = fileName.substring(0, fileName.indexOf("."))

      //read as SpatialRDD
      val geomRdd: RDD[(String, Geometry)] = SpatialRDD.createSpatialRDDFromShp(sc, inputPath, 16)
        .map(x=>(x._1, x._2))
        .cache()

      //initialize grid layout and get RDD[(gridZCode, GeomIds)]
      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdsRdd: RDD[(Long, Iterable[String])] = geomRdd.flatMap(x => GridTransformer.groupGeomId2ZCgrid(x, gridConf)).groupByKey()

      //calculate layer extent and transform to wkt format
      val dataStoreFactory = new ShapefileDataStoreFactory()
      val sds = dataStoreFactory.createDataStore(new File(inputPath).toURI.toURL)
        .asInstanceOf[ShapefileDataStore]
      sds.setCharset(Charset.forName("GBK"))
      val featureSource = sds.getFeatureSource()
      val layerExtent = featureSource.getBounds
      val maxx = layerExtent.getMaxX
      val minx = layerExtent.getMinX
      val maxy = layerExtent.getMaxY
      val miny = layerExtent.getMinY
      val gisUtil = new GISUtil
      val wkt = gisUtil.DoubleToWKT(minx, miny, maxx, maxy)
      val geom = "ST_GeomFromText('" + wkt + "', 4326)"
      println(minx, maxx, miny, maxy)
      sds.dispose()

      //ingest to dimensional table in postgresql
      val postgresqlService = new PostgresqlService

      //get maxProductId and maxProductKey
      var product_key = postgresqlService.getMaxProductKey
      if (product_key == null) product_key = 0
      var product_id = postgresqlService.getMaxProductId
      if (product_id == null) product_id = 0

      //get maxFactkey and maxTileid
      var factId = postgresqlService.getMaxVectorFactId
      if (factId == null) factId = 0
      var factKey = postgresqlService.getMaxVectorFactKey
      if (factKey == null) factKey = 0
      product_id = product_id + 1
      product_key = product_key + 1

      //insert to product table
      val insertProduct = postgresqlService.insertProduct(product_id, product_key, productName, pureFileName, phenomenonTime, resultTime, geom, maxx, minx, maxy, miny)
      if (insertProduct) {
        println("Insert to product table successfully!")
      }

      //insert to fact table
      gridWithGeomIdsRdd.zipWithIndex().foreachPartition{partition =>
        val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val postgresqlService = new PostgresqlService
        val gcVectorTileFacts = new ArrayBuffer[VectorGridFact]()
        val _productName = productName
        val _productKey = product_key
        partition.foreach{record =>
          val gridZorderCode = record._1._1.toString
          val rowkeys = record._1._2.map(x => _productName + "_" + x)
          val _extentKey = GcExtent.getExtentKey(statement, gridZorderCode, "999")
          val _factId: Int = (factId + 1 + record._2).toInt
          val _factKey: Int = (factKey + 1 + record._2).toInt
          gcVectorTileFacts.append(new VectorGridFact(_factId, _factKey, _productKey, _extentKey, rowkeys.toList.toString()))
        }
        val insertVectorFact = postgresqlService.insertFact(statement, gcVectorTileFacts.toList)
        if (!insertVectorFact) {
          throw new RuntimeException("fact insert failed!")
        }
        conn.close()
      }
      println("Insert to fact table successfully!")

      //ingest to data table in hbase
      geomRdd.foreachPartition{partition =>
        val postgresqlService = new PostgresqlService
        val allExtent = GcExtent.getAllExtent("999")
        partition.foreach{ele =>
          val uuid = ele._1
          val rowKey = productName + "_" + uuid
          val geom = ele._2
          val wktValue = geom.toString
          val metaData = "1"
          val zorderCodes = GridTransformer.getGeomZcodes(ele, gridConf)

          val tilesMetaData = zorderCodes.map(x => allExtent.get(x.toString)).mkString("[", ", ", "]")

          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "tile", wktValue.getBytes("utf-8"))
          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "metaData", metaData.getBytes("utf-8"))
          HbaseUtil.insertData("hbase_vector_aigis", rowKey, "vectorData", "tilesMetaData", tilesMetaData.getBytes("utf-8"))
        }
      }
    }finally {
      sc.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    ingestGeometryShp(args(0), args(1), args(2), args(3))
  }

}
