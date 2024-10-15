package whu.edu.cn.oge

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, Point}
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.geocube.util.HbaseUtil._

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4
import geotrellis.raster.{DoubleCellType, DoubleConstantNoDataCellType, MultibandTile, PixelIsPoint, Raster, RasterExtent, Tile, TileLayout, mask}
import geotrellis.vector
import geotrellis.vector.interpolation.{NonLinearSemivariogram, Semivariogram, Spherical}
import geotrellis.vector.{Extent, PointFeature, interpolation}
import org.geotools.referencing.CRS
import geotrellis.spark._
import geotrellis.raster.interpolation._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.rasterize.Rasterizer.Options
import geotrellis.raster.render.{ColorRamp, ColorRamps}
import io.minio.GetObjectArgs
import whu.edu.cn.geocube.core.entity
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.{Log, LogFactory}
import whu.edu.cn
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.debug.CoverageDubug.makeTIFF
import whu.edu.cn.debug.FeatureDebug.saveFeatureRDDToShp
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}
import whu.edu.cn.util.{ClientUtil, PostSender, PostgresqlUtil}

import java.nio.file.Paths
import com.baidubce.services.bos.model.GetObjectRequest
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME

import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.io.Source
import scala.math.{max, min}
import scala.sys.process._

object Feature {
  def load(implicit sc: SparkContext, productName: String = null, dataTime: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    var crs1 = crs
    if (crs == null)
      crs1 = "EPSG:4326"
    val t1 = System.currentTimeMillis()
    val queryRes = query(productName)
    val t2 = System.currentTimeMillis()
    println("从pgsql查询元数据的时间：" + (t2 - t1))
    val metaData = queryRes._3
    val hbaseTableName = queryRes._2
    val productKey = queryRes._1
    //    println(metaData)
    //    println(hbaseTableName)
    //    println(productKey)
    //    if(dataTime==null){
    //      val geometryRdd = sc.makeRDD(metaData).map(t=>t.replace("List(", "").replace(")", "").split(","))
    //        .flatMap(t=>t)
    //        .map(t=>(t, getVectorWithRowkey(hbaseTableName,t)))
    //      geometryRdd.map(t=>{
    //        t._2._1.setSRID(crs.split(":")(1).toInt)
    //        t
    //      })
    //    }
    var prefix = ""
    if (dataTime == null) {
      prefix = productKey
    }
    else {
      prefix = productKey + "_" + dataTime
    }
    val t3 = System.currentTimeMillis()
    val geometryRdd = getVectorWithPrefixFilter(sc, hbaseTableName, prefix).map(t => {
      t._2._1.setSRID(crs1.split(":")(1).toInt)
      t
    })
    val t4 = System.currentTimeMillis()
    println("从hbase加载数据的时间：" + (t4 - t3))
    geometryRdd
  }

  //返回productKey,hbaseTableName,rowkeyList
  def query(productName: String = null): (String, String, ListBuffer[String]) = {
    val metaData = ListBuffer.empty[String]
    var hbaseTableName = ""
    var productKey = ""
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select oge_data_resource_product.name, oge_vector_fact.fact_data_ids, oge_vector_fact.table_name, oge_vector_fact.product_key " +
          "from oge_vector_fact join oge_data_resource_product " +
          "on oge_vector_fact.product_key= oge_data_resource_product.id where "
        if (productName != "" && productName != null) {
          sql ++= "name = " + "'" + productName + "'"
        }
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())


        while (extentResults.next()) {
          val factDataIDs = extentResults.getString("fact_data_ids")
          metaData.append(factDataIDs)
          productKey = extentResults.getString("product_key")
          hbaseTableName = extentResults.getString("table_name")
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
    (productKey, hbaseTableName, metaData)
  }

  def getVectorWithRowkey(hbaseTableName: String, rowKey: String): (Geometry, Map[String, Any]) = {
    val t1 = System.currentTimeMillis()
    val meta = getVectorMeta(hbaseTableName, rowKey, "vectorData", "metaData")
    val cell = getVectorCell(hbaseTableName, rowKey, "vectorData", "geom")
    //    println(meta)
    //    println(cell)
    println(cell)
    val t2 = System.currentTimeMillis()
    println("根据rowkey取数据时间：" + (t2 - t1) / 1000)
    val jsonObject = JSON.parseObject(meta)
    val properties = jsonObject.getJSONArray("properties").getJSONObject(0)
    val propertiesOut = Map.empty[String, Any]
    val sIterator = properties.keySet.iterator
    while (sIterator.hasNext()) {
      val key = sIterator.next();
      val value = properties.getString(key);
      propertiesOut += (key -> value)
    }
    val reader = new WKTReader()
    val geometry = reader.read(cell)
    (geometry, propertiesOut)
  }

  def getVectorWithPrefixFilter(implicit sc: SparkContext, hbaseTableName: String, prefix: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    val t1 = System.currentTimeMillis()
    val queryData = getVectorWithPrefix(hbaseTableName, prefix)
    println(queryData.length)
    val t2 = System.currentTimeMillis()
    println("取数据执行了！")
    println("getVectorWithPrefix时间：" + (t2 - t1))
    val rawRDD = sc.parallelize(queryData)
    rawRDD.map(t => {
      val rowkey = t._1
      val geomStr = t._2._1
      val meta = t._2._2
      val userData = t._2._3
      val reader = new WKTReader()
      val geometry = reader.read(geomStr)
      val jsonobject: JSONObject = JSON.parseObject(meta)
      val prop = jsonobject.getJSONArray("properties").getJSONObject(0).toString
      val metaMap = getMapFromJsonStr(prop)
      val userMap = getMapFromJsonStr(userData)
      (rowkey, (geometry, metaMap ++ userMap))
    })
  }

  /**
   * create a Point, and take the point in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def point(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.point(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromStr(properties)))
    })
  }

  private def getMapFromStr(str: String): Map[String, Any] = {
    val map = Map.empty[String, Any]
    val keyValuePairs = str.stripPrefix("{").stripSuffix("}").split(',')
    keyValuePairs.foreach { pair =>
      val keyValue = pair.split(':')
      if (keyValue.length == 2) {
        val key = keyValue(0).trim
        val value = keyValue(1).trim
        map += (key -> value)
      }
    }
    map
  }

  /**
   * create a LineString, and take the LineString in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def lineString(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.lineString(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromStr(properties)))
    })
  }

  /**
   * create a LinearRing, and take the LinearRing in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def linearRing(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.linearRing(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromJsonStr(properties)))
    })
  }

  /**
   * create a Polygon, and take the Polygon in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def polygon(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.polygon(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromStr(properties)))
    })
  }

  /**
   * create a MultiPoint, and take the MultiPoint in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def multiPoint(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.multiPoint(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromJsonStr(properties)))
    })
  }

  /**
   * create a MultiLineString, and take the MultiLineString in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def multiLineString(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.multiLineString(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromJsonStr(properties)))
    })
  }

  /**
   * create a MultiPolygon, and take the MultiPolygon in RDD
   *
   * @param sc         used to create RDD
   * @param coors      coordinates to create geometry
   * @param properties properties for geometry,it is a json String
   * @param crs        projection of geometry
   * @return
   */
  def multiPolygon(implicit sc: SparkContext, coors: String, properties: String = null, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom = Geometry.multiPolygon(coors, crs)
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromJsonStr(properties)))
    })
  }

  /**
   * create a geometry, and take the geometry in RDD
   *
   * @param sc         used to create RDD
   * @param gjson      to create geometry
   * @param crs        projection of geometry
   * @return
   */

  def geometry(implicit sc: SparkContext, gjson: String, crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    //异常处理，报错信息统一格式
    val escapedJson = gjson.replace("\\", "")//去除转义符
    val jsonobject: JSONObject = JSON.parseObject(escapedJson)
    val array = jsonobject.getJSONArray("features")
    var list: List[(Geometry, String)] = List.empty
    for (i <- 0 until (array.size())) {
      val geom = Geometry.geometry(array.getJSONObject(i), crs)
      list = list :+ (geom, array.getJSONObject(i).get("properties").toString)
    }
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t._1, getMapFromJsonStr(t._2)))
    })
  }

  def feature(implicit sc: SparkContext, geom: Geometry, properties: String = null): RDD[(String, (Geometry, Map[String, Any]))] = {
    var list: List[Geometry] = List.empty
    list = list :+ geom
    val geomRDD = sc.parallelize(list)
    geomRDD.map(t => {
      (UUID.randomUUID().toString, (t, getMapFromJsonStr(properties)))
    })
  }

  def featureCollection(implicit sc: SparkContext, featureList: List[RDD[(String, (Geometry, Map[String, Any]))]]): RDD[(String, (Geometry, Map[String, Any]))] = {
    val len = featureList.length
    var featureCollectionRDD = featureList(0)
    for (a <- 1 until len) {
      featureCollectionRDD = featureCollectionRDD.union(featureList(a))
    }
    featureCollectionRDD
  }

  /**
   * transform json to Map[k,v]
   *
   * @param json the json to operate
   * @return
   */
  def getMapFromJsonStr(json: String): Map[String, Any] = {
    val map = Map.empty[String, Any]
    if (StringUtils.isNotEmpty(json) && !json.equals("")) {
      val jsonObject = JSON.parseObject(json)
      val sIterator = jsonObject.keySet.iterator
      while (sIterator.hasNext()) {
        val key = sIterator.next()
        val value = jsonObject.get(key)
        map += (key -> value)
      }
    }
    map
  }

  /**
   * get area of the feature RDD.
   *
   * @param featureRDD the feature RDD to compute
   * @param crs        the crs for compute area
   * @return
   */
  def area(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:3857"): String = {
    featureRDD.map(t => Geometry.area(t._2._1, crs)).collect().toList.mkString(",")
  }

  /**
   * Returns the bounding rectangle of the geometry.
   *
   * @param featureRDD the featureRDD to operate
   * @param crs        If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def bounds(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.bounds(t._2._1, crs), t._2._2))
    })
  }

  /**
   * Returns the centroid of geometry
   *
   * @param featureRDD the featureRDD to operate
   * @param crs        If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def centroid(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.centroid(t._2._1, crs), t._2._2))
    })
  }

  /**
   * Returns the input buffered by a given distance.
   *
   * @param featureRDD the featureRDD to operate
   * @param distance   The distance of the buffering, which may be negative. If no projection is specified, the unit is meters.
   * @param crs        If specified, the buffering will be performed in this projection and the distance will be interpreted as units of the coordinate system of this projection.
   * @return
   */
  def buffer(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], distance: Double, crs: String = "EPSG:3857"): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.buffer(t._2._1, distance, crs), t._2._2))
    })
  }

  /**
   * Returns the convex hull of the given geometry.
   *
   * @param featureRDD featureRDD to operate
   * @param crs        If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def convexHull(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.convexHull(t._2._1, crs), t._2._2))
    })
  }

  /**
   * Returns a GeoJSON-style array of the geometry's coordinates
   *
   * @param featureRDD the featureRDD to operate
   * @return
   */
  def coordinates(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    val result: List[Array[Coordinate]] =featureRDD.map(t => {
      t._2._1.getCoordinates
    }).collect().toList

    result.flatten.map(coordinate => s"(${coordinate.x}, ${coordinate.y})").mkString(", ")
  }

  /**
   * Transforms the geometry to a specific projection.
   *
   * @param featureRDD the featureRDD to operate
   * @param tarCrsCode target CRS
   * @return
   */
  def reproject(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], tarCrsCode: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.reproject(t._2._1, tarCrsCode), t._2._2))
    })
  }

  /**
   * Returns whether the geometry is unbounded.
   * if false, the geometry has no boundary; if true, the geometry has boundary
   * only 0 dimension geometry has no boundary
   *
   * @param featureRDD
   * @return
   */
  def isUnbounded(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    featureRDD.map(t => {
      if (t._2._1.getBoundaryDimension < 0)
        false
      else
        true
    }).collect().toList.mkString
  }

  /**
   * Returns the GeoJSON type of the geometry.
   *
   * @param featureRDD the featureRDD to opreate
   * @return
   */
  def getType(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    featureRDD.map(t => {
      t._2._1.getGeometryType
    }).collect().toList.mkString
  }

  /**
   * Returns the projection of the geometry.
   *
   * @param featureRDD the featureRDD to operate
   * @return
   */
  def projection(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    featureRDD.map(t => {
      t._2._1.getSRID
    }).collect().toList.mkString
  }

  /**
   * Returns a GeoJSON string representation of the geometry.
   *
   * @param featureRDD the featureRDD to operate
   * @return
   */
  def toGeoJSONString(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    val data = featureRDD.map(t => {
      (Geometry.toGeoJSONString(t._2._1), t._2._2)
    }).collect().toList
    val jsonObject = new JSONObject
    val geoArray = new JSONArray()
    for (feature <- data) {
      val combinedObject = new JSONObject()
      val coors = JSON.parseObject(feature._1)
      val pro = new JSONObject()
      feature._2.foreach(x => {
        pro.put(x._1, x._2)
      })
      combinedObject.put("type", "Feature")
      combinedObject.put("geometry", coors)
      combinedObject.put("properties", pro)
      geoArray.add(combinedObject)
    }

    jsonObject.put("type", "FeatureCollection")
    jsonObject.put("features", geoArray)
    val geoJSONString: String = jsonObject.toJSONString()
    geoJSONString
  }

  def saveJSONToServer(geoJSONString: String): String = {
    val time = System.currentTimeMillis()
    val host = GlobalConfig.QGISConf.QGIS_HOST
    val userName = GlobalConfig.QGISConf.QGIS_USERNAME
    val password = GlobalConfig.QGISConf.QGIS_PASSWORD
    val port = GlobalConfig.QGISConf.QGIS_PORT

    val outputVectorPath = s"${GlobalConfig.Others.jsonSavePath}vector_${time}.json"


    // 创建PrintWriter对象
    val writer: BufferedWriter = new BufferedWriter(new FileWriter(outputVectorPath))

    // 写入JSON字符串
    writer.write(geoJSONString)

    // 关闭PrintWriter
    writer.close()

    versouSshUtil(host, userName, password, port)

    val st = s"scp  $outputVectorPath root@${GlobalConfig.Others.tomcatHost}:/home/oge/tomcat/apache-tomcat-8.5.57/webapps/oge_vector/vector_${time}.json"

    //本地测试使用代码
    //      val exitCode: Int = st.!
    //      if (exitCode == 0) {
    //        println("SCP command executed successfully.")
    //      } else {
    //        println(s"SCP command failed with exit code $exitCode.")
    //      }

    runCmd(st, "UTF-8")
    println(s"st = $st")

    val storageURL = s"http://${GlobalConfig.Others.tomcatHost_public}/tomcat-vector/vector_" + time + ".json"

    storageURL
  }

  /**
   * get length of the feature RDD.
   *
   * @param featureRDD the feature RDD to compute
   * @param crs        the crs for compute length
   * @return
   */
  def length(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:3857"): String = {
    featureRDD.map(t => {
      Geometry.length(t._2._1, crs)
    }).collect().toList.mkString
  }

  /**
   * Returns the list of geometries in a GeometryCollection, or a singleton list of the geometry for single geometries.
   *
   * @param featureRDD the featureRDD to operate
   * @return
   */
  //TODO:未写入json文件中
  def geometries(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): RDD[(String, (List[Geometry], Map[String, Any]))] = {
    featureRDD.map(t => {
      var geomList: List[Geometry] = List.empty
      val geomNum = t._2._1.getNumGeometries
      var i = 0
      for (i <- 0 until geomNum) {
        geomList = geomList :+ t._2._1.getGeometryN(i)
      }
      (UUID.randomUUID().toString, (geomList, t._2._2))
    })
  }

  /**
   * Computes the union of all the elements of this geometry.
   * only for GeometryCollection
   *
   * @param featureRDD the featureRDD to operate
   * @param crs        If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def dissolve(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (UUID.randomUUID().toString, (Geometry.dissolve(t._2._1, crs), t._2._2))
    })
  }

  /**
   * Returns true iff one geometry contains the other.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def contains(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
               featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.contains(geom1, geom2, crs).toString
  }

  /**
   * Returns true iff one geometry is contained in the other.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def containedIn(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                  featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.containedIn(geom1, geom2, crs).toString
  }

  /**
   * Returns true iff the geometries are disjoint.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def disjoint(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
               featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.disjoint(geom1, geom2, crs).toString
  }

  /**
   * Returns the minimum distance between two geometries.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
   * @return
   */
  def distance(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
               featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:3857"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.distance(geom1, geom2, crs).toString
  }

  /**
   * Returns the result of subtracting the 'right' geometry from the 'left' geometry.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def difference(implicit sc: SparkContext, featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                 featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    val res = Geometry.difference(geom1, geom2, crs)
    feature(sc, res)
  }

  /**
   * Returns the intersection of the two geometries.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def intersection(implicit sc: SparkContext, featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                   featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    val res = Geometry.intersection(geom1, geom2, crs)
    feature(sc, res)
  }

  /**
   * Returns true iff the geometries intersect.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def intersects(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                 featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.intersects(geom1, geom2, crs).toString
  }

  /**
   * Returns the symmetric difference between two geometries.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def symmetricDifference(implicit sc: SparkContext, featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                          featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    val res = Geometry.symmetricDifference(geom1, geom2, crs)
    feature(sc, res)
  }

  /**
   * Returns the union of the two geometries.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def union(implicit sc: SparkContext, featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
            featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], crs: String = "EPSG:4326"): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    val res = Geometry.union(geom1, geom2, crs)
    feature(sc, res)
  }

  /**
   * Returns true iff the geometries are within a specified distance.
   * This is for geometry defined by user
   *
   * @param featureRDD1 the left featureRDD, it only has 1 element
   * @param featureRDD2 the right featureRDD, it only has 1 element
   * @param distance    he distance threshold.
   * @param crs         The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def withDistance(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                   featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], distance: Double, crs: String = "EPSG:3857"): String = {
    val geom1 = featureRDD1.first()._2._1
    val geom2 = featureRDD2.first()._2._1
    Geometry.withDistance(geom1, geom2, distance, crs).toString
  }

  /**
   * Copies metadata properties from one element to another.
   *
   * @param featureRDD1 The object whose properties to override. it only has 1 element
   * @param featureRDD2 The object from which to copy the properties. it only has 1 element
   * @param properties  The properties to copy. If omitted, all properties are copied.
   * @return
   */
  def copyProperties(featureRDD1: RDD[(String, (Geometry, Map[String, Any]))],
                     featureRDD2: RDD[(String, (Geometry, Map[String, Any]))], properties: List[String] = null): RDD[(String, (Geometry, Map[String, Any]))] = {
    var destnation = featureRDD1.first()._2._2
    var source = featureRDD2.first()._2._2
    if (properties == null || properties.isEmpty)
      destnation = destnation ++ source
    else {
      for (property <- properties)
        destnation += (property -> source(property))
    }
    featureRDD1.map(t => (t._1, (t._2._1, destnation)))
  }

  /**
   * Extract a property from a feature.
   *
   * @param featureRDD The feature to extract the property from.
   * @param property   The property to extract.
   * @return
   */
  def get(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[Any] = {
    featureRDD.map(t => t._2._2(property)).collect().toList
  }

  /**
   * Extract a property from a feature. Return a number
   *
   * @param featureRDD The feature to extract the property from.
   * @param property   The property to extract.
   * @return
   */
  def getNumber(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[Double] = {
    featureRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect().toList
  }

  /**
   * Extract a property from a feature. Return a string
   *
   * @param featureRDD The feature to extract the property from.
   * @param property   The property to extract.
   * @return
   */
  def getString(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[String] = {
    featureRDD.map(t => t._2._2(property).asInstanceOf[String]).collect().toList
  }

  /**
   * Extract a property from a feature. Return a array, the element of array is String
   *
   * @param featureRDD The feature to extract the property from.
   * @param property   The property to extract.
   * @return
   */
  def getArray(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[Array[String]] = {
    featureRDD.map(t => {
      t._2._2(property).asInstanceOf[String].replace("[", "").replace("]", "").split(",")
    }).collect().toList
  }

  /**
   * Returns the names of properties on this element.
   *
   * @param featureRDD the featureRDD to operate
   * @return
   */
  def propertyNames(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    featureRDD.map(t => t._2._2.keySet.toList).collect().toList.mkString(",")
  }

  /**
   * Overrides one or more metadata properties of an Element.
   *
   * @param featureRDD the featureRDD to operate
   * @param property   the property to set. it is a json
   * @return
   */
  def set(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    featureRDD.map(t => {
      (t._1, (t._2._1, t._2._2 ++ getMapFromStr(property)))
    })
  }

  /**
   * Returns the feature, with the geometry replaced by the specified geometry.
   *
   * @param featureRDD the featureRDD to operate
   * @param geom       the property to set. it is a json
   * @return
   */
  def setGeometry(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                  geom: RDD[(String, (Geometry, Map[String, Any]))]): RDD[(String, (Geometry, Map[String, Any]))] = {
    val geometry = geom.first()._2._1
    featureRDD.map(t => {
      (t._1, (geometry, t._2._2))
    })
  }

  /**
   * TODO: fix bug
   * TODO: 简单克里金插值(奇异矩阵报错), modelType参数未使用，是否需要输入掩膜
   *
   * @param featureRDD
   * @param propertyName
   * @param modelType
   * @return
   */
  //块金：nugget，基台：sill，变程：range
  def simpleKriging(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, Map[String, Any]))], propertyName: String, modelType: String) = {
    //Kriging Interpolation for PointsRDD
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val rows = 256
    val cols = 256
    val rasterExtent = RasterExtent(extent, cols, rows)
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      //TODO 直接转换为Double类型会报错
      //      var data = t._2._2(propertyName).asInstanceOf[Double]
      var data = t._2._2(propertyName).asInstanceOf[String].toDouble
      if (data < 0) {
        data = 100
      }
      PointFeature(p, data)
    }).collect()
    println()
    val sv: Semivariogram = NonLinearSemivariogram(points, 30000, 0, Spherical)
    val method = new SimpleKrigingMethods {
      //TODO fix bug 简单克里金插值(奇异矩阵报错)
      override def self: Traversable[PointFeature[Double]] = points
    }
    val originCoverage = method.simpleKriging(rasterExtent, sv)


    val tl = TileLayout(10, 10, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    //TODO cellType的定义
    val cellType = originCoverage.cellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    originCoverage.toArrayTile()
    var list: List[Tile] = List.empty
    list = list :+ originCoverage
    val tileRDD = sc.parallelize(list)
    val imageRDD = tileRDD.map(t => {
      val k = cn.entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t)
      (k, v)
    })

    (imageRDD, tileLayerMetadata)
  }

  /**
   * 反距离加权插值
   *
   * @param sc
   * @param featureRDD
   * @param propertyName
   * @param maskGeom
   * @return
   */
  def inverseDistanceWeighted(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                              propertyName: String, maskGeom: RDD[(String, (Geometry, Map[String, Any]))]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val t1 = System.currentTimeMillis()
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val t2 = System.currentTimeMillis()
    println("获取点数据extent的时间：" + (t2 - t1) / 1000)
    println("extent:" + extent)
    val rows = 256
    val cols = 256
    val rasterExtent = RasterExtent(extent, cols, rows)
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      //      val p=vector.Point(t._2._1.getCoordinate)
      val p = t._2._1.asInstanceOf[Point]
      var data = t._2._2(propertyName).asInstanceOf[String].toDouble
      if (data < 0) {
        data = 100
      }
      PointFeature(p, data)
    }).collect()
    val t3 = System.currentTimeMillis()
    println("构建PointFeature[]的时间：" + (t3 - t2) / 1000)
    val rasterTile = InverseDistanceWeighted(points, rasterExtent)
    val t4 = System.currentTimeMillis()
    println("调用Geotrellis提供的IDW函数的时间：" + (t4 - t3) / 1000)
    println("初步空间插值的时间（结果未剪裁）：" + (t4 - t1) / 1000)

    //    val maskPolygon=maskGeom.map(t=>t._2._1).reduce((x,y)=>{Geometry.union(x,y)})
    val maskPolygon = maskGeom.map(t => t._2._1).first()
    val t5 = System.currentTimeMillis()
    println("从RDD获取中国国界的时间：" + (t5 - t4) / 1000)
    val maskRaster = rasterTile.mask(maskPolygon)

    //    maskRaster.tile.renderPng(ColorRamps.BlueToOrange).write("D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage2\\code2\\testMask.png")
    val t6 = System.currentTimeMillis()
    println("裁剪结果的时间：" + (t6 - t5) / 1000)
    //maskRaster.tile.renderPng(ColorRamps.BlueToOrange).write("D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage2\\code2\\testMask.png")


    val tl = TileLayout(1, 1, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    //TODO cellType的定义
    val cellType = maskRaster.tile.cellType
    //      val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    var list: List[Tile] = List.empty
    list = list :+ maskRaster.tile
    val tileRDD = sc.parallelize(list)

    val imageRDD = tileRDD.map(t => {
      val k = cn.entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t)
      (k, v)
    })
    val t7 = System.currentTimeMillis()
    println("构造ImageRDD时间：" + (t7 - t6) / 1000)
    println("完整空间插值函数的时间：" + (t7 - t1) / 1000)
    (imageRDD, tileLayerMetadata)
  }


  /**
   * 根据给定的属性值，对featureRDD进行栅格化
   *
   * @param featureRDD
   * @param propertyName
   * @return
   */
  def rasterize(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], propertyName: String) = {

    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      //      val coorArray = t._2._1.getCoordinates
      //      for (item <- coorArray) {
      //
      //      }
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)


    //TODO:tileLayOut的定义
    val tl = TileLayout(1, 1, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    //TODO cellType的定义
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)


    val featureRDDforRaster: RDD[vector.Feature[Geometry, Double]] = featureRDD.map(t => {
      //TODO 直接转换为Double类型会报错
      //val data = t._2._2(propertyName).asInstanceOf[Double]
      val data = t._2._2(propertyName).asInstanceOf[String].toDouble
      val feature = new vector.Feature[Geometry, Double](t._2._1, data)
      feature
    })
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)


    //    //TODO 栅格化（测试）
    //    val rasterRDD = featureRDD.map { case (featureId, (geometry, properties)) =>
    //      val rasterizedData = Rasterizer.rasterize(geometry, RasterExtent(extent,256,256)){
    //        (x: Int, y: Int) => properties.get(propertyName) match{
    //          case Some(value: String) => properties.asInstanceOf[String].toInt// Use the property value as the raster value
    //          case _ => 5000 // Set Nodata value for pixels without the property value
    //        }
    //      }
    //      (featureId, rasterizedData)
    //    }


    val imageRDD = originCoverage.map(t => {
      val k = cn.entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })

    (imageRDD, tileLayerMetadata)
  }

  def visualize(feature: RDD[(String, (Geometry, Map[String, Any]))],color:List[String],attribute:String): Unit = {
    val geoJson = new JSONObject
    val render = new JSONObject
    val geoJSONString = toGeoJSONString(feature)
    val url = saveJSONToServer(geoJSONString)
    geoJson.put(Trigger.layerName, url)
    val colorArray = new JSONArray()
    for (st <- color) {
      colorArray.add(st)
    }
    render.put("color", colorArray)
    render.put("attribute", attribute)
    geoJson.put("render", render)
    PostSender.shelvePost("vector",geoJson)
  }


  // 下载用户上传的geojson文件
  def loadFeatureFromUpload(implicit sc: SparkContext, featureId: String, userID: String, dagId: String, crs: String = "EPSG:4326"): (RDD[(String, (Geometry, Map[String, Any]))]) = {
    var path: String = new String()
    if (featureId.endsWith(".geojson")) {
      path = s"${userID}/$featureId"
    } else {
      path = s"$userID/$featureId.geojson"
    }


    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}_${Trigger.file_id}.geojson"
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    clientUtil.Download(path, filePath)
    println(s"Download $filePath")
    val temp = Source.fromFile(filePath).mkString
    val feature = geometry(sc, temp,crs)
    feature
  }


}
