package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.util.HbaseUtil._
import whu.edu.cn.util.PostgresqlUtil
import java.sql.ResultSet
import java.util.UUID

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object Feature {
  def load(implicit sc: SparkContext, productName: String = null): RDD[(String,(Geometry, Map[String, Any]))] ={
    //RowKey  ProductKey_Geohash_ID
    val a : RDD[(String,(Geometry, Map[String, Any]))] = null

    //用户的定义的对象：用户ID_随机数
    //新几何对象：用户ID_随机数
    val metaData = query(productName)
    println(metaData)
    val geometryRdd = sc.makeRDD(metaData).map(t=>t.replace("List(", "").replace(")", "").split(","))
      .flatMap(t=>t)
      .map(t=>(t, getVector(t)))
    geometryRdd
  }

  def query(productName: String = null): ListBuffer[String] = {
    val metaData = ListBuffer.empty[String]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection()
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select oge_data_resource_product.name, oge_vector_fact.fact_data_ids from oge_vector_fact join oge_data_resource_product " +
          "on oge_vector_fact.product_key= oge_data_resource_product.id where "
        if(productName != "" && productName != null) {
          sql ++= "name = " + "'" + productName + "'"
        }
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())


        while (extentResults.next()) {
          val factDataIDs = extentResults.getString("fact_data_ids")
          metaData.append(factDataIDs)
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
    metaData
  }
  def getVector(rowKey: String):(Geometry, Map[String, Any]) = {
    val meta = getVectorMeta("OGE_Vector_Fact_Table", rowKey, "vectorData", "metaData")
    val cell = getVectorCell("OGE_Vector_Fact_Table", rowKey, "vectorData", "geom")
    //println(meta)
    //println(cell)
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

  /**
    * create a Point, and take the point in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def point(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.point(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a LineString, and take the LineString in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def lineString(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.lineString(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a LinearRing, and take the LinearRing in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def linearRing(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.linearRing(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a Polygon, and take the Polygon in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def polygon(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.polygon(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a MultiPoint, and take the MultiPoint in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def multiPoint(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.multiPoint(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a MultiLineString, and take the MultiLineString in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def multiLineString(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.multiLineString(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * create a MultiPolygon, and take the MultiPolygon in RDD
    *
    * @param sc used to create RDD
    * @param coors coordinates to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def multiPolygon(implicit sc: SparkContext, coors:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.multiPolygon(coors,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  /**
    * transform json to Map[k,v]
    *
    * @param json the json to operate
    * @return
    */
  def getMapFromJsonStr(json:String):Map[String,Any]={
    val jsonObject = JSON.parseObject(json)
    val map = Map.empty[String, Any]
    val sIterator = jsonObject.keySet.iterator
    while (sIterator.hasNext()) {
      val key = sIterator.next();
      val value = jsonObject.getString(key);
      map += (key -> value)
    }
    map
  }

  /**
    * get area of the feature RDD.
    *
    * @param featureRDD the feature RDD to compute
    * @param crs the crs for compute area
    * @return
    */
  def area(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:3857"):Double={
    featureRDD.map(t=>Geometry.area(t._2._1,crs)).reduce((x,y)=>x+y)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)
    val geomRDD=load(sc, "Hubei_ADM_City_Vector")
    val property="{\"name\":\"test\",\"city\":\"wuhan\",\"area\":23}"
    val p=point(sc,"[119.283461766823521,35.113845473433457]",property)
    p.map(t=>{
      println(t._1)
      println(t._2._1.toText)
      println(t._2._2)
    }).count()
    println(area(p))
  }
}
