package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.util.HbaseUtil._
import whu.edu.cn.util.PostgresqlUtil
import java.sql.ResultSet
import java.util.UUID

import org.geotools.referencing.CRS

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
    * create a geometry, and take the geometry in RDD
    *
    * @param sc used to create RDD
    * @param gjson to create geometry
    * @param properties properties for geometry
    * @param crs projection of geometry
    * @return
    */
  def geometry(implicit sc: SparkContext, gjson:String, properties:String=null, crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    val geom=Geometry.geometry(gjson,crs)
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  def feature(implicit sc: SparkContext,geom:Geometry, properties:String=null):RDD[(String,(Geometry, Map[String, Any]))]={
    var list:List[Geometry]=List.empty
    list=list:+geom
    val geomRDD=sc.parallelize(list)
    geomRDD.map(t=>{
      (UUID.randomUUID().toString,(t,getMapFromJsonStr(properties)))
    })
  }

  def featureCollection(implicit sc: SparkContext,featureList:List[(Geometry, Map[String, Any])]):RDD[(String,(Geometry, Map[String, Any]))]={
    val featureRDD=sc.parallelize(featureList)
    featureRDD.map(t=>{
      (UUID.randomUUID().toString,t)
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

  /**
    * Returns the bounding rectangle of the geometry.
    *
    *
    * @param featureRDD the featureRDD to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def bounds(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{
      (UUID.randomUUID().toString,(Geometry.bounds(t._2._1,crs),t._2._2))
    })
  }

  /**
    * Returns the centroid of geometry
    *
    * @param featureRDD the featureRDD to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def centroid(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{(UUID.randomUUID().toString,(Geometry.centroid(t._2._1,crs),t._2._2))})
  }

  /**
    * Returns the input buffered by a given distance.
    *
    * @param featureRDD the featureRDD to operate
    * @param distance The distance of the buffering, which may be negative. If no projection is specified, the unit is meters.
    * @param crs If specified, the buffering will be performed in this projection and the distance will be interpreted as units of the coordinate system of this projection.
    * @return
    */
  def buffer(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],distance:Double,crs:String="EPSG:3857"):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{
      (UUID.randomUUID().toString,(Geometry.buffer(t._2._1,distance,crs),t._2._2))
    })
  }

  /**
    * Returns the convex hull of the given geometry.
    *
    * @param featureRDD featureRDD to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def convexHull(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{(UUID.randomUUID().toString,(Geometry.convexHull(t._2._1,crs),t._2._2))})
  }

  /**
    *Returns a GeoJSON-style array of the geometry's coordinates
    *
    * @param featureRDD the featureRDD to operate
    * @return
    */
  def coordinates(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[Array[Coordinate]]={
    featureRDD.map(t=>{t._2._1.getCoordinates})
  }

  /**
    * Transforms the geometry to a specific projection.
    *
    * @param featureRDD the featureRDD to operate
    * @param tarCrsCode target CRS
    * @return
    */
  def reproject(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],tarCrsCode:String):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{(UUID.randomUUID().toString,(Geometry.reproject(t._2._1,tarCrsCode),t._2._2))})
  }

  /**
    * Returns whether the geometry is unbounded.
    * if false, the geometry has no boundary; if true, the geometry has boundary
    * only 0 dimension geometry has no boundary
    *
    * @param featureRDD
    * @return
    */
  def isUnbounded(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[Boolean]={
    featureRDD.map(t=>{
      if(t._2._1.getBoundaryDimension<0)
        false
      else
        true
    })
  }

  /**
    * Returns the GeoJSON type of the geometry.
    *
    * @param featureRDD the featureRDD to opreate
    * @return
    */
  def getType(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[String]={
    featureRDD.map(t=>{t._2._1.getGeometryType})
  }

  /**
    * Returns the projection of the geometry.
    *
    * @param featureRDD the featureRDD to operate
    * @return
    */
  def projection(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[Int]={
    featureRDD.map(t=>{t._2._1.getSRID})
  }

  /**
    *Returns a GeoJSON string representation of the geometry.
    *
    * @param featureRDD the featureRDD to operate
    * @return
    */
  def toGeoJSONString(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[String]={
    featureRDD.map(t=>{Geometry.toGeoJSONString(t._2._1)})
  }

  /**
    * get length of the feature RDD.
    *
    * @param featureRDD the feature RDD to compute
    * @param crs the crs for compute length
    * @return
    */
  def getLength(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:3857"):RDD[Double]={
    featureRDD.map(t=>{Geometry.length(t._2._1,crs)})
  }

  /**
    * Returns the list of geometries in a GeometryCollection, or a singleton list of the geometry for single geometries.
    *
    * @param featureRDD the featureRDD to operate
    * @return
    */
  def geometries(featureRDD:RDD[(String,(Geometry, Map[String, Any]))]):RDD[(String,(List[Geometry], Map[String, Any]))]={
    featureRDD.map(t=>{
      var geomList:List[Geometry]=List.empty
      val geomNum=t._2._1.getNumGeometries
      var i=0
      for(i <- 0 until  geomNum){
        geomList=geomList:+t._2._1.getGeometryN(i)
      }
      (UUID.randomUUID().toString,(geomList,t._2._2))
    })
  }

  /**
    * Computes the union of all the elements of this geometry.
    * only for GeometryCollection
    *
    * @param featureRDD the featureRDD to operate
    * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
    * @return
    */
  def dissolve(featureRDD:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):RDD[(String,(Geometry, Map[String, Any]))]={
    featureRDD.map(t=>{(UUID.randomUUID().toString,(Geometry.dissolve(t._2._1,crs),t._2._2))})
  }

  /**
    * Returns true iff one geometry contains the other.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def contains(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
               featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Boolean={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.contains(geom1,geom2,crs)
  }

  /**
    * Returns true iff one geometry is contained in the other.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def containedIn(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
               featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Boolean={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.containedIn(geom1,geom2,crs)
  }

  /**
    * Returns true iff the geometries are disjoint.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def disjoint(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
                  featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Boolean={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.disjoint(geom1,geom2,crs)
  }

  /**
    * Returns the minimum distance between two geometries.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
    * @return
    */
  def distance(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
               featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:3857"):Double={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.distance(geom1,geom2,crs)
  }

  /**
    * Returns the result of subtracting the 'right' geometry from the 'left' geometry.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def difference(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
                 featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Geometry={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.difference(geom1,geom2,crs)
  }

  /**
    * Returns the intersection of the two geometries.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def intersection(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
                   featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Geometry={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.intersection(geom1,geom2,crs)
  }

  /**
    * Returns true iff the geometries intersect.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def intersects(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
                 featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Boolean={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.intersects(geom1,geom2,crs)
  }

  /**
    * Returns the symmetric difference between two geometries.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def symmetricDifference(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
                          featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Geometry={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.symmetricDifference(geom1,geom2,crs)
  }

  /**
    * Returns the union of the two geometries.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def union(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
            featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],crs:String="EPSG:4326"):Geometry={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.union(geom1,geom2,crs)
  }

  /**
    * Returns true iff the geometries are within a specified distance.
    * This is for geometry defined by user
    *
    * @param featureRDD1 the left featureRDD, it only has 1 element
    * @param featureRDD2 the right featureRDD, it only has 1 element
    * @param distance he distance threshold.
    * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
    * @return
    */
  def withDistance(featureRDD1:RDD[(String,(Geometry, Map[String, Any]))],
    featureRDD2:RDD[(String,(Geometry, Map[String, Any]))],distance:Double,crs:String="EPSG:3857"):Boolean={
    val geom1=featureRDD1.first()._2._1
    val geom2=featureRDD2.first()._2._1
    Geometry.withDistance(geom1,geom2,distance,crs)
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
