package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.util.HbaseUtil._
import whu.edu.cn.util.PostgresqlUtil

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object Feature {
  def load(implicit sc: SparkContext, productName: String = null): Unit ={
    //RowKey  ProductKey_Geohash_ID
    val a : RDD[(String,(Geometry, Map[String, Any]))] = null

    //用户的定义的对象：用户ID_随机数
    //新几何对象：用户ID_随机数
    val metaData = query(productName)
    val geometryRdd = sc.makeRDD(metaData).map(t=>t.replace("List(", "").replace(")", "").split(","))
      .flatMap(t=>t)
      .map(t=>(productName, getVector(t))).count()
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
    println(meta)
    println(cell)
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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)
    load(sc, "Hubei_ADM_City_Vector")
  }
}
