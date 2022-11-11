package whu.edu.cn.application.oge

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

object Feature {
  def load(): Unit ={
    //RowKey  ProductKey_Geohash_ID
    val a : RDD[(String,(Geometry, Map[String, Any]))] = null

    //用户的定义的对象：用户ID_随机数
    //新几何对象：用户ID_随机数
  }
}
