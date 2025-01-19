package whu.edu.cn.algorithms.SpatialStats.STSampling

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{max, min}
import scala.util.Random

object Sampling {

  def randomSampling(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], n: Int = 10): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val feat = featureRDD.collect()
    val nCounts = featureRDD.count().toInt
    //      val extents = featureRDD.map(t => t._2._1.getCoordinate).map(t => {
    //        (t.x, t.y, t.x, t.y)
    //      }).reduce((coor1, coor2) => {
    //        (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    //      })

    val rand = Array.fill(n)(Random.nextDouble()).map(t => (t * nCounts).toInt)
    val arrbuf = ArrayBuffer.empty[(String, (Geometry, mutable.Map[String, Any]))]
    for (i <- 0 until n) {
      arrbuf += feat(rand(i))
    }
    arrbuf.foreach(t=>println(t._2._2))
    sc.makeRDD(arrbuf)
  }

  def regularSampling(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], x: Int = 10, y : Int = 10): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val feat = featureRDD.collect()
    val nCounts = featureRDD.count().toInt
    val extents = featureRDD.map(t => t._2._1.getCoordinate).map(t => {
      (t.x, t.y, t.x, t.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val dvd=max(x,y)

//    val sortx=featureRDD.sortBy(t=>t._2._1.getCoordinate.x)
//    val sorty=featureRDD.sortBy(t=>t._2._1.getCoordinate.y)
//    featureRDD.collect().foreach(t => println(t._2._2))
    println(extents)
    println("*************")
//    sortx.collect().foreach(t => println(t._2._2))
    val dx=(extents._3-extents._1)/x+1e-5
    val dy=(extents._4-extents._2)/y+1e-5
    println(dx)
    var ox=1
    var oy=1
    if(x>=y){
      ox=x
    }else{
      oy=y
    }
    val groups=featureRDD.groupBy(t=>{
      ((t._2._1.getCoordinate.x-extents._1)/dx).toInt*ox + ((t._2._1.getCoordinate.y-extents._2)/dy).toInt*oy
    }).mapValues(t=>t.toArray)
    groups.foreach(println)
    val ig=groups.map(t=>{
      t._2(Random.nextInt(t._2.length))
    })
    ig.foreach(t=>println(t._2._2))

    val arrbuf=ArrayBuffer.empty[(String, (Geometry, mutable.Map[String, Any]))]
//    arrbuf.foreach(t=>println(t._2._2))
    sc.makeRDD(arrbuf)
  }

  //现在问题是，有可能有一个分组抽到2次同样的点，应该也无所谓吧？
  def stratifiedSampling(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, n:Int=10): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val groups = featureRDD.groupBy(t=>{
      t._2._2(property).asInstanceOf[String]
    }).mapValues(t=>t.toArray)

    val reArr=ArrayBuffer.empty[RDD[(String, (Geometry, mutable.Map[String, Any]))]]
    for(i<-1 to n){
      val res = groups.map(t => {
        t._2(Random.nextInt(t._2.length))
      })
      res.foreach(t=>println(t._2._2))
      reArr+=res
    }
    val mergedRDD = reArr.reduce((rdd1, rdd2) => rdd1.union(rdd2))
    println("**************")
    mergedRDD.foreach(t=>println(t._2._2))
    mergedRDD
  }


  def randomPoints(xmin: Double, ymin: Double, xmax: Double, ymax: Double, np: Int): Array[(Double, Double)] = {
    Array.fill(np)(Random.nextDouble(), Random.nextDouble()).map(t => (t._1 * (xmax - xmin) + xmin, t._2 * (ymax - ymin) + ymin))
  }

  def oneSampling(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val indexArray = inputRDD.zipWithIndex
    val upperBound = inputRDD.count().toInt
    val randomNumber = Random.nextInt(upperBound)
    val filteredRDD = indexArray.filter { case (_, idx) => idx == randomNumber }.map(_._1)

    filteredRDD
  }

  def continuousSampling(inputRDD: RDD[(String, (Geometry, Map[String, Any]))], gap: Double): RDD[(String, (Geometry, Map[String, Any]))] = {
    val indexArray = inputRDD.zipWithIndex
    val len = indexArray.count()
    val upperBound = len.toInt
    val array = Array.range(0, upperBound)
    val multiplesOfGap = array.filter(_ % gap == 0)
    val filteredRDD = multiplesOfGap.map(index => indexArray.filter { case (_, idx) => idx == index }.map(_._1))
    val mergedRDD = filteredRDD.reduce((rdd1, rdd2) => rdd1.union(rdd2))

    mergedRDD
  }

}
