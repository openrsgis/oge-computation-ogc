package whu.edu.cn.algorithms.SpatialStats.Utils

import org.apache.spark.SparkContext
import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{pow, sqrt}

object FeatureDistance {

  /**
   * 输入RDD，获得Array[(Double, Double)]
   */
  def getCoorXY(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Array[(Double, Double)] = {
    val coorxy = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y)
    }).collect()
    coorxy
  }

  def pointDist(x1: (Double, Double), x2: (Double, Double)): Double = {
    sqrt(pow(x1._1 - x2._1, 2) + pow(x1._2 - x2._2, 2))
  }


  def getDist(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Array[Array[Double]] = {
    val rddcoor = inputRDD.map(t => t._2._1.getCentroid.getCoordinate).collect()
    rddcoor.map(t1 => {
      rddcoor.map(t2 => t2.distance(t1))
    })
  }

  /**
   * 输入两个Array[(Double, Double)]，输出他们之间的距离，结果为Array
   *
   * @param arr1 Array[(Double, Double)]的形式
   * @param arr2 Array[(Double, Double)]的形式
   * @return Array[Double] 结果Array应按顺序存储了Arr1第i个坐标和Arr2每个坐标的距离
   */
  def arrayDist(arr1: Array[(Double, Double)], arr2: Array[(Double, Double)]): Array[Array[Double]] = {
    arr1.map(t1=>{
      arr2.map(t2=>pointDist(t2,t1))
    })
  }

  def coorDist(sourceCoor: RDD[Coordinate], targetCoor: RDD[Coordinate]): RDD[Array[Double]] = {
    sourceCoor.map(t => {
      targetCoor.map(t2 => t2.distance(t)).collect()
    })
  }

  def getDistMat(inputshp: RDD[(String, (Geometry, mutable.Map[String, Any]))]): DenseMatrix[Double] = {
    val dist=getDist(inputshp)
    val dmat = new DenseMatrix(dist.length, dist(0).length, dist.flatten)
    dmat.t
  }

  def getArrDistDmat(Arr1: Array[(Double, Double)], Arr2: Array[(Double, Double)]): DenseMatrix[Double] = {
    val arrdist = arrayDist(Arr1, Arr2)
    val dmat= new DenseMatrix(Arr2.length, Arr1.length, arrdist.flatten)
    dmat.t
  }

}