package whu.edu.cn.algorithms.SpatialStats.STCorrelations


import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import breeze.linalg.DenseMatrix
import whu.edu.cn.oge.Feature._

import scala.collection.mutable
import scala.math.{abs, max, min, pow, sqrt}

object CorrelationAnalysis {

  def corrMat(inputshp: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyArr: Array[String], method: String = "pearson"): DenseMatrix[Double] = {
    val n = propertyArr.length
    var cor = new Array[Array[Double]](n)
    if (method == "pearson") {
      val arrList = propertyArr.map(t => getNumber(inputshp, t))
      //    arrList.foreach(println)
      //如果是皮尔逊相关系数才可以用这个
      cor = arrList.map(t => {
        arrList.map(t2 => corr2list(t2, t))
      })
    } else {
      throw new IllegalArgumentException("only support person correlation now")
    }
    //    cor.map(t=>t.foreach(println))
    val corrMat = DenseMatrix.create(rows = n, cols = n, data = cor.flatten)
    println(s"$method correlation result:")
    propertyArr.foreach(t => printf("%-20s\t", t))
    print("\n")
    println(corrMat)
    corrMat
  }

  /**
   * 对lst1和lst2两组数据进行求解得到之间的pearson相关性
   *
   * @param lst1 : List[Double]的形式
   * @param lst2 : List[Double]的形式
   * @return Double 结果correlation为两组数据之间的相关性
   */
  def corr2list(lst1: List[Double], lst2: List[Double]): Double = {
    val sum1 = lst1.sum
    val sum2 = lst2.sum
    val square_sum1 = lst1.map(x => x * x).sum
    val square_sum2 = lst2.map(x => x * x).sum
    val zlst = lst1.zip(lst2)
    val product = zlst.map(x => x._1 * x._2).sum
    val numerator = product - (sum1 * sum2 / lst1.length)
    val dominator = pow((square_sum1 - pow(sum1, 2) / lst1.length) * (square_sum2 - pow(sum2, 2) / lst2.length), 0.5)
    val correlation = numerator / (dominator * 1.0)
    correlation
  }
}