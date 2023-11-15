package whu.edu.cn.algorithms.SpatialStats.STCorrelations


import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import breeze.linalg.{DenseVector, Matrix, ranks}
import whu.edu.cn.oge.Feature._

import scala.collection.mutable
import scala.math.{abs, max, min, pow, sqrt}

object CorrelationAnalysis {

  /** Correlation matrix for properties
   *
   * @param featureRDD   shapefile RDD
   * @param properties properties
   * @param split      properties split, default: ","
   * @param method     pearson or spearman
   * @return correlation matrix
   */
  def corrMat(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], properties: String, method: String = "pearson", split: String = ","): String = {
    val propertyArr = properties.split(split)
    val n = propertyArr.length
    var cor = new Array[Array[Double]](n)
    val arrList = propertyArr.map(p => featureRDD.map(t => t._2._2(p).asInstanceOf[String].toDouble).collect())
    if (method == "pearson") {
      cor = arrList.map(t => {
        arrList.map(t2 => pcorr2arr(t2, t))
      })
    } else if (method == "spearman") {
      cor = arrList.map(t => {
        //        arrList.map(t2 => spcorr2arr(t2, t))
        arrList.map(t2 => pcorr2arr(ranks(DenseVector(t2)), ranks(DenseVector(t))))
      })
    } else {
      throw new IllegalArgumentException("only support person and spearman correlation now")
    }
    //    cor.map(t=>t.foreach(println))
    val corrMat = Matrix.create(rows = n, cols = n, data = cor.flatten)
    var outStr = s"$method correlation result:\n"
    //    println(s"$method correlation result:")
    //    propertyArr.foreach(t => printf("%-20s\t", t))
    //    print("\n")
    //    println(corrMat)
    propertyArr.foreach(t => outStr += s"    \t$t\t    ")
    outStr += "\n" + corrMat.toString()
    println(outStr)
    //    corrMat
    outStr
  }

  /**
   * 对lst1和lst2两组数据进行求解得到之间的pearson相关性
   *
   * @param lst1 : List[Double]的形式
   * @param lst2 : List[Double]的形式
   * @return Double 结果correlation为两组数据之间的相关性
   */
  def pcorr2arr(lst1: Array[Double], lst2: Array[Double]): Double = {
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

  def spcorr2arr(lst1: Array[Double], lst2: Array[Double]): Double = {
    val rank1 = ranks(DenseVector(lst1)).zipWithIndex
    val rank2 = ranks(DenseVector(lst2))
    val righttop = 6.0 * rank1.map(t => (t._1 - rank2(t._2)) * (t._1 - rank2(t._2))).sum
    val rightdown = lst1.length * (lst1.length * lst1.length - 1.0)
    1.0 - righttop / rightdown
  }

}