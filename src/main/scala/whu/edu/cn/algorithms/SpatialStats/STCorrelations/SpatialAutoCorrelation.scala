package whu.edu.cn.algorithms.SpatialStats.STCorrelations

import breeze.linalg._
import breeze.plot._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils.showPng

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.{pow, sqrt}

object SpatialAutoCorrelation {

  /**
   * 输入RDD直接计算全局莫兰指数
   *
   * @param featureRDD     RDD
   * @param property    要计算的属性，String
   * @param plot        bool类型，是否画散点图，默认为否(false)
   * @param test        是否进行测试(计算P值等)
   * @param weightstyle 邻接矩阵的权重类型，参考 getNeighborWeight 函数
   * @return 全局莫兰指数，峰度
   */
  def globalMoranI(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, plot: Boolean = false, test: Boolean = false, weightstyle: String = "W"): String = {
    val nb_weight = getNeighborWeight(featureRDD, weightstyle)
    val sum_weight = sumWeight(nb_weight)
    val arr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect()
    val arr_mean = meandiff(arr)
    val arr_mul = arr_mean.map(t => {
      val re = new Array[Double](arr_mean.length)
      for (i <- 0 until arr_mean.length) {
        re(i) = t * arr_mean(i)
      }
      DenseVector(re)
    })
    val weight_m_arr = arrdvec2multi(nb_weight.collect(), arr_mul)
    val rightup = weight_m_arr.map(t => sum(t)).sum
    val rightdn = arr_mean.map(t => t * t).sum
    val n = arr.length
    val moran_i = n / sum_weight * rightup / rightdn
    val kurtosis = (n * arr_mean.map(t => pow(t, 4)).sum) / pow(rightdn, 2)
    if (plot) {
      plotmoran(arr, nb_weight, moran_i)
    }
    var outStr=s"global Moran's I is: ${moran_i.formatted("%.4f")}\n"
    outStr += s"kurtosis is: ${kurtosis.formatted("%.4f")}\n"
    if (test) {
      val E_I = -1.0 / (n - 1)
      val S_1 = 0.5 * nb_weight.map(t => sum(t * 2.0 * t * 2.0)).sum()
      val S_2 = nb_weight.map(t => sum(t) * 2).sum()
      val E_A = n * ((n * n - 3 * n + 3) * S_1) - n * S_2 + 3 * sum_weight * sum_weight
      val E_B = (arr_mean.map(t => t * t * t * t).sum / (rightdn * rightdn)) * ((n * n - n) * S_1 - 2 * n * S_2 + 6 * sum_weight * sum_weight)
      val E_C = (n - 1) * (n - 2) * (n - 3) * sum_weight * sum_weight
      val V_I = (E_A - E_B) / E_C - pow(E_I, 2)
      val Z_I = (moran_i - E_I) / sqrt(V_I)
      val gaussian = breeze.stats.distributions.Gaussian(0, 1)
      val Pvalue = 2 * (1.0 - gaussian.cdf(Z_I))
      //      println(s"global Moran's I is: $moran_i")
      //      println(s"Z-Score is: $Z_I , p-value is: $Pvalue")
      outStr += s"Z-Score is: ${Z_I.formatted("%.4f")} , "
      outStr += s"p-value is: ${Pvalue.formatted("%.4g")}"
    }
    println(outStr)
    outStr
    //    (moran_i, kurtosis)
  }

  /**
   * 输入RDD直接计算局部莫兰指数
   *
   * @param featureRDD  RDD
   * @param property 要计算的属性，String
   * @param adjust   是否调整n的取值。false(默认):n；true:n-1
   * @return RDD内含局部莫兰指数和预测值等
   */
  def localMoranI(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, adjust: Boolean = false):
  RDD[(String, (Geometry, Map[String, Any]))] = {
    val nb_weight = getNeighborWeight(featureRDD)
    val arr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect()
    val arr_mean = meandiff(arr)
    val arr_mul = arr_mean.map(t => {
      val re = arr_mean.clone()
      DenseVector(re)
    })
    val weight_m_arr = arrdvec2multi(nb_weight.collect(), arr_mul)
    val rightup = weight_m_arr.map(t => sum(t))
    val dvec_mean = DenseVector(arr_mean)
    var n = arr.length
    if (adjust) {
      n = arr.length - 1
    }
    val s2 = arr_mean.map(t => t * t).sum / n
    val lz = DenseVector(rightup)
    val z = dvec_mean
    val m2 = sum(dvec_mean * dvec_mean) / n

    val expectation = -z * z / ((n - 1) * m2)
    val local_moranI = z / s2 * lz

    val wi = DenseVector(nb_weight.map(t => sum(t)).collect())
    val wi2 = DenseVector(nb_weight.map(t => sum(t * t)).collect())
    //    val b2=((z*z*z*z).sum/ n)/ (s2*s2)
    //    val A= (n - b2) / (n - 1)
    //    val B= (2 * b2 - n) / ((n - 1) * (n - 2))
    //    val var_I = A * wi2 + B * (wi*wi - wi2) - expectation*expectation
    val var_I = (z / m2) * (z / m2) * (n / (n - 2.0)) * (wi2 - (wi * wi / (n - 1.0))) * (m2 - (z * z / (n - 1.0)))
    val Z_I = (local_moranI - expectation) / var_I.map(t => sqrt(t))
    val gaussian = breeze.stats.distributions.Gaussian(0, 1)
    val pv_I = Z_I.map(t => 2 * (1.0 - gaussian.cdf(t)))
    val featRDDidx = featureRDD.collect().zipWithIndex
    featRDDidx.map(t => {
      t._1._2._2 += ("local_moranI" -> local_moranI(t._2))
      t._1._2._2 += ("expectation" -> expectation(t._2))
      t._1._2._2 += ("local_var" -> var_I(t._2))
      t._1._2._2 += ("local_z" -> Z_I(t._2))
      t._1._2._2 += ("local_pv" -> pv_I(t._2))
    })
    //    (local_moranI.toArray, expectation.toArray, var_I.toArray, Z_I.toArray, pv_I.toArray)
    sc.makeRDD(featRDDidx.map(_._1))
  }


  def plotmoran(x: Array[Double], w: RDD[DenseVector[Double]], morani: Double): Unit = {
    val xx = x
    val wx = w.map(t => t dot DenseVector(x)).collect()
    val f = Figure()
    val p = f.subplot(0)
    p += plot(xx, wx, '+')
    val xxmean = DenseVector.ones[Double](x.length) :*= (xx.sum / xx.length)
    val wxmean = DenseVector.ones[Double](x.length) :*= (wx.sum / wx.length)
    val xxy = linspace(wx.min - 2, wx.max + 2, x.length)
    val wxy = linspace(xx.min - 2, xx.max + 2, x.length)
    val x1 = DenseMatrix(DenseVector.ones[Double](x.length), DenseVector(x)).t
    val x1t = x1.t
    val x1ty = x1t * DenseVector(wx)
    val x1tx1 = x1t * x1
    val betas = inv(x1tx1) * x1ty
    val y = DenseMatrix(DenseVector.ones[Double](x.length), wxy).t * betas
    p.xlim = (xx.min - 2, xx.max + 2)
    p.ylim = (wx.min - 2, wx.max + 2)
    p += plot(wxy, y)
    p += plot(xxmean, xxy, lines = false, shapes = true, style = '.', colorcode = "[0,0,0]")
    p += plot(wxy, wxmean, lines = false, shapes = true, style = '.', colorcode = "[0,0,0]")
    p.xlabel = "x"
    p.ylabel = "wx"
    val printi = morani.formatted("%.4f")
    p.title = s"Global Moran's I is $printi"
    showPng("MoranI",f)
  }

  def arrdvec2multi(arr1: Array[DenseVector[Double]], arr2: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
    val arr1idx = arr1.zipWithIndex
    arr1idx.map(t => {
      t._1 * arr2(t._2)
    })
  }

  def meandiff(arr: Array[Double]): Array[Double] = {
    val ave = arr.sum / arr.length
    arr.map(t => t - ave)
  }

  def sumWeight(weightRDD: RDD[DenseVector[Double]]): Double = {
    weightRDD.map(t => sum(t)).sum()
  }

}
