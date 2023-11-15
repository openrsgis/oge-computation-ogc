package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

import scala.collection.mutable

/**
 * 空间滞后模型，考虑因变量滞后项ρ。
 */
class SpatialLagModel extends SpatialAutoRegressionBase {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _
  private var _lagY: DenseVector[Double] = _

  private var lm_null: DenseVector[Double] = _
  private var lm_w: DenseVector[Double] = _
  private var _wy: DenseVector[Double] = _
  private var _eigen: eig.DenseEig = _

  /**set x
   *
   * @param properties  String
   * @param split       default:","
   */
  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[String].toDouble).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    _dX = DenseMatrix.create(rows = _xrows, cols = _X.length, data = _X.flatMap(t => t.toArray))
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _1X = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
    _df = _xcols + 1 + 1
  }

  /**set y
   *
   * @param property String
   */
  override def setY(property: String): Unit = {
    _Y = DenseVector(shpRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect())
  }

  /**
   * 回归计算
   *
   * @return 返回拟合值（Array）形式
   */
  def fit(): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    val interval = get_interval()
    val rho = goldenSelection(interval._1, interval._2, function = rho4optimize)._1
    _lagY = _Y - rho * _wy
    val betas = get_betas(X = _1X, Y = _lagY)
    val betas_map = betasMap(betas)
    val res = get_res(X = _1X, Y = _lagY)
    //log likelihood
    val lly = get_logLik(get_res(X = _1X))
    val llx = get_logLik(get_res(X = _1X, Y = _lagY))
    val llrho = rho4optimize(rho)

    fitvalue = (_Y - res).toArray
    var printStr = "------------------------------Spatial Lag Model------------------------------\n" +
      f"rho is $rho%.6f\n"
    printStr += try_LRtest(-llrho, lly)
    printStr += f"coeffients:\n$betas_map\n"
    printStr += calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = llrho, df = _df)
    printStr += "------------------------------------------------------------------------------"
    //    println("--------------------------------spatial Lag model--------------------------------")
    //    println(s"rho is $rho")
    //    try_LRtest(llrho, lly)
    //    println(s"coeffients:\n$betas_map")
    //    calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = llrho, df = _df)
    //    println("--------------------------------------------------------------------------------")
    println(printStr)
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("fitValue" -> fitvalue(t._2))
    })
    (shpRDDidx.map(t => t._1), printStr)
  }

  def get_betas(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xrows)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    betas
  }

  def get_res(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xrows)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    Y - y_hat
  }

  //添加一个判断X，Y为空的情况判断，抛出错误
  private def get_env(): Unit = {
    if (_Y != null && _X != null) {
      if (lm_null == null || lm_w == null || _wy == null) {
        _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
        lm_null = get_res(X = _1X)
        lm_w = get_res(X = _1X, Y = _wy)
      }
      if (spweight_dmat != null) {
        if (_eigen == null) {
          _eigen = breeze.linalg.eig(spweight_dmat.t)
        }
      } else {
        throw new NullPointerException("the shpfile is not initialized! please check!")
      }
    } else {
      throw new IllegalArgumentException("the x or y are not initialized! please check!")
    }
  }

  private def get_interval(): (Double, Double) = {
    if (spweight_dmat == null) {
      throw new NullPointerException("the shpfile is not initialized! please check!")
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
    val eigvalue = _eigen.eigenvalues.copy
    val min = eigvalue.toArray.min
    val max = eigvalue.toArray.max
    (1.0 / min, 1.0 / max)
  }

  private def rho4optimize(rho: Double): Double = {
    get_env()
    val e_a = lm_null.t * lm_null
    val e_b = lm_w.t * lm_null
    val e_c = lm_w.t * lm_w
    val SSE = e_a - 2.0 * rho * e_b + rho * rho * e_c
    val n = _xrows
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    //    val eig_rho = eigvalue :*= rho
    //    val eig_rho_cp = eig_rho.copy
    val ldet = sum(breeze.numerics.log(-eigvalue * rho + 1.0))
    val ret = (ldet - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
    ret
  }

}

object SpatialLagModel {
  /** Spatial Lag Model (SLM) for spatial regression
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @return featureRDD and diagnostic String
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String)
  : (RDD[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    val mdl = new SpatialLagModel
    mdl.init(featureRDD)
    mdl.setX(propertiesX)
    mdl.setY(propertyY)
    val re = mdl.fit()
    (sc.makeRDD(re._1), re._2)
  }

}