package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, eig, inv, qr, sum}
import breeze.numerics.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.oge.Service

import scala.collection.mutable

/**
 * 空间杜宾模型，同时考虑自变量误差项λ与因变量滞后项ρ。
 */
class SpatialDurbinModel  extends SpatialAutoRegressionBase {

  private var _durbinX: DenseMatrix[Double] = _
  private var _durbinY: DenseVector[Double] = _

  private var _wy: DenseVector[Double] = _
  private var _wwy: DenseVector[Double] = _
  private var _wx: DenseMatrix[Double] = _
  private var _eigen: eig.DenseEig = _

  /**
   * 回归计算
   *
   * @return 返回拟合值
   */
  def fit(): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    val arr = firstvalue()
    val optresult = nelderMead(arr, paras4optimize)
    //    println("----------optimize result----------")
    //    optresult.foreach(println)
    val rho = optresult(0)
    val lambda = optresult(1)
    _durbinX = _1X - lambda * _wx
    _durbinY = _Y - rho * _wy - lambda * _wy + rho * lambda * _wwy
    val betas = get_betas(X = _durbinX, Y = _durbinY)
    //    println(betas)
    val betas_map = betasPrint(betas)
    val res = get_res(X = _durbinX, Y = _durbinY)
    //log likelihood
    val llopt = paras4optimize(optresult)
    val lly = get_logLik(get_res(X = _1X))

    fitvalue = (_Y - res).toArray
    var printStr = "\n-----------------------------Spatial Durbin Model-----------------------------\n" +
      f"rho is $rho%.6f\nlambda is $lambda%.6f\n"
    printStr += try_LRtest(-llopt, lly, chi_pama = 2)
    printStr += f"coeffients:\n$betas_map\n"
    printStr += calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = -llopt, df = _df + 2)
    printStr += "------------------------------------------------------------------------------"
    //    println("---------------------------------spatial durbin model---------------------------------")
    //    println(s"rho is $rho\nlambda is $lambda")
    //    try_LRtest(-llopt, lly, chi_pama = 2)
    //    println(s"coeffients:\n$betas_map")
    //    calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = -llopt, df = _df + 2)
    //    println("--------------------------------------------------------------------------------------")
    println(printStr)
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += ("fitValue" -> fitvalue(t._2))
      t._1._2._2 += ("residual" -> res(t._2))
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

  private def get_env(): Unit = {
    if (_Y != null && _X != null) {
      if (_wy == null || _wwy == null) {
        _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
        _wwy = DenseVector(spweight_dvec.map(t => (t dot _wy)))
      }
      if (_wx == null) {
        val _dvecWx = _X.map(t => DenseVector(spweight_dvec.map(i => (i dot t))))
        val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, _dvecWx.flatMap(t => t.toArray))
        _wx = DenseMatrix.create(rows = _xrows, cols = _dvecWx.length + 1, data = ones_x.flatten)
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

  private def firstvalue(): Array[Double] = {
    try {
      if (_eigen == null) {
        _eigen = breeze.linalg.eig(spweight_dmat.t)
      }
      val eigvalue = _eigen.eigenvalues.copy
      //    val min = eigvalue.toArray.min
      //    val max = eigvalue.toArray.max
      val median = (eigvalue.toArray.min + eigvalue.toArray.max) / 2.0
      Array(median, median)
    }
    catch {
      case e: IllegalArgumentException => throw new IllegalArgumentException("spatial weight error to calculate eigen matrix")
    }
  }

  private def paras4optimize(optarr: Array[Double]): Double = {
    get_env()
    if (optarr.length == 2) {
      val rho = optarr(0)
      val lambda = optarr(1)
      val yl = _Y - rho * _wy - lambda * _wy + rho * lambda * _wwy
      val xl = (_1X - lambda * _wx)
      val xl_qr = qr(xl)
      val xl_qr_q = xl_qr.q(::, 0 to _xcols)
      val xl_q_yl = xl_qr_q.t * yl
      val SSE = yl.t * yl - xl_q_yl.t * xl_q_yl
      val n = _xrows
      val s2 = SSE / n
      val eigvalue = _eigen.eigenvalues.copy
      val ldet_rho = sum(breeze.numerics.log(-eigvalue * rho + 1.0))
      val ldet_lambda = sum(breeze.numerics.log(-eigvalue * lambda + 1.0))
      val ret = (ldet_rho + ldet_lambda - ((n / 2.0) * log(2.0 * math.Pi)) - (n / 2.0) * log(s2) - (1.0 / (2.0 * (s2))) * SSE)
      //      println(-ret)
      -ret
    } else {
      throw new IllegalArgumentException("optmize array should have rho and lambda")
    }
  }
}

object SpatialDurbinModel {
  /** Spatial Durbin Model (SDM) for spatial regression
   *
   * @param sc          SparkContext
   * @param featureRDD      shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @return featureRDD and diagnostic String
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))]= {
    val mdl = new SpatialDurbinModel
    mdl.init(featureRDD)
    mdl.setX(propertiesX)
    mdl.setY(propertyY)
    val re = mdl.fit()
    Service.print(re._2,"Spatial Durbin Model","String")
    sc.makeRDD(re._1)
  }

}