package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, eig, inv, qr, sum}
import breeze.numerics.{NaN, sqrt}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.oge.Service

import scala.collection.mutable

/**
 * 空间误差模型，考虑自变量误差项λ。
 */
class SpatialErrorModel extends SpatialAutoRegressionBase {

  private var _errorX: DenseMatrix[Double] = _
  private var _errorY: DenseVector[Double] = _

  private var sum_lw: Double = NaN
  private var sw: DenseVector[Double] = _
  private var _wy: DenseVector[Double] = _
  private var _wx: DenseMatrix[Double] = _
  private var _eigen: eig.DenseEig = _

  /**
   * 回归计算
   *
   * @return 返回拟合值（Array）形式
   */
  def fit(): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    var interval = (0.0, 1.0)
    try {
      interval = get_interval()
    } catch {
      case e: IllegalArgumentException => throw new IllegalArgumentException("spatial weight error to calculate eigen matrix")
    }
    val lambda = goldenSelection(interval._1, interval._2, function = lambda4optimize)._1
    //    println(s"lambda is $lambda")
    _errorX = _1X - lambda * _wx
    _errorY = _Y - lambda * _wy
    val betas = get_betas(X = _errorX, Y = _errorY)
    val betas_map = betasPrint(betas)
    val res = get_res(X = _errorX, Y = _errorY)
    //log likelihood
    val lly = get_logLik(get_res(X = _1X))
    val llx = get_logLik(get_res(X = _1X, Y = _errorY))
    val lllambda = lambda4optimize(lambda)

    fitvalue = (_Y - res).toArray

    var printStr = "\n-----------------------------Spatial Error Model-----------------------------\n" +
      f"lambda is $lambda%.6f\n"
    printStr += try_LRtest(-lllambda, lly)
    printStr += f"coeffients:\n$betas_map\n"
    printStr += calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = lllambda, df = _df)
    printStr += "------------------------------------------------------------------------------"
    //    println("------------------------------spatial error model------------------------------")
    //    println(s"lambda is $lambda")
    //    try_LRtest(lllambda, lly)
    //    println(s"coeffients:\n$betas_map")
    //    calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = lllambda, df = _df)
    //    println("--------------------------------------------------------------------------------")
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
      if (_wy == null) {
        _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
      }
      if (sum_lw.isNaN || sw == null) {
        val weight1: DenseVector[Double] = DenseVector.ones[Double](_xrows)
        sum_lw = weight1.toArray.map(t => log(t)).sum
        sw = sqrt(weight1)
      }
      if (_wx == null) {
        val _dvecWx = _X.map(t => DenseVector(spweight_dvec.map(i => (i dot t))))
        //      val _dmatWx = DenseMatrix.create(rows = _xrows, cols = _dvecWx.length, data = _dvecWx.flatMap(t => t.toArray))
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
    //    println(_wy)
    //    println(s"-----------\n$sum_lw\n$sw")
    //    println(_wx)
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

  private def lambda4optimize(lambda: Double): Double = {
    get_env()
    val yl = sw * (_Y - lambda * _wy)
    val xl = (_1X - lambda * _wx)
    val xl_qr = qr(xl)
    val xl_qr_q = xl_qr.q(::, 0 to _xcols) //列数本来应该+1，由于从0开始计数，反而刚好合适
    //    println(xl_qr_q)
    val xl_q_yl = xl_qr_q.t * yl
    val SSE = yl.t * yl - xl_q_yl.t * xl_q_yl
    val n = _xrows
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    val ldet = sum(breeze.numerics.log(-eigvalue * lambda + 1.0))
    val ret = (ldet + (1.0 / 2.0) * sum_lw - ((n / 2.0) * log(2.0 * math.Pi)) - (n / 2.0) * log(s2) - (1.0 / (2.0 * (s2))) * SSE)
    //    println(SSE, ret)
    ret
  }

}

object SpatialErrorModel {
  /** Spatial Error Model (SEM) for spatial regression
   *
   * @param sc          SparkContext
   * @param featureRDD      shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @return featureRDD and diagnostic String
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))]= {
    val mdl = new SpatialErrorModel
    mdl.init(featureRDD)
    mdl.setX(propertiesX)
    mdl.setY(propertyY)
    val re = mdl.fit()
    Service.print(re._2,"Spatial Error Model","String")
    sc.makeRDD(re._1)
  }

}