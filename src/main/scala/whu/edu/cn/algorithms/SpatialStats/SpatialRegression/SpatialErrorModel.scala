package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, eig, inv, qr, sum}
import breeze.numerics.sqrt
import scala.math._

import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

/**
 * 空间误差模型，考虑自变量误差项λ。
 */
class SpatialErrorModel extends SpatialAutoRegressionBase {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _
  private var _errorX: DenseMatrix[Double] = _
  private var _errorY: DenseVector[Double] = _

  private var sum_lw: Double = _
  private var sw: DenseVector[Double] = _
  private var _wy: DenseVector[Double] = _
  private var _wx: DenseMatrix[Double] = _
  private var _eigen: eig.DenseEig = _

  /**
   * 设置X
   *
   * @param x 自变量
   */
  override def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    _dX = DenseMatrix.create(rows = _xrows, cols = _X.length, data = _X.flatMap(t => t.toArray))
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _1X = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
    _df = _xcols + 1 + 1
  }

  /**
   * 设置Y
   *
   * @param y 因变量
   */
  override def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  /**
   * 回归计算
   *
   * @return 返回拟合值（Array）形式
   */
  def fit(): Array[Double] = {
    val interval = get_interval()
    val lambda = goldenSelection(interval._1, interval._2, function = lambda4optimize)._1
    //    println(s"lambda is $lambda")
    _errorX = _1X - lambda * _wx
    _errorY = _Y - lambda * _wy
    val betas = get_betas(X = _errorX, Y = _errorY)
    val betas_map = betasMap(betas)
    val res = get_res(X = _errorX, Y = _errorY)
    //log likelihood
    val lly = get_logLik(get_res(X = _1X))
    val llx = get_logLik(get_res(X = _1X, Y = _errorY))
    val lllambda = lambda4optimize(lambda)

    fitvalue = (_Y - res).toArray
    println("---------------------------------spatial lag model---------------------------------")
    println(s"rho is $lambda")
    try_LRtest(lllambda, lly)
    println(s"coeffients:\n$betas_map")
    calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = lllambda, df = _df)
    println("------------------------------------------------------------------------------------")
    fitvalue
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
      if (sum_lw == null || sw == null) {
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
