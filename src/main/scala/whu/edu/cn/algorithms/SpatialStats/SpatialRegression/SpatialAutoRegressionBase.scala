package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

//抽象类，不可以被初始化
abstract class SpatialAutoRegressionBase {

  protected var shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _
  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _

  protected var _nameX: Array[String] = _
  protected var geom: RDD[Geometry] = _
  protected var spweight_dvec: Array[DenseVector[Double]] = _
  protected var spweight_dmat: DenseMatrix[Double] = _

  var _xrows = 0
  var _xcols = 0
  protected var _df = _xcols
  protected var _dX: DenseMatrix[Double] = _
  protected var _1X: DenseMatrix[Double] = _

  /**
   * 拟合值
   */
  var fitvalue: Array[Double] = _

  def SARmodels() {

  }

  protected def calDiagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residuals: DenseVector[Double], loglikelihood: Double, df: Double): String = {
    val n = X.rows.toDouble
    val rss = sum(residuals.toArray.map(t => t * t))
    val mean_y = Y.toArray.sum / Y.toArray.length
    val AIC = -2 * loglikelihood + 2 * df
    val AICc = -2 * loglikelihood + 2 * df * (n / (n - df - 1))
    val yss = Y.toArray.map(t => (t - mean_y) * (t - mean_y)).sum
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (n - df - 1)
    f"     diagnostics\nSSE : $rss%.6f\nLog likelihood : $loglikelihood%.6f\nAIC : $AIC%.6f\nAICc: $AICc%.6f\nR2 : $r2%.6f\nadjust R2 : $r2_adj%.6f\n"
    //    println(s"diagnostics:\nSSE : $rss\nLog likelihood : $loglikelihood\nAIC : $AIC \nAICc: $AICc\nR2 : $r2\nadjust R2 : $r2_adj")
  }

  /**
   * 初始化空间数据，输入RDD形式的shpfile
   *
   * @param inputRDD RDD形式的shpfile
   * @param style    非必选参数，邻接矩阵的类型，默认为W
   */
  def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], style: String = "W"): Unit = {
    geom = getGeometry(inputRDD)
    shpRDD = inputRDD
    setweight(style = style)
  }

  /** set x
   *
   * @param properties String
   * @param split      default:","
   */
  protected def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    _dX = DenseMatrix.create(rows = _xrows, cols = _X.length, data = _X.flatMap(t => t.toArray))
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _1X = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
    _df = _xcols + 1 + 1
  }

  /** set y
   *
   * @param property String
   */
  protected def setY(property: String): Unit = {
    _Y = DenseVector(shpRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
  }

  /**
   * 提供更改、设置经纬度信息的函数
   *
   * @param lat latitude
   * @param lon longitude
   */
  def setcoords(lat: Array[Double], lon: Array[Double]): Unit = {
    val geomcopy = geom.zipWithIndex()
    geomcopy.map(t => {
      t._1.getCoordinate.x = lat(t._2.toInt)
      t._1.getCoordinate.y = lon(t._2.toInt)
    })
    geom = geomcopy.map(t => t._1)
  }

  /**
   * 提供更改、设置权重计算方式的函数
   *
   * @param neighbor 是否使用邻接矩阵方式构建权重，默认：是（true）
   * @param k        如果不使用邻接矩阵形式，输入最近邻个数，默认为0
   * @param style    非必选参数，邻接矩阵的类型，默认为W
   */
  def setweight(neighbor: Boolean = true, k: Double = 0, style: String = "W"): Unit = {
    if (neighbor && !geom.isEmpty()) {
      //      val nb_bool = getNeighborBool(geom)
      //      spweight_dvec = boolNeighborWeight(nb_bool).map(t => t * (t / t.sum)).collect()
      spweight_dvec = getNeighborWeight(shpRDD, style = style).collect()
    } else if (!neighbor && !geom.isEmpty() && k >= 0) {
      val dist = getDist(shpRDD).map(t => Array2DenseVector(t))
      spweight_dvec = dist.map(t => getSpatialweightSingle(t, k, kernel = "boxcar", adaptive = true))
    }
    spweight_dmat = DenseMatrix.create(rows = spweight_dvec(0).length, cols = spweight_dvec.length, data = spweight_dvec.flatMap(t => t.toArray))
  }

  /**
   * 输出权重矩阵，输出形式为多个向量
   */
  def printweight(): Unit = {
    spweight_dvec.foreach(println)
  }

  protected def get_logLik(res: DenseVector[Double]): Double = {
    val n = res.length
    val w = DenseVector.ones[Double](n)
    0.5 * (w.toArray.map(t => log(t)).sum - n * (log(2 * math.Pi) + 1.0 - log(n) + log((w * res * res).toArray.sum)))
  }

  protected def try_LRtest(LLx: Double, LLy: Double, chi_pama: Double = 1): String = {
    val score = 2.0 * (LLx - LLy)
    val pchi = breeze.stats.distributions.ChiSquared
    val pvalue = 1 - pchi.distribution(chi_pama).cdf(abs(score))
    f"ChiSquared test, score is $score%.4f, p value is $pvalue%.6g\n"
    //    println(s"ChiSquared test, score is $score, p value is $pvalue")
  }

  protected def betasMap(coef: DenseVector[Double]): mutable.Map[String, Double] = {
    val coefname = Array("Intercept") ++ _nameX
    val coefvalue = coef.toArray
    val betas_map: mutable.Map[String, Double] = mutable.Map()
    for (i <- 0 until coef.length) {
      val coefi = coefvalue(i).formatted("%.6f").toDouble
//      betas_map += (coefname(i) -> coefvalue(i))
      betas_map += (coefname(i) -> coefi)
    }
    //    println(betas_map)
    betas_map
  }

  protected def betasPrint(coef: DenseVector[Double]): String = {
    val coefname = Array("Intercept") ++ _nameX
    val coefvalue = coef.toArray
    var str: String = ""
    for (i <- 0 until coef.length) {
      str += coefname(i) + ":" + coefvalue(i).formatted("%.6g").toString + "    "
    }
    str
  }

}
