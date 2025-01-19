package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, inv, sum}
import breeze.stats.distributions.FDistribution
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm
import whu.edu.cn.oge.Service

import scala.collection.mutable

object LinearRegression extends Algorithm {

  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dmatX: DenseMatrix[Double] = _
  private var _dvecY: DenseVector[Double] = _

  private var _rawX: Array[Array[Double]] = _
  private var _rawdX: DenseMatrix[Double] = _

  private var _nameX: Array[String] = _
  private var _nameY: String = _
  private var _rows: Int = 0
  private var _df: Int = 0

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      _data.map(t => t(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect()
    })
    _rows = x(0).length
    _df = x.length

    _rawX = x
    _rawdX = DenseMatrix.create(rows = _rows, cols = x.length, data = x.flatten)
    val onesX = Array(DenseVector.ones[Double](_rows).toArray, x.flatten)
    _dmatX = DenseMatrix.create(rows = _rows, cols = x.length + 1, data = onesX.flatten)
  }

  override def setY(property: String): Unit = {
    _nameY = property
    _dvecY = DenseVector(_data.map(t => t(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
  }

  /**
   * 线性回归
   *
   * @param data      RDD, csv读入
   * @param y         输入Y
   * @param x         输入X
   * @param Intercept 是否需要截距项，默认：是（true）
   * @return （系数，预测值，残差）各自以Array形式储存
   */
  def fit_legacy(sc: SparkContext, data: RDD[mutable.Map[String, Any]], y: String, x: String, Intercept: Boolean = true)
  : RDD[mutable.Map[String, Any]] = {
    _data = data
    setX(x)
    setY(y)
    val X = if (Intercept) _dmatX else _rawdX
    val Y = _dvecY
    val W = DenseMatrix.eye[Double](_rows)
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    val res = Y - y_hat
    var str = "\n  Linear Regression\n"
    if (Intercept) {
      str += f"Intercept: ${betas(0)}%.4e\n${_nameX(0)}: ${betas(1)}%.4e\n${_nameX(1)}: ${betas(2)}%.4e\n"
    } else {
      str += f"${_nameX(0)}: ${betas(0)}%.4e\n${_nameX(1)}: ${betas(1)}%.4e\n"
    }
    str += diagnostic(X, Y, res, _df)
    //    print(str)
    val shpRDDidx = data.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1 += ("yhat" -> y_hat(t._2.toInt))
      t._1 += ("residual" -> res(t._2.toInt))
    })
    Service.print(str, "Linear Regression", "String")
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  /** Linear Regression for feature
   *
   * @param sc        SparkContext
   * @param data      feature RDD
   * @param y         输入Y
   * @param x         输入X
   * @param Intercept 是否需要截距项，默认：是（true）
   * @return （系数，预测值，残差）各自以Array形式储存
   *
   */
  def fit(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))], y: String, x: String, Intercept: Boolean = true)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    _data = data.map(t => t._2._2)
    setX(x)
    setY(y)
    val X = if (Intercept) _dmatX else _rawdX
    val Y = _dvecY
    val W = DenseMatrix.eye[Double](_rows)
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    val residual = Y - y_hat
    var str = "\n********************Results of Linear Regression********************\n"
    val str_split = ""
    var formula = f"${y} ~ "
    for (i <- 1 until X.cols) {
      if (i == 1) {
        formula += f"${_nameX(i - 1)} "
      } else {
        formula += f"+ ${_nameX(i - 1)} "
      }
    }
    str += "Formula:\n" + formula + f"\n"
    //    if (Intercept) {
    //      str += f"Intercept: ${betas(0)}%.4e\n${_nameX(0)}: ${betas(1)}%.4e\n${_nameX(1)}: ${betas(2)}%.4e\n"
    //    } else {
    //      str += f"${_nameX(0)}: ${betas(0)}%.4e\n${_nameX(1)}: ${betas(1)}%.4e\n"
    //    }

    str += "\n"
    str += f"Residuals: \n" +
      f"min: ${residual.toArray.min.formatted("%.4f")}  " +
      f"max: ${residual.toArray.max.formatted("%.4f")}  " +
      f"mean: ${breeze.stats.mean(residual).formatted("%.4f")}  " +
      f"median: ${breeze.stats.median(residual).formatted("%.4f")}\n"

    str += "\n"
    str += "Coefficients:\n"
    if (Intercept) {
      str += f"Intercept:${betas(0).formatted("%.6f")}\n"
    }
    for (i <- 1 until (X.cols)) {
      str += f"${_nameX(i - 1)}: ${betas(i).formatted("%.6f")}\n"
    }

    str += diagnostic(X, Y, residual, _df)
    str += "**********************************************************************\n"
    //    print(str)
    val shpRDDidx = data.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> y_hat(t._2.toInt))
      t._1._2._2 += ("residual" -> residual(t._2.toInt))
    })
    Service.print(str, "Linear Regression for feature", "String")
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  protected def diagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residuals: DenseVector[Double], df: Double): String = {
    val n = X.rows.toDouble
    val p = X.cols -1
    val k = p+1

    val rss = sum(residuals.toArray.map(t => t * t))
    val mean_y = Y.toArray.sum / Y.toArray.length
    val yss = Y.toArray.map(t => (t - mean_y) * (t - mean_y)).sum

    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (n - df - 1)

    //AIC, AICc and BIC
    val sigma2 = rss/n
    val L = -n/2 * (math.log(2*math.Pi)) - n/2 * math.log(sigma2) - rss/(2*sigma2) // log likelihood
    // fix probably required
    val aic = -2 * L + k * 2
    val aicc = aic + (2*k*(k+1))/(n-k-1)
    val bic = -2 * L + k * math.log(n)

    // Residual Standard Error
    val df_residual = n - df - 1
    val rse = math.sqrt(rss / df_residual)

    // F-statistics
    val msr = (yss-rss)/df
    val mse = rss/df_residual
    val f_statistic = msr/mse
    // p value of F distribution
    val f_dist = new FDistribution(df,df_residual.toDouble)
    val p_value = 1-f_dist.cdf(f_statistic)
    val p_value_str = if(p_value<2.2e-16)"<2.2e-16" else f"$p_value%.4e"

    f"\nDiagnostics:\n"+
      f"Sum of Squares Error (SSE): $rss%.6f\n"+
      f"R-squared:                  $r2%.4f\n"+
      f"Adjusted R-squared:         $r2_adj%.4f\n"+
      f"AIC:  $aic%.4f\n"+
      f"AICc: $aicc%.4f\n"+
      f"BIC:  $bic%.4f\n"+
      f"Residual standard error: $rse%.4f on $df_residual%.0f degrees of freedom\n"+
      f"F-statistic: $f_statistic%.1f on $df%.0f and $df_residual%.0f DF, "+
      f"p-value: $p_value_str\n"
    //    println(s"diagnostics:\nSSE : $rss\nLog logLikelihood : $loglikelihood\nAIC : $AIC \nAICc: $AICc\nR2 : $r2\nadjust R2 : $r2_adj")
  }

}
