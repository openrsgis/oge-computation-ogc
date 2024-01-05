package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, inv, sum}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Service

import scala.collection.mutable

object LinearRegression {

  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _X: DenseMatrix[Double] = _
  private var _Y: DenseVector[Double] = _
  private var _1X: DenseMatrix[Double] = _
  private var _nameX: Array[String]  = _
  private var _rows: Int = 0
  private var _df: Int  = 0

  private def setX(properties: String, split: String = ",", Intercept: Boolean): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      _data.map(t => t(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect()
    })
    _rows = x(0).length
    _df = x.length
    _X = DenseMatrix.create(rows = _rows, cols = x.length, data = x.flatten)
    if (Intercept) {
      val ones_x = Array(DenseVector.ones[Double](_rows).toArray, x.flatten)
      _1X = DenseMatrix.create(rows = _rows, cols = x.length + 1, data = ones_x.flatten)
    }
  }

  private def setY(property: String): Unit = {
    _Y = DenseVector(_data.map(t => t(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
  }

  /**
   * 线性回归
   *
   * @param data      RDD, csv读入
   * @param x         输入X
   * @param y         输入Y
   * @param Intercept 是否需要截距项，默认：是（true）
   * @return          （系数，预测值，残差）各自以Array形式储存
   */
  def LinearRegression(sc: SparkContext, data: RDD[mutable.Map[String, Any]], y: String, x: String, Intercept: Boolean =true)
  : RDD[mutable.Map[String, Any]] = {
    _data=data
    val split = ","
    setX(x, split, Intercept)
    setY(y)
    var X=_1X
    if(! Intercept){
      X= _X
    }
    val Y=_Y
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
      str += f"Intercept: ${betas(0)}%.4f\n${_nameX(0)}: ${betas(1)}%.4f\n${_nameX(1)}: ${betas(2)}%.4f\n"
    } else {
      str += f"${_nameX(0)}: ${betas(0)}%.4f\n${_nameX(1)}: ${betas(1)}%.4f\n"
    }
    str += diagnostic(X, Y, res, _df)
    print(str)
    val shpRDDidx = data.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1 += ("yhat" -> y_hat(t._2))
      t._1 += ("residual" -> res(t._2))
    })
    Service.print(str, "Linear Regression", "String")
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  /** Linear Regression for feature
   *
   * @param data      feature RDD
   * @param x         输入X
   * @param y         输入Y
   * @param Intercept 是否需要截距项，默认：是（true）
   * @return （系数，预测值，残差）各自以Array形式储存
   */
  def LinearReg(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))], y: String, x: String, Intercept: Boolean = true)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    _data=data.map(t=>t._2._2)
    val split=","
    setX(x, split, Intercept)
    setY(y)
    var X = _1X
    if (!Intercept) {
      X = _X
    }
    val Y = _Y
    val W = DenseMatrix.eye[Double](_rows)
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    val res = Y - y_hat
    var str = "\n  Linear Regression\n"
    if(Intercept){
      str += f"Intercept: ${betas(0)}%.4f\n${_nameX(0)}: ${betas(1)}%.4f\n${_nameX(1)}: ${betas(2)}%.4f\n"
    }else{
      str += f"${_nameX(0)}: ${betas(0)}%.4f\n${_nameX(1)}: ${betas(1)}%.4f\n"
    }
    str += diagnostic(X,Y,res,_df)
    print(str)
    val shpRDDidx = data.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> y_hat(t._2))
      t._1._2._2 += ("residual" -> res(t._2))
    })
    Service.print(str,"Linear Regression for feature","String")
    sc.makeRDD(shpRDDidx.map(t=>t._1))
  }

  protected def diagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residuals: DenseVector[Double], df: Double): String = {
    val n = X.rows.toDouble
    val rss = sum(residuals.toArray.map(t => t * t))
    val mean_y = Y.toArray.sum / Y.toArray.length
    val yss = Y.toArray.map(t => (t - mean_y) * (t - mean_y)).sum
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (n - df - 1)
    f"  diagnostics\nSSE : $rss%.6f\nR2 : $r2%.6f\nadjust R2 : $r2_adj%.6f\n"
    //    println(s"diagnostics:\nSSE : $rss\nLog likelihood : $loglikelihood\nAIC : $AIC \nAICc: $AICc\nR2 : $r2\nadjust R2 : $r2_adj")
  }

}
