package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector, inv, sum}
import breeze.linalg.operators.DenseMatrixOps
import breeze.linalg.operators
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm
import whu.edu.cn.oge.Service

import scala.collection.{breakOut, mutable}

object LogisticRegression extends Algorithm {

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

  def fit(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          y: String, x: String, Intercept: Boolean = true,
          maxIter: Int = 100, epsilon: Double = 1e-6, learningRate: Double = 0.01)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    _data = data.map(t=>t._2._2)
    setX(x)
    setY(y)
    val X = if (Intercept) _dmatX else _rawdX
    val Y=_dvecY

    // 初始化参数
    var weights = DenseVector.zeros[Double](X.cols)

    // 训练模型
    var converged = false
    var iter = 0

    while (!converged && iter < maxIter) {
      // 计算预测值
      val z = X * weights
      val p = sigmoid(z)

      val w = p.map(t => t*(1-t))
      val n = w.length
      val W = DenseMatrix.zeros[Double](n,n)
      for(i<-0 until n){
        W(i,i)=w(i)
      }

      // Fisher Update
      val xtwx = X.t * W * X
      val xtwx_inv = inv(xtwx)
      val workingY = z + (Y - p)/w
      val xtwy = X.t * W * workingY
      val newWeights = xtwx_inv*xtwy

      // 检查收敛
      val weightChange = breeze.linalg.norm(newWeights-weights)
      if(weightChange<epsilon){
        converged = true
      }
      weights = newWeights
      iter +=1
    }

    //yhat, residual
    val yhat = sigmoid(X * weights)
    val res = (Y - yhat)

    // deviance residuals
    val devRes = DenseVector.zeros[Double](Y.length)
    for (i<- 0 until Y.length){
      val y = Y(i)
      val mu = yhat(i)
      val eps = 1e-10
      val clippedMu = max(eps,min(1-eps,mu))

      val dev = if(y==1){
        2*log(1/clippedMu)
      }else{
        2*math.log(1/(1-clippedMu))
      }
      devRes(i)=signum(y-mu)*sqrt(abs(dev))
    }


    //输出
    var str = "\n********************Results of Logistic Regression********************\n"

    var formula = f"${y} ~ "
    for(i <- 1 until X.cols){
      if (i==1){
      formula += f"${_nameX(i-1)} "
      }else{
        formula += f"+ ${_nameX(i-1)} "
      }
    }
    str += "Formula:\n" + formula + f"\n"

    str += "\n"
    str += f"Deviance Residuals: \n"+
      f"min: ${devRes.toArray.min.formatted("%.4f")}  "+
      f"max: ${devRes.toArray.max.formatted("%.4f")}  "+
      f"mean: ${breeze.stats.mean(devRes).formatted("%.4f")}  "+
      f"median: ${breeze.stats.median(devRes).formatted("%.4f")}\n"

    str += "\n"
    str += "Coefficients:\n"
    if(Intercept){
      str += f"Intercept:${weights(0).formatted("%.6f")}\n"
    }
    for(i <- 1 until (X.cols)){
      str += f"${_nameX(i-1)}: ${weights(i).formatted("%.6f")}\n"
    }

    str += diagnostic(X, Y, devRes, _df)

//    str += "\n"
//    str += f"${res.toArray.min},${res.toArray.max},${res.toArray.sum/res.length}\n"

    str += "\n"
    str += f"Number of Iterations: ${iter}\n"

    str += "**********************************************************************\n"
    //print(str)

    val shpRDDidx = data.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> yhat(t._2.toInt))
      t._1._2._2 += ("residual" -> res(t._2.toInt))
    })
    Service.print(str,"Logistic Regression for feature","String")
    sc.makeRDD(shpRDDidx.map(t=>t._1))
  }

  // 添加预测方法
  def predict(X: DenseMatrix[Double], weights: DenseVector[Double], intercept: Double): DenseVector[Double] = {
    val z = if (intercept != 0.0) {
      val X1 = DenseMatrix.horzcat(DenseMatrix.ones[Double](X.rows, 1), X)
      X1 * DenseVector.vertcat(DenseVector(intercept), weights)
    } else {
      X * weights
    }

    z.map(x => if (1.0 / (1.0 + math.exp(-x)) > 0.5) 1.0 else 0.0)
  }

  protected def diagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], devRes: DenseVector[Double], df: Double): String = {
    val n = X.rows.toDouble
    val p = df

    // deviance of null model
    val y_mean = breeze.stats.mean(Y)
    val null_deviance = Y.toArray.map(yi => {
      if(yi==1){
        -2 * math.log(y_mean)
      }else{
        -2 * math.log(1-y_mean)
      }
    }).sum

    // deviance redisuals square sum
    val residual_deviance = sum(devRes.map(x => x*x))

    // Cox & Snell R2, Nagelkerke R2
    val cox_snall_r2 = 1 - math.exp((residual_deviance - null_deviance)/n)
    val nagelkerke_r2 = cox_snall_r2/(1-math.exp(-null_deviance/n))

    //AIC
    val aic = residual_deviance + 2*(p+1)

    // degree of freedom
    val null_df = n-1
    val residual_df = n-p-1

    val res = "\nDiagnostics:\n"+
    f"Null deviance:     $null_deviance%.2f on $null_df%.0f degrees of freedom\n"+
    f"Residual deviance: $residual_deviance%.2f on $residual_df%.0f degrees of freedom\n"+
    f"AIC: $aic%.2f\n"+
    f"Pseudo R-squared: $nagelkerke_r2%.4f\n"
    res
  }


}
