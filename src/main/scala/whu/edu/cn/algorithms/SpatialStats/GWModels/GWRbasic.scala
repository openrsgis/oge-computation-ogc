package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}
import breeze.plot.{Figure, plot}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

import scala.collection.mutable

class GWRbasic extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  val select_eps = 1e-2

  var opt_value: Array[Double] = _
  var opt_result: Array[Double] = _
  var opt_iters: Array[Double] = _

  private var _dX: DenseMatrix[Double] = _

  override def setX(properties: String, split:String=","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[String].toDouble).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _dX = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
  }

  override def setY(property: String): Unit = {
    _Y = DenseVector(shpRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect())
  }

  def auto(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): Array[(String, (Geometry, mutable.Map[String, Any]))] = {
    println("start bandwidth selection")
    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    println(s"best bandwidth is $bwselect")
    val f = Figure()
    val p = f.subplot(0)
    val optv_sort = opt_value.zipWithIndex.map(t => (t._1, opt_result(t._2))).sortBy(_._1)
    p += plot(optv_sort.map(_._1), optv_sort.map(_._2))
    p.xlabel = "bandwidth"
    p.ylabel = s"$approach"
    fit(bwselect, kernel = kernel, adaptive = adaptive)
  }

  def fit(bw: Double = 0, kernel: String = "gaussian", adaptive: Boolean = true): Array[(String, (Geometry, mutable.Map[String, Any]))] = {
    if (bw > 0) {
      setweight(bw, kernel, adaptive)
    } else if (spweight_dvec != null) {

    } else {
      throw new IllegalArgumentException("bandwidth should be over 0 or spatial weight should be initialized")
    }
    //    printweight()
    val results = fitFunction(_dX, _Y, spweight_dvec)
    //    val results = fitRDDFunction(sc,_dX, _Y, spweight_dvec)
    val betas = DenseMatrix.create(_xcols + 1, _xrows, data = results._1.flatMap(t => t.toArray))
    val arr_yhat = results._2.toArray
    val arr_residual = results._3.toArray
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t=>t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> arr_yhat(t._2))
      t._1._2._2 += ("residual" -> arr_residual(t._2))
    })
    val name=Array("Intercept")++_nameX
    for(i<-0 until betas.rows){
      shpRDDidx.map(t=>{
        val a=betas(i,t._2)
        t._1._2._2 += (name(i) -> a)
      })
    }
//    val a=shpRDDidx.map(t=>t._1._2._2)
//    a.foreach(println)
    //    sc.makeRDD(shpRDDidx.map(t => t._1))
    //    println(betas)
    //    results._1.foreach(println)
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    println("*********************************************************************************")
    println("*               Results of Geographically Weighted Regression                   *")
    println("*********************************************************************************")
    println("**************************Model calibration information**************************")
    print(s"Kernel function: $kernel\n$bw_type bandwidth: ")
    print(f"$bw%.2f\n")
//    println("Distance metric: Euclidean distance metric is used.")
    calDiagnostic(_dX, _Y, results._3, results._4)
    shpRDDidx.map(t => t._1)
  }

  private def fitFunction(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, weight: Array[DenseVector[Double]] = spweight_dvec):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseVector[Double]]) = {
    //    val xtw = weight.map(w => eachColProduct(X, w).t)
    val xtw = weight.map(w => {
      val v1 = (DenseVector.ones[Double](_xrows) * w).toArray
      val xw = _X.flatMap(t => (t * w).toArray)
      DenseMatrix.create(_xrows, _xcols + 1, data = v1 ++ xw).t
    })
    val xtwx = xtw.map(t => t * X)
    val xtwy = xtw.map(t => t * Y)
    val xtwx_inv = xtwx.map(t => inv(t))
    val xtwx_inv_idx = xtwx_inv.zipWithIndex
    val betas = xtwx_inv_idx.map(t => t._1 * xtwy(t._2))
    val ci = xtwx_inv_idx.map(t => t._1 * xtw(t._2))
    val ci_idx = ci.zipWithIndex
    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => {
      val a = X(t._2, ::).inner.toDenseMatrix
      val b = t._1.toDenseMatrix
      a * b
      //      (X(t._2, ::) * t._1).inner
    })
    val shat = DenseMatrix.create(rows = si.length, cols = si.length, data = si.flatMap(t => t.toArray))
    val yhat = getYHat(X, betas)
    val residual = Y - yhat
    (betas, yhat, residual, shat, sum_ci)
  }

  private def fitRDDFunction(implicit sc: SparkContext, X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, weight: Array[DenseVector[Double]] = spweight_dvec):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseVector[Double]]) = {
    val xtw0 = weight.map(w => {
      val v1 = (DenseVector.ones[Double](_xrows) * w).toArray
      val xw = _X.flatMap(t => (t * w).toArray)
      DenseMatrix.create(_xrows, _xcols + 1, data = v1 ++ xw).t
    })
    val xtw = sc.makeRDD(xtw0)
    val xtwx = xtw.map(t => t * X)
    val xtwy = xtw.map(t => t * Y)
    val xtwx_inv = xtwx.map(t => inv(t))
    val xtwx_inv_idx = xtwx_inv.zipWithIndex
    val arr_xtwy = xtwy.collect()
    val betas = xtwx_inv_idx.map(t => t._1 * arr_xtwy(t._2.toInt))
    val arr_xtw = xtw.collect()
    val ci = xtwx_inv_idx.map(t => t._1 * arr_xtw(t._2.toInt))
    val ci_idx = ci.zipWithIndex
    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => {
      val a = X(t._2.toInt, ::).inner.toDenseMatrix
      val b = t._1.toDenseMatrix
      a * b
    })
    val shat = DenseMatrix.create(rows = si.collect().length, cols = si.collect().length, data = si.collect().flatMap(t => t.toArray))
    val yhat = getYhat(X, betas.collect())
    val residual = Y - yhat
    (betas.collect(), yhat, residual, shat, sum_ci.collect())
  }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): Double = {
    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Double = max_dist, lower: Double = max_dist / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Int = _xrows - 1, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1.toInt
    } catch {
      case e: MatrixSingularException => {
        println("error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def bandwidthAICc(bw: Double): Double = {
    if (_adaptive) {
      setweight(round(bw), _kernel, _adaptive)
    } else {
      setweight(bw, _kernel, _adaptive)
    }
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val residual = results._3
    val shat = results._4
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = _xrows
    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
  }

  private def bandwidthCV(bw: Double): Double = {
    if (_adaptive) {
      setweight(round(bw), _kernel, _adaptive)
    } else {
      setweight(bw, _kernel, _adaptive)
    }
    val spweight_idx = spweight_dvec.zipWithIndex
    spweight_idx.foreach(t => t._1(t._2) = 0)
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val residual = results._3
    residual.toArray.map(t => t * t).sum
  }

  private def getYhat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val arrbuf = new ArrayBuffer[Double]()
    for (i <- 0 until X.rows) {
      val rowvec = X(i, ::).inner
      val yhat = sum(betas(i) * rowvec)
      arrbuf += yhat
    }
    DenseVector(arrbuf.toArray)
  }

  def getYHat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val betas_idx = betas.zipWithIndex
    val yhat = betas_idx.map(t => {
      sum(t._1 * X(t._2, ::).inner)
    })
    DenseVector(yhat)
  }

  //  def eachColProduct(Mat: DenseMatrix[Double], Vec: DenseVector[Double]): DenseMatrix[Double] = {
  //    val arrbuf = new ArrayBuffer[DenseVector[Double]]()
  //    for (i <- 0 until Mat.cols) {
  //      arrbuf += Mat(::, i) * Vec
  //    }
  //    val data = arrbuf.toArray.flatMap(t => t.toArray)
  //    DenseMatrix.create(rows = Mat.rows, cols = Mat.cols, data = data)
  //  }

}