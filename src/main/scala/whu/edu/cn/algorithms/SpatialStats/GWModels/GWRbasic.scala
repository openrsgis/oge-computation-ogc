package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}
import breeze.plot.{Figure, plot}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.oge.Service
import whu.edu.cn.util.ShapeFileUtil.readShp

import scala.collection.mutable

class GWRbasic extends GWRbase {

  val select_eps = 1e-2
  var _nameUsed: Array[String] = _
  var _outString: String = _

  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    _nameUsed = _nameX
    val x = _nameX.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _dX = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
  }

  private def resetX(name: Array[String]): Unit = {
    _nameUsed = name
    val x = name.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _dX = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
  }

  override def setY(property: String): Unit = {
    _nameY = property
    _Y = DenseVector(shpRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
  }

  def auto(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    var printString = "Auto bandwidth selection\n"
    //    println("auto bandwidth selection")
    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    opt_iters.foreach(t=>{
      val i= (t-1).toInt
      printString += (f"iter ${t.toInt}, bandwidth: ${opt_value(i)}%.2f, $approach: ${opt_result(i)}%.3f\n")
    })
    printString += f"Best bandwidth is $bwselect%.2f\n"
    //    println(s"best bandwidth is $bwselect")
//    val f = Figure()
//    f.height = 600
//    f.width = 900
//    val p = f.subplot(0)
//    val optv_sort = opt_value.zipWithIndex.map(t => (t._1, opt_result(t._2))).sortBy(_._1)
//    p += plot(optv_sort.map(_._1), optv_sort.map(_._2))
//    p.title = "bandwidth selection"
//    p.xlabel = "bandwidth"
//    p.ylabel = s"$approach"
    val result = fit(bwselect, kernel = kernel, adaptive = adaptive)
    printString += result._2
    if (_outString == null) {
      _outString = printString
    } else {
      _outString += printString
    }
    (result._1, _outString)
  }

  def variableSelect(kernel: String = "gaussian", select_th: Double = 3.0): (Array[String], Int) = {
    val remainNameBuf = _nameX.toBuffer.asInstanceOf[ArrayBuffer[String]]
    val getNameBuf = ArrayBuffer.empty[String]
    var index = 0
    val plotIdx = ArrayBuffer.empty[Double]
    val plotAic = ArrayBuffer.empty[Double]
    var ordString = ""
    var select_idx = _nameX.length
    var minVal = 0.0
    for (i <- remainNameBuf.indices) {
      _kernel = kernel
      val valBuf = ArrayBuffer.empty[Double]
      for (i <- remainNameBuf.indices) {
        val nameArr = getNameBuf.toArray ++ Array(remainNameBuf(i))
        resetX(nameArr)
        valBuf += variableResult(nameArr)
        index += 1
        plotIdx += index
        ordString += index.toString + ", " + _nameY + "~" + nameArr.mkString("+") + "\n"
      }
      plotAic ++= valBuf
      val valArrIdx = valBuf.toArray.zipWithIndex.sorted
      //      println(valArrIdx.toList)
      getNameBuf += remainNameBuf.apply(valArrIdx(0)._2)
      if (minVal == 0.0) {
        minVal = valArrIdx(0)._1 + 10.0
      }
      if ((minVal - valArrIdx(0)._1) < select_th && select_idx == _nameX.length) {
        select_idx = i
      } else {
        minVal = valArrIdx(0)._1
      }
      remainNameBuf.remove(valArrIdx(0)._2)
    }
//    val f = Figure()
//    f.height = 600
//    f.width = 900
//    val p = f.subplot(0)
//    p += plot(plotIdx.toArray, plotAic.toArray)
//    p.title = "variable selection"
//    p.xlabel = "variable selection order"
//    p.ylabel = "AICc"
    _outString = "Auto variable selection\n"
    _outString += ordString
    (getNameBuf.toArray, select_idx)
  }

  private def variableResult(arrX: Array[String]): Double = {
    resetX(arrX)
    val bw = 10.0 * max_dist
    setweight(bw, _kernel, adaptive = false)
    bandwidthAICc(bw)
  }

  def fit(bw: Double = 0, kernel: String = "gaussian", adaptive: Boolean = true): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    if (bw > 0) {
      setweight(bw, kernel, adaptive)
    } else if (spweight_dvec != null) {

    } else {
      throw new IllegalArgumentException("bandwidth should be over 0 or spatial weight should be initialized")
    }
    val results = fitFunction(_dX, _Y, spweight_dvec)
    //    val results = fitRDDFunction(sc,_dX, _Y, spweight_dvec)
    val betas = DenseMatrix.create(_xcols + 1, _xrows, data = results._1.flatMap(t => t.toArray))
    val arr_yhat = results._2.toArray
    val arr_residual = results._3.toArray
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> arr_yhat(t._2))
      t._1._2._2 += ("residual" -> arr_residual(t._2))
    })
    //    results._1.map(t=>mean(t))
    val name = Array("Intercept") ++ _nameUsed
    for (i <- 0 until betas.rows) {
      shpRDDidx.map(t => {
        val a = betas(i, t._2)
        t._1._2._2 += (name(i) -> a)
      })
    }
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    val fitFormula = _nameY + " ~ " + _nameUsed.mkString(" + ")
    var fitString = "\n*********************************************************************************\n" +
      "*               Results of Geographically Weighted Regression                   *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Formula: $fitFormula" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n"
    fitString += calDiagnostic(_dX, _Y, results._3, results._4)
    (shpRDDidx.map(t => t._1), fitString)
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

object GWRbasic {

  /** Basic GWR calculation with bandwidth and variables auto selection
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param approach    approach function: AICc, CV
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @param varSelTh    threshold of variable selection, default: 3.0
   * @return featureRDD and diagnostic String
   */
  def auto(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
           kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = false, varSelTh: Double = 3.0)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWRbasic
    model.init(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    val vars = model.variableSelect(kernel = kernel, select_th = varSelTh)
    val r = vars._1.take(vars._2)
    model.resetX(r)
    val re = model.auto(kernel = kernel, approach = approach, adaptive = adaptive)
    Service.print(re._2, "Basic GWR calculation with bandwidth and variables auto selection", "String")
    sc.makeRDD(re._1)
  }

  /** Basic GWR calculation with bandwidth auto selection
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param approach    approach function: AICc, CV
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @return featureRDD and diagnostic String
   */
  def autoFit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
              kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = false)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))]= {
    val model = new GWRbasic
    model.init(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    val re = model.auto(kernel = kernel, approach = approach, adaptive = adaptive)
//    print(re._2)
    Service.print(re._2, "Basic GWR calculation with bandwidth auto selection", "String")
    sc.makeRDD(re._1)
  }

  /** Basic GWR calculation with specific bandwidth
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @param bandwidth   bandwidth value
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @return featureRDD and diagnostic String
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double, kernel: String = "gaussian", adaptive: Boolean = false)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWRbasic
    model.init(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    val re = model.fit(bw = bandwidth, kernel = kernel, adaptive = adaptive)
//    print(re._2)
    Service.print(re._2,"Basic GWR calculation with specific bandwidth","String")
    sc.makeRDD(re._1)
  }

}