package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}
import breeze.plot.{Figure, plot}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance.getDist
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.{Array2DenseVector, getSpatialweight, getSpatialweightSingle}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.util.ShapeFileUtil.{builderFeatureType, readShp}
import whu.edu.cn.oge.Service

import java.awt.Graphics2D
import java.awt.image.BufferedImage
import scala.collection.mutable

class GWRbasic(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD) {

  val select_eps = 1e-2
  var _nameUsed: Array[String] = _
  var _outString: String = _

  private val epsilon = 1e-6 // 正则化常数，确保矩阵可逆

  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  private var predRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _

  private def resetX(name: Array[String]): Unit = {
    _nameUsed = name
    val x = name.map(s => {
      DenseVector(_shpRDD.map(t => t._2._2(s).asInstanceOf[String].toDouble).collect())
    })
    _rawX = x
    _rows = x(0).length
    _cols = x.length + 1
    val onesVector = DenseVector.ones[Double](_rows)
    _dvecX = onesVector +: x
    _dmatX = DenseMatrix.create(rows = _rows, cols = _cols, data = _dvecX.flatMap(_.toArray))
  }

  def initPredict(pRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): RDD[Array[Double]] = {
    val predCoor = pRDD.map(t => t._2._1.getCentroid.getCoordinate)
    val rddCoor = _shpRDD.map(t => t._2._1.getCentroid.getCoordinate).collect()
    val pdist = predCoor.map(p => {
      rddCoor.map(t => t.distance(p))
    })
    _dist=pdist//带宽优选的化，_dist要重新赋值。这里能更改到吗？
    pdist
  }

  def auto(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    var printString = "Auto bandwidth selection\n"
    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    opt_iters.foreach(t => {
      val i = (t - 1).toInt
      printString += (f"iter ${t.toInt}, bandwidth: ${opt_value(i)}%.2f, $approach: ${opt_result(i)}%.3f\n")
    })
    printString += f"Best bandwidth is $bwselect%.2f\n"
    //    println(s"best bandwidth is $bwselect")
    //    val d= new BufferedImage(900,600,BufferedImage.TYPE_4BYTE_ABGR)
    //    val g=d.createGraphics()
    //    g.drawString("str",10,10)
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
    val bw = if (_adaptive) {
      10 * _rows
    } else {
      10 * _disMax
    }
    //    setWeight(bw, _kernel, adaptive = false)
    bandwidthAICc(bw)
  }

  def fit(bw: Double = 0, kernel: String = "gaussian", adaptive: Boolean = true): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    if (bw <= 0) {
      throw new IllegalArgumentException("bandwidth should be over 0")
    }
    _kernel=kernel
    _adaptive=adaptive
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = fitFunction(weight = newWeight)
//    results._1.take(20).foreach(println)
//    val betas = DenseMatrix.create(_cols, _rows, data = results._1.flatMap(t => t.toArray))
    val betas = results._1
    val arr_yhat = results._2.toArray
    val arr_residual = results._3.toArray
    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> arr_yhat(t._2))
      t._1._2._2 += ("residual" -> arr_residual(t._2))
    })
    //    results._1.map(t=>mean(t))
    if (_nameUsed == null) {
      _nameUsed = _nameX
    }
    val name = Array("Intercept") ++ _nameUsed
    for (i <- 0 until _cols) {
      shpRDDidx.map(t => {
        t._1._2._2 += (name(i) -> betas(t._2)(i))
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
    fitString += calDiagnostic(_dmatX, _dvecY, results._3, results._4)
    (shpRDDidx.map(t => t._1), fitString)
  }

  def fitFunction(X: DenseMatrix[Double] = _dmatX, Y: DenseVector[Double] = _dvecY, weight: RDD[DenseVector[Double]] = _spWeight):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double]) = {

    val xtw = weight.map(w => {
      val each_col_mat = _dvecX.map(t => t * w).flatMap(_.toArray)
      new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
    })
    //    val xtw0=xtw.collect()
    val betas = xtw.map(xtw => {
      try {
        val xtwx = xtw * X
        val xtwy = xtw * Y
        val xtwx_inv = inv(xtwx)
        xtwx_inv * xtwy
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(xtw * X))
            regularized * xtw * Y
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          println(s"An unexpected error occurred: ${e.getMessage}")
          DenseVector.zeros[Double](_cols)
      }
    })
    //    val betas0 = betas.collect()
    //    betas0.foreach(println)
    val ci: RDD[DenseMatrix[Double]] = xtw.map(t => {
      try {
        val xtwx_inv = inv(t * X)
        xtwx_inv * t // 继续进行矩阵乘法
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(t * X)) // 先进行正则化，再求逆
            regularized * t
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          throw new RuntimeException("An unexpected error occurred during matrix computation.")
      }
    })
    val ci_idx = ci.zipWithIndex
    //    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => {
      val a = X(t._2.toInt, ::).inner.toDenseMatrix
      val b = t._1.toDenseMatrix
      a * b
    })
    val shat = DenseMatrix.create(rows = si.collect().length, cols = si.collect().length, data = si.collect().flatMap(_.toArray))
    val yhat = getYHat(X, betas.collect())
    val residual = Y - yhat
    //    val s = calDiagnostic(X, Y, residual, shat)
    //    println(s)
    (betas.collect(), yhat, residual, shat)
  }

    def predict(pRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], bw: Double, kernel: String, adaptive: Boolean): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
      val pdist=initPredict(pRDD)
      val pn=pdist.count()
      val pweight = getSpatialweight(pdist, bw = bw, kernel = kernel, adaptive = adaptive)
      _spWeight=pweight

      //添加一个approach参数
      //switch case:null=>不优选 case:CV=>优选 case:AIC=>优选 case:其它=>不优选（或报错）

//      var printString = "Auto bandwidth selection\n"
//      val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
//      opt_iters.foreach(t => {
//        val i = (t - 1).toInt
//        printString += (f"iter ${t.toInt}, bandwidth: ${opt_value(i)}%.2f, $approach: ${opt_result(i)}%.3f\n")
//      })
//      printString += f"Best bandwidth is $bwselect%.2f\n"

      val X=_dmatX
      val Y=_dvecY
      val xtw = pweight.map(w => {
        val each_col_mat = _dvecX.map(t => t * w).flatMap(_.toArray)
        new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
      })
      val betas = xtw.map(xtw => {
        try {
          val xtwx = xtw * X
          val xtwy = xtw * Y
          val xtwx_inv = inv(xtwx)
          xtwx_inv * xtwy
        } catch {
          case e: breeze.linalg.MatrixSingularException =>
            try {
              val regularized = inv(regularizeMatrix(xtw * X))
              regularized * xtw * Y
            } catch {
              case e: Exception =>
                throw new IllegalStateException("Matrix inversion failed")
            }
          case e: Exception =>
            println(s"An unexpected error occurred: ${e.getMessage}")
            DenseVector.zeros[Double](_cols)
        }
      }).collect()
      val yhat = getYHat(X, betas)//predict value
      val residual = Y - yhat
      val shpRDDidx = pRDD.collect().zipWithIndex
      shpRDDidx.foreach(t => t._1._2._2.clear())
      shpRDDidx.map(t => {
        t._1._2._2 += ("predicted" -> yhat(t._2))
        t._1._2._2 += ("residual" -> residual(t._2))
      })
      //只留下预测值
//      val name = Array("Intercept") ++ _nameX
//      shpRDDidx.foreach(t=>{
//        for(i<-name.indices){
//          t._1._2._2 += (name(i)+"_coef" -> betas(t._2)(i))
//        }
//      })
      val bw_type = if (adaptive) {
        "Adaptive"
      }else{
        "Fixed"
      }
      val fitFormula = _nameY + " ~ " + _nameX.mkString(" + ")
      val fitString = "\n*********************************************************************************\n" +
        "*               Results of Geographically Weighted Regression                   *\n" +
        "*********************************************************************************\n" +
        "**************************Model calibration information**************************\n" +
        s"Formula: $fitFormula" +
        s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n" +
        s"Prediction established for $pn points \n" +
        "*********************************************************************************\n"
//      shpRDDidx.foreach(t=>println(t._1._2._2))
//      println(fitString)
      (shpRDDidx.map(t => t._1), fitString)
    }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): Double = {
    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Double = _disMax, lower: Double = _disMax / 5000.0): Double = {
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

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Int = _rows - 1, lower: Int = 20): Int = {
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
        println("meet matrix singular error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def bandwidthAICc(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = fitFunction(weight = newWeight)
    val residual = results._3
    val shat = results._4
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = _rows
    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
  }

  private def bandwidthCV(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val cvWeight = modifyWeightCV(newWeight)
    val results = fitFunction(weight = cvWeight)
    val residual = results._3
    residual.toArray.map(t => t * t).sum
  }

  //  private def getYhat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
  //    val arrbuf = new ArrayBuffer[Double]()
  //    for (i <- 0 until X.rows) {
  //      val rowvec = X(i, ::).inner
  //      val yhat = sum(betas(i) * rowvec)
  //      arrbuf += yhat
  //    }
  //    DenseVector(arrbuf.toArray)
  //  }

  protected def getYHat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val betas_idx = betas.zipWithIndex
    val yhat = betas_idx.map(t => {
      sum(t._1 * X(t._2, ::).inner)
    })
    DenseVector(yhat)
  }

  //  private def getYHatRDD(X: DenseMatrix[Double], betas: RDD[DenseVector[Double]]) = {
  //    val betas_idx = betas.zipWithIndex()
  //    val yhat = betas_idx.map(t => {
  //      sum(t._1 * X(t._2.toInt, ::).inner)
  //    })
  //    yhat
  //  }

  //  private def modifyWeight(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
  //    weightRDD.map()
  //  }

  //这个是有问题的
  private def modifyWeightCV(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.map { t =>
      val modifiedVector = t.copy
      for (i <- 0 until t.length) {
        modifiedVector(i) = 0
      }
      modifiedVector
    }
  }

  // 正则化函数，向矩阵的对角线添加小常数 epsilon
  def regularizeMatrix(matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val eye = DenseMatrix.eye[Double](matrix.rows)
    matrix + eye * epsilon //添加epsilon
  }

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
    val model = new GWRbasic(featureRDD)
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
    val model = new GWRbasic(featureRDD)
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
    val model = new GWRbasic(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    val re = model.fit(bw = bandwidth, kernel = kernel, adaptive = adaptive)
    //    print(re._2)
    Service.print(re._2,"Basic GWR calculation with specific bandwidth","String")
    sc.makeRDD(re._1)
  }

  //是否需要带宽优选？

  /**
   *
   * @param sc  SparkContext
   * @param featureRDD  shapefile RDD
   * @param predictRDD  predict shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @param bandwidth   bandwidth value
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @return featureRDD and diagnostic String
   */
  def predict(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],predictRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double, kernel: String = "gaussian", adaptive: Boolean = false)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWRbasic(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    model.initPredict(predictRDD)
    val re=model.predict(predictRDD,bw = bandwidth, kernel = kernel, adaptive = adaptive)
    //    print(re._2)
    Service.print(re._2,"Basic GWR prediction with specific bandwidth","String")
    sc.makeRDD(re._1)
  }

}