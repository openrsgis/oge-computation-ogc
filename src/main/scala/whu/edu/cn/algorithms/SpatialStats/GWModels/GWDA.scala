package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{DenseMatrix, DenseVector, MatrixSingularException, inv, max, norm, sum}
import breeze.numerics.{NaN, log, sqrt}

import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance.getDistRDD
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.{getSpatialweight, getSpatialweightSingle}
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize.goldenSelection
import whu.edu.cn.oge.Service

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GWDA(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD) {

  //其实应该用一个哈希表存，但是不知道有没有。现在就这样写成了算了。
  private val select_eps = 1e-6
  private var _method: String = "wlda"
  private var _inputY: Array[Any] = _
  private var _distinctLevels: Array[_ <: (Any, Int)] = _
  private var _levelArr: Array[Int] = _
  private var _dataLevels: Array[(String, Int, Int)] = _ //Array是 (键，键对应的int值（从0开始计数），对应的索引位置)
  private var _dataGroups: Map[String, Array[(String, Int, Int)]] = _
  private var _nGroups: Int = 0
  private val _groupX: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]

  private var _correction: Double = 0.0

  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  private var _spWeightArray: Array[DenseVector[Double]] = _
  private var _xcols: Int = 0

  override def setY(property: String): Unit = {
    _nameY = property
    _inputY = _shpRDD.map(t => t._2._2(property)).collect()
  }

  def discriminant(bw: Double = -1, kernel: String = "gaussian", adaptive: Boolean = true, method: String = "wlda"): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    if (bw > 0) {
      setWeightArr(bw, kernel, adaptive)
    } else if (_spWeightArray != null) {

    } else {
      throw new IllegalArgumentException("bandwidth should be over 0 or spatial weight should be initialized")
    }
    _method = method
    val result = _method match {
      case "wqda" => wqda()
      case "wlda" => wlda()
      case _ => throw new IllegalArgumentException("method should be wqda or wlda")
    }
    val method_str = _method match {
      case "wqda" => "weighted quadratic discriminant analysis (wqda)"
      case "wlda" => "weighted linear discriminant analysis (wlda)"
    }
    val formula = _nameY + " ~ " + _nameX.mkString(" + ")
    val bw_type = if (adaptive) {
      "Adaptive"
    } else {
      "Fixed"
    }
    _correction = result._1
    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("group_pred" -> result._2(t._2))
      t._1._2._2 += ("entropy" -> result._4(t._2))
      t._1._2._2 += ("pmax" -> result._6(t._2))
    })
    for (i <- 0 until _nGroups) {
      shpRDDidx.map(t => {
        val logp = result._3(i)
        val probs = result._5(i)
        t._1._2._2 += (_distinctLevels(i)._1 + "_logp" -> logp(t._2))
        t._1._2._2 += (_distinctLevels(i)._1 + "_probs" -> probs(t._2))
      })
    }

    val printString = "\n*********************************************************************************\n" +
      "*              Results of Geographically Discriminate Analysis                  *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Grouping factor:  ${_nameY}  with the following groups:\n\t" + _nameX.mkString(" ") + "\n" +
      s"Formula: $formula" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n" +
      s"Using method: $method_str\n" +
      s"The number of points for prediction is ${_inputY.length}\n" +
      f"The correct ratio is validated as ${_correction}%.4f \n" +
      "*********************************************************************************\n"
    //    println(printString)
//    shpRDDidx.foreach(t => println(t._1._2._2))
    (shpRDDidx.map(_._1), printString)
  }

  def setWeightArr(bw: Double, kernel: String, adaptive: Boolean) = {
    if (_dist == null) {
      _dist = getDistRDD(_shpRDD)
      _disMax = _dist.map(t => max(t)).max
    }
    if (_kernel == null) {
      _kernel = kernel
      _adaptive = adaptive
    }
    _spWeightArray = _dist.collect().map(t => getSpatialweightSingle(DenseVector(t), bw = bw, kernel = kernel, adaptive = adaptive))
  }

  private def bandwidthSelection(kernel: String = "gaussian", adaptive: Boolean = true, method: String = "wlda"): (Double, String) = {
    _method = method
    var bwselect = 0.0
    var printString = "Auto bandwidth selection:\n"
    if (adaptive) {
      bwselect = adaptiveBandwidthSelection(kernel = kernel)
    } else {
      bwselect = fixedBandwidthSelection(kernel = kernel)
    }
    opt_iters.foreach(t => {
      val i = (t - 1).toInt
      printString += f"iter ${t.toInt}, bandwidth: ${opt_value(i)}%.5f, correct ratio: ${opt_result(i)}%.5f\n"
    })
    printString += f"Best bandwidth is $bwselect%.2f"
    //    println(printString)
    (bwselect, printString)
  }

  private def bwSelectCriteria(bw: Double): Double = {
    setWeightArr(bw, _kernel, _adaptive)
    val diag_weight0 = _spWeightArray.clone()
    for (i <- 0 until _rows) {
      diag_weight0(i)(i) = 0
    }
    //    diagWeight0.foreach(t=>println(t))
//    val group_weight = getWeightGroups(diag_weight0)
    _method match {
      case "wqda" => wqda()._1
      case "wlda" => wlda()._1
      case _ => throw new IllegalArgumentException("method should be wqda or wlda")
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", upper: Double = _disMax, lower: Double = _disMax / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, function = bwSelectCriteria)
      bw = re._1
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", upper: Int = _rows, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, function = bwSelectCriteria)
      bw = re._1.toInt
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
    } catch {
      case e: MatrixSingularException => {
//        println("meet matrix singular error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, upper, low)
      }
    }
    bw
  }

  private def getYLevels(data: Array[_ <: Any] = _inputY): Unit = {
    val data_idx = data.zipWithIndex
    val distin_idx = data.distinct.zipWithIndex
    val dlevel = data_idx.map(t => {
      distin_idx.find(_._1 == data(t._2))
    })
    //    println(dlevel.toVector)
    val levels = dlevel.map({
      getSomeInt(_)
    })
    //    println(levels.toVector)
    _distinctLevels = distin_idx
    _levelArr = levels
    _dataLevels = dlevel.zipWithIndex.map(t => (getSomeStr(t._1), getSomeInt(t._1), t._2))
    _dataGroups = _dataLevels.groupBy(_._1)
    _nGroups = _dataGroups.size
    //    levels
  }

  private def getXGroups(x: Array[DenseVector[Double]] = _rawX): Unit = {
    _xcols = x.length
    val nlevel = _distinctLevels.length
    val arrbuf = ArrayBuffer.empty[DenseVector[Double]]
    val x_trans = transhape(x)
    for (i <- 0 until nlevel) { //i 是 第几类的类别循环，有几类循环几次
      for (groups <- _dataGroups.values) { //groups里面是每一组的情况，已经区分好了的
        //        for (j <- x.indices) { // j x的循环，x有多少列，循环多少次
        //          groups.foreach(t => {
        //            if (t._2 == i) {
        //              val tmp = x(j)(t._3)
        //              arrbuf += tmp
        //            }
        //          })
        //        }
        groups.foreach(t => {
          if (t._2 == i) {
            arrbuf += x_trans(t._3)
          }
        })
        if (arrbuf.nonEmpty) {
          _groupX += arrbuf.toArray
        }
        arrbuf.clear()
      }
    }
  }

  private def getWeightGroups(allWeight: Array[DenseVector[Double]] = _spWeightArray): Array[Array[DenseVector[Double]]] = {
    val nlevel = _distinctLevels.length
    val weightbuf = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_trans = transhape(allWeight)
    val spweight_groups: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]
    for (i <- 0 until nlevel) {
      for (groups <- _dataGroups.values) {
        groups.foreach(t => {
          if (t._2 == i) {
            weightbuf += spweight_trans(t._3)
          }
        })
        if (weightbuf.nonEmpty) {
          spweight_groups += weightbuf.toArray
        }
        weightbuf.clear()
      }
    }
    spweight_groups.toArray
  }

  private def getSomeInt(x: Option[(Any, Int)]): Int = x match {
    case Some(s) => s._2
    case None => -1
  }

  private def getSomeStr(x: Option[(Any, Int)]): String = x match {
    case Some(s) => s._1.toString
    case None => null
  }

  //  def getSomeAll(x: Option[(Any, Int)]): (String,Int) = x match {
  //    case Some(s) => (s._1.toString,s._2)
  //    case None => (null,-1)
  //  }


  //其实对于Array(Array)来说，直接用transpose就可以了，所以直接写了一个新的transhape
  private def tranShape(target: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
    val nrow = target.length
    val ncol = target(0).length
    val flat = target.flatMap(t => t.toArray)
    val flat_idx = flat.zipWithIndex
    val reshape = flat_idx.groupBy(t => {
      t._2 % ncol
    }).toArray
    //    reshape.sortBy(_._1)//关键
    val result = reshape.sortBy(_._1).map(t => t._2.map(_._1))
    //对比
    //    val mat1=DenseMatrix.create(nrow,ncol,flat)
    //    val mat2=DenseMatrix.create(ncol,nrow,result.flatten)
    //    println(mat1)
    //    println("***trans***")
    //    println(mat2)
    result.map(t => DenseVector(t))
  }

  def transhape(target: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
    val tmp = target.map(t => t.toArray).transpose
    tmp.map(t => DenseVector(t))
  }

  /**
   *
   * @return correct-ratio(double), lev-prediction, logpvalue(variables), entropy, probs(variables), pmax
   */
  private def wqda(): (Double, Array[String], Array[Array[Double]], DenseVector[Double], Array[Array[Double]], Array[Double]) = {
    //    println("wqda")
    val sigmaGw: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localMean: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localPrior: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_groups = getWeightGroups(_spWeightArray)
    for (i <- 0 until _nGroups) {
      val x1 = tranShape(_groupX(i))
      val w1 = tranShape(spweight_groups(i))
      val re = getLocalMeanSigma(x1, w1)
      localMean += re._1
      sigmaGw += re._2
      val sumWeight = _spWeightArray.map(sum(_)).sum
      val aLocalPrior = w1.map(t => {
        sum(t) / sumWeight
      })
      localPrior += DenseVector(aLocalPrior)
    }

    val groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val sigma_wqda = sigmaGw.toArray.map(t => t.transpose.map(DenseVector(_)))
    val xt = _rawX.map(_.toArray).transpose.map(DenseVector(_)) //x转置
    for (i <- 0 until _nGroups) {
      val meani = localMean(i).transpose.map(DenseVector(_))
      val arrPf = ArrayBuffer.empty[Double]
      for (j <- 0 until _rows) {
        val lognorm = _nGroups / 2.0 * log(norm(sigma_wqda(i)(j)))
        val logprior = log(localPrior(i)(j))
        val covmatj = DenseMatrix.create(_xcols, _xcols, sigma_wqda(i)(j).toArray)
        val pf = 0.5 * (xt(j) - meani(j)).t * inv(covmatj) * (xt(j) - meani(j))
        val logpf = lognorm + pf(0) - logprior
        arrPf += logpf
      }
      groupPf += arrPf.toArray
      arrPf.clear()
    }
    //    println("------------log p result---------")
    //    groupPf.foreach(t => println(t.toVector))
    val groupPf_t = groupPf.toArray.transpose
    val minProbIdx = groupPf_t.map(t => {
      t.indexWhere(_ == t.min)
    })
    //    println("minProbIdx", minProbIdx.toList)
    //    println(_distinctLevels.toList)
    val lev = minProbIdx.map(t => {
      figLevelString(t)
    })
    //    println("------------group predicted---------")
    //    println(lev.toList)
    val sumStats = summary(groupPf_t)
    (validation(lev), lev, groupPf.toArray, sumStats._1, sumStats._2, sumStats._3)
  }

  /**
   *
   * @return correct-ratio(double), lev-prediction, logpvalue(variables), entropy, probs(variables), pmax
   */
  private def wlda(): (Double, Array[String], Array[Array[Double]], DenseVector[Double], Array[Array[Double]], Array[Double]) = {
    //    println("wlda")
    val sigmaGw: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localMean: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localPrior: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_groups = getWeightGroups(_spWeightArray)
    for (i <- 0 until _nGroups) {
      val x1 = tranShape(_groupX(i))
      val w1 = tranShape(spweight_groups(i))
      val re = getLocalMeanSigma(x1, w1)
      localMean += re._1
      sigmaGw += re._2
      //      println(_groupX(i).length)
      val sumWeight = _spWeightArray.map(sum(_)).sum
      val aLocalPrior = w1.map(t => {
        sum(t) / sumWeight
      })
      localPrior += DenseVector(aLocalPrior)
    }

    //        println("------------SigmaGw-------------")
    //        SigmaGw.foreach(t=>println(t.toVector))
    //        println("++++++++++local mean++++++++++++")
    //        localMean.foreach(t=>println(t.toList))
    //        println("------------prior-------------")
    //        localPrior.foreach(t=>println(t.toVector))
    //        println("========sigma used(sigma1)=========")
    //        sigma_wlda.foreach(println)
    //        println("-------------localmean----------")
    //        localMean.foreach(t => println(t.toList))

    val groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val sigmaWlda = getSigmai(sigmaGw.toArray)
    val sigma_wlda = sigmaWlda.map(t => DenseVector(t))
    val xt = _rawX.map(_.toArray).transpose.map(DenseVector(_)) //x转置
    for (i <- 0 until _nGroups) {
      val arrPf = ArrayBuffer.empty[Double]
      val meani = localMean(i).transpose.map(DenseVector(_))
      for (j <- 0 until _rows) {
        val lognorm = _nGroups / 2.0 * log(norm(sigma_wlda(j)))
        val logprior = log(localPrior(i)(j))
        val covmatj = DenseMatrix.create(_xcols, _xcols, sigma_wlda(j).toArray)
        val pf = 0.5 * (xt(j) - meani(j)).t * inv(covmatj) * (xt(j) - meani(j))
        val logpf = lognorm + pf(0) - logprior
        arrPf += logpf
      }
      groupPf += arrPf.toArray
      arrPf.clear()
    }
    //    println("------------log p result---------")
    //    groupPf.foreach(t => println(t.toVector))
    val groupPf_t = groupPf.toArray.transpose
    val minProbIdx = groupPf_t.map(t => {
      t.indexWhere(_ == t.min)
    })
    //    println("minProbIdx", minProbIdx.toList)
    val lev = minProbIdx.map(t => {
      figLevelString(t)
    })
    //    println("------------group predicted---------")
    //    println(lev.toList)
    val sumStats = summary(groupPf_t)
    (validation(lev), lev, groupPf.toArray, sumStats._1, sumStats._2, sumStats._3)
  }

  /**
   *
   * @param groupPf_t
   * @return entropy,probs(variables),pmax
   */
  private def summary(groupPf_t: Array[Array[Double]]): (DenseVector[Double], Array[Array[Double]], Array[Double]) = {
    val np_ent = DenseVector.ones[Double](_xcols) / _xcols.toDouble
    val entMax = shannonEntropy(np_ent.toArray)
    val groupPf_t_exp = groupPf_t.map(t => t.map(x => Math.exp(-x)))
    val probs = groupPf_t_exp.map(t => {
      t.map(x => x / t.sum)
    })
    val pmax = probs.map(t => t.max)
    val probs_shannon = probs.map(t => shannonEntropy(t))
    val entropy = DenseVector(probs_shannon) / entMax
    //    println(s"entmax:$entMax")
    //    println("------------entropy---------")
    //    println(entropy)
    //    println("------------probs---------")
    //    probs.transpose.foreach(t => println(t.toList))
    //    println("------------pmax---------")
    //    println(pmax.toVector)
    (entropy, probs.transpose, pmax)
  }

  private def getLocalMeanSigma(x: Array[DenseVector[Double]], w: Array[DenseVector[Double]]): (Array[Array[Double]], Array[Array[Double]]) = {
    val sigmaGw: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val localMean: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val nlevel = _distinctLevels.length
    for (i <- x.indices) {
      val w_i = w.map(t => {
        val tmp = 1 / sum(t)
        t * tmp
      })
      val aLocalMean = w_i.map(w => w.t * x(i))
      for (j <- x.indices) {
        val aSigmaGw = w_i.map(t => {
          covwt(x(i), x(j), t)
        })
        sigmaGw += aSigmaGw
      }
      localMean += aLocalMean
    }
    (localMean.toArray, sigmaGw.toArray)
  }

  private def getSigmai(SigmaGw: Array[Array[Array[Double]]]): Array[Array[Double]] = {
    val sigmaWlda: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    for (i <- 0 until _rows) {
      var sigmaii = DenseVector.zeros[Double](_xcols * _xcols)
      for (j <- 0 until _nGroups) {
        val groupCounts = _groupX(j).length
        val group_sigmai = SigmaGw(j).transpose
        sigmaii = sigmaii + groupCounts.toDouble * DenseVector(group_sigmai(i))
      }
      sigmaWlda += (sigmaii / _rows.toDouble)
    }
    sigmaWlda.map(t => t.toArray).toArray
  }

  private def figLevelString(rowi: Int): String = {
    var re = "NA"
    for (i <- _distinctLevels.indices) {
      if (rowi == _distinctLevels(i)._2) {
        re = _distinctLevels(i)._1.asInstanceOf[String]
      }
    }
    re
  }

  private def figLevelInt(rowi: Int): Int = {
    var re = -1
    for (i <- _distinctLevels.indices) {
      if (rowi == _distinctLevels(i)._2) {
        re = _distinctLevels(i)._2
      }
    }
    re
  }

  private def covwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double = {
    val sqrtw = w.map(t => sqrt(t))
    val re1 = sqrtw * (x1 - sum(x1 * w))
    val re2 = sqrtw * (x2 - sum(x2 * w))
    val sumww = -sum(w.map(t => t * t)) + 1.0
    sum(re1 * re2 * (1 / sumww))
  }

  private def shannonEntropy(data: Array[Double]): Double = {
    if (data.min < 0 || data.sum <= 0) {
      NaN
    }
    else {
      val pnorm = data.filter(_ > 0).map(_ / data.sum)
      pnorm.map { x =>
        -x * (Math.log(x) / Math.log(2))
      }.sum
    }
  }

  //  def entropy(data: RDD[String]) = {
  //    val size = data.count()
  //    val p = data.map(x => (x, 1)).reduceByKey(_ + _).map {
  //      case (value, num) => num.toDouble / size
  //    }
  //    p.map { x =>
  //      -x * (Math.log(x) / Math.log(2))
  //    }.sum
  //  }

  private def validation(predLev: Array[String]): Double = {
    var nCorrect = 0.0
    for (i <- predLev.indices) {
      if (predLev(i) == _dataLevels(i)._1) {
        nCorrect += 1
      }
    }
    //    println(s"correct ratio: ${nCorrect / _xrows}")
    _correction = nCorrect / _rows
    nCorrect / _rows
  }

}

object GWDA{

  /** GW Discriminant Analysis
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependent property
   * @param propertiesX independent properties
   * @param bandwidth   bandwidth value
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @param method      methods being applied, wlda(default) or wqda
   * @return featureRDD and diagnostic String
   */
  def calculate(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
                bandwidth: Double = -1, kernel: String = "gaussian", adaptive: Boolean = true, method: String = "wlda")
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWDA(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    model.getYLevels()
    model.getXGroups()
    var bw=bandwidth
    var output=""
    if(bandwidth == -1){
      val bwselect=model.bandwidthSelection(kernel, adaptive, method)
      bw = bwselect._1
      output += bwselect._2
    }
    val re = model.discriminant(bw = bw, kernel = kernel, adaptive = adaptive, method = method)
    output += re._2
//    println(output)
    Service.print(output, "GW Discriminant Analysis", "String")
    sc.makeRDD(re._1)
  }

}
