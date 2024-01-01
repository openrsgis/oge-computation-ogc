package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, linspace, mmwrite, qr, sum, trace}
import breeze.stats.median
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Service

import scala.math._
import scala.collection.mutable

class GWAverage extends GWRbase {

  private var shpRDDidx: Array[((String, (Geometry, mutable.Map[String, Any])), Int)] = _

  private def findq(x: DenseVector[Double], w: DenseVector[Double], p: DenseVector[Double] = DenseVector(0.25, 0.50, 0.75)): DenseVector[Double] = {
    val lp = p.length
    val q = DenseVector(0.0, 0.0, 0.0)
    val x_ord = x.toArray.sorted
    val w_idx = w.toArray.zipWithIndex
    val x_w = w_idx.map(t => (x(t._2), t._1))
    val x_w_sort = x_w.sortBy(t => t._1)
    val w_ord = x_w_sort.map(t => t._2)
    //    println(w_ord.toVector)
    val w_ord_idx = w_ord.zipWithIndex
    val cumsum = w_ord_idx.map(t => {
      w_ord.take(t._2 + 1).sum
    })
    //    println(cumsum.toVector)
    for (j <- 0 until lp) {
      //找小于等于的，所以找大于的第一个，然后再减1，就是小于等于的最后一个
      val c_find = cumsum.find(_ > p(j))
      val c_first = c_find match {
        case Some(d) => d
        case None => 0.0
      }
      //减1
      var c_idx = cumsum.indexOf(c_first) - 1
      if (c_idx < 0) {
        c_idx = 0
      }
      q(j) = x_ord(c_idx)
      //      println(s"q $j $q")
    }
    q
  }

  def calAverage(bw: Double = 0, kernel: String = "gaussian", adaptive: Boolean = true, quantile: Boolean = false): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    setweight(bw = bw, kernel = kernel, adaptive = adaptive)
    shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    var str = "\n*********************************************************************************\n" +
      "*                Results of Geographically Weighted Average                     *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Kernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n" +
      "******************************Summary  information*******************************\n"
    val Xidx = _X.zipWithIndex
    val reStr = Xidx.map(t => {
      calAverageSerial(t._1, t._2, quantile)
    })
    str += "\t\t\t\t\tmin\t\t\t\t\tmedian\t\t\t\tmax\n"
    reStr.foreach(t => {
      for (i <- t.indices) {
        str += f"${t(i)._1}%-18s${t(i)._2}%-20.4f${t(i)._3}%-20.4f${t(i)._4}%-20.4f\n"
      }
    })
    str += "*********************************************************************************\n"
        print(str)
    (shpRDDidx.map(t => t._1), str)
  }

  private def calAverageSerial(x: DenseVector[Double], num: Int, quantile: Boolean = false): Array[(String, Double, Double, Double)] = {
    val name = _nameX(num)
    val w_i = spweight_dvec.map(t => {
      val tmp = 1 / sum(t)
      t * tmp
    })
    val aLocalMean = w_i.map(w => w.t * x)
    val x_lm = aLocalMean.map(t => {
      x.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(x => x * x))
    val x_lm3 = x_lm.map(t => t.map(x => x * x * x))
    //    x_lm2.map(t => t.foreach(println))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
    //    val quantile = true
    if (quantile) {
      val quant = w_i.map(t => {
        findq(x, t)
      })
      val quant0 = quant.map(t => {
        val tmp = t.toArray
        tmp(0)
      })
      val quant1 = quant.map(t => {
        val tmp = t.toArray
        tmp(1)
      })
      val quant2 = quant.map(t => {
        val tmp = t.toArray
        tmp(2)
      })
      //      println("calculate quantile value")
      val mLocalMedian = quant1
      val mIQR = DenseVector(quant2) - DenseVector(quant0)
      val mQI = ((2.0 * DenseVector(quant1)) - DenseVector(quant2) - DenseVector(quant0)) / mIQR
      shpRDDidx.map(t => {
        t._1._2._2 += (name + "_LMed" -> mLocalMedian(t._2))
        t._1._2._2 += (name + "_IQR" -> mIQR(t._2))
        t._1._2._2 += (name + "_QI" -> mQI(t._2))
      })
    }
    val aStandardDev = aLVar.map(t => sqrt(t))
    val aLocalSkewness = w_ii.map(t => {
      (t._1.t * x_lm3(t._2)) / (aLVar(t._2) * aStandardDev(t._2))
    })
    val mLcv = DenseVector(aStandardDev) / DenseVector(aLocalMean)
    shpRDDidx.map(t => {
      t._1._2._2 += (name + "_LM" -> aLocalMean(t._2))
      t._1._2._2 += (name + "_LVar" -> aLVar(t._2))
      t._1._2._2 += (name + "_LSD" -> aStandardDev(t._2))
      t._1._2._2 += (name + "_LSke" -> aLocalSkewness(t._2))
      t._1._2._2 += (name + "_LCV" -> mLcv(t._2))
    })
    val mmmStr = new Array[(String, Double, Double, Double)](5)
    mmmStr(0) = findmmm(name + "_LM", aLocalMean)
    mmmStr(1) = findmmm(name + "_LVar", aLVar)
    mmmStr(2) = findmmm(name + "_LSD", aStandardDev)
    mmmStr(3) = findmmm(name + "_LSke", aLocalSkewness)
    mmmStr(4) = findmmm(name + "_LCV", mLcv.toArray)
    mmmStr
  }

  private def findmmm(str: String, arr: Array[Double]): (String, Double, Double, Double) = {
    val med = median(DenseVector(arr))
    (str, arr.min, med, arr.max)
  }

}

object GWAverage {

  /** Geographically weighted Statistic Summary: Average
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependant property
   * @param propertiesX independant properties
   * @param bandwidth          bandwidth value
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @param quantile    true for quantile value calculation
   * @return featureRDD and diagnostic String
   */
  def cal(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double = 10, kernel: String = "gaussian", adaptive: Boolean = true, quantile: Boolean = false)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWAverage
    model.init(featureRDD)
    model.setX(propertiesX)
    model.setY(propertyY)
    val r = model.calAverage(bandwidth, kernel, adaptive, quantile)
    //    print(r._2)
    Service.print(r._2, "Geographically weighted Statistic Summary: Average", "String")
    sc.makeRDD(r._1)
  }

}