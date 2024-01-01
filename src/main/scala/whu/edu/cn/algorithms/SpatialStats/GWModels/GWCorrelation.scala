package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, linspace, qr, ranks, sum, trace}
import breeze.stats.median
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.util.control.Breaks
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.oge.Service

import scala.collection.mutable

class GWCorrelation extends GWRbase {

  private var shpRDDidx: Array[((String, (Geometry, mutable.Map[String, Any])), Int)] = _

  def calCorrelation(bw: Double = 100, kernel: String = "gaussian", adaptive: Boolean = true): (Array[(String, (Geometry, mutable.Map[String, Any]))], String) = {
    setweight(bw = bw, kernel = kernel, adaptive = adaptive)
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    var str = "\n*********************************************************************************\n" +
      "*               Results of Geographically Weighted Correlation                  *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Kernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n" +
      "******************************Summary  information*******************************\n"
    shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    val reStr = new ArrayBuffer[Array[(String, Double, Double, Double)]]()
    for (i <- 0 until _xcols) {
      for (j <- i + 1 until _xcols) {
        reStr += calCorrelationSerial(i, j)
      }
    }
    str += "\t\t\t\t\t\t  min\t\t\t\t  median\t\t\t\tmax\n"
    reStr.foreach(t => {
      for (i <- t.indices) {
        str += f"${t(i)._1}%-25s${t(i)._2}%-20.4f${t(i)._3}%-20.4f${t(i)._4}%-20.4f\n"
      }
    })
    str += "*********************************************************************************\n"
        print(str)
    (shpRDDidx.map(t => t._1), str)
  }

  private def calCorrelationSerial(ix1:Int,ix2:Int): Array[(String, Double, Double, Double)] = {
    val x1 = _X(ix1)
    val x2 = _X(ix2)
    val w_i = spweight_dvec.map(t => {
      val tmp = 1 / sum(t)
      t * tmp
    })
    //    val sum_wi2=w_i.map(t1=>sum(t1.map(t2=>t2*t2)))
    val aLocalMean = w_i.map(w => w.t * x1)
    val x_lm = aLocalMean.map(t => {
      x1.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(i => i * i))
    //    val x_lm3 = x_lm.map(t => t.map(i => i * i * i))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
    val aLocalMean2 = w_i.map(w => w.t * x2)
    val x2_lm = aLocalMean2.map(t => {
      x2.map(i => {
        i - t
      })
    })
    val x2_lm2 = x2_lm.map(t => t.map(i => i * i))
    val aLVar2 = w_ii.map(t => {
      t._1.t * x2_lm2(t._2)
    })
    val covmat = w_i.map(t => {
      covwt(x1, x2, t)
    })
    val corrmat = w_ii.map(t => {
      val sum_wi2 = sum(t._1.map(i => i * i))
      val covjj = aLVar(t._2) / (1.0 - sum_wi2)
      val covkk = aLVar2(t._2) / (1.0 - sum_wi2)
      covwt(x1, x2, t._1) / sqrt(covjj * covkk)
    })
    val scorrmat = w_i.map(t => {
      corwt(DenseVector(ranks(x1)), DenseVector(ranks(x2)), t)
    })
    shpRDDidx.map(t => {
      t._1._2._2 += ("cov_" + _nameX(ix1) + "_" + _nameX(ix2) -> covmat(t._2))
      t._1._2._2 += ("corr_" + _nameX(ix1) + "_" + _nameX(ix2) -> corrmat(t._2))
      t._1._2._2 += ("scorr_" + _nameX(ix1) + "_" + _nameX(ix2) -> scorrmat(t._2))
    })
//    println("cov_" + _nameX(ix1) + "_" + _nameX(ix2), covmat.toVector)
//    println("corr_" + _nameX(ix1) + "_" + _nameX(ix2), corrmat.toVector)
//    println("scorr_" + _nameX(ix1) + "_" + _nameX(ix2), scorrmat.toVector)
    val mmmStr = new Array[(String, Double, Double, Double)](3)
    mmmStr(0) = findmmm("cov_" + _nameX(ix1) + "_" + _nameX(ix2), covmat)
    mmmStr(1) = findmmm("corr_" + _nameX(ix1) + "_" + _nameX(ix2), corrmat)
    mmmStr(2) = findmmm("scorr_" + _nameX(ix1) + "_" + _nameX(ix2), scorrmat)
    mmmStr
  }

  private def covwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double  = {
    val sqrtw = w.map(t => sqrt(t))
    val re1 = sqrtw * (x1 - sum(x1 * w))
    val re2 = sqrtw * (x2 - sum(x2 * w))
    val sumww = - sum(w.map(t => t * t)) + 1.0
    sum(re1 * re2 * (1 / sumww))
  }

  private def corwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double = {
    covwt(x1, x2, w) / sqrt(covwt(x1, x1, w) * covwt(x2, x2, w))
  }

  private def findmmm(str: String, arr: Array[Double]): (String, Double, Double, Double) = {
    val med = median(DenseVector(arr))
    (str, arr.min, med, arr.max)
  }

}

object GWCorrelation {

  /** Geographically weighted Statistic Summary: Correlation
   *
   * @param sc          SparkContext
   * @param featureRDD  shapefile RDD
   * @param propertyY   dependant property
   * @param propertiesX independant properties
   * @param bandwidth          bandwidth value
   * @param kernel      kernel function: including gaussian, exponential, bisquare, tricube, boxcar
   * @param adaptive    true for adaptive distance, false for fixed distance
   * @return featureRDD and diagnostic String
   */
  def cal(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double = 10, kernel: String = "gaussian", adaptive: Boolean = true)
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val model = new GWCorrelation
    model.init(featureRDD)
    model.setX(propertiesX)
    model.setY(propertyY)
    val r = model.calCorrelation(bandwidth, kernel, adaptive)
    //    print(r._2)
    Service.print(r._2, "Geographically weighted Statistic Summary: Correlation", "String")
    sc.makeRDD(r._1)
  }

}