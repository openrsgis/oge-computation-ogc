package whu.edu.cn.algorithms.SpatialStats.STCorrelations

import breeze.linalg.{DenseVector, linspace}
import breeze.plot._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.math.pow

object TemporalAutoCorrelation {

  /** Autocorrelation Coefficient
   *
   * @param featureRDD   shapefile RDD
   * @param property property
   * @param timelag  time lag, default:20
   * @return pic, ACF list
   */
  def ACF(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, timelag: Int = 20): String = {
    val timeArr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect()
    timeSeriesACF(timeArr, timelag.toInt)
  }

  /**
   * 输入属性Array计算自相关滞后结果
   *
   * @param timeArr 属性Arr
   * @param timelag 阶数，默认为20
   * @return ACF list
   */
  def timeSeriesACF(timeArr: Array[Double], timelag: Int = 20): String = {
    val acfarr = DenseVector.zeros[Double](timelag + 1).toArray
    if (timelag > 0) {
//      val f = Figure()
//      val p = f.subplot(0)
      for (i <- 0 until timelag + 1) {
        acfarr(i) = getacf(timeArr, i)
//        val x = DenseVector.ones[Double](5) :*= i.toDouble
//        val y = linspace(0, acfarr(i), 5)
//        p += plot(x, y, colorcode = "[0,0,255]")
      }
//      p.xlim = (-0.1, timelag + 0.1)
//      p.xlabel = "lag"
//      p.ylabel = "ACF"
    } else {
      throw new IllegalArgumentException("Illegal Argument of time lag")
    }
    val str = acfarr.map(t=>t.formatted("%.4f")).mkString("ACF-list(", ", ", ")")
    println(str)
    str
  }

  def getacf(timeArr: Array[Double], timelag: Int): Double = {
    val lagArr = timeArr.drop(timelag)
    val tarridx = timeArr.take(lagArr.length).zipWithIndex
    val avg = timeArr.sum / timeArr.length
    val tarr_avg = timeArr.map(t => t * avg).sum
    val tarr_avg2 = tarridx.map(t => t._1 * avg).sum
    val lag_avg = lagArr.map(t => t * avg).sum
    //    var tarr_lag:Double =0.0
    //    for(i<-0 until lagArr.length){
    //      tarr_lag += timeArr(i)*lagArr(i)
    //    }
    val tarr_lag = tarridx.map(t => t._1 * lagArr(t._2)).sum
    val acf = (tarr_lag - tarr_avg2 - lag_avg + pow(avg, 2) * lagArr.length) / (timeArr.map(t => t * t).sum - 2 * tarr_avg + pow(avg, 2) * timeArr.length)
    acf
  }

}
