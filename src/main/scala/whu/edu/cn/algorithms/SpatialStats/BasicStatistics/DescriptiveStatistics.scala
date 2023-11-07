package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.plot.{Figure, HistogramBins, hist}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.Map


object DescriptiveStatistics {

  /** Descriptive statistics for specific property of feature
   *
   * @param featureRDD  shapefile
   * @param property property
   * @param histBins number of histogram bins
   * @return pic and count, sum, stdev .etc
   */
  def result(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, histBins: Int = 10): String = {
    val lr = featureRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble)
    describe(lr, lr.collect().toList, histBins)
  }


  /** descriptive_statistics:for the given list,get the result,containing:
   * 1)the count,mean,standard deviation,maximum,minimum,sum,and variance
   * 2)the histogram with the number of bins given
   *
   * @param listrdd :RDD[Double],created by list[Double]
   * @param list    :List[Double]
   * @param m_bins  :number of histogram bins
   */
  def describe(listrdd: RDD[Double], list: List[Double], m_bins: HistogramBins = 10): String = {
    val stats = listrdd.stats()
    //    val sum = listrdd.sum()
    //    val variance = listrdd.variance()
    //    println(stats)
    //    println("sum", sum)
    //    println("variance", variance)

    val f = Figure()
    val p = f.subplot(0)
    p += hist(list, m_bins)
    p.title = "histogram"
    //    f.saveas("hist.png")
    var str = f"count : ${stats.count % .4f}\n"
    str += f"sum : ${stats.sum % .4f}\n"
    str += f"stdev : ${stats.stdev % .4f}\n"
    str += f"variance : ${stats.variance % .4f}\n"
    str += f"max : ${stats.max % .4f}\n"
    str += f"min : ${stats.min % .4f}\n"
    println(str)
    str
  }
}
