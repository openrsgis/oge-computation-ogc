package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.plot.{Figure, HistogramBins, hist}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.List


object DescriptiveStatistics{

  /**descriptive_statistics:for the given list,get the result,containing:
    * 1)the count,mean,standard deviation,maximum,minimum,sum,and variance
    * 2)the histogram with the number of bins given
    *
    * @param listrdd:RDD[Double],created by list[Double]
    * @param list:List[Double]
    * @param m_bins:number of histogram bins
    */
  def describe(listrdd:RDD[Double],list:List[Double],m_bins:HistogramBins):Unit={
    val stats=listrdd.stats()
    val sum=listrdd.sum()
    val variance=listrdd.variance()
    println(stats)
    println("sum",sum)
    println("variance",variance)

    val f=Figure()
    val p=f.subplot(0)
    p+=hist(list,m_bins)
    p.title="histogram"
    f.saveas("hist.png")

  }
}
