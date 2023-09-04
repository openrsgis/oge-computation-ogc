package whu.edu.cn.entity

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextSingleton {
  private val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("Test")
  private val sc = new SparkContext(conf)

  def getSparkSc: SparkContext = {
    sc
  }
}
