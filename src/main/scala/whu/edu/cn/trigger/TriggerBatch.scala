package whu.edu.cn.trigger

import org.apache.spark.{SparkConf, SparkContext}

object TriggerBatch {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("batch")
    val sc = new SparkContext(conf)
    Trigger.runBatch(sc, args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
  }

}
