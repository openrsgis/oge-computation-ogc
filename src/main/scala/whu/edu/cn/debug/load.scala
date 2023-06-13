package whu.edu.cn.debug

import org.apache.spark.{SparkConf, SparkContext}

object load {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)


    println("_")
  }


}
