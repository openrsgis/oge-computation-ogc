package whu.edu.cn.geocube.conf

/**
 * SparkConf config parameters
 */
object SparkCustomConf {
  final val jars = "/home/geocube/mylib/geocube/*"
  final val driverMemory = "16G"
  final val executorMemory = "8G"
  final val coresMax = "16"
  final val executorCores = "1"
  final val bufferMax = "512m"
  final val messageMaxSize = "1024"
  final val maxResultSize = "8g"
}
