package whu.edu.cn.config

import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import java.net.URL
import java.util

object GlobalConfig {
  //  def main(args: Array[String]): Unit = {
  //    loadConfig()
  //  }

  {
    println("尝试通过配置文件初始化")
    loadConfig()
  }


  def loadConfig(): Unit = {
    val yaml = new Yaml()
    try {
      //      val location: URL = GlobalConfig.getClass.getProtectionDomain.getCodeSource.getLocation
      //      val inputStream = new FileInputStream(location.getPath + "config.yaml")

      var inputStream = new FileInputStream("/home/ogeStorage/config.yaml")
      if (null == inputStream) {
        return
      }
      val data: util.Map[String, Any] = yaml.load(inputStream).asInstanceOf[java.util.Map[String, Any]]
      inputStream.close()

      // 先这样写

      // DagBootConf
      DagBootConf.DAG_ROOT_URL = data.get("DagBootConf").asInstanceOf[java.util.Map[String, String]].get("DAG_ROOT_URL")


      // PostgreSqlConf
      PostgreSqlConf.POSTGRESQL_URL = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, String]].get("POSTGRESQL_URL")
      PostgreSqlConf.POSTGRESQL_DRIVER = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, String]].get("POSTGRESQL_DRIVER")
      PostgreSqlConf.POSTGRESQL_USER = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, String]].get("POSTGRESQL_USER")
      PostgreSqlConf.POSTGRESQL_PWD = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, String]].get("POSTGRESQL_PWD")
      PostgreSqlConf.POSTGRESQL_MAX_RETRIES = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, Int]].get("POSTGRESQL_MAX_RETRIES")
      PostgreSqlConf.POSTGRESQL_RETRY_DELAY = data.get("PostgreSqlConf").asInstanceOf[java.util.Map[String, Int]].get("POSTGRESQL_RETRY_DELAY")


      // Others
      Others.tmsPath = data.get("Others").asInstanceOf[java.util.Map[String, String]].get("tmsPath")
      Others.jsonAlgorithms = data.get("Others").asInstanceOf[java.util.Map[String, String]].get("jsonAlgorithms")
      Others.ontheFlyStorage = data.get("Others").asInstanceOf[java.util.Map[String, String]].get("ontheFlyStorage")
      Others.jsonSavePath = data.get("Others").asInstanceOf[java.util.Map[String, String]].get("jsonSavePath")
      Others.tempFilePath = data.get("Others").asInstanceOf[java.util.Map[String, String]].get("tempFilePath")


      println(data)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  object DagBootConf {
    // dag-boot 服务根路径
    var DAG_ROOT_URL: String = "http://172.22.1.13:8085/oge-dag-22"
    var EDU_ROOT_URL: String = "http://172.22.1.13:8085/oge-dag-22"
  }

  object RedisConf {
    // Redis 基础配置
    final val JEDIS_HOST: String = "125.220.153.26"
    final val JEDIS_PORT: Int = 6379
    final val JEDIS_PWD: String = "ypfamily608"
    // Redis 超时时间
    final val REDIS_CACHE_TTL: Long = 2 * 60L
  }

  object MinioConf {
    // MinIO 基础配置
    final val MINIO_ENDPOINT: String = "http://172.22.1.28:9006"
    final val MINIO_ACCESS_KEY: String = "oge"
    final val MINIO_SECRET_KEY: String = "ypfamily608"
    final val MINIO_BUCKET_NAME: String = "ogebos"
    final val MINIO_HEAD_SIZE: Int = 5000000
    final val MINIO_MAX_CONNECTIONS: Int = 10000
  }

  object BosConf{
    //Bos基础配置
    final val BOS_ENDPOINT: String = "https://s3.bj.bcebos.com"
  }

  object PostgreSqlConf {
    // PostgreSQL 基础配置
    var POSTGRESQL_URL: String = "jdbc:postgresql://172.22.1.13:30865/oge" // "jdbc:postgresql://10.101.240.21:30865/oge""jdbc:postgresql://125.220.153.23:30865/oge"
    var POSTGRESQL_DRIVER: String = "org.postgresql.Driver"
    var POSTGRESQL_USER: String = "oge"
    var POSTGRESQL_PWD: String = "ypfamily608"
    var POSTGRESQL_MAX_RETRIES: Int = 3
    var POSTGRESQL_RETRY_DELAY: Int = 500
  }
  // GcConst
  object GcConf {

    final val localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/gdc_api/"
    final val httpDataRoot = "http://125.220.153.26:8093/data/gdc_api/"
    final val localHtmlRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"
    final val algorithmJson = "/home/geocube/kernel/geocube-core/tb19/process_description.json"


    //landsat-8 pixel value in BQA band from USGS
    final val cloudValueLs8: Array[Int] = Array(2800, 2804, 2808, 2812, 6896, 6900, 6904, 6908)
    final val cloudShadowValueLs8: Array[Int] = Array(2976, 2980, 2984, 2988, 3008, 3012, 3016, 3020, 7072, 7076,
      7080, 7084, 7104, 7108, 7112, 7116)
  }
  object QGISConf {
    // PostgreSQL 基础配置
    var QGIS_DATA: String = "/mnt/storage/algorithmData/"
    var QGIS_ALGORITHMCODE: String = "cd /mnt/storage/qgis/;"
    var QGIS_HOST: String = "172.22.1.19"
    var QGIS_USERNAME: String = "root"
    var QGIS_PASSWORD: String = "Ypfamily608!"
    var QGIS_PORT: Int = 22
  }
  object OTBConf{
    var OTB_DATA: String = "/mnt/storage/otbData/algorithmData/"
    var OTB_ALGORITHMCODE: String = "cd /mnt/storage/otbData/algorithmCodeByOTB/;"
    var OTB_DOCKERDATA : String = "/tmp/otb/"   // docker的临时目录
    var OTB_HOST: String = "172.22.1.19"
    var OTB_USERNAME: String = "root"
    var OTB_PASSWORD: String = "Ypfamily608!"
    var OTB_PORT: Int = 22
  }

  //TODO：改为私有云
  object SAGAConf {
    var SAGA_DATA: String = "/mnt/storage/SAGA/sagaData/"  // docker挂载目录
    var SAGA_DOCKERDATA: String = "/tmp/saga/"   // docker的临时目录
    var SAGA_HOST: String = "172.22.1.19"
    var SAGA_USERNAME: String = "root"
    var SAGA_PASSWORD: String = "Ypfamily608!"
    var SAGA_PORT: Int = 22
  }

  // 第三方算子
  object ThirdApplication {
    final var THIRD_HOST: String = "172.22.1.19"
    final var THIRD_USERNAME: String = "root"
    final var THIRD_PASSWORD: String = "Ypfamily608!"
    final var DOCKER_DATA: String = "/home/dell/cppGDAL/"
    final var SERVER_DATA: String = "/mnt/storage/dem/"
    final var THIRD_PORT: Int = 22
  }

  object Others {
    //    final var thirdJson = "src/main/scala/whu/edu/cn/jsonparser/third-algorithm-infos.json" //存储第三方算子解析文件地址
    final var thirdJson = "/mnt/storage/data/third-algorithm-infos.json"

    final var jsonAlgorithms = "/mnt/storage/algorithms_ogc.json" //存储json解析文件地址
    final var tempFilePath = "/mnt/storage/temp/" //各类临时文件的地址
    final var sagatempFilePath = "/mnt/storage/SAGA/sagaData/" //SAGA各类临时文件的地址/mnt/storage/SAGA/sagaData
    final var otbtempFilePath = "/mnt/storage/otbData/algorithmData/" //OTB各类临时文件的地址/mnt/storage/otbData/algorithmData/
    final var tmsPath = "http://111.37.195.68:8888/api/oge-tms-png/" //tms服务url
    final var tmsHost = "172.22.1.19" //tms服务ip
    final var tomcatHost = "172.22.1.12"
    final var tomcatHost_public = "111.37.195.68"
    final var ontheFlyStorage = "/mnt/storage/on-the-fly/" //tms瓦片存储地址
    final var jsonSavePath = "/mnt/storage/algorithmData/" //geojson临时存储地址
    final var bucketName = "ogebos"
    var platform = "cc"
    final var hbaseHost = "172.22.1.8:2181"
  }
}