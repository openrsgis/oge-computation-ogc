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
    var DAG_ROOT_URL: String = "http://125.220.153.22:8085/oge-dag-22"

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
    final val MINIO_ENDPOINT: String = "http://125.220.153.23:9006"
    final val MINIO_ACCESS_KEY: String = "rssample"
    final val MINIO_SECRET_KEY: String = "ypfamily608"
    final val MINIO_BUCKET_NAME: String = "oge"
    final val MINIO_HEAD_SIZE: Int = 5000000
    final val MINIO_MAX_CONNECTIONS: Int = 10000
  }

  object BosConf{
    //Bos基础配置
    final val BOS_ENDPOINT: String = "https://s3.bj.bcebos.com"
  }

  object PostgreSqlConf {
    // PostgreSQL 基础配置
    var POSTGRESQL_URL: String = "jdbc:postgresql://125.220.153.23:30865/oge" //"jdbc:postgresql://10.101.240.21:30865/oge"
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
    var QGIS_DATA: String = "/home/ogeStorage/algorithmData/"
    var QGIS_ALGORITHMCODE: String = "cd /home/geocube/oge/oge-server/dag-boot/qgis;"
    var QGIS_HOST: String = "10.101.240.10"
    var QGIS_USERNAME: String = "root"
    var QGIS_PASSWORD: String = "ypfamily"
    var QGIS_PORT: Int = 22
    var QGIS_PYTHON: String = "/home/ogeStorage/miniconda3/bin/python"
    var QGIS_RS: String = "/home/geocube/oge/oge-server/dag-boot/qgis/rs/"
  }
  object Others {
    final var jsonAlgorithms = "/home/ogeStorage/algorithms_ogc.json" //存储json解析文件地址
    final var tempFilePath = "/home/ogeStorage/temp/" //各类临时文件的地址
    final var tmsPath = "http://oge.whu.edu.cn/api/oge-tms-png/" //tms服务url
    final var ontheFlyStorage = "/home/ogeStorage/on-the-fly" //tms瓦片存储地址
    final var jsonSavePath = "/home/ogeStorage/algorithmData/" //geojson临时存储地址
    final var platform = "bmr"//平台名称
  }
}