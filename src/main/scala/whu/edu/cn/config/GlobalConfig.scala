package whu.edu.cn.config

object GlobalConfig {
  object DagBootConf {
    // dag-boot 服务根路径
    final val DAG_ROOT_URL: String = "http://125.220.153.22:8085/oge-dag-22"

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
    final val MINIO_ENDPOINT: String = "http://125.220.153.22:9006"
    final val MINIO_ACCESS_KEY: String = "rssample"
    final val MINIO_SECRET_KEY: String = "ypfamily608"
    final val MINIO_BUCKET_NAME: String = "oge"
    final val MINIO_HEAD_SIZE: Int = 5000000
    final val MINIO_MAX_CONNECTIONS: Int = 10000
  }

  object PostgreSqlConf {
    // PostgreSQL 基础配置
    final val POSTGRESQL_URL: String = "jdbc:postgresql://125.220.153.23:30865/oge"
    final val POSTGRESQL_DRIVER: String = "org.postgresql.Driver"
    final val POSTGRESQL_USER: String = "oge"
    final val POSTGRESQL_PWD: String = "ypfamily608"
    final val POSTGRESQL_MAX_RETRIES: Int = 3
    final val POSTGRESQL_RETRY_DELAY: Int = 500
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

  object Others{
    final val jsonAlgorithms = "src/main/scala/whu/edu/cn/jsonparser/algorithms_ogc.json" //存储json解析文件地址
    final val tempFilePath = "/mnt/storage/temp/" //各类临时文件的地址
    final val tmsPath = "http://oge.whu.edu.cn/api/oge-tms-png/" //tms服务url
    final val ontheFlyStorage = "/mnt/storage/on-the-fly" //tms瓦片存储地址
    final val jsonSavePath = "/mnt/storage/algorithmData/" //geojson临时存储地址
  }
}
