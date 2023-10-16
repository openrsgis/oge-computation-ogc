package whu.edu.cn.util;

object GlobalConstantUtil {
  // dag-boot 服务根路径
  final val DAG_ROOT_URL: String = "http://125.220.153.22:8085/oge-dag-22"
  // Redis 基础配置
  final val JEDIS_HOST: String = "125.220.153.26"
  final val JEDIS_PORT: Int = 6379
  final val JEDIS_PWD: String = "ypfamily608"
  // Redis 超时时间
  final val REDIS_CACHE_TTL: Long = 2 * 60L
  // MinIO 基础配置
  final val MINIO_ENDPOINT: String = "http://125.220.153.22:9006"
  final val MINIO_ACCESS_KEY: String = "rssample"
  final val MINIO_SECRET_KEY: String = "ypfamily608"
  final val MINIO_BUCKET_NAME: String = "oge"
  final val MINIO_HEAD_SIZE: Int = 5000000
  final val MINIO_MAX_CONNECTIONS: Int = 10000
  // PostgreSQL 基础配置
  final val POSTGRESQL_URL: String = "jdbc:postgresql://125.220.153.23:30865/oge"
  final val POSTGRESQL_DRIVER: String = "org.postgresql.Driver"
  final val POSTGRESQL_USER: String = "oge"
  final val POSTGRESQL_PWD: String = "ypfamily608"
  final val POSTGRESQL_MAX_RETRIES: Int = 3
  final val POSTGRESQL_RETRY_DELAY: Int = 500

  final val WORK_PREFIX: String = "oge:computation_ogc:existedTiles:"


}
