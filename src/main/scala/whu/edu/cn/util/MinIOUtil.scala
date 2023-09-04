package whu.edu.cn.util

import io.minio._
import whu.edu.cn.util.GlobalConstantUtil.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_MAX_CONNECTIONS, MINIO_SECRET_KEY}

object MinIOUtil {
  private val connectionPool: Array[MinioClient] = Array.fill(MINIO_MAX_CONNECTIONS)(createMinioClient())

  private def createMinioClient(): MinioClient = {
    lazy val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10*60*10000, 10*60*10000,10*60*10000)
    minioClient
  }

  def getMinioClient: MinioClient = {
    // 从连接池中获取可用的 MinioClient
    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }

  def releaseMinioClient(client: MinioClient): Unit = {
    // 将 MinioClient 放回连接池
    connectionPool.synchronized {
      val index: Int = connectionPool.indexOf(client)
      if (index != -1) {
        connectionPool(index) = null
      }
    }
  }
}
