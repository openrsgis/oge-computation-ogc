package whu.edu.cn.util

import io.minio._
import whu.edu.cn.util.GlobalConstantUtil.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_MAX_CONNECTIONS, MINIO_SECRET_KEY}

class MinIOUtil {

  private val connectionPool: Array[MinioClient] = Array.fill(MINIO_MAX_CONNECTIONS)(createMinioClient())

  private def createMinioClient(): MinioClient = {
    lazy val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(1000, 1000, 1000)
    minioClient
  }

  def getMinioClient: MinioClient = {
    // 从连接池中获取可用的 MinioClient
    lazy val client: Option[MinioClient] = connectionPool.synchronized {
      connectionPool.find(_ != null)
    }

    if (client.isDefined) {
      client.get
    } else {
      throw new Exception("No available MinioClient in the connection pool.")
    }
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
