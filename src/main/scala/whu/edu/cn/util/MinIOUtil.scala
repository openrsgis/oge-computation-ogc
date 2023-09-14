package whu.edu.cn.util

import io.minio._
import whu.edu.cn.util.GlobalConstantUtil.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_MAX_CONNECTIONS, MINIO_SECRET_KEY}

import scala.collection.mutable.ArrayBuffer

object MinIOUtil {
  private val connectionPool: ArrayBuffer[MinioClient] = ArrayBuffer.fill(MINIO_MAX_CONNECTIONS)(createMinioClient())

  private def createMinioClient(): MinioClient = {
    lazy val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }

  def getMinioClient: MinioClient = {
    connectionPool.synchronized {
      // 为空则新建连接
      if (connectionPool.isEmpty) {
        return MinioClient.builder()
          .endpoint(MINIO_ENDPOINT)
          .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
          .build()
      }

      // 否则从连接池获取
      return connectionPool.remove(connectionPool.size - 1)
    }
  }

  def releaseMinioClient(client: MinioClient): Unit = {
    // 将使用后的 MinioClient 放回连接池
    connectionPool.synchronized {
      if (client != null) {
        connectionPool.append(client)
      }
    }
  }
}
