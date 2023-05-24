package whu.edu.cn.util

import io.minio._

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

class MinIOConnectionPool(endpoint: String, accessKey: String, secretKey: String, maxConnections: Int) {
  private val connectionPool: BlockingQueue[MinioClient] = new LinkedBlockingQueue[MinioClient](maxConnections)

  private def createConnection(): MinioClient = {
    new MinioClient.Builder()
      .endpoint(endpoint)
      .credentials(accessKey, secretKey)
      .build()
  }

  private def getConnection(): MinioClient = {
    Option(connectionPool.poll()).getOrElse(createConnection())
  }

  private def returnConnection(client: MinioClient): Unit = {
    if (!connectionPool.offer(client)) {
      // 如果连接池已满，不做任何操作
    }
  }

  def withClient[T](f: MinioClient => T): T = {
    val client = getConnection()
    try {
      f(client)
    } finally {
      returnConnection(client)
    }
  }
}
