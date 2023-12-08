package whu.edu.cn.util

import io.minio._
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY}

import scala.collection.mutable.ArrayBuffer

object MinIOUtil {
  def getMinioClient: MinioClient = {
    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }

}
