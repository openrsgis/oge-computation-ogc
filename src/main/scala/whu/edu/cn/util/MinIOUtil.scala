package whu.edu.cn.util

import io.minio._
import SystemConstants._

class MinIOUtil() {
  private lazy val minioClient: MinioClient = MinioClient.builder()
    .endpoint(MINIO_URL)
    .credentials(MINIO_KEY, MINIO_PWD)
    .build()

  def getMinioClient: MinioClient = minioClient
}
