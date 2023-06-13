package whu.edu.cn.util

import io.minio._
import whu.edu.cn.util.GlobalConstantUtil.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY}

class MinIOUtil() {
  private lazy val minioClient: MinioClient = MinioClient.builder()
    .endpoint(MINIO_ENDPOINT)
    .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    .build()

  def getMinioClient: MinioClient = minioClient
}
