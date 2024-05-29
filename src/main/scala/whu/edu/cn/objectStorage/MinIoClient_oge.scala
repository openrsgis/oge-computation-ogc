package whu.edu.cn.objectStorage

import io.minio.{GetObjectArgs, MinioClient, UploadObjectArgs}
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_ACCESS_KEY, MINIO_BUCKET_NAME, MINIO_ENDPOINT, MINIO_HEAD_SIZE, MINIO_SECRET_KEY}

import java.io.{File, InputStream}


class MinIOClient_oge(endpoint: String) extends ObjectStorageClient {
  val client = getMinioClient

  override def getInputStream(bucketName: String, path: String, range: Int): InputStream = {
    client.getObject(GetObjectArgs.builder.bucket(MINIO_BUCKET_NAME).`object`(path).offset(0L).length(range).build)
  }

  private def getMinioClient: MinioClient = {
    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(endpoint)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }

  override def DownloadObject(path: String, filepath: String, bucketName: String): Unit = {
    client.getObject(GetObjectArgs.builder.bucket(bucketName).`object`(path).build())
  }

  override def UploadObject(path: String, filepath: String, bucketName: String): Unit = {
    client.uploadObject(UploadObjectArgs.builder.bucket(bucketName).`object`(path).filename(filepath).build())
  }
}