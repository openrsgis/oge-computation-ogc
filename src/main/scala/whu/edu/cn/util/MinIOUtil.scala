package whu.edu.cn.util

import io.minio._
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_HEAD_SIZE, MINIO_SECRET_KEY}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.tempFileList

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.file.Paths
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
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

  def getMinioObject(bucketName: String, objectName: String, offset: Long, length: Long): Array[Byte] = {
    val minioClient: MinioClient = getMinioClient
    val getObjArgs: GetObjectArgs = GetObjectArgs.builder()
      .bucket(bucketName)
      .`object`(objectName)
      .offset(offset)
      .length(length)
      .build()
    val inputStream: InputStream = minioClient.getObject(getObjArgs)
    // Read data from stream
    val outStream = new ByteArrayOutputStream
    val buffer = new Array[Byte](MINIO_HEAD_SIZE)
    var len: Int = 0
    while ( {
      len = inputStream.read(buffer)
      len != -1
    }) {
      outStream.write(buffer, 0, len)
    }
    outStream.toByteArray
  }

  def MinIODownload(bucketName: String, objectName: String,downloadPath: String):Unit = {
    val client = MinIOUtil.getMinioClient
    val filePath = downloadPath
    val inputStream = client.getObject(GetObjectArgs.builder.bucket(bucketName).`object`(objectName).build())
    val outputPath = Paths.get(filePath)
    tempFileList.append(filePath)
    Trigger.file_id += 1
    java.nio.file.Files.copy(inputStream, outputPath, REPLACE_EXISTING)
    inputStream.close()
  }

  def MinIOUpload(bucketName: String, objectName: String, filePath: String): Unit = {
    val client = MinIOUtil.getMinioClient
    client.uploadObject(UploadObjectArgs.builder.bucket(bucketName).`object`(objectName).filename(filePath).build())
  }

}
