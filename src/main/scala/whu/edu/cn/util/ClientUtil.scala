package whu.edu.cn.util

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.{BosObject, GetObjectRequest}
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}
import io.minio.{GetObjectArgs, MinioClient, UploadObjectArgs}
import whu.edu.cn.config.GlobalConfig.BosConf.{BOS_ACCESS_KEY, BOS_BUCKET_NAME, BOS_ENDPOINT, BOS_SECRET_ACCESS}
import whu.edu.cn.config.GlobalConfig.ClientConf.USER_BUCKET_NAME
import whu.edu.cn.config.GlobalConfig.MinioConf.{MINIO_ACCESS_KEY, MINIO_BUCKET_NAME, MINIO_ENDPOINT, MINIO_HEAD_SIZE, MINIO_SECRET_KEY}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.tempFileList

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

abstract class ClientUtil extends Serializable {
  def getClient: Any

  def getObject(bucketName: String, path: String): InputStream

  def Download(objectName: String, downloadPath: String): Unit

  def Upload(objectName: String, filePath: String): Unit
}

class MinioClientUtil extends ClientUtil {
  override def getClient: Any = {
    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_ENDPOINT)
      .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      .build()
    minioClient.setTimeout(10 * 60 * 10000, 10 * 60 * 10000, 10 * 60 * 10000)
    minioClient
  }

  override def getObject(bucketName: String ,path: String): InputStream = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    minioClient.getObject(GetObjectArgs.builder.bucket(bucketName).`object`(path).build())
  }

  override def Download(objectName: String, downloadPath: String): Unit = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    val filePath = downloadPath
    val inputStream = minioClient.getObject(GetObjectArgs.builder.bucket(USER_BUCKET_NAME).`object`(objectName).build())
    val outputPath = Paths.get(filePath)
    tempFileList.append(filePath)
    Trigger.file_id += 1
    java.nio.file.Files.copy(inputStream, outputPath, REPLACE_EXISTING)
    inputStream.close()
  }

  override def Upload(objectName: String, filePath: String): Unit = {
    val minioClient: MinioClient = getClient.asInstanceOf[MinioClient]
    minioClient.uploadObject(UploadObjectArgs.builder.bucket(USER_BUCKET_NAME).`object`(objectName).filename(filePath).build())
  }
}

class BosClientUtil extends ClientUtil  {

  override def getClient: Any = {
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(BOS_ACCESS_KEY, BOS_SECRET_ACCESS))
    config.setEndpoint(BOS_ENDPOINT)
    val client = new BosClient(config)
    client
  }

  override def getObject(bucketName: String, path: String): InputStream = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val getObjectRequest : GetObjectRequest = new GetObjectRequest(bucketName, path)
    val bucketObject: BosObject = bosClient.getObject(getObjectRequest)
    bucketObject.getObjectContent()
  }

  override def Download(objectName: String,downloadPath: String): Unit = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val getObjectRequest = new GetObjectRequest(USER_BUCKET_NAME, objectName)
    val tempfile = new File(downloadPath)
    tempfile.createNewFile()
    val bosObject = bosClient.getObject(getObjectRequest,tempfile)

  }

  override def Upload(objectName: String, filePath: String): Unit = {
    val bosClient: BosClient = getClient.asInstanceOf[BosClient]
    val file: File = new File(filePath)
    bosClient.putObject(USER_BUCKET_NAME, objectName, file)
  }

}

object ClientUtil {
  def createClientUtil(serviceType: String): ClientUtil = {
    serviceType.toLowerCase match {
      case "minio" => new MinioClientUtil()
      case "bos" => new BosClientUtil()
      case _ => throw new IllegalArgumentException("Invalid service type")
    }
  }
}
