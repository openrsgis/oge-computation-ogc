package whu.edu.cn.util

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.{BosObject, GetObjectRequest}
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}
import io.minio.MinioClient
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_HEAD_SIZE

import java.io.{ByteArrayOutputStream, InputStream}

object BosClientUtil_scala1 {
  val ACCESS_KEY_ID = "ALTAKetCGvRVdSsIa1C9CR81Cm"
  val SECRET_ACCESS_ID = "45624b0ae0c94c66877f75c6219b25f7"
  val ENDPOINT_1 = "https://ogebos.bj.bcebos.com" // 使用这个endpoint貌似也可以bj.bcebos.com
  val ENDPOINT_2 = "https://s3.bj.bcebos.com"
  val ENDPOINT_3 = "https://bj.bcebos.com"
  def getClient ={
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID))
    config.setEndpoint(ENDPOINT_1)
    val client_1 = new BosClient(config)
    client_1
  }

  def getClient2 = {
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID))
    config.setEndpoint(ENDPOINT_2)
    val client_1 = new BosClient(config)
    client_1
  }

  def getClient3 = {
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID))
    config.setEndpoint(ENDPOINT_3)
    val client_1 = new BosClient(config)
    client_1
  }

  def getBosObject(bucketName: String, objectName: String, offset: Long, length: Long): Array[Byte] = {
    throw new Exception("所请求的数据在Bos中不存在！")
//    val client: BosClient = BosClientUtil_scala.getClient3
//    val getObjectRequest : GetObjectRequest = new GetObjectRequest(bucketName, objectName)
//    getObjectRequest.setRange(offset, offset + length)
//    var headerBytes: Array[Byte] = Array()
//    try{
//      val bucketObject: BosObject = client.getObject(getObjectRequest)
//      val inputStream: InputStream = bucketObject.getObjectContent()
//      //    val inputStream: InputStream = minioClient.getObject(GetObjectArgs.builder.bucket("oge").`object`(coverageMetadata.getPath).offset(0L).length(MINIO_HEAD_SIZE).build)
//      // Read data from stream
//      val outStream = new ByteArrayOutputStream
//      val buffer = new Array[Byte](MINIO_HEAD_SIZE)
//      var len: Int = 0
//      while ( {
//        len = inputStream.read(buffer)
//        len != -1
//      }) {
//        outStream.write(buffer, 0, len)
//      }
//      headerBytes = outStream.toByteArray
//      outStream.close()
//      inputStream.close()
//      headerBytes
//    }catch {
//      case e:Exception =>
//        throw new Exception("所请求的数据在Bos中不存在！")
//    }
  }

  def main(args: Array[String]):Unit={
    val client = getClient
    client.getObject("ogebos","DEM/ALOS_PALSAR-DEM12.5/n000-n009/n000e013_dem12.5.tif")
    println("Finish")
  }
}
