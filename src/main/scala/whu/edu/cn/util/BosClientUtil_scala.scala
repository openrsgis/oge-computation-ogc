package whu.edu.cn.util

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}

object BosClientUtil_scala {
  val ACCESS_KEY_ID = "ALTAKetCGvRVdSsIa1C9CR81Cm"
  val SECRET_ACCESS_ID = "45624b0ae0c94c66877f75c6219b25f7"
  val ENDPOINT_1 = "https://ogebos.bj.bcebos.com"
  val ENDPOINT_2 = "https://s3.bj.bcebos.com"
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
  def main(args: Array[String]):Unit={
    val client = getClient
    client.getObject("ogebos","DEM/ALOS_PALSAR-DEM12.5/n000-n009/n000e013_dem12.5.tif")
    println("Finish")
  }
}
