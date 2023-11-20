package whu.edu.cn.util

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}

object BosClientUtil_scala {
  val ACCESS_KEY_ID = "ALTAKetCGvRVdSsIa1C9CR81Cm"
  val SECRET_ACCESS_ID = "45624b0ae0c94c66877f75c6219b25f7"
  val ENDPOINT = "https://s3.bj.bcebos.com"
  def getClient ={
    val config = new BosClientConfiguration
    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID))
    config.setEndpoint(ENDPOINT)
    val client_1 = new BosClient(config)
    client_1
  }
}
