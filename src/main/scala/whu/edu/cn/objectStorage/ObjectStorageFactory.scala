package whu.edu.cn.objectStorage

import whu.edu.cn.objectStorage.BosClient_oge

object ObjectStorageFactory {
  def getClient(platform: String, endPoint: String): ObjectStorageClient = {
    if (platform.equals("bmr")) {
      return new BosClient_oge(endPoint)
    } else if (platform.equals("cc")) {
      new MinIOClient_oge("")
    } else {
      null
    }
  }
}
