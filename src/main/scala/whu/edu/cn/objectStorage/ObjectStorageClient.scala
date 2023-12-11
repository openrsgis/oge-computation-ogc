package whu.edu.cn.objectStorage

import java.io.InputStream
import java.io.File

trait ObjectStorageClient {
  def getInputStream(bucketName: String, path: String, range: Int): InputStream

  // path为对象存储路径，filepath为文件本地存储路径
  def DownloadObject(path: String, filepath: String, bucketName: String): Unit

  // path为对象存储路径，filepath为文件本地存储路径
  def UploadObject(path: String, filepath: String, bucketName: String): Unit
}


