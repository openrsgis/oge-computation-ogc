package whu.edu.cn.application.oge

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Table {
  def getDownloadUrl(url: String, fileName: String): Unit ={
    val writeFile = new File(fileName)
    val writerOutput = new BufferedWriter(new FileWriter(writeFile))
    val outputString = "{\"table\":[{\"url\":" + "\"" + url + "\"}], \"vector\":[], \"raster\":[]}"
    writerOutput.write(outputString)
    writerOutput.close()
  }
}
