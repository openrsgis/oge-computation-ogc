package whu.edu.cn.oge.extension.convert

import org.apache.spark.SparkContext
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType

import scala.io.Source

abstract class FileToSimpleType[T](convertFunc: String => T) extends ParamConverter[String, T] {

  /**
   * 参数转换
   *
   * @param source
   * @param dataType 文件类型，区分SHP/GEOJSON/GEOPKG
   * @param sc
   * @param target   输出文件全路径，非必填
   * @return
   */
  override def convert(source: String, dataType: ThirdOperationDataType, sc: SparkContext, target: String = ""): T = {
    try {
      convertFunc(readFileContent(source))
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"文件读取失败: $source", e)
    }
  }

  private def readFileContent(filePath: String): String = {
    val source = Source.fromFile(filePath)
    try source.mkString finally source.close()
  }
}