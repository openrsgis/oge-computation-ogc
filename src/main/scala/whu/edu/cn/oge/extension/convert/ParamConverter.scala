package whu.edu.cn.oge.extension.convert

import org.apache.spark.SparkContext
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType


trait ParamConverter[T, V] {

  /**
   * 参数转换
   *
   * @param source
   * @param dataType 文件类型，区分SHP/GEOJSON/GEOPKG
   * @param sc
   * @param target   输出文件全路径，非必填
   * @return
   */
  def convert(source: T, dataType: ThirdOperationDataType, sc: SparkContext, target: String = ""): V
}
