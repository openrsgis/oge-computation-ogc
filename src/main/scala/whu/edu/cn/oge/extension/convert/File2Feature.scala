package whu.edu.cn.oge.extension.convert

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType
import whu.edu.cn.util.RDDTransformerUtil

import scala.collection.mutable

object File2Feature extends ParamConverter[String, RDD[(String, (Geometry, mutable.Map[String, Any]))]] {

  /**
   * 参数转换
   *
   * @param source
   * @param dataType 文件类型，区分SHP/GEOJSON/GEOPKG
   * @param sc
   * @param target   输出文件全路径，非必填
   * @return
   */
  override def
  convert(source: String, dataType: ThirdOperationDataType, sc: SparkContext, target: String = "")
  : RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    RDDTransformerUtil.makeFeatureRDDFromShp(sc, source)

  }
}
