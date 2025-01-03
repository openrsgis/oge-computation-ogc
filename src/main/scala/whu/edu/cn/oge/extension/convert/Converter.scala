package whu.edu.cn.oge.extension.convert

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType

import scala.collection.mutable

object Converter {

  implicit val tif2CoverageConverter: ParamConverter[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Tif2Coverage
  implicit val coverage2TifConverter: ParamConverter[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), String] = Coverage2Tif
  implicit val file2FeatureConverter: ParamConverter[String, RDD[(String, (Geometry, mutable.Map[String, Any]))]] = File2Feature
  implicit val feature2FileConverter: ParamConverter[RDD[(String, (Geometry, mutable.Map[String, Any]))], String] = Feature2File

  /**
   *
   * @param source
   * @param thirdOperationDataType
   * @param sc
   * @param target 输出文件全路径，非必填
   * @param paramConverter
   * @tparam T
   * @tparam V
   * @return
   */
  def convert[T, V](source: T, thirdOperationDataType: ThirdOperationDataType, sc: SparkContext, target: String = "")
                   (implicit paramConverter: ParamConverter[T, V]): V = {
    paramConverter.convert(source, thirdOperationDataType, sc, target)
  }

}
