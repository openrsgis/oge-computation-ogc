package whu.edu.cn.oge.extension.convert

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType
import whu.edu.cn.util.RDDTransformerUtil

object Tif2Coverage extends ParamConverter[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] {

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
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    RDDTransformerUtil.makeChangedRasterRDDFromTif(sc, source)

  }
}
