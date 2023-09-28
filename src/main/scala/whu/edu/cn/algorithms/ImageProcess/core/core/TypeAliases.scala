package whu.edu.cn.algorithms.ImageProcess.core

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey
object TypeAliases {

  type RDDImage = (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])

}
