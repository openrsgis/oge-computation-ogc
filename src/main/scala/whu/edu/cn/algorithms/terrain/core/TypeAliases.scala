package whu.edu.cn.algorithms.terrain.core

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable

object TypeAliases {

  type RDDImage = (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
  type RDDFeature = RDD[(String, (Geometry, mutable.Map[String, Any]))]
}
