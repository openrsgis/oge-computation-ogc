package whu.edu.cn.entity

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CoverageRDDAccumulator extends AccumulatorV2[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])), mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]] {

  private var map: mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])), mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]] = {
    val newAcc = new CoverageRDDAccumulator()
    newAcc.map = this.map.clone()
    newAcc
  }

  override def reset(): Unit = {
    map = mutable.Map.empty
  }

  override def add(v: (String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))): Unit = {
    val (key, value) = v
    map += (key -> value)
  }

  override def merge(other: AccumulatorV2[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])), mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]]): Unit = {
    other.value.foreach { case (key, value) =>
      map += (key -> value)
    }
  }

  override def value: mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = map
}