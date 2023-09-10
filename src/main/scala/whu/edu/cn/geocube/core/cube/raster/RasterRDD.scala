package whu.edu.cn.geocube.core.cube.raster

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.Tile
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import scala.beans.BeanProperty
import whu.edu.cn.geocube.core.entity.{RasterTileLayerMetadata, SpaceTimeBandKey}

/**
 * A wrapper for RDD[(SpaceTimeBandKey, Tile)] and RasterTileLayerMetadata[SpaceTimeKey].
 *
 * @param rddPrev RDD[(SpaceTimeBandKey, Tile)]
 * @param meta RasterTileLayerMetadata[SpaceTimeKey]
 */
class RasterRDD(val rddPrev: RDD[(SpaceTimeBandKey, Tile)], val meta:RasterTileLayerMetadata[SpaceTimeKey]) extends RDD[(SpaceTimeBandKey, Tile)](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeBandKey, Tile)] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)

  @BeanProperty
  var rasterTileLayerMetadata = meta
}

