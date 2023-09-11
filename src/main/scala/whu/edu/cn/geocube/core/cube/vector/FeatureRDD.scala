package whu.edu.cn.geocube.core.cube.vector

import geotrellis.layer.SpaceTimeKey
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import whu.edu.cn.geocube.core.entity.VectorGridLayerMetadata

import scala.beans.BeanProperty

/**
 * A wrapper for RDD[(SpaceTimeKey, Iterable[GeoObject])].
 *
 * @param rddPrev
 */
class FeatureRDD(val rddPrev: RDD[(SpaceTimeKey, Iterable[GeoObject])], val meta: VectorGridLayerMetadata[SpaceTimeKey]) extends RDD[(SpaceTimeKey, Iterable[GeoObject])](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, Iterable[GeoObject])] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions
  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)

  @BeanProperty
  var vectorGridLayerMetadata = meta
}
