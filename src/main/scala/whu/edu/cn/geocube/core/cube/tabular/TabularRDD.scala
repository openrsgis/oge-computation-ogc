package whu.edu.cn.geocube.core.cube.tabular

import geotrellis.layer.SpaceTimeKey
import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.entity.TabularGridLayerMetadata

import scala.beans.BeanProperty

class TabularRDD(val rddPrev: RDD[(SpaceTimeKey, Iterable[TabularRecord])], val meta: TabularGridLayerMetadata[SpaceTimeKey]) extends RDD[(SpaceTimeKey, Iterable[TabularRecord])](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, Iterable[TabularRecord])] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions
  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)

  @BeanProperty
  var tabularGridLayerMetadata = meta
}