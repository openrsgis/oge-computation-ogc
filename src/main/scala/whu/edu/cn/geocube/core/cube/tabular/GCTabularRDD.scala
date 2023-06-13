package whu.edu.cn.geocube.core.cube.tabular

import geotrellis.layer.SpaceTimeKey
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import whu.edu.cn.geocube.core.entity.{SpaceTimeProductKey, TabularGridLayerMetadata}

import scala.beans.BeanProperty

class GCTabularRDD(val rddPrev: RDD[(SpaceTimeProductKey, Iterable[TabularRecord])], val meta: TabularGridLayerMetadata[SpaceTimeKey]) extends RDD[(SpaceTimeProductKey, Iterable[TabularRecord])](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeProductKey, Iterable[TabularRecord])] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions
  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)

  @BeanProperty
  var tabularGridLayerMetadata = meta
}