package whu.edu.cn.geocube.core.cube


import geotrellis.layer.SpaceTimeKey
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.entity.{RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey, VectorGridLayerMetadata}

/**
 * Abstract cube model.
 *
 * @param rddPrev
 * @tparam T
 */
class CubeRDD[T](val rddPrev: RDD[(SpaceTimeKey, T)]) extends RDD[(SpaceTimeKey, T)](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, T)] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)

  /**
   * Transformed to FeatureRDD.
   *
   * @param meta
   * @return
   */
  def toFeatureRDD(meta: VectorGridLayerMetadata[SpaceTimeKey]): FeatureRDD = {
    val gridGeoObjectsRdd = this.map(x=>(x._1, x._2.asInstanceOf[Iterable[GeoObject]]))
    new FeatureRDD(gridGeoObjectsRdd, meta)
  }

  /**
   * Transformed to RasterRDD.
   *
   * @param meta
   * @return
   */
  def toRasterRdd(meta: RasterTileLayerMetadata[SpaceTimeKey]):RasterRDD = {
    val transformTileRdd = this.map(x=>(x._1, x._2.asInstanceOf[RasterTile]))
    val rasterTileRdd = transformTileRdd.map(x => (SpaceTimeBandKey(x._1, x._2.measurementMeta.getMeasurementName), x._2.data))
    new RasterRDD(rasterTileRdd, meta)
  }


}
