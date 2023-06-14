package whu.edu.cn.geocube.core.cube

import java.text.SimpleDateFormat
import java.util.Date
import geotrellis.layer.SpaceTimeKey
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext, TaskContext}
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.tabular.{TabularCollection, TabularRDD}
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, QueryParams, RasterTile, RasterTileLayerMetadata, VectorGridLayerMetadata}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles
import whu.edu.cn.geocube.core.tabular.query.QueryTabularCollection.getTabularCollection
import whu.edu.cn.geocube.core.tabular.query.DistributedQueryTabularRecords
import whu.edu.cn.geocube.core.vector.query.DistributedQueryVectorObjects

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

object CubeRDD{
  implicit def getRasterRdd(sc: SparkContext, p: QueryParams): RasterRDD = DistributedQueryRasterTiles.getRasterTiles(sc, p)
  implicit def getFeatureRdd(sc: SparkContext, p: QueryParams): FeatureRDD = DistributedQueryVectorObjects.getFeatures(sc, p)
  implicit def getTabularRdd(sc: SparkContext, p: QueryParams): TabularRDD = DistributedQueryTabularRecords.getTabulars(sc, p)

  implicit def getTabularArray(sc: SparkContext, p: QueryParams): TabularCollection = getTabularCollection(sc, p)

//  implicit def getGcRasterRdd(sc: SparkContext, p: QueryParams): GCRasterRDD = GcDistributedQueryRasterTiles.getRasterTiles(sc, p)
//  implicit def getGcFeatureRdd(sc: SparkContext, p: QueryParams): GCFeatureRDD = GcDistributedQueryVectorObjects.getFeatures(sc, p)
//  implicit def getGcTabularRdd(sc: SparkContext, p: QueryParams): GCTabularRDD = GcDistributedQueryTabularRecords.getTabulars(sc, p)

  def getData[T](sc: SparkContext, p: QueryParams)(implicit op: (SparkContext, QueryParams) =>T): T = {
    op(sc, p)
  }

  def envInitializer(application: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(application)
      .setMaster("local[8]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rpc.message.maxSize", "1024")
    new SparkContext(conf)
  }

  def printD(args: Any) : Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + args)
  }

  /*implicit def str2int(str: String) = new StringOps(str).toInt
  implicit def str2Long(str: String) = new StringOps(str).toLong
  implicit def str2Double(str: String) = new StringOps(str).toDouble

  def f[T](s: String)(implicit op: String=>T): Map[T, Double] = {
    val dbl = 1.0
    Map((op(s), dbl))
  }

  f[Int]("10")
  f[Long]("10")
  f[Double]("10")*/
}
