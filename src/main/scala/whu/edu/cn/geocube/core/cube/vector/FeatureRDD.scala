package whu.edu.cn.geocube.core.cube.vector

import java.text.SimpleDateFormat
import java.time.ZonedDateTime

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.TileLayout
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{RasterTileLayerMetadata, VectorGridLayerMetadata}
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomGridInfo

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

  /**
   * Get the number of grids along spatial x dimension
   * @return
   */
  def gridX(): Long = {
    this.getVectorGridLayerMetadata.getGridConf.gridDimX
  }

  /**
   * Get the number of grids along spatial y dimension
   * @return
   */
  def gridY(): Long = {
    this.getVectorGridLayerMetadata.getGridConf.gridDimY
  }

  /**
   * Get the extent of a grid along spatial x dimension
   * @return
   */
  def gridSizeX(): Double = {
    this.getVectorGridLayerMetadata.getGridConf.gridSizeX
  }

  /**
   * Get the extent of a grid along spatial y dimension
   * @return
   */
  def gridSizeY(): Double = {
    this.getVectorGridLayerMetadata.getGridConf.gridSizeY
  }

  /**
   * Get the spatial extent
   * @return
   */
  def extent(): Extent = this.getVectorGridLayerMetadata.extent

  /**
   * Get the time range
   * @return
   */
  def timeRange(): (ZonedDateTime, ZonedDateTime) = {
    val bounds = this.getVectorGridLayerMetadata.bounds.get
    val start = bounds._1.time
    val end = bounds._2.time
    (start, end)
  }

  /**
   * Get available time node
   * @return
   */
  def timeNodes(): Array[ZonedDateTime] = {
    this.map(_._1.time).collect().toSet.toArray
  }

  /**
   * Get available instants
   * @return
   */
  def instants():Array[Long] = {
    this.map(_._1.instant).collect().toSet.toArray
  }

  /**
   * Get the product list
   * @return
   */
  def products(): Array[String] = {
    if (this.getVectorGridLayerMetadata.getProductNames.size == 0) Array(this.getVectorGridLayerMetadata.getProductName)
    else this.getVectorGridLayerMetadata.getProductNames.toArray
  }

  /**概念模型上一个RasterCube具有产品、时、空、波段维度，实现上一个RasterCube/RasterRDD中只有一个产品，不同产品对应不同RasterRDD
   * 这样做有利于不同产品即可独立的分析，又可基于时、空、波段维度融合进行联合分析
   */
  /*def product(productName: String): RasterRDD = {
    null
  }

  def products(productNames: Array[String]): RasterRDD = {
    null
  }*/

  /**
   * Trimming along spatial dimension
   * @param leftBottomLong
   * @param leftBottomLat
   * @param rightUpperLong
   * @param rightUpperLat
   * @return
   */
  def extent(leftBottomLong: Double, leftBottomLat: Double, rightUpperLong: Double, rightUpperLat: Double): FeatureRDD = {
    val metaData = this.getVectorGridLayerMetadata.copy()
    /*val metaData = new VectorGridLayerMetadata[SpaceTimeKey](this.getVectorGridLayerMetadata.getGridConf, this.getVectorGridLayerMetadata.getExtent,
      this.getVectorGridLayerMetadata.getBounds, this.getVectorGridLayerMetadata.getCrs,
      this.getVectorGridLayerMetadata.getProductName, this.getVectorGridLayerMetadata.getProductNames)*/
    val queryExtent = new Extent(leftBottomLong, leftBottomLat, rightUpperLong, rightUpperLat)
    if (!metaData.extent.intersects(queryExtent))
      throw new RuntimeException("The query extent does not intersects with the cube extent: " + metaData.extent.toString())
    val newExtent = metaData.extent.intersection(queryExtent).get
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(newExtent.toPolygon(), metaData.gridConf.gridDimX, metaData.gridConf.gridDimY, metaData.gridConf.extent, colRow, longLati, 4000)
    val minCol = colRow(0); val minRow = colRow(1); val maxCol = colRow(2); val maxRow = colRow(3)
    val minLong = longLati(0); val minLat = longLati(1) ; val maxLong = longLati(2); val maxLat = longLati(3)
    val (minInstant, maxInstant) = (metaData.bounds.get._1.instant, metaData.bounds.get._2.instant)
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))

    metaData.setExtent(newExtent)
    metaData.setBounds(bounds)

    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tl = TileLayout(360, 180, metaData.gridConf.gridDimX.toInt, metaData.gridConf.gridDimY.toInt)
    val ld = LayoutDefinition(extent, tl)
    val rdd = this.filter(x => x._1.spatialKey.extent(ld).intersects(queryExtent))
    new FeatureRDD(rdd, metaData)
  }

  /**
   * Trimming along temporal dimension
   * @param startTime
   * @param endTime
   * @return
   */
  def time(startTime: String, endTime: String): FeatureRDD = {
    val metaData = this.getVectorGridLayerMetadata.copy()

    val srcBounds = metaData.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val queryStartInstant = sdf.parse(startTime).getTime
    val queryEndInstant = sdf.parse(endTime).getTime
    if(queryStartInstant > srcEndInstant || queryEndInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)

    val maxStartInstant = if (srcStartInstant > queryStartInstant) srcStartInstant else queryStartInstant
    val minEndInstant = if (srcEndInstant < queryEndInstant) srcEndInstant else queryEndInstant
    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,maxStartInstant),SpaceTimeKey(maxCol,maxRow,minEndInstant))

    metaData.setBounds(bounds)

    val rdd = this.filter(x => x._1.instant >= maxStartInstant && x._1.instant <= minEndInstant)
    new FeatureRDD(rdd, metaData)
  }

  /**
   * Slicing along temporal dimension
   * @param time
   * @return
   */
  def time(time: String): FeatureRDD = {
    val metaData = this.getVectorGridLayerMetadata.copy()

    val srcBounds = metaData.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val queryInstant = sdf.parse(time).getTime

    if(queryInstant > srcEndInstant || queryInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)
    if(!this.instants().contains(queryInstant)) throw new RuntimeException("The query time is not included by the cube time instants")

    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol, minRow, queryInstant), SpaceTimeKey(maxCol, maxRow, queryInstant))

    metaData.setBounds(bounds)

    val rdd = this.filter(x => x._1.instant.equals(queryInstant))
    new FeatureRDD(rdd, metaData)
  }

  /**
   * Trimming along temporal dimension
   * @param startTime
   * @param endTime
   * @return
   */
  def time(startTime: ZonedDateTime, endTime: ZonedDateTime): FeatureRDD = {
    val metaData = this.getVectorGridLayerMetadata.copy()

    val srcBounds = metaData.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)

    val queryStartInstant = startTime.toInstant.toEpochMilli
    val queryEndInstant = endTime.toInstant.toEpochMilli
    if(queryStartInstant > srcEndInstant || queryEndInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)

    val maxStartInstant = if (srcStartInstant > queryStartInstant) srcStartInstant else queryStartInstant
    val minEndInstant = if (srcEndInstant < queryEndInstant) srcEndInstant else queryEndInstant
    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,maxStartInstant),SpaceTimeKey(maxCol,maxRow,minEndInstant))

    metaData.setBounds(bounds)

    val rdd = this.filter(x => x._1.instant >= maxStartInstant && x._1.instant <= minEndInstant)
    new FeatureRDD(rdd, metaData)
  }

  /**
   * Slicing along temporal dimension
   * @param time
   * @return
   */
  def time(time: ZonedDateTime): FeatureRDD = {
    val metaData = this.getVectorGridLayerMetadata.copy()

    val srcBounds = metaData.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)
    val queryInstant = time.toInstant.toEpochMilli

    if(queryInstant > srcEndInstant || queryInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)
    if(!this.instants().contains(queryInstant)) throw new RuntimeException("The query time is not included by  the cube time instants")

    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol, minRow, queryInstant), SpaceTimeKey(maxCol, maxRow, queryInstant))

    metaData.setBounds(bounds)

    val rdd = this.filter(x => x._1.instant.equals(queryInstant))
    new FeatureRDD(rdd, metaData)
  }

  def intersects(other: FeatureRDD, productDim: Boolean = false, temporalDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim ) this.extent.intersects(other.extent)
    else if (!productDim && temporalDim ) this.extent.intersects(other.extent) && this.timeNodes().intersect(other.timeNodes()).size != 0
    else if (productDim && !temporalDim) this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0
    else this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0 && this.timeNodes().intersect(other.timeNodes()).size != 0
  }

  /**
   * Predicate for whether this cube is disjoint from another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def disjoint(other: FeatureRDD, productDim: Boolean = false, temporalDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim ) this.extent.disjoint(other.extent)
    else if (!productDim && temporalDim ) this.extent.disjoint(other.extent) && this.timeNodes().intersect(other.timeNodes()).size == 0
    else if (productDim && !temporalDim ) this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0
    else this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0 && this.timeNodes().intersect(other.timeNodes()).size == 0
    }

  /**
   * Predicate for whether this cube contains another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def contains(other: FeatureRDD, productDim: Boolean = false, temporalDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim) this.extent.contains(other.extent)
    else if (!productDim && temporalDim) this.extent.contains(other.extent) && this.timeNodes().contains(other.timeNodes())
    else if (productDim && !temporalDim) this.extent.contains(other.extent) && this.products().contains(other.products())
    else this.extent.contains(other.extent) && this.products().contains(other.products()) && this.timeNodes().contains(other.timeNodes())
  }

  /**
   * Predicate for whether this cube is within another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def within(other: FeatureRDD, productDim: Boolean = false, temporalDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim) this.extent.within(other.extent)
    else if (!productDim && temporalDim) this.extent.within(other.extent) && other.timeNodes().contains(this.timeNodes())
    else if (productDim && !temporalDim) this.extent.within(other.extent) && other.products().contains(this.products())
    else this.extent.within(other.extent) && other.products().contains(this.products()) && other.timeNodes().contains(this.timeNodes())
  }

  /**
   * Predicate for whether this cube overlaps another along spatial dimension
   * @param other
   * @return
   */
  def overlaps(other: FeatureRDD): Boolean = this.extent.overlaps(other.extent)

  /**
   * Tests whether the distance from this rdd
   * to another is less than or equal to a specified value.
   *
   * @param other
   * @return
   */
  def isWithinDistance(other: FeatureRDD, distance: Double): Boolean = this.extent.isWithinDistance(other.extent, distance)

  /**
   * Predicate for whether this cube touches another along spatial dimension
   * @param other
   * @return
   */
  def touches(other: FeatureRDD): Boolean = this.extent.touches(other.extent)

  /**
   * Predicate for whether this cube covers another along spatial dimension
   * @param other
   * @return
   */
  def covers(other: FeatureRDD): Boolean = this.extent.coveredBy(other.extent)

  /**
   * Predicate for whether this cube is covered by another along spatial dimension
   * @param other
   * @return
   */
  def coveredBy(other: FeatureRDD): Boolean = this.extent.covers(other.extent)

  /**
   * Predicate for whether this cube crosses another along spatial dimension
   * @param other
   * @return
   */
  def crosses(other: FeatureRDD): Boolean = this.extent.crosses(other.extent)

  /**
   * Distance from another cube extent
   * @param other
   * @return
   */
  def distance(other: FeatureRDD): Double = this.extent.distance(other.extent)

}
