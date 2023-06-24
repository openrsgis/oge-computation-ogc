package whu.edu.cn.geocube.core.cube.raster
import java.io.File
import java.text.SimpleDateFormat
import java.time.{ZoneId, ZoneOffset, ZonedDateTime}
import java.util.{Date, UUID}
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.ProcessLogger
import sys.process._
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.layer.{Bounds, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.{Add, Divide, Multiply, Subtract}
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.{ByteArrayTile, DoubleArrayTile, DoubleConstantNoDataCellType, MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext, TaskContext}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.opengis.feature.simple.SimpleFeature
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, QueryParams, RasterTileLayerMetadata, SpaceTimeBandProductKey}
import whu.edu.cn.geocube.core.cube.tabular.{TabularRDD, TabularRecord}
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomGridInfo
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.view.{Info, RasterView}
import whu.edu.cn.geocube.core.cube.CubeRDD.{getData, _}

/**
 * A wrapper for RDD[(SpaceTimeBandKey, Tile)] and RasterTileLayerMetadata[SpaceTimeKey].
 *
 * 概念模型上一个RasterCube具有产品、时、空、波段维度，实现上一个RasterCube/RasterRDD中只有一个产品，不同产品对应不同RasterRDD。
 * 这样做有利于不同产品即可独立的分析，又可基于时、空、波段维度融合进行联合分析
 *
 * 调用接口前建议执行 RasterRDD.cache()操作，避免重复触发获取操作
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

  /**
   * Get the number of tiles along spatial x dimension
   * @return
   */
  def tileX(): Long = {
    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds.get
    bounds._2.spatialKey.col - bounds._1.spatialKey.col
  }

  /**
   * Get the number of tiles along spatial y dimension
   * @return
   */
  def tileY(): Long = {
    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds.get
    bounds._2.spatialKey.row - bounds._1.spatialKey.row
  }

  /**
   * Get the number of pixels along spatial x dimension
   * @return
   */
  def pixelX(): Long = {
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    ld.tileCols * tileX()
  }

  /**
   * Get the number of pixels along spatial y dimension
   * @return
   */
  def pixelY(): Long = {
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    ld.tileRows * tileY()
  }

  /**
   * Get the band list
   * @return
   */
  def bands(): Array[String] = this.getRasterTileLayerMetadata.getMeasurementNames.toArray

  /**
   * Get the spatial extent
   * @return
   */
  def extent(): Extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent

  /**
   * Get the time range
   * @return
   */
  def timeRange(): (ZonedDateTime, ZonedDateTime) = {
    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds.get
    val start = bounds._1.time
    val end = bounds._2.time
    (start, end)
  }

  /**
   * Get available time node
   * @return
   */
  def timeNodes(): Array[ZonedDateTime] = {
    this.map(_._1.spaceTimeKey.time).collect().toSet.toArray
  }

  /**
   * Get available instants
   * @return
   */
  def instants():Array[Long] = {
    this.map(_._1.spaceTimeKey.instant).collect().toSet.toArray
  }

  /**
   * Get the product list
   * @return
   */
  def products(): Array[String] = {
    if (this.getRasterTileLayerMetadata.getProductNames.size == 0) Array(this.getRasterTileLayerMetadata.getProductName)
    else this.getRasterTileLayerMetadata.getProductNames.toArray
  }

  /**
   * Predicate for whether this cube intersects another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def intersects(other: RasterRDD, productDim: Boolean = false, temporalDim: Boolean = false, bandDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim && !bandDim) this.extent.intersects(other.extent)
    else if (!productDim && temporalDim && !bandDim) this.extent.intersects(other.extent) && this.timeNodes().intersect(other.timeNodes()).size != 0
    else if (!productDim && !temporalDim && bandDim) this.extent.intersects(other.extent) && this.bands().intersect(other.bands()).size != 0
    else if (!productDim && temporalDim && bandDim) this.extent.intersects(other.extent) && this.timeNodes().intersect(other.timeNodes()).size != 0 && this.bands().intersect(other.bands()).size != 0
    else if (productDim && !temporalDim && !bandDim) this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0
    else if (productDim && temporalDim && !bandDim) this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0 && this.timeNodes().intersect(other.timeNodes()).size != 0
    else if (productDim && !temporalDim && bandDim) this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0 && this.bands().intersect(other.bands()).size != 0
    else this.extent.intersects(other.extent) && this.products().intersect(other.products()).size != 0 && this.timeNodes().intersect(other.timeNodes()).size != 0 && this.bands().intersect(other.bands()).size != 0
  }

  /**
   * Predicate for whether this cube is disjoint from another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def disjoint(other: RasterRDD, productDim: Boolean = false, temporalDim: Boolean = false, bandDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim && !bandDim) this.extent.disjoint(other.extent)
    else if (!productDim && temporalDim && !bandDim) this.extent.disjoint(other.extent) && this.timeNodes().intersect(other.timeNodes()).size == 0
    else if (!productDim && !temporalDim && bandDim) this.extent.disjoint(other.extent) && this.bands().intersect(other.bands()).size == 0
    else if (!productDim && temporalDim && bandDim) this.extent.disjoint(other.extent) && this.timeNodes().intersect(other.timeNodes()).size == 0 && this.bands().intersect(other.bands()).size == 0
    else if (productDim && !temporalDim && !bandDim) this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0
    else if (productDim && temporalDim && !bandDim) this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0 && this.timeNodes().intersect(other.timeNodes()).size == 0
    else if (productDim && !temporalDim && bandDim) this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0 && this.bands().intersect(other.bands()).size == 0
    else this.extent.disjoint(other.extent) && this.products().intersect(other.products()).size == 0 && this.timeNodes().intersect(other.timeNodes()).size == 0 && this.bands().intersect(other.bands()).size == 0
  }

  /**
   * Predicate for whether this cube contains another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def contains(other: RasterRDD, productDim: Boolean = false, temporalDim: Boolean = false, bandDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim && !bandDim) this.extent.contains(other.extent)
    else if (!productDim && temporalDim && !bandDim) this.extent.contains(other.extent) && this.timeNodes().contains(other.timeNodes())
    else if (!productDim && !temporalDim && bandDim) this.extent.contains(other.extent) && this.bands().contains(other.bands())
    else if (!productDim && temporalDim && bandDim) this.extent.contains(other.extent) && this.timeNodes().contains(other.timeNodes()) && this.bands().contains(other.bands())
    else if (productDim && !temporalDim && !bandDim) this.extent.contains(other.extent) && this.products().contains(other.products())
    else if (productDim && temporalDim && !bandDim) this.extent.contains(other.extent) && this.products().contains(other.products()) && this.timeNodes().contains(other.timeNodes())
    else if (productDim && !temporalDim && bandDim) this.extent.contains(other.extent) && this.products().contains(other.products()) && this.bands().contains(other.bands())
    else this.extent.contains(other.extent) && this.products().contains(other.products()) && this.timeNodes().contains(other.timeNodes()) && this.bands().contains(other.bands())
  }

  /**
   * Predicate for whether this cube is within another along spatial, product, temporal and band dimension
   * @param other
   * @return
   */
  def within(other: RasterRDD, productDim: Boolean = false, temporalDim: Boolean = false, bandDim: Boolean = false): Boolean = {
    if (!productDim && !temporalDim && !bandDim) this.extent.within(other.extent)
    else if (!productDim && temporalDim && !bandDim) this.extent.within(other.extent) && other.timeNodes().contains(this.timeNodes())
    else if (!productDim && !temporalDim && bandDim) this.extent.within(other.extent) && other.bands().contains(this.bands())
    else if (!productDim && temporalDim && bandDim) this.extent.within(other.extent) && other.timeNodes().contains(this.timeNodes()) && other.bands().contains(this.bands())
    else if (productDim && !temporalDim && !bandDim) this.extent.within(other.extent) && other.products().contains(this.products())
    else if (productDim && temporalDim && !bandDim) this.extent.within(other.extent) && other.products().contains(this.products()) && other.timeNodes().contains(this.timeNodes())
    else if (productDim && !temporalDim && bandDim) this.extent.within(other.extent) && other.products().contains(this.products()) && other.bands().contains(this.bands())
    else this.extent.within(other.extent) && other.products().contains(this.products()) && other.timeNodes().contains(this.timeNodes()) && other.bands().contains(this.bands())
  }

  /**
   * Predicate for whether this cube overlaps another along spatial dimension
   * @param other
   * @return
   */
  def overlaps(other: RasterRDD): Boolean = this.extent.overlaps(other.extent)

  /**
   * Tests whether the distance from this rdd
   * to another is less than or equal to a specified value.
   *
   * @param other
   * @return
   */
  def isWithinDistance(other: RasterRDD, distance: Double): Boolean = this.extent.isWithinDistance(other.extent, distance)

  /**
   * Predicate for whether this cube touches another along spatial dimension
   * @param other
   * @return
   */
  def touches(other: RasterRDD): Boolean = this.extent.touches(other.extent)

  /**
   * Predicate for whether this cube covers another along spatial dimension
   * @param other
   * @return
   */
  def covers(other: RasterRDD): Boolean = this.extent.coveredBy(other.extent)

  /**
   * Predicate for whether this cube is covered by another along spatial dimension
   * @param other
   * @return
   */
  def coveredBy(other: RasterRDD): Boolean = this.extent.covers(other.extent)

  /**
   * Predicate for whether this cube crosses another along spatial dimension
   * @param other
   * @return
   */
  def crosses(other: RasterRDD): Boolean = this.extent.crosses(other.extent)

  /**
   * Distance from another cube extent
   * @param other
   * @return
   */
  def distance(other: RasterRDD): Double = this.extent.distance(other.extent)

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
   * Slicing along band dimension
   * @param measurementName
   * @return
   */
  def band(measurementName: String): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    if(!this.bands().contains(measurementName)) throw new RuntimeException("There is no " + measurementName + "in the cube")
    metaData.setMeasurementNames(Array(measurementName))
    val rdd = this.filter(_._1.getMeasurementName.equals(measurementName))
    new RasterRDD(rdd, metaData)
  }

  /**
   * Trimming along band dimension
   * @param measurementArray
   * @return
   */
  def bands(measurementArray: Array[String]): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    if(this.bands().intersect(measurementArray).size == 0) throw new RuntimeException("There is no " + measurementArray.toList + "in the cube")
    metaData.setMeasurementNames(this.bands().intersect(measurementArray))
    val rdd = this.filter(x => measurementArray.contains(x._1.measurementName))
    new RasterRDD(rdd, metaData)
  }

  /**
   * Slicing along temporal dimension
   * @param time
   * @return
   */
  def time(time: String): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    val tileLayerMetadata = metaData.getTileLayerMetadata
    val srcBounds = tileLayerMetadata.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val queryInstant = sdf.parse(time).getTime

    if(queryInstant > srcEndInstant || queryInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)
    if(!this.instants().contains(queryInstant)) throw new RuntimeException("The query time is not included by the cube time instants")

    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol, minRow, queryInstant), SpaceTimeKey(maxCol, maxRow, queryInstant))

    metaData.setTileLayerMetadata(TileLayerMetadata[SpaceTimeKey](tileLayerMetadata.cellType, tileLayerMetadata.layout, tileLayerMetadata.extent, tileLayerMetadata.crs, bounds))

    val rdd = this.filter(x => x._1.spaceTimeKey.instant.equals(queryInstant))
    new RasterRDD(rdd, metaData)
  }

  /**
   * Slicing along temporal dimension
   * @param time
   * @return
   */
  def time(time: ZonedDateTime): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    val tileLayerMetadata = metaData.getTileLayerMetadata
    val srcBounds = tileLayerMetadata.bounds.get
    val (srcStartTime, srcEndTime) = (srcBounds._1.time, srcBounds._2.time)
    val (srcStartInstant, srcEndInstant) = (srcBounds._1.instant, srcBounds._2.instant)
    val queryInstant = time.toInstant.toEpochMilli//new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(time.toString).getTime

    if(queryInstant > srcEndInstant || queryInstant < srcStartInstant) throw new RuntimeException("The query time does not intersects with the cube time range: " + srcStartTime + ", " + srcEndTime)
    if(!this.instants().contains(queryInstant)) throw new RuntimeException("The query time is not included by the cube time instants")

    val (minCol, minRow) = (srcBounds._1.spatialKey.col, srcBounds._1.spatialKey.row)
    val (maxCol, maxRow) = (srcBounds._2.spatialKey.col, srcBounds._2.spatialKey.row)
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,queryInstant),SpaceTimeKey(maxCol,maxRow,queryInstant))

    metaData.setTileLayerMetadata(TileLayerMetadata[SpaceTimeKey](tileLayerMetadata.cellType, tileLayerMetadata.layout, tileLayerMetadata.extent, tileLayerMetadata.crs, bounds))

    val rdd = this.filter(x => x._1.spaceTimeKey.instant.equals(queryInstant))
    new RasterRDD(rdd, metaData)
  }

  /**
   * Trimming along temporal dimension
   * @param startTime
   * @param endTime
   * @return
   */
  def time(startTime: String, endTime: String): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    val tileLayerMetadata = metaData.getTileLayerMetadata
    val srcBounds = tileLayerMetadata.bounds.get
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

    metaData.setTileLayerMetadata(TileLayerMetadata[SpaceTimeKey](tileLayerMetadata.cellType, tileLayerMetadata.layout, tileLayerMetadata.extent, tileLayerMetadata.crs, bounds))

    val rdd = this.filter(x => x._1.spaceTimeKey.instant >= maxStartInstant && x._1.spaceTimeKey.instant <= minEndInstant)
    new RasterRDD(rdd, metaData)
  }

  /**
   * Trimming along temporal dimension
   * @param startTime
   * @param endTime
   * @return
   */
  def time(startTime: ZonedDateTime, endTime: ZonedDateTime): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    val tileLayerMetadata = metaData.getTileLayerMetadata
    val srcBounds = tileLayerMetadata.bounds.get
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

    metaData.setTileLayerMetadata(TileLayerMetadata[SpaceTimeKey](tileLayerMetadata.cellType, tileLayerMetadata.layout, tileLayerMetadata.extent, tileLayerMetadata.crs, bounds))

    val rdd = this.filter(x => x._1.spaceTimeKey.instant >= maxStartInstant && x._1.spaceTimeKey.instant <= minEndInstant)
    new RasterRDD(rdd, metaData)
  }

  /**
   * Trimming along spatial dimension
   * @param leftBottomLong
   * @param leftBottomLat
   * @param rightUpperLong
   * @param rightUpperLat
   * @return
   */
  def extent(leftBottomLong: Double, leftBottomLat: Double, rightUpperLong: Double, rightUpperLat: Double): RasterRDD = {
    val metaData = new RasterTileLayerMetadata[SpaceTimeKey](this.getRasterTileLayerMetadata.getTileLayerMetadata,
      _productName = this.getRasterTileLayerMetadata.getProductName,_productNames = this.getRasterTileLayerMetadata.getProductNames, _measurementNames = this.getRasterTileLayerMetadata.getMeasurementNames)
    val tileLayerMetadata = metaData.getTileLayerMetadata
    val queryExtent = new Extent(leftBottomLong, leftBottomLat, rightUpperLong, rightUpperLat)
    if (!metaData.getTileLayerMetadata.extent.intersects(queryExtent))
      throw new RuntimeException("The query extent does not intersects with the cube extent: " + metaData.getTileLayerMetadata.extent.toString())
    val newExtent = metaData.getTileLayerMetadata.extent.intersection(queryExtent).get
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    val ld = tileLayerMetadata.layout
    getGeomGridInfo(newExtent.toPolygon(), ld.layoutCols, ld.layoutRows, ld.extent, colRow, longLati,4000)
    val (minCol, minRow, maxCol, maxRow) = (colRow(0), colRow(1), colRow(2), colRow(3))
    val (minInstant, maxInstant) = (tileLayerMetadata.bounds.get._1.instant, tileLayerMetadata.bounds.get._2.instant)
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))

    metaData.setTileLayerMetadata(TileLayerMetadata[SpaceTimeKey](tileLayerMetadata.cellType, tileLayerMetadata.layout, newExtent, tileLayerMetadata.crs, bounds))

    val rdd = this.filter(x => x._1.spaceTimeKey.spatialKey.extent(ld).intersects(queryExtent))
    new RasterRDD(rdd, metaData)
  }

  def subset(productName: Array[String], bandValue: Array[String], spatialExtent: String, timeRange: String): RasterRDD = {
    null
  }

  /**
   * Add pixel value of two RasterRDD
   * 1.两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd的时间为该相同时刻，若不相同则取中值
   *    (1)两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd的波段为该相同波段，若不相同则波段随机取名
   *    (2)两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
   * 2.两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
   *    (1)两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd的波段为该相同波段，若不相同则波段随机取名
   *    (2)两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
   * @param other
   * @return
   */
  def add(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Add(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Add(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Add(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Add(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Add pixel value of two RasterRDD
   * @param other
   * @return
   */
  def +(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Add(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Add(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Add(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Add(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Subtract pixel value of two RasterRDD
   * @param other
   * @return
   */
  def subtract(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Subtract(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Subtract(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Subtract(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Subtract(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Subtract pixel value of two RasterRDD
   * @param other
   * @return
   */
  def -(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Subtract(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Subtract(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Subtract(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Subtract(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }

  }

  /**
   * Divide pixel value of two RasterRDD
   * @param other
   * @return
   */
  def divide(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Divide(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Divide(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Divide(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Divide(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Divide pixel value of two RasterRDD
   * @param other
   * @return
   */
  def /(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Divide(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Divide(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Divide(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Divide(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Multiply pixel value of two RasterRDD
   * @param other
   * @return
   */
  def multiply(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Multiply(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Multiply(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Multiply(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Multiply(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Multiply pixel value of two RasterRDD
   * @param other
   * @return
   */
  def *(other: RasterRDD): RasterRDD = {
    assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
    val thisBands = this.bands()
    val otherBands = other.bands()
    val thisInstants = this.instants()
    val otherInstants = other.instants()
    if(thisInstants.size == 1 && otherInstants.size == 1) { //两个rdd都只有一个时刻，或相同或不相同，都进行运算。若相同则返回的rdd为该相同时刻，若不相同则取中值
      val newInstant: Long = (thisInstants(0) + otherInstants(0)) / 2
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val otherRdd = other.map(x => (SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Multiply(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)
      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val thisRdd = this.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val otherRdd = other.map(x => (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, newInstant), x._1.measurementName), x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Multiply(tile1, tile2))
        }

        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row)).collect()
        val colSet = spatialKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, newInstant), SpaceTimeKey(maxCol, maxRow, newInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    } else { //两个rdd至少有一个有多个时刻，对相同的进行运算。返回的rdd中只有相同的时刻，丢弃不同的时刻
      if(thisBands.size == 1 && otherBands.size == 1) { //两个rdd都只有一个波段，或相同或不相同，都进行运算。若相同则返回的rdd为该相同波段，若不相同则波段随机取名
        val newBand = if(!thisBands(0).equals(otherBands(0))) "band" + UUID.randomUUID().toString else thisBands(0)
        val thisRdd = this.map(x => (x._1.spaceTimeKey, x._2))
        val otherRdd = other.map(x => (x._1.spaceTimeKey, x._2))
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = thisRdd.join(otherRdd).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (SpaceTimeBandKey(key, newBand), Multiply(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        newMeta.setMeasurementNames(Array(newBand))
        new RasterRDD(rdd, newMeta)*/

        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = Array(newBand).toBuffer.asInstanceOf[ArrayBuffer[String]])
        new RasterRDD(rdd, newMeta)

      }else { //两个rdd至少有一个有多个波段，对相同波段的进行运算。返回的rdd中只有相同的波段，丢弃不同的波段
        val rdd: RDD[(SpaceTimeBandKey, Tile)] = this.join(other).map{x =>
          val key = x._1
          val tile1 = DoubleArrayTile(x._2._1.toArrayDouble(), x._2._1.cols, x._2._1.rows)
            .convert(DoubleConstantNoDataCellType)
          val tile2 = DoubleArrayTile(x._2._2.toArrayDouble(), x._2._2.cols, x._2._2.rows)
            .convert(DoubleConstantNoDataCellType)
          (key, Multiply(tile1, tile2))
        }
        /*val thisMeta = this.meta
        val otherMeta = other.meta
        val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](thisMeta.getTileLayerMetadata.merge(otherMeta.getTileLayerMetadata))
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        newMeta.setMeasurementNames(bands)
        new RasterRDD(rdd, newMeta)*/

        val thisMeta = this.meta
        val otherMeta = other.meta
        val spatialTemporalKeyArray = rdd.map(x => (x._1.spaceTimeKey.spatialKey.col, x._1.spaceTimeKey.spatialKey.row, x._1.spaceTimeKey.instant)).collect()
        val colSet = spatialTemporalKeyArray.map(_._1).toSet; val minCol = colSet.min; val maxCol = colSet.max
        val rowSet = spatialTemporalKeyArray.map(_._2).toSet; val minRow = rowSet.min; val maxRow = rowSet.max
        val instantSet = spatialTemporalKeyArray.map(_._3).toSet; val minInstant = instantSet.min; val maxInstant = instantSet.max
        val intersectExtent = this.extent().intersection(other.extent()).get
        val bounds = Bounds(SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
        val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
        val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
        val productName = this.getRasterTileLayerMetadata.getProductName
        val bands = thisMeta.getMeasurementNames.intersect(otherMeta.getMeasurementNames)
        val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, intersectExtent, crs, bounds), productName, _measurementNames = bands)
        new RasterRDD(rdd, newMeta)
      }
    }
  }

  /**
   * Add pixel value inside a RasterRDD
   * This function can be replaced by function binaryOperator(leftBand, rightBand, (a, b) => a + b))
   * 内部波段运算，时、空、产品维信息不变
   * @param leftBand
   * @param rightBand
   * @return
   */
  def add(leftBand: String, rightBand: String): RasterRDD = {
    if(!(this.bands().contains(leftBand) && this.bands().contains(rightBand)))
      throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!leftBand.equals(rightBand)) "band" + UUID.randomUUID().toString else leftBand

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (leftBandTile, rightBandTile) = (bandTileMap.get(leftBand).get, bandTileMap.get(rightBand).get)
        val tile1 = DoubleArrayTile(leftBandTile.toArrayDouble(), leftBandTile.cols, leftBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(rightBandTile.toArrayDouble(), rightBandTile.cols, rightBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None)
          throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")
        val resultTile: Tile = Add(tile1, tile2)
        //val resultTile: Tile = if(tile1 == None || tile2 == None) null else Add(tile1, tile2)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Subtract pixel value inside a RasterRDD
   * This function can be replaced by function binaryOperator(leftBand, rightBand, (a, b) => a - b)
   * 内部波段运算，时、空、产品维信息不变
   * @param leftBand
   * @param rightBand
   * @return
   */
  def subtract(leftBand: String, rightBand: String): RasterRDD = {
    if(!(this.bands().contains(leftBand) && this.bands().contains(rightBand)))
      throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!leftBand.equals(rightBand)) "band" + UUID.randomUUID().toString else leftBand

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (leftBandTile, rightBandTile) = (bandTileMap.get(leftBand).get, bandTileMap.get(rightBand).get)
        val tile1 = DoubleArrayTile(leftBandTile.toArrayDouble(), leftBandTile.cols, leftBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(rightBandTile.toArrayDouble(), rightBandTile.cols, rightBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None)
          throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")
        val resultTile: Tile = Subtract(tile1, tile2)
        //val resultTile: Tile = if(tile1 == None || tile2 == None) null else Add(tile1, tile2)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Divide pixel value inside a RasterRDD
   * This function can be replaced by function binaryOperator(leftBand, rightBand, (a, b) => a / b)
   * 内部波段运算，时、空、产品维信息不变
   * @param leftBand
   * @param rightBand
   * @return
   */
  def divide(leftBand: String, rightBand: String): RasterRDD = {
    if(!(this.bands().contains(leftBand) && this.bands().contains(rightBand)))
      throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!leftBand.equals(rightBand)) "band" + UUID.randomUUID().toString else leftBand

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (leftBandTile, rightBandTile) = (bandTileMap.get(leftBand).get, bandTileMap.get(rightBand).get)
        val tile1 = DoubleArrayTile(leftBandTile.toArrayDouble(), leftBandTile.cols, leftBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(rightBandTile.toArrayDouble(), rightBandTile.cols, rightBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None)
          throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")
        val resultTile: Tile = Divide(tile1, tile2)
        //val resultTile: Tile = if(tile1 == None || tile2 == None) null else Add(tile1, tile2)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Multiply pixel value inside a RasterRDD
   * This function can be replaced by function binaryOperator(leftBand, rightBand, (a, b) => a * b)
   * 内部波段运算，时、空、产品维信息不变
   * @param leftBand
   * @param rightBand
   * @return
   */
  def multiply(leftBand: String, rightBand: String): RasterRDD = {
    if(!(this.bands().contains(leftBand) && this.bands().contains(rightBand)))
      throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!leftBand.equals(rightBand)) "band" + UUID.randomUUID().toString else leftBand

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (leftBandTile, rightBandTile) = (bandTileMap.get(leftBand).get, bandTileMap.get(rightBand).get)
        val tile1 = DoubleArrayTile(leftBandTile.toArrayDouble(), leftBandTile.cols, leftBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(rightBandTile.toArrayDouble(), rightBandTile.cols, rightBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None)
          throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")
        val resultTile: Tile = Multiply(tile1, tile2)
        //val resultTile: Tile = if(tile1 == None || tile2 == None) null else Add(tile1, tile2)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Custom binary operation inside a RasterRDD
   * Example: binaryOperator("Green", "Near-Infrared", (a, b) => Divide(Add(a, b), Subtract(a, b)))
   * @param leftBand
   * @param rightBand
   * @param reduce
   * @return
   */
  def binaryOperator(leftBand: String, rightBand: String, reduce: (Tile, Tile) => Tile): RasterRDD = {
    if (!(this.bands().contains(leftBand) && this.bands().contains(rightBand)))
      throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!leftBand.equals(rightBand)) "band" + UUID.randomUUID().toString else leftBand

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (leftBandTile, rightBandTile) = (bandTileMap.get(leftBand).get, bandTileMap.get(rightBand).get)
        val tile1 = DoubleArrayTile(leftBandTile.toArrayDouble(), leftBandTile.cols, leftBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(rightBandTile.toArrayDouble(), rightBandTile.cols, rightBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None)
          throw new RuntimeException("There is no " + leftBand + " or " + rightBand + "!")

        val resultTile: Tile = reduce.apply(tile1, tile2)
        //val resultTile: Tile = if(tile1 == None || tile2 == None) null else reduce.apply(tile1, tile2)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Custom triple operation inside a RasterRDD
   * Example: tripleOperator("Red", "Green", "Blue", (a, b, c) => MultibandTile(a, b, c))
   * @param band1
   * @param band2
   * @param band3
   * @param reduce
   * @return
   */
  def tripleOperator(band1: String, band2: String, band3: String, reduce: (Tile, Tile, Tile) => Tile): RasterRDD = {
    if (!(this.bands().contains(band1) && this.bands().contains(band2) && this.bands().contains(band3)))
      throw new RuntimeException("There is no " + band1 + " or " + band2 + " or " + band3 + "!")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (this.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), this.getRasterTileLayerMetadata)
    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1

    val newBand = if(!(band1.equals(band2) && band1.equals(band3))) "band" + UUID.randomUUID().toString else band1

    val rdd: RDD[(SpaceTimeBandKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //RDD[(SpaceTimeKey, Iterable((String, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (bandTile1, bandTile2, bandTile3) = (bandTileMap.get(band1).get, bandTileMap.get(band2).get, bandTileMap.get(band3).get)
        val tile1 = DoubleArrayTile(bandTile1.toArrayDouble(), bandTile1.cols, bandTile1.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile2 = DoubleArrayTile(bandTile2.toArrayDouble(), bandTile2.cols, bandTile2.rows)
          .convert(DoubleConstantNoDataCellType)
        val tile3 = DoubleArrayTile(bandTile3.toArrayDouble(), bandTile3.cols, bandTile3.rows)
          .convert(DoubleConstantNoDataCellType)

        if (tile1 == None || tile2 == None || tile3 == None)
          throw new RuntimeException("There is no " + band1 + " or " + band2 + " or " + band3 + "!")

        val resultTile: Tile = reduce.apply(tile1, tile2, tile3)
        //val resultTile: Tile = if(tile1 == None || tile2 == None || tile3 == None) null else reduce.apply(tile1, tile2, tile3)

        (SpaceTimeBandKey(spaceTimeKey, newBand), resultTile)
      }.filter(x => x._2 != null)

    /*val newMeta = new RasterTileLayerMetadata[SpaceTimeKey](this.meta.getTileLayerMetadata)
    newMeta.setMeasurementNames(Array(newBand))
    new RasterRDD(rdd, newMeta)*/

    val bounds = this.getRasterTileLayerMetadata.getTileLayerMetadata.bounds
    val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
    val extent = this.getRasterTileLayerMetadata.getTileLayerMetadata.extent
    val crs = this.getRasterTileLayerMetadata.getTileLayerMetadata.crs
    val productName = this.getRasterTileLayerMetadata.getProductName
    val bands = Array(newBand)
    val newMeta = RasterTileLayerMetadata(TileLayerMetadata(DoubleConstantNoDataCellType, ld, extent, crs, bounds), productName, _measurementNames = bands.toBuffer.asInstanceOf[ArrayBuffer[String]])
    new RasterRDD(rdd, newMeta)
  }

  /**
   * Save RasterRDD as images
   *
   * @param fileName
   * @param mosaic mosaic tiles if true
   * @param composite composite multiple band if true
   */
  def save(fileName: String, mosaic: Boolean = true, composite: Boolean = false): Unit = {
    if (!composite) saveSingleBandTile(fileName, mosaic)
    else saveMultiBandTile(fileName, mosaic)
  }

  /**
   * Save RasterRDD as multi-band images
   * @param fileName
   * @param mosaic mosaic tiles if true
   */
  def saveMultiBandTile(fileName: String, mosaic: Boolean = true): Unit = {
    val localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/" + UUID.randomUUID().toString + "/"
    val urlDataRoot = localDataRoot.replace("/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/",
      "http://125.220.153.26:8093/data/temp/")

    if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!

    if(mosaic){  //stitch extent-series tiles of each time instant
      val bands = this.bands()
      val srcMetadata = this.getRasterTileLayerMetadata.getTileLayerMetadata
      val bandGroupRdd: RDD[(SpaceTimeKey, Iterable[(String, Tile)])] = this.groupBy(_._1.spaceTimeKey).map(x => (x._1, x._2.map(ele => (ele._1.measurementName, ele._2))))
      val multiBandGroupRdd: RDD[(SpaceTimeKey, MultibandTile)] = bandGroupRdd.map{x =>
        val orderedTiles = new ArrayBuffer[Tile]()
        val tileMaps = x._2.toMap
        for (i <- bands) orderedTiles.append(tileMaps.get(i).get)
        (x._1, MultibandTile(orderedTiles.toArray))
      }
      val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,MultibandTile)])] = multiBandGroupRdd.groupBy(_._1.instant)

      val resultUrl = temporalGroupRdd.map{x =>
        val metadata = srcMetadata
        val layout = metadata.layout
        val crs = metadata.crs
        val instant: Long = x._1
        val time = x._2.iterator.next()._1.time
        val multiBandTileLayerArray: Array[(SpatialKey, MultibandTile)] = x._2.map(ele => (ele._1.spatialKey, ele._2)).toArray
        val stitched: Raster[MultibandTile] = TileUtil.stitchMultiband(multiBandTileLayerArray, layout)

        val outputPath = localDataRoot + time + "_" + fileName
        if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!
        if(fileName.endsWith("png") || fileName.endsWith("PNG")) throw new RuntimeException("PNG format does not support multi-bands image storage")
        else if(fileName.endsWith("jpg") || fileName.endsWith("JPG") || fileName.endsWith("JPEG")) throw new RuntimeException("JPG format does not support multi-bands image storage")
        else if(fileName.endsWith("tif") || fileName.endsWith("tiff") || fileName.endsWith("TIF") || fileName.endsWith("TIFF")) GeoTiff(stitched, crs).write(outputPath)
        val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + localDataRoot
        scpPngCommand.!
        urlDataRoot  + time + "_" + fileName
        }.cache()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The links of the result: ")
      resultUrl.collect().foreach(println(_))

    }else {
      val srcMetadata = this.getRasterTileLayerMetadata.getTileLayerMetadata
      val bandGroupRdd: RDD[(SpaceTimeKey, Iterable[(String, Tile)])] = this.groupBy(_._1.spaceTimeKey).map(x => (x._1, x._2.map(ele => (ele._1.measurementName, ele._2))))
      val multiBandGroupRdd: RDD[(SpaceTimeKey, MultibandTile)] = bandGroupRdd.map(x => (x._1, MultibandTile(x._2.map(_._2).toArray)))
      val resultUrl = multiBandGroupRdd.map{x =>
        val metadata = srcMetadata
        val layout = metadata.layout
        val crs = metadata.crs
        val time = x._1.time
        val col = x._1.col
        val row = x._1.row
        val outputPath = localDataRoot + time + "_" + col + "_" + row + "_" + fileName
        if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!
        if(fileName.endsWith("png") || fileName.endsWith("PNG")) throw new RuntimeException("PNG format does not support multi-bands image storage")
        else if(fileName.endsWith("jpg") || fileName.endsWith("JPG") || fileName.endsWith("JPEG")) throw new RuntimeException("JPG format does not support multi-bands image storage")
        else if(fileName.endsWith("tif") || fileName.endsWith("tiff") || fileName.endsWith("TIF") || fileName.endsWith("TIFF")) GeoTiff(x._2, x._1.spatialKey.extent(layout), crs).write(outputPath)
        val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + localDataRoot
        scpPngCommand.!
        urlDataRoot  + time + "_" + col + "_" + row + "_" + fileName
      }.cache()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The links of the result: ")
      resultUrl.collect().foreach(println(_))
    }
  }

  /**
   * Save RasterRDD as single-band images
   * @param fileName
   * @param mosaic mosaic tiles if true
   */
  def saveSingleBandTile(fileName: String, mosaic: Boolean = true): Unit = {
    val localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/" + UUID.randomUUID().toString + "/"
    val urlDataRoot = localDataRoot.replace("/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/",
      "http://125.220.153.26:8093/data/temp/")

    if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!

    if(mosaic){ //stitch extent-series tiles of each time instant
      val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeBandKey,Tile)])] = this.groupBy(_._1.spaceTimeKey.instant)
      val srcMetadata = this.getRasterTileLayerMetadata.getTileLayerMetadata
      val colorRamp = ColorRamp(
        0xD76B27FF,
        0xE68F2DFF,
        0xF9B737FF,
        0xF5CF7DFF,
        0xF0E7BBFF,
        0xEDECEAFF,
        0xC8E1E7FF,
        0xADD8EAFF,
        0x7FB8D4FF,
        0x4EA3C8FF,
        0x2586ABFF
      )
      val resultUrl = temporalGroupRdd.flatMap{x =>
        val metadata = srcMetadata
        val layout = metadata.layout
        val crs = metadata.crs
        val instant: Long = x._1
        val time = x._2.iterator.next()._1.spaceTimeKey.time
        val temporalBandGroupArray: Array[(String, Array[(SpaceTimeBandKey,Tile)])] = x._2.toArray.groupBy(_._1.measurementName).toArray
        val urls = new ArrayBuffer[String]()
        temporalBandGroupArray.foreach{ele =>
          val bandName = ele._1
          val tileLayerArray: Array[(SpatialKey, Tile)] = ele._2.map(ele=>(ele._1.spaceTimeKey.spatialKey, ele._2))
          val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)
          val outputPath = localDataRoot + bandName + "_" + time + "_" + fileName
          if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!
          if(fileName.endsWith("png") || fileName.endsWith("PNG")) stitched.tile.renderPng(colorRamp).write(outputPath)
          else if(fileName.endsWith("jpg") || fileName.endsWith("JPG") || fileName.endsWith("JPEG")) stitched.tile.renderJpg(colorRamp).write(outputPath)
          else if(fileName.endsWith("tif") || fileName.endsWith("tiff") || fileName.endsWith("TIF") || fileName.endsWith("TIFF")) GeoTiff(stitched, crs).write(outputPath)
          val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + localDataRoot
          scpPngCommand.!
          urls.append(urlDataRoot + bandName + "_" + time + "_" +fileName)
        }
        urls
      }.cache()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The links of the result: ")
      resultUrl.collect().foreach(println(_))
    }else { // output tiles
      val srcMetadata = this.getRasterTileLayerMetadata.getTileLayerMetadata
      val colorRamp = ColorRamp(
        0xD76B27FF,
        0xE68F2DFF,
        0xF9B737FF,
        0xF5CF7DFF,
        0xF0E7BBFF,
        0xEDECEAFF,
        0xC8E1E7FF,
        0xADD8EAFF,
        0x7FB8D4FF,
        0x4EA3C8FF,
        0x2586ABFF
      )
      val resultUrl = this.map{x =>
        val metadata = srcMetadata
        val layout = metadata.layout
        val crs = metadata.crs
        val time = x._1.spaceTimeKey.time
        val bandName = x._1.measurementName
        val col = x._1.spaceTimeKey.col
        val row = x._1.spaceTimeKey.row
        val outputPath = localDataRoot + bandName + "_" + time + "_" + col + "_" + row + "_" + fileName
        if (!new File(localDataRoot).exists()) ("mkdir " + localDataRoot).!
        if(fileName.endsWith("png") || fileName.endsWith("PNG")) x._2.renderPng(colorRamp).write(outputPath)
        else if(fileName.endsWith("jpg") || fileName.endsWith("JPG") || fileName.endsWith("JPEG")) x._2.renderJpg(colorRamp).write(outputPath)
        else if(fileName.endsWith("tif") || fileName.endsWith("tiff") || fileName.endsWith("TIF") || fileName.endsWith("TIFF")) GeoTiff(x._2, x._1.spaceTimeKey.spatialKey.extent(layout), crs).write(outputPath)
        val scpPngCommand = "scp " + outputPath + " geocube@gisweb1:" + localDataRoot
        scpPngCommand.!
        urlDataRoot  + bandName + "_" + time + "_" + col + "_" + row + "_" + fileName
      }.cache()
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The links of the result: ")
      resultUrl.collect().foreach(println(_))
    }
  }

  /**
   * Get features that intersects with the pixels whose value is larger than the $pixelValue
   * @param pixelValue
   * @param featureRdd
   * @param temporalDim
   * @return
   */
  implicit def intersectionWithFeature(featureRdd: FeatureRDD, pixelValue: Double, pixelBuffer: Int = 0, temporalDim: Boolean = true): Array[SimpleFeature] = {
    if (this.bands().size > 1) throw new RuntimeException("This function only supports single-band RasterRDD!")

    if(temporalDim){
      val geoObjectsRdd: RDD[(SpaceTimeKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1, x._2))
      val rasterTileRdd: RDD[(SpaceTimeKey, Tile)] = this.map(x=>(x._1.spaceTimeKey, x._2))
      val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout

      val joinedRdd:RDD[(SpaceTimeKey, (Tile, Iterable[GeoObject]))] =
        rasterTileRdd.join(geoObjectsRdd)
      val overlappedGeoObjectRdd:RDD[GeoObject] = joinedRdd.flatMap(x=>{
        val affectedGeoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
        val spatialKey = x._1.spatialKey
        val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
        val tile = x._2._1
        val geoObjectList = x._2._2

        val tileCols = ld.tileCols
        val tileRows = ld.tileRows
        val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
        val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
        val geomIterator = geoObjectList.iterator

        while (geomIterator.hasNext) {
          val geoObject: GeoObject = geomIterator.next()
          val feature: SimpleFeature = geoObject.feature
          val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
          val pixelX: Int = math.floor((geometry.getCoordinate.x - tileExtent.xmin) / pixelWidth).toInt
          val pixelY: Int = tileRows - 1 - math.floor((geometry.getCoordinate.y - tileExtent.ymin) / pixelHeight).toInt
          /*if(tile.getDouble(pixelX, pixelY) == 255.0)
            affectedGeoObjects.append(geoObject)*/
          var flag = true
          val start = 0 - pixelBuffer
          val end = pixelBuffer
          (start until end + 1).foreach{i =>
            (start until end + 1).foreach{j =>
              if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
                if (tile.getDouble(pixelX + i, pixelY + j) == pixelValue && flag) {
                  affectedGeoObjects.append(geoObject)
                  flag = false
                }
              }
            }
          }
        }
        affectedGeoObjects
      })
      overlappedGeoObjectRdd.map(_.feature).collect()
    }else{
      val geoObjectsRdd: RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x=>(x._1.spatialKey, x._2))
      val rasterTileRdd: RDD[(SpatialKey, Tile)] = this.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
      val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout

      val joinedRdd:RDD[(SpatialKey, (Tile, Iterable[GeoObject]))] =
        rasterTileRdd.join(geoObjectsRdd)
      val overlappedGeoObjectRdd:RDD[GeoObject] = joinedRdd.flatMap(x=>{
        val affectedGeoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
        val spatialKey = x._1
        val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
        val tile = x._2._1
        val geoObjectList = x._2._2

        val tileCols = ld.tileCols
        val tileRows = ld.tileRows
        val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
        val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
        val geomIterator = geoObjectList.iterator

        while (geomIterator.hasNext) {
          val geoObject: GeoObject = geomIterator.next()
          val feature: SimpleFeature = geoObject.feature
          val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
          val pixelX: Int = math.floor((geometry.getCoordinate.x - tileExtent.xmin) / pixelWidth).toInt
          val pixelY: Int = tileRows - 1 - math.floor((geometry.getCoordinate.y - tileExtent.ymin) / pixelHeight).toInt
          /*if(tile.getDouble(pixelX, pixelY) == 255.0)
            affectedGeoObjects.append(geoObject)*/
          var flag = true
          val start = 0 - pixelBuffer
          val end = pixelBuffer
          (start until end + 1).foreach{i =>
            (start until end + 1).foreach{j =>
              if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
                if (tile.getDouble(pixelX + i, pixelY + j) == pixelValue && flag) {
                  affectedGeoObjects.append(geoObject)
                  flag = false
                }
              }
            }
          }
        }
        affectedGeoObjects
      })
      overlappedGeoObjectRdd.map(_.feature).collect()
    }

  }

  /**
   * Get tabulars that intersects with the pixels whose value is larger than the $pixelValue
   * @param pixelValue
   * @param tabularRdd
   * @param temporalDim
   * @return
   */
  implicit def intersectionWithTabular(tabularRdd: TabularRDD, pixelValue: Double, pixelBuffer: Int, temporalDim: Boolean = true): Array[TabularRecord] = {
    if (this.bands().size > 1) throw new RuntimeException("This function only supports single-band RasterRDD!")

    if(temporalDim){
      val tabularRecordsRdd:RDD[(SpaceTimeKey, Iterable[TabularRecord])] = tabularRdd.map(x=>(x._1, x._2))
      val rasterTileRdd: RDD[(SpaceTimeKey, Tile)] = this.map(x=>(x._1.spaceTimeKey, x._2))
      val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
      val joinedRdd:RDD[(SpaceTimeKey, (Tile, Iterable[TabularRecord]))] =
        rasterTileRdd.join(tabularRecordsRdd)

      val overlappedTabularRecordRdd:RDD[TabularRecord] = joinedRdd.flatMap(x=>{
        val affectedTabularRecords: ArrayBuffer[TabularRecord] = new ArrayBuffer[TabularRecord]()
        val spatialKey = x._1.spatialKey
        val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
        val tile = x._2._1
        val tabularRecordList = x._2._2

        val tileCols = ld.tileCols
        val tileRows = ld.tileRows
        val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
        val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
        val tabularIterator = tabularRecordList.iterator

        while (tabularIterator.hasNext) {
          val tabularRecord: TabularRecord = tabularIterator.next()
          val attributs = tabularRecord.getAttributes
          val x = attributs.get("longitude").get.toDouble
          val y = attributs.get("latitude").get.toDouble
          val pixelX: Int = math.floor((x - tileExtent.xmin) / pixelWidth).toInt
          val pixelY: Int = tileRows - 1 - math.floor((y - tileExtent.ymin) / pixelHeight).toInt

          /*if(tile.getDouble(pixelX, pixelY) == 255.0)
            affectedGeoObjects.append(geoObject)*/
          var flag = true
          val start = 0 - pixelBuffer
          val end = pixelBuffer
          (start until end + 1).foreach{i =>
            (start until end + 1).foreach{j =>
              if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
                if (tile.getDouble(pixelX + i, pixelY + j) == pixelValue && flag) {
                  affectedTabularRecords.append(tabularRecord)
                  flag = false
                }
              }
            }
          }
        }
        affectedTabularRecords
      })
      overlappedTabularRecordRdd.collect()
    }else{
      val tabularRecordsRdd:RDD[(SpatialKey, Iterable[TabularRecord])] = tabularRdd.map(x=>(x._1.spatialKey, x._2))
      val rasterTileRdd: RDD[(SpatialKey, Tile)] = this.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
      val ld = this.getRasterTileLayerMetadata.getTileLayerMetadata.layout
      val joinedRdd:RDD[(SpatialKey, (Tile, Iterable[TabularRecord]))] =
        rasterTileRdd.join(tabularRecordsRdd)

      val overlappedTabularRecordRdd:RDD[TabularRecord] = joinedRdd.flatMap(x=>{
        val affectedTabularRecords: ArrayBuffer[TabularRecord] = new ArrayBuffer[TabularRecord]()
        val spatialKey = x._1
        val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
        val tile = x._2._1
        val tabularRecordList = x._2._2

        val tileCols = ld.tileCols
        val tileRows = ld.tileRows
        val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
        val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
        val tabularIterator = tabularRecordList.iterator

        while (tabularIterator.hasNext) {
          val tabularRecord: TabularRecord = tabularIterator.next()
          val attributs = tabularRecord.getAttributes
          val x = attributs.get("longitude").get.toDouble
          val y = attributs.get("latitude").get.toDouble
          val pixelX: Int = math.floor((x - tileExtent.xmin) / pixelWidth).toInt
          val pixelY: Int = tileRows - 1 - math.floor((y - tileExtent.ymin) / pixelHeight).toInt

          /*if(tile.getDouble(pixelX, pixelY) == 255.0)
            affectedGeoObjects.append(geoObject)*/
          var flag = true
          val start = 0 - pixelBuffer
          val end = pixelBuffer
          (start until end + 1).foreach{i =>
            (start until end + 1).foreach{j =>
              if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
                if (tile.getDouble(pixelX + i, pixelY + j) == pixelValue && flag) {
                  affectedTabularRecords.append(tabularRecord)
                  flag = false
                }
              }
            }
          }
        }
        affectedTabularRecords
      })
      overlappedTabularRecordRdd.collect()
    }
  }

  /**
   * Get intersected raster
   * @param featureRdd
   * @return
   */
  def intersection(featureRdd: FeatureRDD): RasterRDD = {
    null
  }

  /**
   * Get intersected raster
   * @param tabularRdd
   * @return
   */
  def intersection(tabularRdd: TabularRDD): RasterRDD = {
    null
  }

  def merge(other: RasterRDD, layoutDefinition: LayoutDefinition = null): RasterRDD = {
    if (layoutDefinition == null) {
      assert(this.getRasterTileLayerMetadata.getTileLayerMetadata.layout.equals(other.getRasterTileLayerMetadata.getTileLayerMetadata.layout))
      null
    }
    else {
      null
    }

  }

}
object RasterRDD{
  def main(args: Array[String]): Unit = {
    /*val myFloatData: Array[Float] = Array(42.0f, Float.NaN, 2.0f, 3.0f)
    val customTile = FloatArrayTile(myFloatData, 2, 2, FloatConstantNoDataCellType)
    println(customTile.getDouble(1, 0))
    println(customTile.get(1, 0))
    val customTile1 = customTile.convert(DoubleConstantNoDataCellType)
    println(customTile1.getDouble(1, 0))
    println(customTile1.get(1, 0))
    val defaultCT = FloatUserDefinedNoDataCellType(42.0f)
    val customTile3 = FloatArrayTile(myFloatData, 2, 2, defaultCT)
    println(customTile3.getDouble(1, 0))
    println(customTile3.get(1, 0))
    println(customTile3.getDouble(0, 0))
    println(customTile3.get(0, 0))
    println(Float.NaN.toInt)
    val customTile4 = customTile3.convert(DoubleConstantNoDataCellType)
    println(customTile4.getDouble(1, 0))
    println(customTile4.get(1, 0))
    println(customTile4.getDouble(0, 0))
    println(customTile4.get(0, 0))
    println()*/

    /**slice and trim along spatial, temporal and band dimension**/
    /*val conf = new SparkConf()
      .setAppName("RasterRDD API Test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,30.073457222586285,113.9181165740333,30.9597805438586)
    queryParams.setTime("2015-09-06 00:00:00.000", "2015-09-08 00:00:00.000")
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    queryParams.setLevel("4000")
    val rasterRdd: RasterRDD = getData(sc, queryParams)
    println(rasterRdd.products().toList)
    println(rasterRdd.bands().toList)
    println(rasterRdd.extent().toString())
    println(rasterRdd.timeRange())
    println(rasterRdd.getRasterTileLayerMetadata.getTileLayerMetadata.toString)

    val extentTrimRdd: RasterRDD = rasterRdd.extent(113.51494046724021,30.473457222586285,113.7181165740333,30.7597805438586)
    println(extentTrimRdd.products().toList)
    println(extentTrimRdd.bands().toList)
    println(extentTrimRdd.extent().toString())
    println(extentTrimRdd.timeRange())
    println(extentTrimRdd.getRasterTileLayerMetadata.getTileLayerMetadata.toString)

    val timeTrimRdd: RasterRDD = rasterRdd.time("2015-09-06 01:00:00.000", "2015-09-07 23:00:00.000")
    println(timeTrimRdd.products().toList)
    println(timeTrimRdd.bands().toList)
    println(timeTrimRdd.extent().toString())
    println(timeTrimRdd.timeRange())
    println(timeTrimRdd.getRasterTileLayerMetadata.getTileLayerMetadata.toString)

    val bandTrimRdd: RasterRDD = rasterRdd.bands(Array("Green", "Blue"))
    println(bandTrimRdd.products().toList)
    println(bandTrimRdd.bands().toList)
    println(bandTrimRdd.extent().toString())
    println(bandTrimRdd.timeRange())
    println(bandTrimRdd.getRasterTileLayerMetadata.getTileLayerMetadata.toString)

    val bandSliceRdd: RasterRDD = rasterRdd.band("Near-Infrared")
    println(bandSliceRdd.products().toList)
    println(bandSliceRdd.bands().toList)
    println(bandSliceRdd.extent().toString())
    println(bandSliceRdd.timeRange())
    println(bandSliceRdd.getRasterTileLayerMetadata.getTileLayerMetadata.toString)*/


    /**二目运算的两种方式, 以及存储接口测试**/
    /*val conf = new SparkConf()
      .setAppName("RasterRDD API Test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,29.073457222586285,113.9181165740333,30.9597805438586)
    queryParams.setTime("2015-09-06 00:00:00.000", "2015-09-08 00:00:00.000")
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))
    queryParams.setLevel("4000")
    val rasterRdd: RasterRDD = getData(sc, queryParams)

    println(rasterRdd.bands().toList)
    println(rasterRdd.extent().toString())
    println(rasterRdd.timeRange())
    rasterRdd.getRasterTileLayerMetadata.printString()
    //rasterRdd.save("test.tif", mosaic = true, composite = true)
    val queryEnd = System.currentTimeMillis()

    //简单测试
    /*val testRdd1 = rasterRdd + rasterRdd.band("Green")
    val testRdd2 = rasterRdd + rasterRdd
    val testRdd3 = rasterRdd.band("Green") + rasterRdd.band("Near-Infrared")
    val testRdd4 = rasterRdd.band("Green") + rasterRdd.band("Green")
    val testRdd5 = rasterRdd.binaryOperator("Green", "Near-Infrared", (a, b)=>(a - b)/(a + b))
    val testRdd6 = rasterRdd.binaryOperator("Green", "Green", (a, b)=>(a - b)/(a + b))
    println(testRdd1.count())
    println(testRdd2.count())
    println(testRdd3.count())
    println(testRdd4.count())
    println(testRdd5.count())
    println(testRdd6.count())
    testRdd1.getRasterTileLayerMetadata.printString()
    testRdd2.getRasterTileLayerMetadata.printString()
    testRdd3.getRasterTileLayerMetadata.printString()
    testRdd4.getRasterTileLayerMetadata.printString()
    testRdd5.getRasterTileLayerMetadata.printString()
    testRdd6.getRasterTileLayerMetadata.printString()*/

    //两种方式做二目运算
    val analysisBegin = System.currentTimeMillis()

    //单个rdd内部运算，更快
    val resultRdd: RasterRDD = rasterRdd.binaryOperator("Green", "Near-Infrared", (a, b)=>(a - b)/(a + b)) //val resultRdd: RasterRDD = rasterRdd.binaryOperator("Green", "Near-Infrared", (a, b) => Divide(Subtract(a, b), Add(a, b)))

    //rdd之间做运算，更慢，因为每个rdd都要执行一边active操作
    /*val greenRdd = rasterRdd.band("Green")
    val nirRdd = rasterRdd.band("Near-Infrared")
    val resultRdd: RasterRDD = (greenRdd - nirRdd) / (greenRdd + nirRdd) //val resultRdd: RasterRDD = greenRdd.subtract(nirRdd).divide(greenRdd.add(nirRdd))*/

    resultRdd.save("ndwi.tif", mosaic = true, composite = false)
    resultRdd.getRasterTileLayerMetadata.printString()
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))*/

    /**Flood test**/
    val conf = new SparkConf()
      .setAppName("GeoCube-Dianmu Hurrican Flood Analysis")
      //.setMaster("local[8]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rpc.message.maxSize", "1024")
    val sc = new SparkContext(conf)

    val queryParams = new QueryParams
    //queryParams.setCubeId("Hainan_Daguangba")
    queryParams.setRasterProductName("GF_Hainan_Daguangba_NDWI_EO")
    queryParams.setVectorProductName("Hainan_Daguangba_Village_Vector")
    queryParams.setTabularProductName("Hainan_Daguangba_Village_Tabular")
    queryParams.setExtent(108.90494046724021,18.753457222586285,109.18763565740333,19.0497805438586)
    queryParams.setTime("2016-06-01 00:00:00.000", "2016-09-01 00:00:00.000")
    val rasterRdd: RasterRDD = getData(sc, queryParams)
    val featureRdd: FeatureRDD = getData(sc, queryParams)
    val tabularRdd: TabularRDD = getData(sc, queryParams)

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Task is running ...")

    val timeNode = rasterRdd.timeNodes()
    assert(timeNode.size == 2)
    val changedRdd: RasterRDD =
      if (timeNode(1).toInstant.toEpochMilli > timeNode(0).toInstant.toEpochMilli) rasterRdd.time(timeNode(1)) - rasterRdd.time(timeNode(0))
      else rasterRdd.time(timeNode(0)) - rasterRdd.time(timeNode(1))
    val affectedFeatures: Array[SimpleFeature] = changedRdd.intersectionWithFeature(featureRdd, 255.0, 2, false)
    val affectedTabulars: Array[TabularRecord] = changedRdd.intersectionWithTabular(tabularRdd, 255.0, 2, false)

    var affectedPopulations = 0
    affectedFeatures.foreach{feature =>
      val vectorGeom = feature.getDefaultGeometry.asInstanceOf[Geometry]
      affectedTabulars.foreach{tabularRecord =>
        val attributes = tabularRecord.getAttributes
        val x = attributes.get("longitude").get.toDouble
        val y = attributes.get("latitude").get.toDouble
        val coord = new Coordinate(x, y)
        val tabularGeom = new GeometryFactory().createPoint(coord)
        if (tabularGeom.buffer(0.0001).intersects(vectorGeom)){
          affectedPopulations += attributes.get("population").get.toDouble.toInt
        }
      }
    }
    val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
    affectedFeatures.foreach{ features =>
      val affectedGeoName = features.getAttribute("geo_name").asInstanceOf[String]
      affectedGeoNames.append(affectedGeoName)
    }

    print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " villages are impacted including: ")
    affectedGeoNames.foreach(x => print(x + " ")); println()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedPopulations + " people are affected")


  }
}

