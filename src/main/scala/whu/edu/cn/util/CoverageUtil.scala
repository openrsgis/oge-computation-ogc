package whu.edu.cn.util

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.mapalgebra.focal.{CellwiseCalculation, DoubleArrayTileResult, FocalCalculation, Neighborhood, Square, TargetCell}
import geotrellis.raster.mapalgebra.local.{LocalTileBinaryOp, LocalTileComparatorOp}
import geotrellis.raster.{ArrayTile, ByteConstantNoDataCellType, CellType, DoubleConstantNoDataCellType, FloatConstantNoDataCellType, GridBounds, IntConstantNoDataCellType, MultibandTile, NODATA, ShortConstantNoDataCellType, Tile, TileLayout, UByteCellType, UByteConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType, isNoData}
import geotrellis.spark._
import geotrellis.util.MethodExtensions
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{RawTile, SpaceTimeBandKey}
import whu.edu.cn.util.TileSerializerCoverageUtil.deserializeTileData

import java.time.{Instant, ZoneOffset}
import scala.collection.mutable
import scala.math.{max, min}

object CoverageUtil {
  // TODO: lrx: 函数的RDD大写，变量的Rdd小写，为了开源全局改名，提升代码质量
  def makeCoverageRDD(tileRDDReP: RDD[RawTile]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val extents: (Double, Double, Double, Double) = tileRDDReP.map(t => {
      (t.getExtent.xmin, t.getExtent.ymin, t.getExtent.xmax, t.getExtent.ymax)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant: (Int, Int, Long, Int, Int, Long) = tileRDDReP.map(t => {
      (t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)), t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)))
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), min(a._3, b._3), max(a._4, b._4), max(a._5, b._5), max(a._6, b._6))
    })
    //    if (colRowInstant._1 != 0 || colRowInstant._2 != 0 || (colRowInstant._3 != colRowInstant._6)) {
    //      throw new InternalError(s"内部错误！瓦片序号出错或时间不一致！请查看$colRowInstant")
    //    }


    val firstTile: RawTile = tileRDDReP.first()
    val layoutCols: Int = math.max(math.ceil((extents._3 - extents._1 - firstTile.getResolutionCol) / firstTile.getResolutionCol / 256.0).toInt, 1)
    val layoutRows: Int = math.max(math.ceil((extents._4 - extents._2 - firstTile.getResolutionRow) / firstTile.getResolutionRow / 256.0).toInt, 1)

    val extent: Extent = geotrellis.vector.Extent(extents._1, extents._2, extents._1 + layoutCols * firstTile.getResolutionCol * 256.0, extents._2 + layoutRows * firstTile.getResolutionRow * 256.0)

    val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
    val ld: LayoutDefinition = LayoutDefinition(extent, tl)
    val cellType: CellType = CellType.fromName(firstTile.getDataType.toString)
    val crs: CRS = firstTile.getCrs
    val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
    val tileLayerMetadata: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    val tileRdd: RDD[(SpaceTimeKey, (Int, String, Tile))] = tileRDDReP.map(tile => {
      val phenomenonTime: Long = tile.getTime.toEpochSecond(ZoneOffset.ofHours(0))
      val rowNum: Int = tile.getSpatialKey.row
      val colNum: Int = tile.getSpatialKey.col
      val measurementRank: Int = tile.getMeasurementRank
      val measurement: String = tile.getMeasurement
      val Tile: Tile = deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString)
      val k: SpaceTimeKey = SpaceTimeKey(colNum, rowNum, phenomenonTime)
      val v: Tile = Tile
      (k, (measurementRank, measurement, v))
    })
    val multibandTileRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = tileRdd.groupByKey.map(t => {
      val tileSorted: Array[(Int, String, Tile)] = t._2.toArray.sortBy(x => x._1)
      val listBuffer: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      val tileMeasurement: Array[String] = tileSorted.map(x => x._2)
      listBuffer ++= tileMeasurement
      val tileArray: Array[Tile] = tileSorted.map(x => x._3)
      (SpaceTimeBandKey(t._1, listBuffer), MultibandTile(tileArray))
    })
    var coverage = (multibandTileRdd, tileLayerMetadata)

    if (coverage._2.cellType.equalDataType(UByteConstantNoDataCellType) || coverage._2.cellType.equalDataType(UByteCellType))
      coverage = toInt8(coverage)
    else if (coverage._2.cellType.equalDataType(UShortConstantNoDataCellType) || coverage._2.cellType.equalDataType(UShortCellType))
      coverage = toInt16(coverage)

    val coverage1 = removeZeroFromCoverage(coverage)

    coverage1
  }

  def removeZeroFromCoverage(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, (tile) => removeZeroFromTile(tile))
  }

  def removeZeroFromTile(tile: Tile): Tile = {
    if (tile.cellType.isFloatingPoint)
      tile.mapDouble(i => if (i.equals(0.0)) Double.NaN else i)
    else
      tile.map(i => if (i == 0) NODATA else i)
  }

  def toInt8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted =
      (coverage._1.map(t => {
        (t._1, t._2.convert(ByteConstantNoDataCellType))
      }), TileLayerMetadata(ByteConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a unsigned 8-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteConstantNoDataCellType))
    }), TileLayerMetadata(UByteConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted

  }


  /**
   * Casts the input value to a signed 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(ShortConstantNoDataCellType))
    }), TileLayerMetadata(ShortConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a unsigned 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UShortConstantNoDataCellType))
    }), TileLayerMetadata(UShortConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs,
      coverage._2.bounds))
    coverageConverted
  }


  /**
   * Casts the input value to a signed 32-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt32(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(IntConstantNoDataCellType))
    }), TileLayerMetadata(IntConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a 32-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toFloat(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(FloatConstantNoDataCellType))
    }), TileLayerMetadata(FloatConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a 64-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toDouble(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(DoubleConstantNoDataCellType))
    }), TileLayerMetadata(DoubleConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }


  def checkProjResoExtent(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): ((RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])) = {
    val tile1: (SpaceTimeBandKey, MultibandTile) = coverage1._1.first()
    val tile2: (SpaceTimeBandKey, MultibandTile) = coverage2._1.first()
    val time1: Long = tile1._1.spaceTimeKey.instant
    val time2: Long = tile2._1.spaceTimeKey.instant
    val band1: mutable.ListBuffer[String] = tile1._1.measurementName
    val band2: mutable.ListBuffer[String] = tile2._1.measurementName
    val coverage1tileRDD: RDD[(SpatialKey, MultibandTile)] = coverage1._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val coverage2tileRDD: RDD[(SpatialKey, MultibandTile)] = coverage2._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    // 投影坐标系、分辨率、范围有一个不一样都要这样处理
    if (coverage1._2.crs != coverage2._2.crs || coverage1._2.cellSize.resolution != coverage2._2.cellSize.resolution || coverage1._2.extent != coverage2._2.extent) {
      var reso: Double = 0.0
      var crs: CRS = null
      var resoRatio: Double = 0.0
      // flag是针对影像分辨率的，flag=1，代表第1张影像分辨率低
      var flag: Int = 0

      // 假如投影不一样，都投影到4326
      if (coverage1._2.crs != coverage2._2.crs) {
        // 这里投影到统一的坐标系下只是为了比较分辨率大小
        val reso1: Double = coverage1._2.cellSize.resolution
        // 把第二张影像投影到第一张影像的坐标系下，然后比较分辨率
        val reso2: Double = tile2._2.reproject(coverage2._2.layout.mapTransform(tile2._1.spaceTimeKey.spatialKey), coverage2._2.crs, coverage1._2.crs).cellSize.resolution
        if (reso1 < reso2) {
          reso = reso1
          resoRatio = reso2 / reso1
          flag = 2
          crs = coverage1._2.crs
        }
        else {
          reso = reso2
          resoRatio = reso1 / reso2
          flag = 1
          crs = coverage2._2.crs
        }
      }
      else {
        crs = coverage1._2.crs
        if (coverage1._2.cellSize.resolution < coverage2._2.cellSize.resolution) {
          reso = coverage1._2.cellSize.resolution
          resoRatio = coverage2._2.cellSize.resolution / coverage1._2.cellSize.resolution
          flag = 2
        }
        else {
          reso = coverage2._2.cellSize.resolution
          resoRatio = coverage1._2.cellSize.resolution / coverage2._2.cellSize.resolution
          flag = 1
        }
      }

      val extent1: Extent = coverage1._2.extent.reproject(coverage1._2.crs, crs)
      val extent2: Extent = coverage2._2.extent.reproject(coverage2._2.crs, crs)

      if (extent1.intersects(extent2)) {
        // 这里一定不会null，但是API要求orNull
        var extent: Extent = extent1.intersection(extent2).orNull

        // 先重投影，重投影到原始范围重投影后的范围、这个范围除以256, 顺势进行裁剪
        val layoutCols: Int = math.max(math.ceil((extent.xmax - extent.xmin) / reso / 256.0).toInt, 1)
        val layoutRows: Int = math.max(math.ceil((extent.ymax - extent.ymin) / reso / 256.0).toInt, 1)
        val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
        // Extent必须进行重新计算，因为layoutCols和layoutRows加了黑边，导致范围变化了
        val newExtent: Extent = new Extent(extent.xmin, extent.ymin, extent.xmin + reso * 256.0 * layoutCols, extent.ymin + reso * 256.0 * layoutRows)
        extent = newExtent
        val ld: LayoutDefinition = LayoutDefinition(extent, tl)

        // 这里是coverage1开始进行重投影
        val srcBounds1: Bounds[SpaceTimeKey] = coverage1._2.bounds
        val newBounds1: Bounds[SpatialKey] = Bounds(SpatialKey(srcBounds1.get.minKey.spatialKey._1, srcBounds1.get.minKey.spatialKey._2), SpatialKey(srcBounds1.get.maxKey.spatialKey._1, srcBounds1.get.maxKey.spatialKey._2))
        val rasterMetaData1: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage1._2.cellType, coverage1._2.layout, coverage1._2.extent, coverage1._2.crs, newBounds1)
        val coverage1Rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = MultibandTileLayerRDD(coverage1tileRDD, rasterMetaData1)

        var coverage1tileLayerRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = coverage1Rdd
        // 如果flag=1， 代表第一张影像的分辨率较低
        if (flag == 1) {
          val tileRatio1: Int = (math.log(resoRatio) / math.log(2)).toInt
          if (tileRatio1 != 0) {
            // 对影像进行瓦片大小重切分
            val newTileSize1: Int = 256 / math.pow(2, tileRatio1).toInt
            val coverage1Retiled: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = coverage1Rdd.regrid(newTileSize1)
            // 进行裁剪
            val cropExtent1: Extent = extent.reproject(crs, coverage1Retiled.metadata.crs)
            coverage1tileLayerRdd = coverage1Retiled.crop(cropExtent1)
          }
        }


        val (_, reprojectedRdd1): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          coverage1tileLayerRdd.reproject(crs, ld)

        // Filter配合extent的强制修改，达到真正裁剪到我们想要的Layout的目的
        val reprojFilter1: RDD[(SpatialKey, MultibandTile)] = reprojectedRdd1.filter(layer => {
          val key: SpatialKey = layer._1
          val extentR: Extent = ld.mapTransform(key)
          extentR.xmin >= extent.xmin && extentR.xmax <= extent.xmax && extentR.ymin >= extent.ymin && extentR.ymax <= extent.ymax
        })

        // metadata需要添加time
        val srcMetadata1: TileLayerMetadata[SpatialKey] = reprojectedRdd1.metadata
        val srcProjBounds1: Bounds[SpatialKey] = srcMetadata1.bounds
        val newProjBounds1: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(srcProjBounds1.get.minKey._1, srcProjBounds1.get.minKey._2, time1), SpaceTimeKey(srcProjBounds1.get.maxKey._1, srcProjBounds1.get.maxKey._2, time1))
        val newMetadata1: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(srcMetadata1.cellType, srcMetadata1.layout, extent, srcMetadata1.crs, newProjBounds1)

        // 这里LayerMetadata直接使用reprojectRdd1的，尽管SpatialKey有负值也不影响
        val newCoverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (reprojFilter1.map(t => {
          (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, time1), band1), t._2)
        }), newMetadata1)

        // 这里是coverage2开始进行重投影
        val srcBounds2: Bounds[SpaceTimeKey] = coverage2._2.bounds
        val newBounds2: Bounds[SpatialKey] = Bounds(SpatialKey(srcBounds2.get.minKey.spatialKey._1, srcBounds2.get.minKey.spatialKey._2), SpatialKey(srcBounds2.get.maxKey.spatialKey._1, srcBounds2.get.maxKey.spatialKey._2))
        val rasterMetaData2: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage2._2.cellType, coverage2._2.layout, coverage2._2.extent, coverage2._2.crs, newBounds2)
        val coverage2Rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = MultibandTileLayerRDD(coverage2tileRDD, rasterMetaData2)


        var coverage2tileLayerRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = coverage2Rdd
        // 如果flag=2， 代表第二张影像的分辨率较低
        if (flag == 2) {
          val tileRatio2: Int = (math.log(resoRatio) / math.log(2)).toInt
          if (tileRatio2 != 0) {
            // 对影像进行瓦片大小重切分
            val newTileSize2: Int = 256 / math.pow(2, tileRatio2).toInt
            val coverage2Retiled: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = coverage2Rdd.regrid(newTileSize2)
            // 进行裁剪
            val cropExtent2: Extent = extent.reproject(crs, coverage2Retiled.metadata.crs)
            coverage2tileLayerRdd = coverage2Retiled.crop(cropExtent2)
          }
        }

        val (_, reprojectedRdd2): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          coverage2tileLayerRdd.reproject(crs, ld)

        val reprojFilter2: RDD[(SpatialKey, MultibandTile)] = reprojectedRdd2.filter(layer => {
          val key: SpatialKey = layer._1
          val extentR: Extent = ld.mapTransform(key)
          extentR.xmin >= extent.xmin && extentR.xmax <= extent.xmax && extentR.ymin >= extent.ymin && extentR.ymax <= extent.ymax
        })

        // metadata需要添加time
        val srcMetadata2: TileLayerMetadata[SpatialKey] = reprojectedRdd2.metadata
        val srcProjBounds2: Bounds[SpatialKey] = srcMetadata2.bounds
        val newProjBounds2: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(srcProjBounds2.get.minKey._1, srcProjBounds2.get.minKey._2, time2), SpaceTimeKey(srcProjBounds2.get.maxKey._1, srcProjBounds2.get.maxKey._2, time2))
        val newMetadata2: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(srcMetadata2.cellType, srcMetadata2.layout, extent, srcMetadata2.crs, newProjBounds2)

        // 这里LayerMetadata直接使用reprojectRdd2的，尽管SpatialKey有负值也不影响
        val newCoverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (reprojFilter2.map(t => {
          (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, time2), band2), t._2)
        }), newMetadata2)


        (newCoverage1, newCoverage2)
      }
      else {
        (coverage1, coverage2)
      }
    }
    else {
      (coverage1, coverage2)
    }
  }


  // TODO lrx: ！！！这里要注意一下数据类型！！！
  // Geotrellis的MultibandTile必须要求数据类型一致
  // TODO lrx: 检查TileLayerMetadata的CellType会不会影像原来的TileLayerRDD重投影后的数据类型
  def coverageTemplate(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), func: (Tile, Tile) => Tile): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    def funcMulti(multibandTile1: MultibandTile, multibandTile2: MultibandTile): MultibandTile = {
      val bands1: Vector[Tile] = multibandTile1.bands
      val bands2: Vector[Tile] = multibandTile2.bands
      val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
      for (i <- bands1.indices) {
        bandsFunc.append(func(bands1(i), bands2(i)))
      }
      MultibandTile(bandsFunc)
    }

    var cellType: CellType = null
    val resampleTime: Long = Instant.now.getEpochSecond
    val (coverage1Reprojected, coverage2Reprojected) = checkProjResoExtent(coverage1, coverage2)
    val bandList1: mutable.ListBuffer[String] = coverage1Reprojected._1.first()._1.measurementName
    val bandList2: mutable.ListBuffer[String] = coverage2Reprojected._1.first()._1.measurementName
    if (bandList1.length == bandList2.length) {
      val coverage1NoTime: RDD[(SpatialKey, MultibandTile)] = coverage1Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
      val coverage2NoTime: RDD[(SpatialKey, MultibandTile)] = coverage2Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
      val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1NoTime.join(coverage2NoTime)
      if (bandList1.diff(bandList2).isEmpty && bandList2.diff(bandList1).isEmpty) {
        val indexMap: Map[String, Int] = bandList2.zipWithIndex.toMap
        val indices: mutable.ListBuffer[Int] = bandList1.map(indexMap)
        (rdd.map(t => {
          val bands1: Vector[Tile] = t._2._1.bands
          val bands2: Vector[Tile] = t._2._2.bands
          cellType = func(bands1.head, bands2(indices.head)).cellType
          val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
          for (i <- bands1.indices) {
            bandsFunc.append(func(bands1(i), bands2(indices(i))))
          }
          (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), MultibandTile(bandsFunc))
        }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
      }
      else {
        cellType = funcMulti(coverage1Reprojected._1.first()._2, coverage2Reprojected._1.first()._2).cellType
        (rdd.map(t => {
          (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), funcMulti(t._2._1, t._2._2))
        }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
      }
    }
    else {
      throw new IllegalArgumentException("Error: 波段数量不相等")
    }
  }

  def coverageTemplate(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), func: Tile => Tile): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    def funcMulti(multibandTile: MultibandTile): MultibandTile = {
      val bands: Vector[Tile] = multibandTile.bands
      val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
      for (i <- bands.indices) {
        bandsFunc.append(func(bands(i)))
      }
      MultibandTile(bandsFunc)
    }

    val cellType: CellType = coverage._1.first()._2.cellType

    (coverage._1.map(t => (t._1, funcMulti(t._2))), TileLayerMetadata(cellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  }

  //获取指定位置的tile
  def getTileFromCoverage(coverage: Map[SpaceTimeBandKey, MultibandTile]
                          , spaceTimeBandKey: SpaceTimeBandKey): Option[MultibandTile] = {
    coverage.get(spaceTimeBandKey)
  }

  def focalMethods(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                   focalFunc: (Tile, Neighborhood, Option[GridBounds[Int]], TargetCell) => Tile, radius: Int): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    var neighborhood: Neighborhood = null
    kernelType match {
      case "square" => {
        neighborhood = focal.Square(radius)
      }
      case "circle" => {
        neighborhood = focal.Circle(radius)
      }
    }

    //根据当前tile的col和row，查询相邻瓦块，查询对应的位置的值进行填充，没有的视为0
    val coverage1 = coverage.collect().toMap

    val coverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {
      val col = t._1.spaceTimeKey.col
      val row = t._1.spaceTimeKey.row
      val time0 = t._1.spaceTimeKey.time

      val cols = t._2.cols
      val rows = t._2.rows

      var arrayBuffer: mutable.ArrayBuffer[Tile] = new mutable.ArrayBuffer[Tile] {}
      for (index <- t._2.bands.indices) {
        val arrBuffer: Array[Double] = Array.ofDim[Double]((cols + 2 * radius) * (rows + 2 * radius))
        //arr转换为Tile，并拷贝原tile数据
        val tilePadded: Tile = ArrayTile(arrBuffer, cols + 2 * radius, rows + 2 * radius).convert(t._2.cellType)
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            tilePadded.mutable.setDouble(i + radius, j + radius, t._2.bands(index).getDouble(i, j))
          }
        }
        //填充八邻域数据及自身数据，使用mutable进行性能优化
        //左上
        var tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row - 1, time0), t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, 0)
            }
          }
        }
        //上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, 0)
            }
          }
        }

        //右上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, 0)
            }
          }
        }
        //左
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, 0)
            }
          }
        }

        //右
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, 0)
            }
          }
        }

        //左下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, 0)
            }
          }
        }

        //下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, 0)
            }
          }
        }
        //右下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, 0)
            }
          }
        }

        //运算
        val tilePaddedRes: Tile = focalFunc(tilePadded, neighborhood, None, focal.TargetCell.All)


        //将tilePaddedRes 中的值转移进tile
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            if (!isNoData(t._2.bands(index).getDouble(i, j)))
              t._2.bands(index).mutable.setDouble(i, j, tilePaddedRes.getDouble(i + radius, j + radius))
            else
              t._2.bands(index).mutable.setDouble(i, j, Double.NaN)
          }
        }
      }
      (t._1, t._2)
    })
    (coverageRdd, coverage._2)
  }
}

//取余运算
object Mod extends LocalTileBinaryOp {
  def combine(z1: Int, z2: Int): Int = {
    if (isNoData(z1) || isNoData(z2)) NODATA
    else z1 % z2
  }

  override def combine(z1: Double, z2: Double): Double = {
    if (isNoData(z1) || isNoData(z2)) NODATA
    else z1 % z2
  }
}

trait ModMethods extends MethodExtensions[Tile] {
  /** Mod a constant Int value to each cell. */
  def localMod(i: Int): Tile = Mod(self, i)

  /** Mod a constant Int value to each cell. */
  def %(i: Int): Tile = localMod(i)

  /** Mod a constant Int value to each cell. */
  def %:(i: Int): Tile = localMod(i)

  /** Mod a constant Double value to each cell. */
  def localMod(d: Double): Tile = Mod(self, d)

  /** Mod a constant Double value to each cell. */
  def %(d: Double): Tile = localMod(d)

  /** Mod a constant Double value to each cell. */
  def %:(d: Double): Tile = localMod(d)

  /** Mod the values of each cell in each raster. */
  def localMod(r: Tile): Tile = Mod(self, r)

  /** Mod the values of each cell in each raster. */
  def %(r: Tile): Tile = localMod(r)

}

object Cbrt extends Serializable {
  def apply(r: Tile): Tile =
    r.dualMap { z: Int => if (isNoData(z)) NODATA else math.cbrt(z).toInt } { z: Double => if (z == Double.NaN) Double.NaN else math.cbrt(z) }
}

object RemapWithoutDefaultValue extends Serializable {
  def apply(r: Tile, m: Map[Int, Double]): Tile = {

    r.dualMap {
      z: Int => if (m.contains(z.toInt)) m(z).toInt else z
    } {
      z: Double => if (m.contains(z.toInt)) m(z.toInt) else z
    }
  }

}

object RemapWithDefaultValue extends Serializable {
  def apply(r: Tile, m: Map[Int, Double], num: Double): Tile = {
    r.dualMap {
      z: Int => if (m.contains(z)) m(z).toInt else num.toInt
    } {
      z: Double => if (m.contains(z.toInt)) m(z.toInt) else num
    }
  }
}


// 交叉熵处理类
object Entropy {
  def calculation(tile: Tile,
                  n: Neighborhood,
                  bounds: Option[GridBounds[Int]],
                  target: TargetCell = TargetCell.All)
  : FocalCalculation[Tile] = {
    n match {
      case Square(ext) =>
        new CellEntropyCalcDouble(tile, n, bounds, ext, target)
      case _ =>
        throw new RuntimeException("这个非正方形的还没写！")
    }
  }

  def apply(tile: Tile,
            n: Neighborhood,
            bounds: Option[GridBounds[Int]],
            target: TargetCell = TargetCell.All)
  : Tile =
    calculation(tile, n, bounds, target).execute()

}


class CellEntropyCalcDouble(r: Tile,
                            n: Neighborhood,
                            bounds: Option[GridBounds[Int]],
                            extent: Int,
                            target: TargetCell)
  extends CellwiseCalculation[Tile](r, n, bounds, target)
    with DoubleArrayTileResult {

  //  println(s"Calc extent = ${extent}")

  // map[灰度值, 出现次数]
  val grayMap = new mutable.HashMap[Int, Int]()
  var currRangeMax = 0


  private final def isIllegalData(v: Int): Boolean = {
    isNoData(v) && (v < 0 || v > 255)
  }


  def add(r: Tile, x: Int, y: Int): Unit = {
    val v: Int = r.get(x, y)
    // assert v isData
    if (isIllegalData(v)) {
      //      assert(false)
    } else if (!grayMap.contains(v)) {
      grayMap.put(v, 1)
      currRangeMax += 1
    } else {
      grayMap(v) += 1
      currRangeMax += 1
    }
  }

  def remove(r: Tile, x: Int, y: Int): Unit = {
    val v: Int = r.get(x, y)
    if (isIllegalData(v)) {
      //      assert(false)
    } else if (grayMap.contains(v) &&
      grayMap(v) > 0 &&
      currRangeMax > 0) {
      grayMap(v) -= 1
      currRangeMax -= 1
      if (grayMap(v).equals(0)) {
        grayMap.remove(v)
      }
    }
  }

  def setValue(x: Int, y: Int): Unit = {
    resultTile.setDouble(x, y,
      if (currRangeMax == 0) NODATA else {

        var sum = 0.0
        for (kv <- grayMap) if (kv._2 > 0) {
          val p: Double = kv._2.toDouble / currRangeMax.toDouble
          sum -= kv._2 * p * math.log10(p) / math.log10(2)
        }
        sum
      })
  }

  def reset(): Unit = {
    grayMap.clear()
    currRangeMax = 0
  }

}
