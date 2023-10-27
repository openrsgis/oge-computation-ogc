package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{ByteConstantNoDataCellType, CellType, DoubleConstantNoDataCellType, MultibandTile, Raster, ShortConstantNoDataCellType, Tile, TileLayout, UByteCellType, UByteConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import io.minio.{MinioClient, UploadObjectArgs}
import javafx.scene.paint.Color
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import redis.clients.jedis.Jedis
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.config.GlobalConfig.RedisConf.REDIS_CACHE_TTL
import whu.edu.cn.entity.{BatchParam, CoverageMetadata, RawTile, VisualizationParam}
import whu.edu.cn.geocube.application.gdc.RasterCubeFun.{writeResultJson, zonedDateTime2String}
import whu.edu.cn.geocube.application.gdc.gdcCoverage._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{GcDimension, GcMeasurement, GcProduct, QueryParams, RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.NetcdfUtil.{isAddDimensionSame, rasterRDD2Netcdf}
import whu.edu.cn.geocube.util.PostgresqlService
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage
import whu.edu.cn.util.TileSerializerCoverageUtil.deserializeTileData
import whu.edu.cn.util.{JedisUtil, MinIOUtil}

import java.text.SimpleDateFormat
import java.time.{ZoneOffset, ZonedDateTime}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{max, min}

object Cube {
  /**
   * 根据提供的数据列表、时间范围、空间范围生成RDD，用字典形式记录
   *
   * @param productList 需要生成Cube的产品列表
   * @param startTime   起始时间
   * @param endTime     结束时间
   * @param geom        空间范围
   * @return 返回字典类型 Map[String, Any] cube data map
   */
  def load(sc: SparkContext, cubeName: String, extent: String = null, dateTime: String = null,
           queryParams: QueryParams = new QueryParams(), scaleSize: Array[java.lang.Integer] = null, scaleAxes: Array[java.lang.Double] = null,
           scaleFactor: java.lang.Double = null): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    if (queryParams.getCubeId == "" || queryParams.getRasterProductNames.isEmpty) {
      // 这种情况发生在直接调用处理时 是没有查询参数的
      //TODO  比较时间和空间范围
      val postgresqlService = new PostgresqlService
      val cubeId = postgresqlService.getCubeIdByCubeName(cubeName)
      val productName = postgresqlService.getProductNameByCubeName(postgresqlService.getMeasurementsProductViewName(cubeName))
      queryParams.setCubeId(cubeId)
      queryParams.setRasterProductName(productName)
      // TODO 如果extent和dateTime为null 应该从数据库中获取整个的范围
      if (extent != null) {
        val extentArray: Array[String] = extent.replace("[", "").replace("]", "").split(",")
        queryParams.setExtent(extentArray(0).toDouble, extentArray(1).toDouble, extentArray(2).toDouble, extentArray(3).toDouble)
      }
      if (dateTime != null) {
        val timeArray: Array[String] = dateTime.replace("[", "").replace("]", "").split(",")
        queryParams.setTime(timeArray.head, timeArray(1))
      }
    }
    var rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, queryParams)
    if (scaleSize != null) {
      // 多少列
      val layoutCols = rasterTileLayerRdd._2.tileLayerMetadata.layout.tileLayout.layoutCols
      // 多少行
      val layoutRows = rasterTileLayerRdd._2.tileLayerMetadata.layout.tileLayout.layoutRows
      val tileCols = rasterTileLayerRdd._2.tileLayerMetadata.layout.tileLayout.tileCols
      val tileRows = rasterTileLayerRdd._2.tileLayerMetadata.layout.tileLayout.tileRows
      scaleSize(0) = scala.math.round(tileCols * (scaleSize(0) / (layoutCols * tileCols))).toInt
      scaleSize(1) = scala.math.round(tileRows * (scaleSize(1) / (layoutRows * tileRows))).toInt
    }
    if (!(scaleSize == null && scaleAxes == null && scaleFactor == null)) {
      rasterTileLayerRdd = (rasterTileLayerRdd._1.map(rdd => {
        val scaleExtent = scaleCoverage(rdd._2, scaleSize, scaleAxes, scaleFactor)
        val scaleTile = rdd._2.resample(scaleExtent._1, scaleExtent._2)
        (rdd._1, scaleTile)
      }), rasterTileLayerRdd._2)
    }
    rasterTileLayerRdd
  }

  def calculateAlongDimensionWithString(input: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                                        dimensionName: String, dimensionMembersStr: String, method: String,
                                        outputDimensionMember: String = null, isOutput: Boolean = false): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    var dimensionMembers: Array[String] = null
    if (dimensionMembersStr != null) {
      dimensionMembers = dimensionMembersStr
        .stripPrefix("[").stripSuffix("]")
        .split(",")
        .map(_.trim.stripPrefix("\"").stripSuffix("\""))
      calculateAlongDimension(input, dimensionName, dimensionMembers, method, outputDimensionMember, isOutput)
    } else {
      null
    }
  }

  def calculateAlongDimension(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                              dimensionName: String, dimensionMembers: Array[String], method: String, outputDimensionMember: String = null, isOutput: Boolean = false)
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    var newDimensionName: String = null
    if (outputDimensionMember == null) {
      newDimensionName = method
    } else {
      newDimensionName = outputDimensionMember
    }
    if (dimensionName.equals("time") || dimensionName.equals("phenomenonTime")) {
      val rasterTileRddAlongTime = data._1.map(v => ((v._1.spaceTimeKey.spatialKey, v._1.measurementName, v._1.additionalDimensions.toSeq), (v._1.spaceTimeKey.time, v._2, v._1)))
      val rasterTileSelectDimensionMember = rasterTileRddAlongTime.filter {
        case (_, (time, _, _)) => {
          zonedDateTime2String(time).equals(dimensionMembers(0)) || zonedDateTime2String(time).equals(dimensionMembers(1))
        }
      }
      val rasterReadyToCalculate: RDD[((SpatialKey, String, Seq[GcDimension]), (Tile, Tile, SpaceTimeBandKey))] = rasterTileSelectDimensionMember.groupBy(_._1).mapValues(iterable => {
        val tileSeq: Seq[(ZonedDateTime, Tile, SpaceTimeBandKey)] = iterable.map { case (_, v) => v }.toSeq
        var tile1: Tile = null
        var tile2: Tile = null
        for (element <- tileSeq) {
          if (zonedDateTime2String(element._1).equals(dimensionMembers(0))) {
            tile1 = element._2
          }
          if (zonedDateTime2String(element._1).equals(dimensionMembers(1))) {
            tile2 = element._2
          }
        }
        (tile1, tile2, tileSeq.head._3)
      })
      var maxMin: ((Double, Double), (Double, Double)) = ((0.0, 0.0), (0.0, 0.0))
      //      if (method == "normalize") {
      //        maxMin = rasterReadyToCalculate.map { rdd: ((SpatialKey, String, Seq[GcDimension]), (Tile, Tile, SpaceTimeBandKey)) =>
      //          // 计算第一类 Tile 的最小值和最大值
      //          val (minValue1, maxValue1) = rdd._2._1.findMinMaxDouble
      //          // 计算第二类 Tile 的最小值和最大值
      //          val (minValue2, maxValue2) = rdd._2._2.findMinMaxDouble
      //          // 返回结果为两个元组，分别表示第一类 Tile 和第二类 Tile 的最小值和最大值
      //          ((minValue1, maxValue1), (minValue2, maxValue2))
      //        }.filter(x => !x._1._1.isNaN() && !x._1._2.isNaN() && !x._2._1.isNaN() && !x._2._2.isNaN())
      //          .reduce((x, y) =>
      //            ((Math.min(x._1._1, y._1._1), Math.max(x._1._2, y._1._2)), (Math.min(x._2._1, y._2._1), Math.max(x._2._2, y._2._2)))
      //          )
      //      }
      val newTime = ZonedDateTime.now.toInstant.toEpochMilli
      val resultRdd = (rasterReadyToCalculate.map { v =>
        v._2._3.setSpaceTimeKey(new SpaceTimeKey(v._2._3._spaceTimeKey.col, v._2._3._spaceTimeKey.row, newTime))
        (v._2._3, mathCalculate(v._2._1, v._2._2, method, maxMin))
      }, data._2)
      val preTileLayerMetadata = data._2.tileLayerMetadata
      val newBounds = Bounds(preTileLayerMetadata.bounds.get.minKey,
        new layer.SpaceTimeKey(preTileLayerMetadata.bounds.get.maxKey.spatialKey.col, preTileLayerMetadata.bounds.get.maxKey.spatialKey.row, newTime))
      resultRdd._2.setTileLayerMetadata(new TileLayerMetadata[SpaceTimeKey](preTileLayerMetadata.cellType, preTileLayerMetadata.layout, preTileLayerMetadata.extent, preTileLayerMetadata.crs, newBounds))
      if (isOutput) {
        exportFile(resultRdd)
      }
      return resultRdd

    }
    if (dimensionName.contains("band") || dimensionName.contains("measurement")) {
      val rasterTileRddAlongMeasurement: RDD[((SpatialKey, ZonedDateTime, Seq[GcDimension]), (String, Tile, SpaceTimeBandKey))] =
        data._1.map(v => ((v._1.spaceTimeKey.spatialKey, v._1.spaceTimeKey.time, v._1.additionalDimensions.toSeq), (v._1.measurementName, v._2, v._1)))
      val rasterTileSelectDimensionMember = rasterTileRddAlongMeasurement.filter {
        case (_, (measurement, _, _)) => {
          measurement.equals(dimensionMembers(0)) || measurement.equals(dimensionMembers(1))
        }
      }
      val rasterReadyToCalculate: RDD[((SpatialKey, ZonedDateTime, Seq[GcDimension]), (Tile, Tile, SpaceTimeBandKey))] = rasterTileSelectDimensionMember.groupBy(_._1).mapValues(iterable => {
        val tileSeq: Seq[(String, Tile, SpaceTimeBandKey)] = iterable.map { case (_, v) => v }.toSeq
        var tile1: Tile = null
        var tile2: Tile = null
        for (element <- tileSeq) {
          if (element._1.equals(dimensionMembers(0))) {
            tile1 = element._2
          }
          if (element._1.equals(dimensionMembers(1))) {
            tile2 = element._2
          }
        }
        (tile1, tile2, tileSeq.head._3)
      })
      var maxMin: ((Double, Double), (Double, Double)) = ((0.0, 0.0), (0.0, 0.0))
      //      if (method == "normalize") {
      //        val maxMinRDD = rasterReadyToCalculate.map { rdd: ((SpatialKey, ZonedDateTime, Seq[GcDimension]), (Tile, Tile, SpaceTimeBandKey)) =>
      //          // 计算第一类 Tile 的最小值和最大值
      //          val (minValue1, maxValue1) = rdd._2._1.findMinMaxDouble
      //          // 计算第二类 Tile 的最小值和最大值
      //          val (minValue2, maxValue2) = rdd._2._2.findMinMaxDouble
      //          // 返回结果为两个元组，分别表示第一类 Tile 和第二类 Tile 的最小值和最大值
      //          ((minValue1, maxValue1), (minValue2, maxValue2))
      //        }.filter(x => !x._1._1.isNaN() && !x._1._2.isNaN() && !x._2._1.isNaN() && !x._2._2.isNaN())
      ////        if (!maxMinRDD.isEmpty()) {
      ////          maxMin = maxMinRDD.reduce((x, y) =>
      ////            ((Math.min(x._1._1, y._1._1), Math.max(x._1._2, y._1._2)), (Math.min(x._2._1, y._2._1), Math.max(x._2._2, y._2._2)))
      ////          )
      ////        }
      //        maxMin = maxMinRDD.reduce((x, y) =>
      //          ((Math.min(x._1._1, y._1._1), Math.max(x._1._2, y._1._2)), (Math.min(x._2._1, y._2._1), Math.max(x._2._2, y._2._2)))
      //        )
      //      }

      val resultRdd = (rasterReadyToCalculate.map { v =>
        v._2._3.setMeasurementName(newDimensionName)
        (v._2._3, mathCalculate(v._2._1, v._2._2, method, maxMin))
      }, data._2)
      resultRdd._2.setMeasurementNames(Array(newDimensionName))
      if (isOutput) {
        exportFile(resultRdd)
      }
      return resultRdd
    }

    return null
  }

  def aggregateAlongDimension(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                              dimensionName: String, method: String, outputDimensionMember: String = null, isOutput: Boolean = false)
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    var newDimension: String = null
    if (outputDimensionMember == null) {
      newDimension = method
    } else {
      newDimension = outputDimensionMember
    }
    if (dimensionName.equals("time") || dimensionName.equals("phenomenonTime")) {
      val rasterTileRddAlongTime: RDD[((SpatialKey, String, Seq[GcDimension]), (ZonedDateTime, Tile, SpaceTimeBandKey))] = data._1.map(v => ((v._1.spaceTimeKey.spatialKey, v._1.measurementName, v._1.additionalDimensions.toSeq), (v._1.spaceTimeKey.time, v._2, v._1)))

      val rasterReadyToCalculate: RDD[((SpatialKey, String, Seq[GcDimension]), Seq[Tile])] = rasterTileRddAlongTime.groupBy(_._1).mapValues(iterable => {
        iterable.map { case (_, v) => v._2 }.toSeq
      })
      val newTime = ZonedDateTime.now.toInstant.toEpochMilli
      val resultRdd = (rasterReadyToCalculate.map { v =>
        val spaceTimeBandKey = SpaceTimeBandKey(new SpaceTimeKey(v._1._1.col, v._1._1.row, newTime), v._1._2, v._1._3.toArray)
        (spaceTimeBandKey, aggregateCalculate(v._2, method))
      }, data._2)
      val preTileLayerMetadata = data._2.tileLayerMetadata
      val newBounds = Bounds(new layer.SpaceTimeKey(preTileLayerMetadata.bounds.get.minKey.spatialKey.col, preTileLayerMetadata.bounds.get.minKey.spatialKey.row, newTime),
        new layer.SpaceTimeKey(preTileLayerMetadata.bounds.get.maxKey.spatialKey.col, preTileLayerMetadata.bounds.get.maxKey.spatialKey.row, newTime))
      resultRdd._2.setTileLayerMetadata(new TileLayerMetadata[SpaceTimeKey](preTileLayerMetadata.cellType, preTileLayerMetadata.layout, preTileLayerMetadata.extent, preTileLayerMetadata.crs, newBounds))
      if (isOutput) {
        exportFile(resultRdd)
      }
      return resultRdd
    }
    else if (dimensionName.contains("band") || dimensionName.contains("measurement")) {
      val rasterTileRddAlongMeasurement: RDD[((SpatialKey, ZonedDateTime, Seq[GcDimension]), (String, Tile, SpaceTimeBandKey))] =
        data._1.map(v => ((v._1.spaceTimeKey.spatialKey, v._1.spaceTimeKey.time, v._1.additionalDimensions.toSeq), (v._1.measurementName, v._2, v._1)))
      val rasterReadyToCalculate: RDD[((SpatialKey, ZonedDateTime, Seq[GcDimension]), Seq[Tile])] = rasterTileRddAlongMeasurement.groupBy(_._1).mapValues(iterable => {
        iterable.map { case (_, v) => v._2 }.toSeq
      })

      val resultRdd = (rasterReadyToCalculate.map { v =>
        val spaceTimeBandKey = SpaceTimeBandKey(new SpaceTimeKey(v._1._1.col, v._1._1.row, v._1._2.toInstant.toEpochMilli), newDimension, v._1._3.toArray)
        (spaceTimeBandKey, aggregateCalculate(v._2, method))
      }, data._2)
      resultRdd._2.setMeasurementNames(Array(newDimension))
      if (isOutput) {
        exportFile(resultRdd)
      }
      return resultRdd
    } else {
      null
    }
  }

  /**
   *
   * @param tile1  the first tile
   * @param tile2  the second tile
   * @param method the calculate method
   * @param maxMin (will observe?)
   * @return the calculated tile
   */
  def mathCalculate(tile1: Tile, tile2: Tile, method: String, maxMin: ((Double, Double), (Double, Double))): Tile = {
    var tileType = tile1.cellType
    method match {
      case "subtract" =>
        if (tileType == UByteCellType || tileType == UByteConstantNoDataCellType)
          tileType = ByteConstantNoDataCellType
        if (tileType == UShortCellType || tileType == UShortConstantNoDataCellType)
          tileType = ShortConstantNoDataCellType
      case "divide" =>
        tileType = DoubleConstantNoDataCellType
      case "normalize" =>
        tileType = DoubleConstantNoDataCellType
    }
    if ((!tile1.isNoDataTile) && (!tile2.isNoDataTile)) {
      method match {
        case "add" =>
          tile1.localAdd(tile2)
        case "subtract" =>
          tile1.convert(tileType).localSubtract(tile2.convert(tileType))
        case "divide" =>
          val tileDouble1 = tile1.convert(DoubleConstantNoDataCellType)
          val tileDouble2 = tile2.convert(DoubleConstantNoDataCellType)
          tileDouble1.localDivide(tileDouble2)
        case "normalize" =>
          //          val tileN1 = normalizeOneTile(tile1, maxMin._1)
          //          val tileN2 = normalizeOneTile(tile2, maxMin._2)
          val tileN1 = tile1.convert(DoubleConstantNoDataCellType)
          val tileN2 = tile2.convert(DoubleConstantNoDataCellType)
          val subtract = tileN1.localSubtract(tileN2)
          val add = tileN1.localAdd(tileN2)
          val normalize = subtract.localDivide(add)
          normalize
        case _ => tile1
      }
    } else { // return the no data tile
      if (tile1.isNoDataTile) {
        if (tile1.cellType != tileType) {
          tile1.convert(tileType)
        } else {
          tile1
        }
      } else {
        if (tile2.cellType != tileType) {
          tile2.convert(tileType)
        } else {
          tile2
        }
      }
    }
  }

  /**
   * aggregate along the dimension
   *
   * @param tileSeq the tile seq
   * @param method  the aggregate method
   * @return calculated tile
   */
  def aggregateCalculate(tileSeq: Seq[Tile], method: String): Tile = {
    method match {
      case "max" =>
        Max(tileSeq.map({ x => x }))
      case "min" =>
        Min(tileSeq.map({ x => x }))
      case "mean" =>
        Mean(tileSeq)
      case _ => tileSeq.head
    }
  }

  def normalizeOneTile(tile: Tile): Tile = {
    val tileD = tile.convert(DoubleConstantNoDataCellType)
    val (minValue1, maxValue1) = tileD.findMinMaxDouble
    val normalizedTile: Tile = (tileD - minValue1) / (maxValue1 - minValue1)
    normalizedTile
  }

  def normalizeOneTile(tile: Tile, maxMin: (Double, Double)): Tile = {
    val tileD = tile.convert(DoubleConstantNoDataCellType)
    val normalizedTile: Tile = (tileD - maxMin._1) / (maxMin._2 - maxMin._1)
    normalizedTile
  }

  def normalize(input: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                dimensionName: String, dimensionMembers: Array[String], outputDimensionMember: String = "normalized")
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    calculateAlongDimension(input, dimensionName, dimensionMembers, "normalize", outputDimensionMember)
  }

  def add(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
          dimensionName: String, dimensionMembers: Array[String], outputDimensionMember: String = "add")
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    calculateAlongDimension(data, dimensionName, dimensionMembers, "add", outputDimensionMember)
  }

  def subtract(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
               dimensionName: String, dimensionMembers: Array[String], outputDimensionMember: String = "subtract")
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    calculateAlongDimension(data, dimensionName, dimensionMembers, "subtract", outputDimensionMember)
  }

  def divide(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
             dimensionName: String, dimensionMembers: Array[String], outputDimensionMember: String = "divide")
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    calculateAlongDimension(data, dimensionName, dimensionMembers, "divide", outputDimensionMember)
  }

  def exportFile(rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), extent: ArrayBuffer[Double] = null,
                 queryParams: QueryParams = null, outputDir: String = "E:\\LaoK\\data2\\GDC_API\\", imageFormat: String = "tif"): Unit = {
    val crs = rasterTileLayerRdd._2.tileLayerMetadata.crs
    if (imageFormat.equals("netcdf")) {
      rasterRDD2Netcdf(rasterTileLayerRdd._2._measurementNames, rasterTileLayerRdd, queryParams, null, null, null, outputDir)
    }
    // 多余维度只取第一个
    //    val dimensionsRDD = rasterTileLayerRdd._1.map(v => (v._1.spaceTimeKey.time, v._1.measurementName, v._1.additionalDimensions))
    val minTime = rasterTileLayerRdd._2.tileLayerMetadata.bounds.get.minKey.time.toInstant.toEpochMilli
    //    val minTime = dimensionsRDD.map(_._1).distinct().map(zonedDateTime => {
    //      zonedDateTime.toInstant.toEpochMilli
    //    }).sortBy(identity).first()
    var rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
      rdd => {
        rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == minTime && isAddDimensionSame(rdd._1.additionalDimensions)
      }
    }, rasterTileLayerRdd._2)

    if (rasterTileLayerRddFirstDim._2._measurementNames.size > 1) {
      var stitchedMultiBandTile = getMultiBandStitchRDD(rasterTileLayerRddFirstDim._2.measurementNames, rasterTileLayerRddFirstDim)
      var finalExtent = stitchedMultiBandTile.extent
      if (extent != null) {
        val cropExtent = getCropExtent(extent, stitchedMultiBandTile.extent, stitchedMultiBandTile)
        stitchedMultiBandTile = stitchedMultiBandTile.crop(cropExtent._1, cropExtent._2, cropExtent._3, cropExtent._4)
        finalExtent = cropExtent._5
      }
      val outputPath = stitchRDDWriteFile(stitchedMultiBandTile, outputDir, imageFormat, stitchedMultiBandTile.extent, crs)
      writeResultJson(outputPath, outputDir, extent.mkString(","))
    } else {
      var stitchedSingleTile = getSingleStitchRDD(rasterTileLayerRddFirstDim)
      var finalExtent = stitchedSingleTile.extent
      if (extent != null) {
        val cropExtent = getCropExtent(extent, stitchedSingleTile.extent, stitchedSingleTile)
        stitchedSingleTile = stitchedSingleTile.crop(cropExtent._1, cropExtent._2, cropExtent._3, cropExtent._4)
        finalExtent = cropExtent._5
      }
      val outputPath = stitchRDDWriteFile(stitchedSingleTile.tile, outputDir, imageFormat, finalExtent, crs)
      if (extent == null) {
        writeResultJson(outputPath, outputDir, "")
      } else {
        writeResultJson(outputPath, outputDir, extent.mkString(","))
      }
    }
  }


  def binarization(input: mutable.Map[String, Any], product: String, name: String, threshold: Double): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val binarizationRdd: RasterRDD = new RasterRDD(rasterRdd.rddPrev.map(t => (t._1, t._2.mapDouble(pixel => {
          if (pixel > threshold) 255.0
          else if (pixel >= -1) 0.0
          else Double.NaN
        }))), rasterRdd.meta)
        input += (name -> binarizationRdd)
        input
      }
    }
  }

  def addStyles(rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), visualizationParam: VisualizationParam):
  (RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // the min time
    val minTime = rasterTileLayerRdd._2.tileLayerMetadata.bounds.get.minKey.time.toInstant.toEpochMilli
    // get the first image
    val rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
      rdd => {
        rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == minTime && isAddDimensionSame(rdd._1.additionalDimensions)
      }
    }, rasterTileLayerRdd._2)
    var rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRddFirstDim.map(x => (x._1.spaceTimeKey, x._1.measurementName, x._2)).groupBy(_._1).map {
      x => {
        val tilePair = x._2.toArray
        val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
        rasterTileLayerRddFirstDim._2.measurementNames.foreach { measurement => {
          tilePair.foreach { ele =>
            if (ele._2.equals(measurement)) {
              multiBandTiles.append(ele._3)
            }
          }
        }
        }
        (x._1, MultibandTile(multiBandTiles))
      }
    }
    if (rasterTileLayerRddFirstDim._2._measurementNames.size > 1) {
      // multiband ignore
      null
    } else {
      // min的数量
      val minNum: Int = visualizationParam.getMin.length
      // max的数量
      val maxNum: Int = visualizationParam.getMax.length
      if (minNum * maxNum != 0) {
        // 首先找到现有的最小最大值
        val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
          val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
          if (noNaNArray.nonEmpty) {
            (noNaNArray.min, noNaNArray.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
        val minVis: Double = visualizationParam.getMin.headOption.getOrElse(0.0)
        val maxVis: Double = visualizationParam.getMax.headOption.getOrElse(1.0)
        val gainBand: Double = (maxVis - minVis) / (minMaxBand._2 - minMaxBand._1)
        val biasBand: Double = (minMaxBand._2 * minVis - minMaxBand._1 * maxVis) / (minMaxBand._2 - minMaxBand._1)
        rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => (t._1, t._2.mapBands((_, tile) => {
          Add(Multiply(tile, gainBand), biasBand)
        })))
      }
      // 如果存在palette
      if (visualizationParam.getPalette.nonEmpty) {
        val paletteVis: List[String] = visualizationParam.getPalette
        val colorVis: List[Color] = paletteVis.map(t => {
          try {
            val color: Color = Color.valueOf(t)
            color
          } catch {
            case e: Exception =>
              throw new IllegalArgumentException(s"输入颜色有误，无法识别$t")
          }
        })
        val colorRGB: List[(Double, Double, Double)] = colorVis.map(t => (t.getRed, t.getGreen, t.getBlue))
        val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
          val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
          if (noNaNArray.nonEmpty) {
            (noNaNArray.min, noNaNArray.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
        val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
        rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => {
          val bandR: Tile = t._2.bands.head.mapDouble(d => {
            var R: Double = 0.0
            for (i <- colorRGB.indices) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                R = colorRGB(i)._1 * 255.0
              }
            }
            R
          })
          val bandG: Tile = t._2.bands.head.mapDouble(d => {
            var G: Double = 0.0
            for (i <- colorRGB.indices) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                G = colorRGB(i)._2 * 255.0
              }
            }
            G
          })
          val bandB: Tile = t._2.bands.head.mapDouble(d => {
            var B: Double = 0.0
            for (i <- colorRGB.indices) {
              if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                B = colorRGB(i)._3 * 255.0
              }
            }
            B
          })
          (t._1, MultibandTile(bandR, bandG, bandB))
        })
      }
      (rasterMultiBandTileRdd, rasterTileLayerRddFirstDim._2.tileLayerMetadata)
    }
  }

  def visualizeOnTheFly(implicit sc: SparkContext, rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), visualizationParam: VisualizationParam): Unit = {
    val styledRasterRDD: (RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = addStyles(rasterTileLayerRdd, visualizationParam)
    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)
    val tileLayerRdd: MultibandTileLayerRDD[SpatialKey] = ContextRDD(styledRasterRDD._1.map { x => (x._1.spatialKey, x._2) }, spatialMetadata)

    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      tileLayerRdd.reproject(tmsCrs, layoutScheme)

    val outputPath: String = "/mnt/storage/on-the-fly"
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
    // Create the writer that we will use to store the tiles in the local catalog.
    val writer: FileLayerWriter = FileLayerWriter(attributeStore)

    if (zoom < Trigger.level) {
      throw new InternalError("内部错误，切分瓦片层级没有前端TMS层级高")
    }

    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      if (z == Trigger.level) {
        val layerId: LayerId = LayerId(Trigger.dagId, z)
        println(layerId)
        // If the layer exists already, delete it out before writing
        if (attributeStore.layerExists(layerId)) {
          //        new FileLayerManager(attributeStore).delete(layerId)
          try {
            writer.overwrite(layerId, rdd)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
        else {
          writer.write(layerId, rdd, ZCurveKeyIndexMethod)
        }
      }
    }

    // 回调服务
    val jsonObject: JSONObject = new JSONObject
    val rasterJsonObject: JSONObject = new JSONObject
    rasterJsonObject.put(Trigger.layerName, "http://oge.whu.edu.cn/api/oge-tms-png/" + Trigger.dagId + "/{z}/{x}/{y}.png")
    jsonObject.put("raster", rasterJsonObject)

    val outJsonObject: JSONObject = new JSONObject
    outJsonObject.put("workID", Trigger.dagId)
    outJsonObject.put("json", jsonObject)

    sendPost(DAG_ROOT_URL + "/deliverUrl", outJsonObject.toJSONString)

    println("outputJSON: ", outJsonObject.toJSONString)
    val zIndexStrArray: mutable.ArrayBuffer[String] = Trigger.zIndexStrArray
    val jedis: Jedis = new JedisUtil().getJedis
    jedis.select(1)
    zIndexStrArray.foreach(zIndexStr => {
      val key: String = Trigger.dagId + ":solvedTile:" + Trigger.level + zIndexStr
      jedis.sadd(key, "cached")
      jedis.expire(key, REDIS_CACHE_TTL)
    })
    jedis.close()


    // 清空list
    Trigger.optimizedDagMap.clear()
    Trigger.coverageCollectionMetadata.clear()
    Trigger.lazyFunc.clear()
    Trigger.coverageCollectionRddList.clear()
    Trigger.coverageRddList.clear()
    Trigger.zIndexStrArray.clear()
    JsonToArg.dagMap.clear()
    //    // TODO lrx: 以下为未检验
    Trigger.tableRddList.clear()
    Trigger.kernelRddList.clear()
    Trigger.featureRddList.clear()
    Trigger.cubeRDDList.clear()
    Trigger.cubeLoad.clear()

    if (sc.master.contains("local")) {
      whu.edu.cn.debug.CoverageDubug.makeTIFF(reprojected, "cube")
    }
  }

  def visualizeBatch(implicit sc: SparkContext, rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), batchParam: BatchParam, dagId: String) : Unit = {
    val rasterArray: Array[(SpatialKey, Tile)] = rasterTileLayerRdd._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(rasterArray)
    val stitchedTile: Raster[Tile] = Raster(tile, rasterTileLayerRdd._2.tileLayerMetadata.extent)
    var reprojectTile: Raster[Tile] = stitchedTile.reproject(rasterTileLayerRdd._2.tileLayerMetadata.crs, CRS.fromName("EPSG:3857"))
    val resample: Raster[Tile] = reprojectTile.resample(math.max((reprojectTile.cellSize.width * reprojectTile.cols / batchParam.getScale).toInt, 1), math.max((reprojectTile.cellSize.height * reprojectTile.rows / batchParam.getScale).toInt, 1))
    reprojectTile = resample.reproject(CRS.fromName("EPSG:3857"), batchParam.getCrs)

    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.tiff"
    val minIOUtil = MinIOUtil
    val client: MinioClient = minIOUtil.getMinioClient
    GeoTiff(reprojectTile, batchParam.getCrs).write(saveFilePath)

    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
    client.uploadObject(UploadObjectArgs.builder.bucket("oge-user").`object`(path).filename(saveFilePath).build())
  }

  def cubeBuild(implicit sc: SparkContext, coverageId: String, productKey: String, level: Int): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val time1 = System.currentTimeMillis()
    val zIndexStrArray: mutable.ArrayBuffer[String] = Trigger.zIndexStrArray
    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    }
    var union: Extent = Trigger.windowExtent
    if(union == null){
      union = Extent(metaList.head.getGeom.getEnvelopeInternal)
    }
    val queryGeometry = metaList.head.getGeom
    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .mapPartitions(par => {
        val minIOUtil = MinIOUtil
        val client: MinioClient = minIOUtil.getMinioClient
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val tiles: mutable.ArrayBuffer[RawTile] = tileQuery(client, level, t, union, queryGeometry)
            tiles
          }
          val time2: Long = System.currentTimeMillis()
          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
        result
      }).flatMap(t => t).persist()

    val tileNum: Int = tileRDDFlat.count().toInt
    println("tileNum = " + tileNum)
    val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 16))
    tileRDDFlat.unpersist()
    val rawTileRdd: RDD[RawTile] = tileRDDRePar.mapPartitions(par => {
      val client: MinioClient = MinIOUtil.getMinioClient
      par.map(t => {
        getTileBuf(client, t)

      })
    })

    println(rawTileRdd.first().toString)
    println("Loading data Time: " + (System.currentTimeMillis() - time1))
    val time2 = System.currentTimeMillis()
    val cube = makeCubeRDD(rawTileRdd)
    println("Make RDD Time: " + (System.currentTimeMillis() - time2))
    cube
  }


  def makeCubeRDD(tileRDDReP: RDD[RawTile]): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
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

    val extent: Extent = geotrellis.vector.Extent(extents._1, extents._2, extents._3, extents._4)

    val firstTile: RawTile = tileRDDReP.first()
    val layoutCols: Int = math.max(math.ceil((extents._3 - extents._1 - firstTile.getResolutionCol) / firstTile.getResolutionCol / 256.0).toInt, 1)
    val layoutRows: Int = math.max(math.ceil((extents._4 - extents._2 - firstTile.getResolutionRow) / firstTile.getResolutionRow / 256.0).toInt, 1)
    val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)

    val ld: LayoutDefinition = LayoutDefinition(extent, tl)
    val cellType: CellType = CellType.fromName(firstTile.getDataType.toString)
    val crs: CRS = firstTile.getCrs
    val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))

    val dtype = firstTile.getDataType.toString
    val rasterTileLayerMetadata: RasterTileLayerMetadata[SpaceTimeKey] = RasterTileLayerMetadata(TileLayerMetadata(cellType, ld, extent, crs, bounds), firstTile.getCoverageId)
    val cubeRDD: RDD[(SpaceTimeBandKey, Tile)] = tileRDDReP.map { tile =>
      val rowNum: Int = tile.getSpatialKey.row
      val colNum: Int = tile.getSpatialKey.col
      val spaceTimeKey: SpaceTimeKey = SpaceTimeKey(colNum, rowNum, tile.getTime.toEpochSecond(ZoneOffset.ofHours(0)))
      val Tile: Tile = deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString)
      val bandKey = SpaceTimeBandKey(spaceTimeKey, tile.getCoverageId, null)
      (bandKey, Tile)
    }

    (cubeRDD, rasterTileLayerMetadata)
  }


}