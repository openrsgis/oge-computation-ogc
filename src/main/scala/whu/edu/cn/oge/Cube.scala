package whu.edu.cn.oge

import geotrellis.layer
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.{Add, Max, Mean, Min, Multiply}
import geotrellis.raster.{ByteConstantNoDataCellType, ColorMap, ColorRamp, DoubleConstantNoDataCellType, MultibandTile, Raster, ShortConstantNoDataCellType, Tile, TileLayout, UByteCellType, UByteConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType}
import geotrellis.raster.render.Png
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector.Extent
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Polygon}
import javafx.scene.paint.Color
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import whu.edu.cn.entity.VisualizationParam
import whu.edu.cn.geocube.application.gdc.RasterCubeFun.{writeResultJson, zonedDateTime2String}
import whu.edu.cn.geocube.application.gdc.gdcCoverage.{getCropExtent, getMultiBandStitchRDD, getSingleStitchRDD, scaleCoverage, stitchRDDWriteFile}
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.vector.GeoObjectRDD
import whu.edu.cn.geocube.core.entity.{GcDimension, QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.io.Output.saveAsGeojson
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.NetcdfUtil.{isAddDimensionSame, rasterRDD2Netcdf}
import whu.edu.cn.geocube.util.{PostgresqlService, TileUtil}

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

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

  def visualizeOnTheFly(rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), visualizationParam: VisualizationParam): Unit = {
    val styledRasterRDD: (RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = addStyles(rasterTileLayerRdd, visualizationParam)
    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)
    val tileLayerRdd: MultibandTileLayerRDD[SpatialKey] = ContextRDD(styledRasterRDD._1.map { x => (x._1.spatialKey, x._2) }, spatialMetadata)
    val stitched: Raster[MultibandTile] = tileLayerRdd.stitch()
    val time = System.currentTimeMillis()
    val sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val date = new Date(time)
    val instantRet = sdf.format(date)
    val outputImagePath = "E:\\LaoK\\data2\\oge\\" + "Coverage_" + instantRet + ".png"
    stitched.tile.renderPng().write(outputImagePath)
  }

  /**
   *
   * @param input    input cube data map
   * @param products 要显示的产品列表
   * @return cube data map
   */
  def visualize(implicit sc: SparkContext, cube: mutable.Map[String, Any], products: String, fileName: String = null) = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val result = mutable.Map[String, Any]()
    val vectorCubeList = new ArrayBuffer[Map[String, Any]]()
    val rasterCubeList = new ArrayBuffer[Map[String, Any]]()
    val rasterList = new ArrayBuffer[Map[String, Any]]()
    val vectorList = new ArrayBuffer[Map[String, Any]]()
    val tableList = new ArrayBuffer[Map[String, Any]]()
    val executorOutputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogeoutput/"
    val productList = products.replace("[", "").replace("]", "").split(",")
    val time = System.currentTimeMillis();
    for (product <- productList) {
      cube(product) match {
        case affectedGeoObjectRdd: GeoObjectRDD => {
          //save vector
          val affectedFeatures = affectedGeoObjectRdd.map(x => x.feature).collect()
          val outputVectorPath = executorOutputDir + "oge_flooded_" + time + ".geojson"
          saveAsGeojson(affectedFeatures, outputVectorPath)
          vectorCubeList.append(Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_flooded_" + time + ".geojson")))
        }
        case changedRdd: RasterRDD => {
          if (product == "Change_Product") {
            val cellType = changedRdd.meta.tileLayerMetadata.cellType
            val srcLayout = changedRdd.meta.tileLayerMetadata.layout
            val srcExtent = changedRdd.meta.tileLayerMetadata.extent
            val srcCrs = changedRdd.meta.tileLayerMetadata.crs
            val srcBounds = Bounds(changedRdd.meta.tileLayerMetadata.bounds.get._1.spatialKey, changedRdd.meta.tileLayerMetadata.bounds.get._2.spatialKey)
            val changedMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, srcBounds)

            val changedStitchRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRdd.rddPrev.map(t => (t._1.spaceTimeKey.spatialKey, t._2)), changedMetaData)
            val stitched = changedStitchRdd.stitch()
            val extentRet = stitched.extent
            val outputRasterPath = executorOutputDir + "oge_flooded_" + time + ".png"
            rasterCubeList.append(Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_flooded_" + time + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
            stitched.tile.renderPng().write(outputRasterPath)
          }
          if (product == "Binarization_Product") {
            val ndwiRdd = changedRdd.rddPrev.groupBy(_._1.spaceTimeKey.instant)
            val ndwiInfo = ndwiRdd.map({ x =>
              //stitch extent-series tiles of each time instant to pngs
              val metadata = changedRdd.meta.tileLayerMetadata
              val layout = metadata.layout
              val crs = metadata.crs
              val instant = x._1
              val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spaceTimeKey.spatialKey, ele._2))
              val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

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
              var accum = 0.0
              tileLayerArray.foreach { x =>
                val tile = x._2
                tile.foreachDouble { x =>
                  if (x == 255.0) accum += x
                }
              }
              val sdf = new SimpleDateFormat("yyyy-MM-dd");
              val str = sdf.format(instant);
              System.out.println(str);
              //val outputRasterPath = executorOutputDir + "ndwi_" + str + ".png"
              val extentRet = stitched.extent
              //rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "ndwi_" + str + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
              val stitchedPng: Png = stitched.tile.renderPng(colorRamp)
              //stitched.tile.renderPng(colorRamp).write(outputRasterPath)
              //generate ndwi thematic product
              val outputTiffPath = executorOutputDir + "_ndwi_" + instant + ".TIF"
              GeoTiff(stitched, crs).write(outputTiffPath)
              (stitchedPng, str, extentRet)
            })
            ndwiInfo.collect().foreach(t => {
              val str = t._2
              val extentRet = t._3
              val outputRasterPath = executorOutputDir + "ndwi_" + str + "_" + time + ".png"
              t._1.write(outputRasterPath)
              rasterCubeList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "ndwi_" + str + "_" + time + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
            })
          }
          if (product == "LC08_L1TP_ARD_EO") {
            val ndwiRdd = changedRdd.rddPrev.groupBy(_._1.spaceTimeKey.instant)
            val ndwiInfo = ndwiRdd.map({ x =>
              //stitch extent-series tiles of each time instant to pngs
              val metadata = changedRdd.meta.tileLayerMetadata
              val layout = metadata.layout
              val crs = metadata.crs
              val instant = x._1
              val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spaceTimeKey.spatialKey, ele._2))
              val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

              val sdf = new SimpleDateFormat("yyyy-MM-dd");
              val str = sdf.format(instant);
              System.out.println(str);
              val outputTiffPath = executorOutputDir + str + "_ndwi_" + instant + ".TIF"
              GeoTiff(stitched, crs).write(outputTiffPath)
              str
            })
            ndwiInfo.count()
          }
        }
      }
    }
    result += ("vectorCube" -> vectorCubeList)
    result += ("rasterCube" -> rasterCubeList)
    result += ("raster" -> rasterCubeList)
    result += ("vector" -> rasterCubeList)
    result += ("table" -> rasterCubeList)
    val jsonStr: String = write(result)
    val writeFile = new File(fileName)
    val writer = new BufferedWriter(new FileWriter(writeFile))
    writer.write(jsonStr)
    writer.close()
  }
}