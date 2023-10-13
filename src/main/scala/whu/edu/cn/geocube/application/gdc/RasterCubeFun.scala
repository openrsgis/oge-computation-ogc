package whu.edu.cn.geocube.application.gdc

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer
import geotrellis.layer.{Bounds, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{ByteConstantNoDataCellType, DoubleConstantNoDataCellType, Raster, ShortConstantNoDataCellType, Tile, UByteCellType, UByteConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.application.gdc.gdcCoverage.{getCropExtent, getMultiBandStitchRDD, getSingleStitchRDD, scaleCoverage, stitchRDDWriteFile}
import whu.edu.cn.geocube.core.entity.{GcDimension, QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import geotrellis.raster.mapalgebra.local._
import whu.edu.cn.geocube.util.NetcdfUtil.{isAddDimensionSame, rasterRDD2Netcdf}
import whu.edu.cn.geocube.util.PostgresqlService
import geotrellis.spark._
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}
import whu.edu.cn.geocube.application.gdc.GDCTrigger.isOptionalArg

import java.io.{File, FileOutputStream}
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import sys.process._
import scala.collection.mutable.ArrayBuffer

object RasterCubeFun {

  def loadCube(sc: SparkContext, queryParams: QueryParams, scaleSize: Array[java.lang.Integer],
               scaleAxes: Array[java.lang.Double], scaleFactor: java.lang.Double): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, queryParams)
    rasterTileLayerRdd
  }

  /**
   * load the cube data, it support to filter data by extent and start-end time
   *
   * @param sc        the spark context
   * @param cubeName  the cube name
   * @param extent    the extent
   * @param startTime the startTime yy-mm-dd HH:mm:ss
   * @param endTime   the endTime yy-mm-dd HH:mm:ss
   * @return the data selected
   */
  def loadCube(sc: SparkContext, cubeName: String, extent: String = null, startTime: String = null, endTime: String = null,
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
      if (extent != null) {
        queryParams.setExtent(extent.split(",")(0).toDouble, extent.split(",")(1).toDouble, extent.split(",")(2).toDouble, extent.split(",")(3).toDouble)
      }
      if (startTime != null || endTime != null) {
        queryParams.setTime(startTime, endTime)
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

  def calculateAlongDimensionWithString(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                                        dimensionName: String, dimensionMembersStr: String, method: String,
                                        outputDimensionMember: String = null, isOutput: Boolean = false): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    var dimensionMembers: Array[String] = null
    if (dimensionMembersStr != null) {
      dimensionMembers = dimensionMembersStr
        .stripPrefix("[").stripSuffix("]")
        .split(",")
        .map(_.trim.stripPrefix("\"").stripSuffix("\""))
      calculateAlongDimension(data, dimensionName, dimensionMembers, method, outputDimensionMember, isOutput)
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

  def normalize(data: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                dimensionName: String, dimensionMembers: Array[String], outputDimensionMember: String = "normalized")
  : (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    calculateAlongDimension(data, dimensionName, dimensionMembers, "normalize", outputDimensionMember)
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

  /**
   * 写一个结果配置文件，存文件路径
   *
   * @param filePath  the result file path
   * @param outputDir the result directory
   * @param extent    the extent of image
   */
  def writeResultJson(filePath: String, outputDir: String, extent: String): Unit = {
    val objectMapper = new ObjectMapper()
    val outputMetaPath = filePath.split("\\.")(0) + ".json"
    println(outputMetaPath)
    val node = objectMapper.createObjectNode()
    node.put("path", filePath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extent)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
    val scpMetaCommand = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir
    scpMetaCommand.!
  }

  /**
   * transform the zonedDateTime to the time string in "yyyy-MM-dd HH:mm:ss" format
   *
   * @param time ZonedDateTime
   * @return the time string in format "yyyy-MM-dd HH:mm:ss"
   */
  def zonedDateTime2String(time: ZonedDateTime): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    time.format(formatter)
  }

  def main(args: Array[String]): Unit = {


    //    val cubeId = "35"
    //    val rasterProductName = "SENTINEL-2 Level-2A MSI"
    //    val extent = "4.42,51.63,4.8,51.84"
    //    //    val extent = "4.695,51.716,4.7245,51.7392"
    //    val startTime = "2019-01-17 08:59:59"
    //    val endTime = "2019-01-19 09:00:01"
    val measurements: Array[String] = Array("B8", "B3")
    //    val bbox = "null args"
    //    val coordinates = "null args"
    //    val outputDir = "E:\\LaoK\\data2\\GDC_API\\"
    //    val imageFormat = "tif"
    //    val queryParams = new QueryParams()
    //    queryParams.setCubeId(cubeId)
    //    queryParams.setRasterProductName(rasterProductName)
    //
    //    queryParams.setExtent(extent.split(",")(0).toDouble, extent.split(",")(1).toDouble, extent.split(",")(2).toDouble, extent.split(",")(3).toDouble)
    //    queryParams.setTime(startTime, endTime)
    //    queryParams.setMeasurements(measurements)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("gdcCoverage")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)
    //    val crs = CRS.fromEpsgCode(4326) // 将3857替换为您的CRS所对应的EPSG代码
    //    //    val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeotiffRDD("文件路径", crs)
    //    val rdd2: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("E:\\LaoK\\data2\\GDC_API\\sentinel-2-zaici.tif")
    //    val c = rdd2.first()
    //    GeoTiff(c._2, c._1.extent, c._1.crs).write("E:\\LaoK\\data2\\GDC_API\\wapian2-zaici-2.tif")

    //    val rdd = loadCube(sc, queryParams, null, null, null)
    val rdd = loadCube(sc, "SENTINEL-2 Level-2A MSI", "4.42,51.63,4.8,51.84", "2019-01-17 08:59:59", "2019-01-19 09:00:01")
    normalize(rdd, "bands", measurements)
  }
}
