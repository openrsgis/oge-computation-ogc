package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.baidubce.services.bos.BosClient
import geotrellis.layer
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{ByteConstantNoDataCellType, CellType, DoubleArrayTile, DoubleConstantNoDataCellType, MultibandTile, Raster, ShortConstantNoDataCellType, Tile, TileLayout, UByteCellType, UByteConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.{Extent, Geometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import io.minio.{MinioClient, UploadObjectArgs}
import javafx.scene.paint.Color
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.geotools.data.{FeatureWriter, Transaction}
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import redis.clients.jedis.Jedis
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.config.GlobalConfig.RedisConf.REDIS_CACHE_TTL
import whu.edu.cn.entity.{BatchParam, CoverageMetadata, RawTile, VisualizationParam}
import whu.edu.cn.geocube.application.conjoint.Flood.impactedFeaturesService
import whu.edu.cn.geocube.application.gdc.RasterCubeFun.{writeResultJson, zonedDateTime2String}
import whu.edu.cn.geocube.application.gdc.gdcCoverage._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject}
import whu.edu.cn.geocube.core.entity.{GcDimension, GcMeasurement, GcProduct, QueryParams, RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey, VectorGridLayerMetadata}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.core.raster.query.QueryRasterTiles
import whu.edu.cn.geocube.core.vector.grid.GridConf
import whu.edu.cn.geocube.core.vector.query.QueryVectorObjects
import whu.edu.cn.geocube.util.NetcdfUtil.{isAddDimensionSame, rasterRDD2Netcdf}
import whu.edu.cn.geocube.util.PostgresqlService
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.oge.Coverage.removeZeroFromTile
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage
import whu.edu.cn.util.TileSerializerCoverageUtil.deserializeTileData
import whu.edu.cn.util.{COGUtil, ClientUtil, JedisUtil, PostSender}

import java.io.{File, FileWriter, Reader, StringReader}
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.time.{ZoneOffset, ZonedDateTime}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import java.time.Instant
import java.util
import java.util.Date
import scala.Console.println
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
      val productName: String = postgresqlService.getProductNameByCubeName(postgresqlService.getMeasurementsProductViewName(cubeName))
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
  ArrayBuffer[(RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    // 从所有波段中提取出对应的波段

    val multibandRaster: ArrayBuffer[(RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = ArrayBuffer()
    if (visualizationParam.getBands.nonEmpty) {
      val bands: Array[String] = visualizationParam.getBands.toArray
      val tar_bands = rasterTileLayerRdd._1.map { x => x._1.measurementName }.collect()

      for (xband <- bands if tar_bands.contains(xband)) {
        // the min time
        val allTime: Array[Long] = rasterTileLayerRdd._1.map(_._1.spaceTimeKey.time.toInstant.toEpochMilli).collect().distinct.sorted

        if (allTime.length <= 5) {
          for (time <- allTime) {
            // get the first image
            val rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
              rdd => {
                rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == time && (if (rdd._1.additionalDimensions != null) isAddDimensionSame(rdd._1.additionalDimensions) else true) && rdd._1.measurementName == xband
              }
            }, rasterTileLayerRdd._2)
            var rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRddFirstDim._1.map {
              x => {
                val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
                multiBandTiles.append(x._2)
                (x._1.spaceTimeKey, MultibandTile(multiBandTiles))
              }
            }

            // min的数量
            val minNum: Int = visualizationParam.getMin.length
            // max的数量
            val maxNum: Int = visualizationParam.getMax.length
            if (minNum * maxNum != 0) {
              // 首先找到现有的最小最大值
              val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
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
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN)
                if (noNaNArray.nonEmpty) {
                  (noNaNArray.min, noNaNArray.max)
                }
                else {
                  (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                }
              }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

              val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
              rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => {
                val len: Int = colorRGB.length - 1
                val bandR: Tile = t._2.bands(0).mapDouble(d => {
                  var R: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      R = colorRGB(i)._1 * 255.0
                    }
                  }
                  R
                })
                val bandG: Tile = t._2.bands(0).mapDouble(d => {
                  var G: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      G = colorRGB(i)._2 * 255.0
                    }
                  }
                  G
                })
                val bandB: Tile = t._2.bands(0).mapDouble(d => {
                  var B: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      B = colorRGB(i)._3 * 255.0
                    }
                  }
                  B
                })
                (t._1, MultibandTile(bandR, bandG, bandB))
              })
            }
            multibandRaster.append((rasterMultiBandTileRdd, rasterTileLayerRddFirstDim._2.tileLayerMetadata))
          }
        }
        else {
          val startIndex = 0
          val endIndex = allTime.length - 1
          val numElements = 5
          val stepSize: Int = (endIndex - startIndex) / (numElements - 1)
          val tenTime = (startIndex to endIndex by stepSize).map(allTime(_))

          for (time <- tenTime) {
            // get the first image
            val rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
              rdd => {
                rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == time && (if (rdd._1.additionalDimensions != null) isAddDimensionSame(rdd._1.additionalDimensions) else true) && rdd._1.measurementName == xband
              }
            }, rasterTileLayerRdd._2)
            var rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRddFirstDim._1.map {
              x => {
                val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
                multiBandTiles.append(x._2)
                (x._1.spaceTimeKey, MultibandTile(multiBandTiles))
              }
            }

            // min的数量
            val minNum: Int = visualizationParam.getMin.length
            // max的数量
            val maxNum: Int = visualizationParam.getMax.length
            if (minNum * maxNum != 0) {
              // 首先找到现有的最小最大值
              val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
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
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN)
                if (noNaNArray.nonEmpty) {
                  (noNaNArray.min, noNaNArray.max)
                }
                else {
                  (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                }
              }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

              val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
              rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => {
                val len: Int = colorRGB.length - 1
                val bandR: Tile = t._2.bands(0).mapDouble(d => {
                  var R: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      R = colorRGB(i)._1 * 255.0
                    }
                  }
                  R
                })
                val bandG: Tile = t._2.bands(0).mapDouble(d => {
                  var G: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      G = colorRGB(i)._2 * 255.0
                    }
                  }
                  G
                })
                val bandB: Tile = t._2.bands(0).mapDouble(d => {
                  var B: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      B = colorRGB(i)._3 * 255.0
                    }
                  }
                  B
                })
                (t._1, MultibandTile(bandR, bandG, bandB))
              })
            }
            multibandRaster.append((rasterMultiBandTileRdd, rasterTileLayerRddFirstDim._2.tileLayerMetadata))
          }
        }
      }
    }
    else {
      val tar_bands = rasterTileLayerRdd._1.map { x => x._1.measurementName }.collect().distinct
      for (xband <- tar_bands) {
        val allTime: Array[Long] = rasterTileLayerRdd._1.map(_._1.spaceTimeKey.time.toInstant.toEpochMilli).collect().distinct.sorted
        //        val minTime = rasterTileLayerRdd._2.tileLayerMetadata.bounds.get.minKey.time.toInstant.toEpochMilli
        if (allTime.length <= 5) {
          for (time <- allTime) {
            // get the first image
            val rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
              rdd => {
                rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == time && (if (rdd._1.additionalDimensions != null) isAddDimensionSame(rdd._1.additionalDimensions) else true) && rdd._1.measurementName == xband
              }
            }, rasterTileLayerRdd._2)
            var rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRddFirstDim._1.map {
              x => {
                val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
                multiBandTiles.append(x._2)
                (x._1.spaceTimeKey, MultibandTile(multiBandTiles))
              }
            }

            // min的数量
            val minNum: Int = visualizationParam.getMin.length
            // max的数量
            val maxNum: Int = visualizationParam.getMax.length
            if (minNum * maxNum != 0) {
              // 首先找到现有的最小最大值
              val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
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
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN)
                if (noNaNArray.nonEmpty) {
                  (noNaNArray.min, noNaNArray.max)
                }
                else {
                  (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                }
              }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

              val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
              rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => {
                val len: Int = colorRGB.length - 1
                val bandR: Tile = t._2.bands(0).mapDouble(d => {
                  var R: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      R = colorRGB(i)._1 * 255.0
                    }
                  }
                  R
                })
                val bandG: Tile = t._2.bands(0).mapDouble(d => {
                  var G: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      G = colorRGB(i)._2 * 255.0
                    }
                  }
                  G
                })
                val bandB: Tile = t._2.bands(0).mapDouble(d => {
                  var B: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      B = colorRGB(i)._3 * 255.0
                    }
                  }
                  B
                })
                (t._1, MultibandTile(bandR, bandG, bandB))
              })
            }
            multibandRaster.append((rasterMultiBandTileRdd, rasterTileLayerRddFirstDim._2.tileLayerMetadata))
          }
        }
        else {
          val startIndex = 0
          val endIndex = allTime.length - 1
          val numElements = 5
          val stepSize: Int = (endIndex - startIndex) / (numElements - 1)
          val tenTime = (startIndex to endIndex by stepSize).map(allTime(_))

          for (time <- tenTime) {
            // get the first image
            val rasterTileLayerRddFirstDim: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterTileLayerRdd.filter {
              rdd => {
                rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == time && (if (rdd._1.additionalDimensions != null) isAddDimensionSame(rdd._1.additionalDimensions) else true) && rdd._1.measurementName == xband
              }
            }, rasterTileLayerRdd._2)
            var rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRddFirstDim._1.map {
              x => {
                val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
                multiBandTiles.append(x._2)
                (x._1.spaceTimeKey, MultibandTile(multiBandTiles))
              }
            }

            // min的数量
            val minNum: Int = visualizationParam.getMin.length
            // max的数量
            val maxNum: Int = visualizationParam.getMax.length
            if (minNum * maxNum != 0) {
              // 首先找到现有的最小最大值
              val minMaxBand: (Double, Double) = rasterMultiBandTileRdd.map(t => {
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN).filter(x => x != Int.MinValue)
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
                val noNaNArray: Array[Double] = t._2.band(0).toArrayDouble().filter(!_.isNaN)
                if (noNaNArray.nonEmpty) {
                  (noNaNArray.min, noNaNArray.max)
                }
                else {
                  (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                }
              }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

              val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
              rasterMultiBandTileRdd = rasterMultiBandTileRdd.map(t => {
                val len: Int = colorRGB.length - 1
                val bandR: Tile = t._2.bands(0).mapDouble(d => {
                  var R: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      R = colorRGB(i)._1 * 255.0
                    }
                  }
                  R
                })
                val bandG: Tile = t._2.bands(0).mapDouble(d => {
                  var G: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      G = colorRGB(i)._2 * 255.0
                    }
                  }
                  G
                })
                val bandB: Tile = t._2.bands(0).mapDouble(d => {
                  var B: Double = 0.0
                  for (i <- 0 to len) {
                    if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                      B = colorRGB(i)._3 * 255.0
                    }
                  }
                  B
                })
                (t._1, MultibandTile(bandR, bandG, bandB))
              })
            }
            multibandRaster.append((rasterMultiBandTileRdd, rasterTileLayerRddFirstDim._2.tileLayerMetadata))
          }
        }
      }
    }
    multibandRaster
  }


  def visualizeOnTheFly(implicit sc: SparkContext, rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), visualizationParam: VisualizationParam): Unit = {
    throw new Exception("所请求的数据在Bos中不存在！")
    //    val styledRasterRDD: ArrayBuffer[(RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Cube.addStyles(rasterTileLayerRdd, visualizationParam)
    //
    //    val bands: Array[String] = visualizationParam.getBands.toArray
    //    val tol_bands: ArrayBuffer[String] = ArrayBuffer()
    //    val tol_Extent: ArrayBuffer[String] = ArrayBuffer()
    //    val tol_Time: ArrayBuffer[Instant] = ArrayBuffer()
    //    val tol_urljson: ArrayBuffer[JSONObject] = ArrayBuffer()
    //
    //    for (i <- styledRasterRDD.indices) {
    //
    //      val tileRdd: (RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = styledRasterRDD(i)
    //      val styledRasterRDDtime = (tileRdd.map (_._1.time.toInstant.toEpochMilli)).collect().distinct(0)
    //
    //      val spatialMetadata = TileLayerMetadata(
    //        tileRdd._2.cellType,
    //        tileRdd._2.layout,
    //        tileRdd._2.extent,
    //        tileRdd._2.crs,
    //        tileRdd._2.bounds.get.toSpatial)
    //      var tileLayerRdd: MultibandTileLayerRDD[SpatialKey] = ContextRDD(tileRdd._1.map { x => (x._1.spatialKey, x._2) }, spatialMetadata)
    //
    //      if (BosCOGUtil.tileDifference > 0) {
    //        // 首先对其进行上采样
    //        // 上采样必须考虑范围缩小，不然非常占用内存
    //        val levelUp: Int = BosCOGUtil.tileDifference
    //        val layoutOrigin: LayoutDefinition = tileLayerRdd.metadata.layout
    //        val extentOrigin: Extent = tileLayerRdd.metadata.layout.extent
    //        val extentIntersect: Extent = extentOrigin.intersection(BosCOGUtil.extent).orNull
    //        val layoutCols: Int = math.max(math.ceil((extentIntersect.xmax - extentIntersect.xmin) / 256.0 / layoutOrigin.cellSize.width * (1 << levelUp)).toInt, 1)
    //        val layoutRows: Int = math.max(math.ceil((extentIntersect.ymax - extentIntersect.ymin) / 256.0 / layoutOrigin.cellSize.height * (1 << levelUp)).toInt, 1)
    //        val extentNew: Extent = Extent(extentIntersect.xmin, extentIntersect.ymin, extentIntersect.xmin + layoutCols * 256.0 * layoutOrigin.cellSize.width / (1 << levelUp), extentIntersect.ymin + layoutRows * 256.0 * layoutOrigin.cellSize.height / (1 << levelUp))
    //
    //        val tileLayout: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
    //        val layoutNew: LayoutDefinition = LayoutDefinition(extentNew, tileLayout)
    //        tileLayerRdd = tileLayerRdd.reproject(tileLayerRdd.metadata.crs, layoutNew)._2
    //      }
    //
    //      val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    //      val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    //      val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
    //        tileLayerRdd.reproject(tmsCrs, layoutScheme)
    //
    //      val outputPath: String = "/mnt/storage/on-the-fly"
    //      // Create the attributes store that will tell us information about our catalog.
    //      val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
    //      // Create the writer that we will use to store the tiles in the local catalog.
    //      val writer: FileLayerWriter = FileLayerWriter(attributeStore)
    //
    //      if (zoom < Trigger.level) {
    //        throw new InternalError("内部错误，切分瓦片层级没有前端TMS层级高")
    //      }
    //
    //      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
    //        if (z == Trigger.level) {
    //          val layerId: LayerId = LayerId(Trigger.dagId + styledRasterRDDtime, z)
    //          // If the layer exists already, delete it out before writing
    //          if (attributeStore.layerExists(layerId)) {
    //            //        new FileLayerManager(attributeStore).delete(layerId)
    //            try {
    //              writer.overwrite(layerId, rdd)
    //            } catch {
    //              case e: Exception =>
    //                e.printStackTrace()
    //            }
    //          }
    //          else {
    //            writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    //          }
    //        }
    //      }
    //
    //      tol_bands.append(bands(0))
    //      val Time: Instant = tileRdd.map(_._1.time.toInstant).collect().distinct(0)
    //      //    val dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    //      //    val TimeUtcString: String = dateTimeFormatter.format(Time)
    //      tol_Time.append(Time)
    //      val extent: String = tileRdd._2.extent.toString()
    //      val subExtent: String = extent.substring(6, extent.length).replace("(", "[").replace(")", "]")
    //      tol_Extent.append(subExtent)
    //
    //      val urlObject: JSONObject = new JSONObject
    //      urlObject.put(Trigger.layerName, "http://oge.whu.edu.cn/api/oge-tms-png/" + Trigger.dagId  + styledRasterRDDtime + "/{z}/{x}/{y}.png")
    //      tol_urljson.append(urlObject)
    //
    //
    //      //      val zIndexStrArray: mutable.ArrayBuffer[String] = Trigger.zIndexStrArray
    //      //      val jedis: Jedis = new JedisUtil().getJedis
    //      //      jedis.select(1)
    //      //      zIndexStrArray.foreach(zIndexStr => {
    //      //        val key: String = Trigger.dagId + ":solvedTile:" + Trigger.level + zIndexStr
    //      //        jedis.sadd(key, "cached")
    //      //        jedis.expire(key, REDIS_CACHE_TTL)
    //      //      })
    //      //      jedis.close()
    //
    //      if (sc.master.contains("local")) {
    //        whu.edu.cn.debug.CoverageDubug.makeTIFF(reprojected, "cube" + styledRasterRDDtime)
    //      }
    //    }
    //
    //    // 回调服务
    //
    //    val cl_Extent: Array[String] = tol_Extent.distinct.toArray
    //    val cl_Time: Array[Instant] = tol_Time.distinct.toArray
    //    val jsonObject: JSONObject = new JSONObject
    //    val dim: ArrayBuffer[JSONObject] = ArrayBuffer()
    //
    //    val dimObject1: JSONObject = new JSONObject
    //    dimObject1.put("name", "extent")
    //    dimObject1.put("values", cl_Extent)
    //    dim.append(dimObject1)
    //
    //    val dimObject2: JSONObject = new JSONObject
    //    dimObject2.put("name", "dateTime")
    //    dimObject2.put("values", cl_Time)
    //    dim.append(dimObject2)
    //
    //    val dimObject3: JSONObject = new JSONObject
    //    dimObject3.put("name", "bands")
    //    dimObject3.put("values", bands)
    //    dim.append(dimObject3)
    //
    //    jsonObject.put("raster", tol_urljson.toArray)
    //    jsonObject.put("extent", tol_Extent.toArray)
    //    jsonObject.put("dateTime", tol_Time.toArray)
    //    jsonObject.put("bands", bands)
    //    jsonObject.put("dimension", dim.toArray)
    //
    //
    //    PostSender.shelvePost("cube",jsonObject)
    //
    //
    //
    //    // 清空list
    //    Trigger.optimizedDagMap.clear()
    //    Trigger.coverageCollectionMetadata.clear()
    //    Trigger.lazyFunc.clear()
    //    Trigger.coverageCollectionRddList.clear()
    //    Trigger.coverageRddList.clear()
    //    Trigger.zIndexStrArray.clear()
    //    JsonToArg.dagMap.clear()
    //    //    // TODO lrx: 以下为未检验
    //    Trigger.tableRddList.clear()
    //    Trigger.kernelRddList.clear()
    //    Trigger.featureRddList.clear()
    //    Trigger.cubeRDDList.clear()
    //    Trigger.cubeLoad.clear()

  }

  //  def selectProducts(implicit sc: SparkContext, productList: String = null, dateTime: String = null, geom: String = null, bands: String = null): Unit = {
  //    // 将对应的参数赋值并得到应有的格式
  //    // cube的特性如何体现在将将productList中的产品统一至该维度geom, bandList等规定的维度下，但是格网如何统一的？
  //    val products = if (productList != null) productList.replace("[", "").replace("]", "").split(",") else null
  //    val geomList = if (geom != null) geom.replace("[", "").replace("]", "").split(",").map(t => {
  //      t.toDouble
  //    }) else null
  //    val dateTimeArray = if (dateTime != null) dateTime.replace("[", "").replace("]", "").split(",") else null
  //    val startTime = if (dateTimeArray.length == 2) dateTimeArray(0) else null
  //    val endTime = if (dateTimeArray.length == 2) dateTimeArray(1) else null
  //    val bandList = if (bands != null) bands.replace("[", "").replace("]", "").split(",") else null
  //    for (i <- 0 until products.length) {
  //      if (products(i).contains("EO")) { // EO:栅格数据
  //        val queryParams = new QueryParams
  //        // 有很多参数还没有设定，如何得到，如：cityCodes，cityNames
  //        queryParams.setCubeId("27") // 后期需要修改成可改变的。
  //        queryParams.setLevel("4000") // 瓦片分辨率，为什么这两个值是确定的？
  //        queryParams.setRasterProductName(products(i)) // 产品名
  //        queryParams.setExtent(geomList(0), geomList(1), geomList(2), geomList(3)) // 空间维度，矩形四点
  //        queryParams.setTime(startTime, endTime)
  //        queryParams.setMeasurements(bandList)
  //        val rasterRdd: RasterRDD = getData(sc, queryParams)
  ////        result += (products(i) -> rasterRdd) // products(i)
  //      }
  //      else if (products(i).contains("Vector")) { // EO:矢量数据
  //        val queryParams = new QueryParams
  //        queryParams.setCubeId("27")
  //        queryParams.setCRS("WGS84")
  //        queryParams.setVectorProductName(products(i))
  //        queryParams.setExtent(geomList(0), geomList(1), geomList(2), geomList(3))
  //        queryParams.setTime(startTime, endTime)
  //        val featureRdd: FeatureRDD = getData(sc, queryParams)
  ////        result += (products(i) -> featureRdd)
  //      }
  //      else if (products(i).contains("Tabular")) {
  //        val tabularRdd: RDD[(LocationTimeGenderKey, Int)] = getBITabulars(sc, new BiQueryParams)
  ////        result += (products(i) -> tabularRdd)
  //      }
  //    }
  //  }

  // 为cube划分网格
  def meshing(gridDimX: Int, gridDimY: Int, startTime: String, endTime: String, extents: Extent):
  (LayoutDefinition, Bounds[SpaceTimeKey]) = {

    val tl: TileLayout = TileLayout(gridDimX, gridDimY, 256, 256)
    val ld: LayoutDefinition = LayoutDefinition(extents, tl)
    val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(startTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(endTime).getTime
    val colRowInstant: (Int, Int, Long, Int, Int, Long) = (0, 0, minInstant, gridDimX, gridDimY, maxInstant)
    val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(0, 0, colRowInstant._3),
      SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
    (ld, bounds)
  }

  def getRawTileRDDBos(implicit sc: SparkContext,coverageId: String, productKey: String, level: Int): RDD[RawTile] = {
    throw new Exception("所请求的数据在Bos中不存在！")
    //    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId, productKey)
    //    if (metaList.isEmpty) {
    //      throw new Exception("No such coverage in database!")
    //    }
    //
    //    var union: Extent = Trigger.windowExtent
    //    if(union == null){
    //      union = Extent(metaList.head.getGeom.getEnvelopeInternal)
    //    }
    //    val queryGeometry: Geometry = metaList.head.getGeom
    //
    //    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    //    val tileRDDFlat: RDD[RawTile] = tileMetadata
    //      .mapPartitions(par => {
    //        val minIOUtil = MinIOUtil
    //        val client: BosClient = BosClientUtil_scala.getClient
    //        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
    //          val time1: Long = System.currentTimeMillis()
    //          val rawTiles: mutable.ArrayBuffer[RawTile] = {
    //            val tiles: mutable.ArrayBuffer[RawTile] = BosCOGUtil.tileQuery(client, level, t, union,queryGeometry)
    //            tiles
    //          }
    //          val time2: Long = System.currentTimeMillis()
    //          println("Get Tiles Meta Time is " + (time2 - time1))
    //          // 根据元数据和范围查询后端瓦片
    //          if (rawTiles.nonEmpty) rawTiles
    //          else mutable.Buffer.empty[RawTile]
    //        })
    //        result
    //      }).flatMap(t => t).persist()
    //
    //    val tileNum: Int = tileRDDFlat.count().toInt
    //    println("tileNum = " + tileNum)
    //    val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 16))
    //    tileRDDFlat.unpersist()
    //    val rawTileRdd: RDD[RawTile] = tileRDDRePar.mapPartitions(par => {
    //      val client: BosClient = BosClientUtil_scala.getClient
    //      par.map(t => {
    //        BosCOGUtil.getTileBuf(client, t)
    //      })
    //    })
    //    rawTileRdd
  }

  def getRawTileRDD(implicit sc: SparkContext,coverageId: String, productKey: String, level: Int): RDD[RawTile] = {
    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    }

    var union: Extent = Trigger.windowExtent
    if(union == null){
      union = Extent(metaList.head.getGeom.getEnvelopeInternal)
    }
    val queryGeometry: Geometry = metaList.head.getGeom

    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    val cogUtil: COGUtil = COGUtil.createCOGUtil(CLIENT_NAME)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .mapPartitions(par => {
        val client = clientUtil.getClient
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val tiles: mutable.ArrayBuffer[RawTile] = cogUtil.tileQuery(client, level, t, union, queryGeometry)
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
      val client = clientUtil.getClient
      par.map(t => {
        cogUtil.getTileBuf(client, t)
      })
    })
    rawTileRdd
  }

  // 为rawTile进行处理
  def genCubeRDD(implicit sc: SparkContext, rdd: RDD[RawTile], meta: TileLayerMetadata[SpaceTimeKey]): ArrayBuffer[(SpaceTimeBandKey, Tile)] = {
    val reprojectRdd = rdd.map(t => {
      if (t.getCrs.toString() != "EPSG:4326") {
        val reprojectedExtent: Extent = t.getExtent.reproject(t.getCrs, CRS.fromName("EPSG:4326"))
        t.setCrs(CRS.fromName("EPSG:4326"))
        t.setExtent(reprojectedExtent)
      }
      t
    })
    val mbr: (Double, Double, Double, Double) = reprojectRdd.map(t => {
      (t.getExtent.xmin, t.getExtent.ymin, t.getExtent.xmax, t.getExtent.ymax)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })

    val rddExtent: Extent = Extent(mbr._1, mbr._2, mbr._3, mbr._4)
    val extent = meta.extent
    val gridDimX = meta.tileLayout.layoutRows
    val gridDimY = meta.tileLayout.layoutCols
    val gridSizeX = (extent.xmax - extent.xmin) / gridDimY.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimX.toDouble

    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._3 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._2 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    val result: ArrayBuffer[(SpaceTimeBandKey, Tile)] = ArrayBuffer[(SpaceTimeBandKey, Tile)]()
    //    var measurement: String = ""
    //    var timeKey: Long = 0L
    //    val tileArray: ArrayBuffer[(SpatialKey, Tile)] = reprojectRdd.map { tile =>
    //      val Tile = deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString)
    //      measurement = tile.getCoverageId
    //      timeKey = tile.getTime.toEpochSecond(ZoneOffset.ofHours(0))
    //      (tile.getSpatialKey, Tile)
    //    }.collect().to[ArrayBuffer]
    //    val (totalTile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
    val ld = meta.layout
    //    val totalTileArray: ArrayBuffer[(String, Tile, Extent, Long)] = ArrayBuffer[(String, Tile, Extent, Long)]()
    val groupedRdd = reprojectRdd.groupBy(t => t.getMeasurement)
    val totalTileArray: Array[(String, Tile, Extent, Long)] = groupedRdd.aggregate[Array[(String, Tile, Extent, Long)]](Array.empty[(String, Tile, Extent, Long)])(
      (acc, group) => {
        val measurement = group._1
        val tiles = group._2
        val tileArray: Array[(SpatialKey, Tile)] = tiles.map { tile =>
          val tileGet = deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString)
          (tile.getSpatialKey, tileGet)
        }.toArray
        val groupMbr: (Double, Double, Double, Double) = tiles.map(t => {
          (t.getExtent.xmin, t.getExtent.ymin, t.getExtent.xmax, t.getExtent.ymax)
        }).reduce((a, b) => {
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        })
        val timeKey = tiles.head.getTime.toEpochSecond(ZoneOffset.ofHours(0))
        val groupExtent: Extent = Extent(groupMbr._1, groupMbr._2, groupMbr._3, groupMbr._4)
        val (totalTile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
        acc :+ (measurement, totalTile, groupExtent, timeKey)
      },
      (acc1, acc2) => acc1 ++ acc2
    )

    println("totalArray长度为" + totalTileArray.length)

    for (col <- _xmin until _xmax; row <- _ymin until _ymax) {
      val geotrellis_j = (gridDimY - 1 - row)
      val tempExtent = ld.mapTransform(SpatialKey(col, geotrellis_j))
      totalTileArray.foreach { case (measurement, totalTile, ex, timeKey) =>
        if (tempExtent.intersects(ex)) {
          val spaceTimeKey: SpaceTimeKey = SpaceTimeKey(col, geotrellis_j, timeKey)
          val intersectTile: Tile = totalTile.crop(ex, tempExtent)
          val bandKey = SpaceTimeBandKey(spaceTimeKey, measurement, null)
          removeZeroFromTile(intersectTile)
          result.append((bandKey, intersectTile))
        }
      }
    }
    result
  }

  // coverage visualize
  def visualizeBatch(implicit sc: SparkContext, exportedRasterRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]), batchParam: BatchParam, dagId: String) : Unit = {
    throw new Exception("所请求的数据在Bos中不存在！")
    //    val rasterArray: Array[(SpatialKey, Tile)] = exportedRasterRdd._1.map(t => {
    //      (t._1.spaceTimeKey.spatialKey, t._2)
    //    }).collect()
    //    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(rasterArray)
    //    val stitchedTile: Raster[Tile] = Raster(tile, exportedRasterRdd._2.tileLayerMetadata.extent)
    //    var reprojectTile: Raster[Tile] = stitchedTile.reproject(exportedRasterRdd._2.tileLayerMetadata.crs, CRS.fromName("EPSG:3857"))
    //    val resample: Raster[Tile] = reprojectTile.resample(math.max((reprojectTile.cellSize.width * reprojectTile.cols / batchParam.getScale).toInt, 1), math.max((reprojectTile.cellSize.height * reprojectTile.rows / batchParam.getScale).toInt, 1))
    //    reprojectTile = resample.reproject(CRS.fromName("EPSG:3857"), batchParam.getCrs)
    //
    //    // 上传文件
    //    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.tiff"
    //    GeoTiff(reprojectTile, batchParam.getCrs).write(saveFilePath)
    //    val file :File = new File(saveFilePath)
    //
    //    val client: BosClient = BosClientUtil_scala.getClient2
    //    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
    //    client.putObject("oge-user", path, file)
  }


  // feature visualize
  def visualizeBatch(implicit sc: SparkContext, exportedFeature: (RDD[(SpaceTimeKey, Iterable[GeoObject])], VectorGridLayerMetadata[SpaceTimeKey]),
                     dagId: String, batchParam: BatchParam): Unit = {
    val featureJSON = new FeatureJSON()
    val setGeo: Set[SimpleFeature] = exportedFeature.flatMap { case (_, geoObjects) =>
      geoObjects.map(g => g.feature)
    }.collect().toSet
    // 将 SimpleFeature 转换为 GeoJSON 字符串
    val geoJSONStrings: Seq[String] = setGeo.map { simpleFeature =>
      val str = featureJSON.toString(simpleFeature)
      str
    }.toSeq


    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat

    val geoJSONFeatureCollection = geoJSONStrings.mkString(",")
    val geoJSON = s"""{"type": "FeatureCollection", "features": [$geoJSONFeatureCollection]}"""
    if (batchParam.getFormat == "geojson") {
      val saveGJFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.json"
      val outputWriter = new FileWriter(new File(saveGJFilePath))
      outputWriter.write(geoJSON)
      outputWriter.close()
    } else if (batchParam.getFormat == "shp") {
      val saveShpFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.shp"
      geoJson2Shape(geoJSON, saveShpFilePath)
      clientUtil.Upload(path,saveShpFilePath)
    }
  }

  def cubeBuild(implicit sc: SparkContext, coverageIdList: ArrayBuffer[String], productKeyList: ArrayBuffer[String], level: Int,
                gridDimX: Int, gridDimY: Int, startTime: String, endTime: String, extents: String): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val tic = System.nanoTime()
    val extentArray: Array[String] = extents.replace("[", "").replace("]", "").split(",")
    val extent: Extent = Extent(extentArray(0).toDouble, extentArray(1).toDouble, extentArray(2).toDouble, extentArray(3).toDouble)
    val mesh = meshing(gridDimX, gridDimY, startTime, endTime, extent)
    val crs: CRS = CRS.fromEpsgCode(4326)
    val ld = mesh._1
    val bounds = mesh._2
    val metadata: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(CellType.fromName("int16"), ld, extent, crs, bounds)
    val rasterTileLayerMetadata: RasterTileLayerMetadata[SpaceTimeKey] = RasterTileLayerMetadata(metadata, _productNames = coverageIdList)

    val cubeArray: ArrayBuffer[(SpaceTimeBandKey, Tile)] = ArrayBuffer[(SpaceTimeBandKey, Tile)]()
    coverageIdList.zipWithIndex.foreach { case (coverageId, index) =>
      val productKey = productKeyList(index)
      //      val rawTileRdd = getRawTileRDD(sc, coverageId, productKey, level)
      val rawTileRdd = getRawTileRDDBos(sc, coverageId, productKey, level)

      val tic = System.nanoTime()
      val result: ArrayBuffer[(SpaceTimeBandKey, Tile)] = genCubeRDD(sc, rawTileRdd, metadata)
      val toc = System.nanoTime()
      val executionTime = (toc - tic) / 1e9
      println(s"执行时间cube建立时间: $executionTime seconds")
      cubeArray.appendAll(result)
    }
    println(rasterTileLayerMetadata)
    val executionTime = (System.nanoTime() - tic) / 1e9
    println(s"Make RDD Time: $executionTime seconds")
    (sc.makeRDD(cubeArray), rasterTileLayerMetadata)
  }

  def getFeatureCube(rdd: RDD[(String, (Geometry, Map[String, Any]))], meta: VectorGridLayerMetadata[SpaceTimeKey]): ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])] = {
    val (startTime, endTime): (Long, Long) = meta.bounds.foldLeft((Long.MaxValue, Long.MinValue)) { case ((minTime, maxTime), bounds) =>
      val currentMinTime: Long = Math.min(minTime, bounds._1.instant)
      val currentMaxTime: Long = Math.max(maxTime, bounds._2.instant)
      (currentMinTime, currentMaxTime)
    }
    println(startTime, endTime)
    val extent = meta.extent
    val gridDimY = meta.gridConf.gridDimY.toInt
    val gridDimX = meta.gridConf.gridDimX.toInt
    val gridWidth = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridHeight = (extent.ymax - extent.ymin) / gridDimY.toDouble

    val colRowInstant: (Int, Int, Long, Int, Int, Long) = (0, 0, startTime, gridDimX, gridDimY, endTime)

    val rddExtents = rdd.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val rddExtent = Extent(rddExtents._1, rddExtents._2, rddExtents._3, rddExtents._4)
    val productName: String = rdd.first()._2._2("productName").toString

    println("rddExtent: " + rddExtent)

    // 1. 遍历每个网格, 生成为其建立空间索引
    val result: ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])] = ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])]()
    for (col <- 0 until gridDimX) {
      for (row <- 0 until gridDimY) {
        val minX = extent.xmin + col * gridWidth
        val minY = extent.ymin + row * gridHeight
        val tempExtent = Extent(minX, minY, minX + gridWidth, minY + gridHeight)
        // 遍历rdd
        val geometries: List[GeoObject] = rdd.flatMap { case (_, (geom, info)) =>
          val envelope = geom.getEnvelopeInternal
          val localExtent = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
          if (localExtent.intersects(tempExtent)) {
            val geometryJSON = new GeometryJSON()
            val geoJSONString: String = geometryJSON.toString(geom)
            val prop = info("properties").toString
            val geoJson = "{\"type\":\"Feature\",\"geometry\": " + geoJSONString + ",\"properties\":" + prop.substring(1, prop.length - 1) + "}"
            val fjson = new FeatureJSON()
            val feature = fjson.readFeature(geoJson)
            val data = new GeoObject(productName, feature)
            Seq(data)
          } else {
            Seq.empty[GeoObject]
          }
        }.collect().toList
        val spaceTimeKey = SpaceTimeKey(col, row, 0L)
        result.append((spaceTimeKey, geometries))
      }
    }
    println("长度为： " + result.length)
    result
  }

  // feature的cube构建
  def cubeBuild(implicit sc: SparkContext, gridDimX: Int, gridDimY: Int,
                extents: String, startTime: String, endTime: String, featureIdList: List[String]):
  (RDD[(SpaceTimeKey, Iterable[GeoObject])], VectorGridLayerMetadata[SpaceTimeKey]) = {
    val extentArray: Array[String] = extents.replace("[", "").replace("]", "").split(",")
    val data: List[RDD[(String, (Geometry, Map[String, Any]))]] = featureIdList.map {
      feature =>
        val rdd = Feature.load(sc, productName = feature)
        rdd
    }

    // 建立元数据
    val crs: CRS = CRS.fromEpsgCode(4326)
    val gridConf: GridConf = new GridConf(gridDimX, gridDimY, Extent(-180, -90, 180, 90))
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(extentArray(0).toDouble, extentArray(1).toDouble),
      new Coordinate(extentArray(0).toDouble, extentArray(3).toDouble),
      new Coordinate(extentArray(2).toDouble, extentArray(3).toDouble),
      new Coordinate(extentArray(2).toDouble, extentArray(1).toDouble),
      new Coordinate(extentArray(0).toDouble, extentArray(1).toDouble)
    ))
    val colRow: Array[Int] = new Array[Int](4)
    val longLati: Array[Double] = new Array[Double](4)
    getGeomGridInfo(queryGeometry, gridConf.gridDimX, gridConf.gridDimX, gridConf.extent, colRow, longLati,1)
    val minCol = colRow(0); val minRow = colRow(1); val maxCol = colRow(2); val maxRow = colRow(3)
    val minLong = longLati(0); val minLat = longLati(1) ; val maxLong = longLati(2); val maxLat = longLati(3)
    val minInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(startTime).getTime
    val maxInstant = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(endTime).getTime
    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))
    val extent = Extent(minLong, minLat, maxLong, maxLat)

    val productNames = featureIdList.mkString(",")
    val metaData = VectorGridLayerMetadata[SpaceTimeKey](gridConf, extent, bounds, crs, productNames)

    // 建立RDD
    val featureRdd: ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])] = ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])]()
    data.foreach {
      rdd =>
        val startTime = System.nanoTime()
        val res = getFeatureCube(rdd, metaData)
        featureRdd.appendAll(res)
        val endTime = System.nanoTime()
        val executionTime = (endTime - startTime) / 1e9
        println(s"执行时间: $executionTime seconds")
    }
    (sc.makeRDD(featureRdd), metaData)
  }


  def getGeomGridInfo(geom: Geometry, gridDimX: Long, gridDimY: Long, extent: Extent,
                      colRow: Array[Int], longLat: Array[Double],cellSize:Double): Unit = {
    val spatialKeyResults: ArrayBuffer[SpatialKey] = new ArrayBuffer[SpatialKey]()
    print("geom" + geom.toString)
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)
    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt
    val tl = TileLayout(gridDimX.toInt, gridDimY.toInt, ((extent.xmax - extent.xmin) / cellSize).toInt, ((extent.ymax - extent.ymin) / cellSize).toInt)
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        spatialKeyResults.append(SpatialKey(i, geotrellis_j))
      }
    }
    val cols = spatialKeyResults.map(_.col)
    val minCol = cols.min
    val maxCol = cols.max
    val rows = spatialKeyResults.map(_.row)
    val minRow = rows.min
    val maxRow = rows.max
    colRow(0) = minCol; colRow(1) = minRow;colRow(2) = maxCol; colRow(3) = maxRow

    val longtitude = spatialKeyResults.flatMap(x => Array(x.extent(ld).xmin, x.extent(ld).xmax))
    val minLongtitude = longtitude.min
    val maxLongtitude = longtitude.max
    val latititude = spatialKeyResults.flatMap(x => Array(x.extent(ld).ymin, x.extent(ld).ymax))
    val minLatititude = latititude.min
    val maxLatititude = latititude.max
    longLat(0) = minLongtitude; longLat(1) = minLatititude; longLat(2) = maxLongtitude; longLat(3) = maxLatititude
  }


  def NDVI(tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
           bandNames: List[String]): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    if (bandNames.length != 2) {
      throw new IllegalArgumentException(s"输入的波段数量不为2，输入了${bandNames.length}个波段")
    }
    else {
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is submitted")
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- NDVI task is running ...")
      //    val bandsArray: Array[String] = bands.replace("[", "").replace("]", "").replace(" ", "").split(",")
      val redBand = bandNames.head
      val nirRedBand = bandNames(1)

      def ndviTile(redBandTile: Tile, nirBandTile: Tile): Tile = {

        //convert stored tile with constant Float.NaN to Double.NaN
        val doubleRedBandTile = DoubleArrayTile(redBandTile.toArrayDouble(), redBandTile.cols, redBandTile.rows)
          .convert(DoubleConstantNoDataCellType)
        val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
          .convert(DoubleConstantNoDataCellType)

        //calculate ndvi tile
        val ndviTile = Divide(
          Subtract(doubleNirBandTile, doubleRedBandTile),
          Add(doubleNirBandTile, doubleRedBandTile))
        ndviTile
      }

      val analysisBegin = System.currentTimeMillis()
      val RedOrNearRdd: RDD[(SpaceTimeBandKey, Tile)] = tileLayerRddWithMeta._1.filter { x => {
        x._1.measurementName == redBand || x._1.measurementName == nirRedBand
      }
      }
      val ndviRDD: RDD[(SpaceTimeBandKey, Tile)] = RedOrNearRdd.map(x => (x._1.spaceTimeKey, x._1.measurementName, x._2)).groupBy(_._1).map {
        x => {
          val tilePair = x._2.toArray
          val spaceTimeBandKey: SpaceTimeBandKey = SpaceTimeBandKey(x._1, "ndvi")
          val RedTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
          val NearInfraredTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
          tilePair.foreach { ele =>
            if (ele._2 == redBand) RedTiles.append(ele._3)
            if (ele._2 == nirRedBand) NearInfraredTiles.append(ele._3)
          }
          (spaceTimeBandKey, ndviTile(RedTiles(0), NearInfraredTiles(0)).withNoData(Some(0)))
        }
      }
      println("求解ndvi的时间为：" + (System.currentTimeMillis() - analysisBegin) + "ms")
      (ndviRDD, tileLayerRddWithMeta._2)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //    val coverage1: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = Cube.load(sc, cubeName = "SENTINEL-2 Level-2A MSI", extent = "[4.7, 51.7, 4.71, 51.71]", dateTime = "[2019-01-17 08:9:59,2019-01-18 21:00:01]")
    //    val coverage1: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = Cube.load(sc, cubeName = "SentinelTest cube", extent = "[106.00, 27.60, 106.02, 27.62]", dateTime = "[2019-07-09 07:00:00,2019-07-09 09:00:00]")
    val coverage1: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = Cube.load(sc, cubeName = "Long_sequence_cube", extent = "[104.50, 27.2, 104.8, 27.38]", dateTime = "[2014-01-16 00:00:00, 2016-01-17 23:59:59]")
    //    coverage1._1.map{x => x._1.measurementName}.foreach(x => println(x))

    var vis = new VisualizationParam
    //    vis.setAllParam(bands = "Red", min = "0", max = "500", palette = "[oldlace,peachpuff,gold,olive,lightyellow,yellow,lightgreen,limegreen,brown,lightblue,blue]"), palette = "[lightyellow,yellow,lightgreen,green,brown,OrangeRed,Red]"
    vis.setAllParam(bands = "ndvi", min = "0", max = "500",palette = "[#CEFFCE,#53FF53,#00DB00,#00BB00,#007500,#FF2D2D,#FF0000,#AE0000,#600000]")

    //    val coverage2:(RDD[(SpaceTimeKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Cube.addStyles(coverage1, vis)
    val coverage2: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = Cube.NDVI(tileLayerRddWithMeta = coverage1, bandNames = List("Red", "Near-Infrared"))
    visualizeOnTheFly(sc, coverage2, vis)

  }

  def geoJson2Shape(jsonSting: String, shpPath: String): Unit = {
    val map: mutable.Map[String, String] = mutable.Map[String, String]()
    val gjson: GeometryJSON = new GeometryJSON()
    try {
      val json: JSONObject = JSON.parseObject(jsonSting)
      val features: JSONArray = json.get("features").asInstanceOf[JSONArray]
      val feature0: JSONObject = JSON.parseObject(features.get(0).toString)
      val properties: util.Set[String] = JSON.parseObject(feature0.getString("properties")).keySet()
      val strType: String = feature0.get("geometry").asInstanceOf[JSONObject].getString("type").toString
      var geoType: Class[_] = null
      strType match {
        case "Point" =>
          geoType = classOf[Point]
        case "MultiPoint" =>
          geoType = classOf[MultiPoint]
        case "LineString" =>
          geoType = classOf[LineString]
        case "MultiLineString" =>
          geoType = classOf[MultiLineString]
        case "Polygon" =>
          geoType = classOf[Polygon]
        case "MultiPolygon" =>
          geoType = classOf[MultiPolygon]
      }
      val file: File = new File(shpPath)
      val params: util.Map[String, java.io.Serializable] = new util.HashMap[String, java.io.Serializable]()
      params.put(ShapefileDataStoreFactory.URLP.key, file.toURI.toURL)
      val ds: ShapefileDataStore = new ShapefileDataStoreFactory().createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      val tb: SimpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder()
      tb.setCRS(DefaultGeographicCRS.WGS84)
      tb.setName("shapefile")
      tb.add("the_geom", geoType)
      val propertiesIter: util.Iterator[String] = properties.iterator()
      while (propertiesIter.hasNext) {
        var str: String = propertiesIter.next
        if (str == "省") str = "Province"
        if (str == "省代码")  str = "PCode"
        if (str == "市代码")  str = "CCode"
        if (str == "市") str = "City"
        if (str == "类型") str = "Type"
        tb.add(str, classOf[String])
      }
      val charset: Charset = Charset.forName("utf-8")
      ds.setCharset(charset)
      ds.createSchema(tb.buildFeatureType)
      val writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)
      println(writer.getFeatureType)
      for (i <- 0 until features.size) {
        val strFeature: String = features.get(i).toString
        val reader: Reader = new StringReader(strFeature)
        val feature: SimpleFeature = writer.next
        strType match {
          case "Point" =>
            feature.setAttribute("the_geom", gjson.readPoint(reader))
          case "MultiPoint" =>
            feature.setAttribute("the_geom", gjson.readMultiPoint(reader))
          case "LineString" =>
            feature.setAttribute("the_geom", gjson.readLine(reader))
          case "MultiLineString" =>
            feature.setAttribute("the_geom", gjson.readMultiLine(reader))
          case "Polygon" =>
            feature.setAttribute("the_geom", gjson.readPolygon(reader))
          case "MultiPolygon" =>
            feature.setAttribute("the_geom", gjson.readMultiPolygon(reader))
        }
        val propertiesset: util.Iterator[String] = properties.iterator()
        while (propertiesset.hasNext) {
          var str: String = propertiesset.next
          val featurei: JSONObject = JSON.parseObject(features.get(i).toString)
          val propValue = JSON.parseObject(featurei.getString("properties")).get(str)
          if (str == "省") str = "Province"
          if (str == "省代码")  str = "PCode"
          if (str == "市代码")  str = "CCode"
          if (str == "市") str = "City"
          if (str == "类型") str = "Type"
          try {
            feature.setAttribute(str, propValue.toString)
          } catch {
            case e: Exception =>
              //              feature.setAttribute(str, "")
              println(e)
          }
        }
        writer.write()
      }
      writer.close()
      ds.dispose()
      map.put("status", "success")
      map.put("message", shpPath)
    } catch {
      case e: Exception =>
        map.put("status", "failure")
        map.put("message", e.getMessage)
        e.printStackTrace()
    }
    println(map)
  }

  def floodServices(implicit sc: SparkContext, cubeId: String, rasterProductNames: ArrayBuffer[String], vectorProductNames: ArrayBuffer[String], extentString: String, startTime: String, endTime: String): Unit = {
    //query and access
    val extent: Array[Double] = extentString.replace("[", "").replace("]", "").split(",").map(_.toDouble)
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setVectorProductNames(vectorProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val gridLayerGeoObjectArray: Array[(SpaceTimeKey, Iterable[GeoObject])] = QueryVectorObjects.getGeoObjectsArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    var geoObjectsNum = 0
    val outputDir = s"${GlobalConfig.Others.tempFilePath}"
    gridLayerGeoObjectArray.foreach(x=> geoObjectsNum += x._2.size)
    println("tiles sum: " + tileLayerArrayWithMeta._1.length)
    println("geoObjects sum: " + geoObjectsNum)

    //flood
    val analysisBegin = System.currentTimeMillis()
    impactedFeaturesService(sc, tileLayerArrayWithMeta, gridLayerGeoObjectArray, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles and " + geoObjectsNum + " vector geoObjects: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))
  }



}