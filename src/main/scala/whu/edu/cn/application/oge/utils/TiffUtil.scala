package whu.edu.cn.application.oge.utils

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Raster, Tile, TileLayout}
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import io.minio.{GetObjectArgs, MinioClient}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.Geometry
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.{CoordinateReferenceSystem, ProjectedCRS}
import org.opengis.referencing.operation.{MathTransform, TransformException}
import redis.clients.jedis.Jedis
import whu.edu.cn.application.oge.COGHeaderParse.{getTileBuf, getTiles, parse, tileQuery}
import whu.edu.cn.application.oge.HttpRequest.writeTIFF
import whu.edu.cn.application.oge.Image.{noReSlice, query}
import whu.edu.cn.application.oge.{COGHeaderParse, ImageTrigger, RawTile}
import whu.edu.cn.core.entity
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.ogc.entity.process.CoverageMediaType
import whu.edu.cn.util.{JedisConnectionFactory, SystemConstants, ZCurveUtil}
import whu.edu.cn.util.SystemConstants.{HEAD_SIZE, MINIO_KEY, MINIO_PWD, MINIO_URL}
import whu.edu.cn.util.TileSerializerImage.deserializeTileData

import java.io.{ByteArrayOutputStream, InputStream, RandomAccessFile}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{max, min}
import scala.util.control.Breaks

object TiffUtil {
  /**
   * RDD转tif
   * @param input (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]))
   * @return url of the tif
   */
  def tileRDD2Tiff(input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), coverageType:String) : String = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val coverageTypeEnum = CoverageMediaType.valueOf(coverageType.toUpperCase())
    coverageTypeEnum match{
      case CoverageMediaType.GEOTIFF => {
        val outputTiffPath = "E:\\LaoK\\data2\\APITest\\" + time + ".tif"
        GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
        // TODO 这里应该返回一个href
        outputTiffPath
      }
      case CoverageMediaType.PNG => {
        // TODO 这里将RDD转换为PNG
        null
      }
      case CoverageMediaType.BINARY =>{
        // TODO 这里将RDD转换为Binary
        null
      }
    }

  }

  // TODO geotiff转RDD
  def tiff2RDD(implicit sc: SparkContext, tifHref: String, coverageType:String, geom: String = null,
               mapLevel:Int, productName:String, time:String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageTypeEnum = CoverageMediaType.valueOf(coverageType.toUpperCase())
    val storagePath = "E:\\LaoK\\data2\\APITest\\LC08_L1TP_121037_20180410_20180417_01_T1_B3.tif"
//    writeTIFF(tifHref, storagePath)
    coverageTypeEnum match{
      case CoverageMediaType.GEOTIFF => {
        // TODO geotiff转RDD
        loadTif(sc, (storagePath, productName, time), geom, mapLevel)._1
//        tileRDD2Tiff(a, "geotiff")
//        a
      }
      case CoverageMediaType.PNG => {
        // TODO png转RDD
        null
      }
      case CoverageMediaType.BINARY=>{
        // TODO Binary 转RDD
        null
      }
    }
  }

  /**
   * 加载Tif 转换为RDD
   * @param sc Spark环境
   * @param fileMeta [filePath productName timeStamp]
   * @param geom 空间查询范围
   * @param mapLevel 前端地图层级
   * @return
   */
  def loadTif(implicit sc: SparkContext, fileMeta:(String, String, String), geom:String = null, mapLevel:Int):
  ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile]) ={
    val zIndexStrArray: ArrayBuffer[String] = ImageTrigger.zIndexStrArray

    if (geom != null) {
      val lonLatOfBBox: ListBuffer[Double] = geom.replace("[", "")
        .replace("]", "")
        .split(",").map(_.toDouble).to[ListBuffer]

      val queryExtent = new ArrayBuffer[Array[Double]]()

      val key: String = ImageTrigger.originTaskID + ":solvedTile:" + mapLevel
      val jedis: Jedis = JedisConnectionFactory.getJedis
      jedis.select(1)

      // 通过传进来的前端瓦片编号反算出它们各自对应的经纬度范围 V
      for (zIndexStr <- zIndexStrArray) {
        val xy: Array[Int] = ZCurveUtil.zCurveToXY(zIndexStr, mapLevel)
        val lonMinOfTile: Double = ZCurveUtil.tile2Lon(xy(0), mapLevel)
        val latMinOfTile: Double = ZCurveUtil.tile2Lat(xy(1) + 1, mapLevel)
        val lonMaxOfTile: Double = ZCurveUtil.tile2Lon(xy(0) + 1, mapLevel)
        val latMaxOfTile: Double = ZCurveUtil.tile2Lat(xy(1), mapLevel)

        // 改成用所有前端瓦片范围  和  bbox 的交集
        val lonMinOfQueryExtent: Double =
          if (lonLatOfBBox.head > lonMinOfTile) lonLatOfBBox.head else lonMinOfTile

        val latMinOfQueryExtent: Double =
          if (lonLatOfBBox(1) > latMinOfTile) lonLatOfBBox(1) else latMinOfTile

        val lonMaxOfQueryExtent: Double =
          if (lonLatOfBBox(2) < lonMaxOfTile) lonLatOfBBox(2) else lonMaxOfTile

        val latMaxOfQueryExtent: Double =
          if (lonLatOfBBox.last < latMaxOfTile) lonLatOfBBox(3) else latMaxOfTile
        // 解决方法是用每个瓦片分别和bbox取交集

        if (lonMinOfQueryExtent < lonMaxOfQueryExtent &&
          latMinOfQueryExtent < latMaxOfQueryExtent
        ) {
          // 加入集合，可以把存redis的步骤转移到这里，减轻redis压力
          queryExtent.append(
            Array(
              lonMinOfQueryExtent,
              latMinOfQueryExtent,
              lonMaxOfQueryExtent,
              latMaxOfQueryExtent
            )
          )
//          jedis.sadd(key, zIndexStr)
//          jedis.expire(key, SystemConstants.REDIS_CACHE_TTL)

        } // end if
      } // end for

      val imageInfo = getImageInfo(fileMeta._1)
      jedis.close()
      val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = sc.makeRDD(queryExtent)
        .map(t => { // 合并所有的元数据（追加了范围）
          val rawTiles: util.ArrayList[RawTile] = {
            tileQuery(mapLevel, fileMeta._1, fileMeta._3, imageInfo._1, imageInfo._2, imageInfo._3, imageInfo._4,
              fileMeta._2, t, imageInfo._5, imageInfo._6, imageInfo._7)
          } //TODO
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.size() > 0) asScalaBuffer(rawTiles)
          else mutable.Buffer.empty[RawTile]
        }).distinct()
      // TODO 转化成Scala的可变数组并赋值给tileRDDNoData

      val tileNum: Int = tileRDDNoData.map(t => t.length).reduce((x, y) => {
        x + y
      })
      println("tileNum = " + tileNum)
      val tileRDDFlat: RDD[RawTile] = tileRDDNoData.flatMap(t => t)
      var repNum: Int = tileNum
      if (repNum > 90) {
        repNum = 90
      }
      val tileRDDReP: RDD[RawTile] = tileRDDFlat.repartition(repNum).persist()
      (noReSliceFromFile(sc, tileRDDReP), tileRDDReP)
    }
    else { // geom2 == null，之前批处理使用的代码
      val imageInfo = getImageInfo(fileMeta._1)
//      val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = getTileFromFile(mapLevel, fileMeta._1, null, fileMeta._3, fileMeta._2)
      val metaData = Array[(String, String, String)](fileMeta)
      val imagePathRdd: RDD[(String, String, String)] = sc.parallelize(metaData, metaData.length)
      val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = imagePathRdd.map(t => {
//        val tiles = tileQuery(level, t._1, t._2, t._3, t._4, t._5, t._6, productName, query_extent)
      val tiles = tileQuery(mapLevel, t._1, t._3, imageInfo._1, imageInfo._2, imageInfo._3, imageInfo._4,
          t._2, null, imageInfo._5, imageInfo._6, imageInfo._7)
//        val tiles = getTileFromFile(mapLevel, t._1, null, t._3, t._2)
        if (tiles.size() > 0) asScalaBuffer(tiles)
        else mutable.Buffer.empty[RawTile]
      }).persist() // TODO 转化成Scala的可变数组并赋值给tileRDDNoData
      val tileNum = tileRDDNoData.map(t => t.length).reduce((x, y) => {
        x + y
      })
      println("tileNum = " + tileNum)
      val tileRDDFlat: RDD[RawTile] = tileRDDNoData.flatMap(t => t)
      var repNum = tileNum
      if (repNum > 90) repNum = 90
      val tileRDDReP = tileRDDFlat.repartition(repNum).persist()
      (noReSliceFromFile(sc, tileRDDReP), tileRDDReP)
    }
  }

  def noReSliceFromFile(implicit sc: SparkContext, tileRDDReP: RDD[RawTile]): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val extents: (Double, Double, Double, Double) = tileRDDReP.map(t => {
      (t.getLngLatBottomLeft.head, t.getLngLatBottomLeft.last,
        t.getLngLatUpperRight.head, t.getLngLatUpperRight.last)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant = tileRDDReP.map(t => {
      (t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime, t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), min(a._3, b._3), max(a._4, b._4), max(a._5, b._5), max(a._6, b._6))
    })
    val extent = geotrellis.vector.Extent(extents._1, extents._2, extents._3, extents._4)
    val firstTile = tileRDDReP.take(1)(0)
    val tl = TileLayout(Math.round((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.round((extents._4 - extents._2) / firstTile.getResolution / 256).toInt, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDataType)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    println(tileRDDReP.count())
    val tileRDD = tileRDDReP.map(t => {
      println("ssssssssssss")
      val tile = getTileBufFromFile(t)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val phenomenonTime = sdf.parse(tile.getTime).getTime
      val measurement = tile.getMeasurement
      val rowNum = tile.getRow
      val colNum = tile.getCol
      val Tile = deserializeTileData("", tile.getTileBuf, 256, tile.getDataType)
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum - colRowInstant._1, rowNum - colRowInstant._2, phenomenonTime), measurement)
      val v = Tile
      (k, v)
    })
    val a = tileRDD.collect()
    (tileRDD, tileLayerMetadata)
  }

  /**
   * 获取 tile 影像本体
   *
   * @param tile tile相关数据
   * @return
   */
  def getTileBufFromFile(tile: RawTile): RawTile = {

    val tileLength:Long = Math.abs(tile.getOffset()(1) - tile.getOffset()(0))
    println("长度为"+tileLength)
    val filePath = tile.getPath
    val outStream = new ByteArrayOutputStream
    try {
      val imageFile = new RandomAccessFile(filePath, "r")

      val buffer = new Array[Byte](tileLength.toInt) //16383
      imageFile.seek(tile.getOffset()(0))
      imageFile.read(buffer)
      imageFile.close()
      outStream.write(buffer, 0, tileLength.toInt)
      tile.setTileBuf(outStream.toByteArray)
      outStream.close()

      tile
    }catch {
      case e:Exception => println(e.printStackTrace())
        null
    }
    finally {
      outStream.close()
    }
  }

   def getImageInfo(filePath: String):(String, String, String, String, Int, Int, Int) ={
     // 我需要从影像文件中读取坐标系ID、波段名称（？）、dtype数据位数、分辨率
     // 这些应该要从coverages api中获取
     // 波段名称为1
     val measurement = "1"
     // 初始化GDAL
     gdal.AllRegister()
     gdal.SetConfigOption("GTIFF_SRS_SOURCE", "EPSG")
     // 打开影像文件
     val dataset = gdal.Open(filePath, gdalconstConstants.GA_ReadOnly)
     // 获取影像的分辨率信息
     val geotransform = dataset.GetGeoTransform()
     val height: Int = dataset.getRasterYSize
     val width: Int = dataset.getRasterXSize
     println("影像分辨率：" + geotransform(1) + ", " + geotransform(5))
     // 获取影像的波段数
     val bandCount = dataset.getRasterCount
     println("波段数目" + bandCount)
     // 获取影像的坐标系
     val projection = dataset.GetProjectionRef()
     val crsCode = "EPSG:" + getCRSFromWKT(projection)
     println("CRS的编号" + crsCode)
     val resolutionMeter = transformResolutionByCrs(crsCode, geotransform(1))
     println("转换后的分辨率：" + resolutionMeter)
     // 获取影像的数据位数
     val band = dataset.GetRasterBand(1)
     val dataType = gdal.GetDataTypeName(band.getDataType()).toLowerCase
     println("影像数据位数：" + dataType)
     // 关闭数据集
     dataset.delete()
      (crsCode, measurement, dataType, resolutionMeter.toString, height, width, bandCount)
   }

//  /**
//   *
//   * @param filePath tif 的文件路径
//   * @param queryExtent 空间查询范围
//   */
//  def getTileFromFile(mapLevel:Int, filePath: String, queryExtent: Array[Double], timeStamp:String,
//                      productName:String): util.ArrayList[RawTile]={
//
//
//
//    tileQuery(mapLevel, filePath, timeStamp , crsCode, measurement, dataType, resolutionMeter.toString, productName,
//      queryExtent, height, width, bandCount)
//  }

  /**
   * 根据元数据查询 tile
   * @param mapLevel JSON中的level字段，前端层级
   * @param filePath COG的路径
   * @param time  影像元数据 时间
   * @param crs 影像的crs
   * @param measurement 影像的波段 当波段数不为1的时候设置为空
   * @param dType 影像的位数
   * @param resolution 影像的分辨率
   * @param productName 产品名
   * @param queryExtent 前端视口范围内的每一个瓦片和Bbox的交集
   * @param bandCount   波段数目
   * @param tileSize    瓦片尺寸
   * @return 后端瓦片
   */
  def tileQuery(mapLevel: Int, filePath: String,
                time: String, crs: String,
                measurement: String, dType: String,
                resolution: String, productName: String,
                queryExtent: Array[Double],
                imageHeight:Int, imageWidth: Int,
                bandCount: Int = 1,
                tileSize: Int = 256): util.ArrayList[RawTile] = {

    val imageSize: Array[Int] = Array[Int](0, 0) // imageLength & imageWidth
    val tileByteCounts = new util.ArrayList[util.ArrayList[util.ArrayList[Integer]]]
    val geoTrans = new util.ArrayList[Double] //TODO 经纬度
    val cell = new util.ArrayList[Double]
    val tileOffsets = new util.ArrayList[util.ArrayList[util.ArrayList[Integer]]]
    try {
      // 根据前端图层层级，判断需要切的层级，然后调用脚本切COG
      var targetFilePath = "E:\\LaoK\\data2\\APITest\\LC08_L1TP_121037_20180410_20180417_01_T1_B3.tif"
      val COGLevel = transformCOG(mapLevel, filePath, targetFilePath, imageHeight, imageWidth, resolution.toDouble)
      // 读取转换后的COG
      val imageFile = new RandomAccessFile(targetFilePath, "r")
      val outStream = new ByteArrayOutputStream
      val buffer = new Array[Byte](HEAD_SIZE) //16383
      imageFile.seek(0)
      imageFile.read(buffer)
      imageFile.close()
      outStream.write(buffer, 0, HEAD_SIZE)

      val headerByte: Array[Byte] = outStream.toByteArray
      outStream.close()
      // 填充信息
      parse(headerByte, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount, tileSize)
      //
      getCOGTiles(COGLevel, queryExtent, crs, cell, geoTrans, tileByteCounts, productName, targetFilePath, bandCount, tileSize, tileOffsets, time, measurement, dType)
      //getTiles(level, queryExtent, crs, filePath, time, measurement, dType, resolution, productName, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, tileSize)

    }
    catch {
      case e: Exception => println("Caught exception in finally block: " + e.getMessage)
        null
    }
//    finally {
//      println("error in read file")
//    }

  }

  /**
   * 该函数根据前端地图层级将一张tif影像按照前端地图层级切COG（只切那与前端地图层级对应的那一级），返回后端COG的层级
   * COGLevel,该层级是二叉树划分的层级 2的COGLevel次方
   * @param mapLevel 前端地图层级
   * @param sourceFilePath 原影像文件
   * @param targetSourcePath 目标影像文件
   * @param imageHeight 影像的高度
   * @param imageWidth 影像的宽度
   * @param imageResolution 影像的分辨率
   * @return COGLevel 前端对应的COG的层级
   */
  def transformCOG(mapLevel:Int, sourceFilePath: String, targetSourcePath: String, imageHeight:Int, imageWidth:Int, imageResolution:Double):Int={
    var COGLevel = 0
    var resolutionTMS = 0.0
    val resolutionTMSArray: Array[Double] =
      Array(
        156543.033928, /* 地图 zoom 为0时的分辨率，以下按zoom递增 */
        78271.516964,
        39135.758482,
        19567.879241,
        9783.939621,
        4891.969810,
        2445.984905,
        1222.992453,
        611.496226,
        305.748113,
        152.874057,
        76.437028,
        38.218514,
        19.109257,
        9.554629,
        4.777314,
        2.388657,
        1.194329,
        0.597164,
        0.298582,
        0.149291
      )

    if (mapLevel == -1){
      // 批处理
      COGLevel = 0
    } else {
      // 获取前端分辨率
      resolutionTMS = resolutionTMSArray(mapLevel)
      // 获取与前端分辨率对应的COG的level，注意这里的level是值相对于0层的2的level次方
//      COGLevel = Math.ceil(Math.log(resolutionTMS / imageResolution) / Math.log(2)).toInt + 1
      COGLevel = Math.ceil(Math.log(resolutionTMS / imageResolution) / Math.log(2)).toInt
      // maxMapLevle是COG最高分辨率对应的前端分辨率
      var maxMapLevel = 0
      val loop = new Breaks
      loop.breakable {
        for (i <- resolutionTMSArray.indices) {
//          if (Math.ceil(Math.log(resolutionTMSArray(i) / imageResolution)
//            / Math.log(2)).toInt + 1 == 0) {
          if (Math.ceil(Math.log(resolutionTMSArray(i) / imageResolution)
            / Math.log(2)).toInt == 0) {
            maxMapLevel = i
            loop.break()
          }
        }
      }
      // 判断COG能否切到COGLevel层
      // 首先计算COG能切到的最高层级的level
      var maxCOGLevel = 0
      while((imageWidth/Math.pow(2, maxCOGLevel))>256 && (imageHeight/Math.pow(2, maxCOGLevel)>256)){
        maxCOGLevel = maxCOGLevel + 1
      }
      //这是指前端最接近的层级
      COGHeaderParse.nearestZoom = ImageTrigger.level
      // 如果大于前端层级 那么就使用最高层级
      if(COGLevel > maxCOGLevel){
        COGLevel = maxCOGLevel
        COGHeaderParse.nearestZoom = maxMapLevel - maxCOGLevel
      }
    }
    // TODO 调用Python脚本切成COG 只切特定层级的 即COGLevel
    COGLevel
  }

  /**
   * 该函数用于获取瓦片信息,不包括Tile实际信息
   * @param COGLevel COG层级
   * @param queryExtent 查询范围
   * @param crs 坐标系
   * @param cell cell 一个Double数组
   * @param geoTrans 空间转换
   * @param tileByteCounts tileByteCounts
   * @param productName 产品名称
   * @param filePath 文件路径
   * @param bandCount 波段数
   * @param tileSize 瓦片大小
   * @param tileOffsets 瓦片偏移量
   * @param time 时间
   * @param measurement 波段
   * @param dType 位数 float32
   * @return RawTile列表 util.ArrayList[RawTile]
   */
  def getCOGTiles(COGLevel:Int, queryExtent:Array[Double], crs:String, cell: util.ArrayList[Double], geoTrans: util.ArrayList[Double],
                  tileByteCounts: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]], productName:String,  filePath:String,
                  bandCount: Int, tileSize: Int, tileOffsets: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                  time:String, measurement:String, dType:String):util.ArrayList[RawTile]={
    var lowerLeftLongitude: Double = -180
    var lowerLeftLatitude: Double = -90
    var upperRightLongitude: Double = 180
    var upperRightLatitude: Double = 90
    if(queryExtent != null){
      lowerLeftLongitude = queryExtent(0)
      lowerLeftLatitude = queryExtent(1)
      upperRightLongitude = queryExtent(2)
      upperRightLatitude = queryExtent(3)
    }
    val pointLower = new Coordinate
    val pointUpper = new Coordinate
    pointLower.setX(lowerLeftLatitude)
    pointLower.setY(lowerLeftLongitude)
    pointUpper.setX(upperRightLatitude)
    pointUpper.setY(upperRightLongitude)
    var pointLowerReprojected = new Coordinate
    var pointUpperReprojected = new Coordinate
    var flag = false
    var flagReader = false
    try if ("EPSG:4326" == crs) {
      pointLowerReprojected = pointLower
      pointUpperReprojected = pointUpper
    }
    else {
      val crsSource: CoordinateReferenceSystem = org.geotools.referencing.CRS.decode("EPSG:4326")
      val crsTarget: CoordinateReferenceSystem = org.geotools.referencing.CRS.decode(crs)
      if (crsTarget.isInstanceOf[ProjectedCRS]) flag = true
      val transform: MathTransform = org.geotools.referencing.CRS.findMathTransform(crsSource, crsTarget)
      JTS.transform(pointLower, pointLowerReprojected, transform)
      JTS.transform(pointUpper, pointUpperReprojected, transform)
    }
    catch {
      case e@(_: FactoryException | _: TransformException) =>
        e.printStackTrace()
    }
    val pMin = new Array[Double](2)
    val pMax = new Array[Double](2)
    pMin(0) = pointLowerReprojected.getX
    pMin(1) = pointLowerReprojected.getY
    pMax(0) = pointUpperReprojected.getX
    pMax(1) = pointUpperReprojected.getY
    // 图像范围
    // 东西方向空间分辨率  --->像素宽度
    val w_src: Double = cell.get(0)
    // 南北方向空间分辨率 ---> 像素高度 // TODO
    val h_src: Double = cell.get(1)
    // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
    var xMin = .0
    var yMax = .0
    if (flag) {
      xMin = geoTrans.get(3)
      yMax = geoTrans.get(4)
    }
    else {
      xMin = geoTrans.get(4)
      yMax = geoTrans.get(3)
    }
    val tileSearch = new util.ArrayList[RawTile]
    productName match {
      case "MOD13Q1_061" =>
      case "LJ01_L2" =>
      case "ASTER_GDEM_DEM30" =>
      case "GPM_Precipitation_China_Month" =>
        flagReader = true
      case _ =>
    }
    //计算目标影像的左上和右下图上坐标
    var p_left = 0
    var p_right = 0
    var p_lower = 0
    var p_upper = 0
    if (flag) {
      p_left = ((pMin(0) - xMin) / (tileSize * w_src * Math.pow(2, COGLevel).toInt)).toInt
      p_right = ((pMax(0) - xMin) / (tileSize * w_src * Math.pow(2, COGLevel).toInt)).toInt
      p_lower = ((yMax - pMax(1)) / (tileSize * h_src * Math.pow(2, COGLevel).toInt)).toInt
      p_upper = ((yMax - pMin(1)) / (tileSize * h_src * Math.pow(2, COGLevel).toInt)).toInt
    }
    else {
      p_lower = ((pMin(1) - yMax) / (tileSize * w_src * Math.pow(2, COGLevel).toInt)).toInt
      p_upper = ((pMax(1) - yMax) / (tileSize * w_src * Math.pow(2, COGLevel).toInt)).toInt
      p_left = ((xMin - pMax(0)) / (tileSize * h_src * Math.pow(2, COGLevel).toInt)).toInt
      p_right = ((xMin - pMin(0)) / (tileSize * h_src * Math.pow(2, COGLevel).toInt)).toInt
    }
    var pCoordinate: Array[Int] = null
    var srcSize: Array[Double] = null
    var pRange: Array[Double] = null
    if (!flagReader) {
      pCoordinate = Array[Int](p_lower, p_upper, p_left, p_right)
      srcSize = Array[Double](w_src, h_src)
      pRange = Array[Double](xMin, yMax)
    }
    else {
      srcSize = Array[Double](h_src, w_src)
      pRange = Array[Double](yMax, xMin)
      pCoordinate = Array[Int](p_left, p_right, p_lower, p_upper)
    }
    // levelIndex 只有0和1
    var levelIndex = 0
    if(COGLevel != 0){
      levelIndex = 1
    }
    try{
      for (i <- Math.max(pCoordinate(0), 0) to (
        if (pCoordinate(1) >= tileOffsets.get(levelIndex).size / bandCount)
          tileOffsets.get(levelIndex).size / bandCount - 1
        else pCoordinate(1))
           ) {
        for (j <- Math.max(pCoordinate(2), 0) to (
          if (pCoordinate(3) >= tileOffsets.get(levelIndex).get(i).size)
            tileOffsets.get(levelIndex).get(i).size - 1
          else pCoordinate(3))
             ) {
          for (k <- 0 until bandCount) {
            val t = new RawTile
            t.setOffset(
              tileOffsets.get(levelIndex).get(i).get(j).toLong,
              tileByteCounts.get(levelIndex).get(i).get(j) + tileOffsets.get(levelIndex).get(i).get(j).toLong
//              tileByteCounts.get(levelIndex).get(i).get(j) + t.getOffset()(0)
            )


            t.setLngLatBottomLeft(
              j * (tileSize * srcSize(0) * Math.pow(2, COGLevel).toInt) + pRange(0),
              (i + 1) * (tileSize * -srcSize(1) * Math.pow(2, COGLevel).toInt) + pRange(1)
            )
            t.setLngLatUpperRight(
              (j + 1) * (tileSize * srcSize(0) * Math.pow(2, COGLevel).toInt) + pRange(0),
              i * (tileSize * -srcSize(1) * Math.pow(2, COGLevel).toInt) + pRange(1)
            )
            t.setRotation(geoTrans.get(5))
            t.setResolution(w_src * Math.pow(2, COGLevel).toInt)
            t.setRowCol(i, j)
            t.setLevel(COGLevel)
            t.setPath(filePath)
            t.setTime(time)
            if (bandCount == 1) {
              t.setMeasurement(measurement)
            }
            else {
              t.setMeasurement(bandCount.toString) //TODO
            }
            t.setCrs(crs.replace("EPSG:", "").toInt)
            t.setDataType(dType)
            t.setProduct(productName)
            tileSearch.add(t)
          }
        }
      }
    } catch {
      case e: Exception => println("Caught exception in finally block: " + e.getMessage)
        null
    }
    tileSearch
  }

  /**
   * 获取4326下的坐标系 但是效率很低 会将整个影像读取到内存中 不建议使用
   * @param filePath 影像路径
   * @return
   */
  def getSpatialExtent(filePath:String):Array[Double]={
  val tiffPath = filePath
  val geoTiff = SinglebandGeoTiff(tiffPath)

  val sourceCRS = geoTiff.crs
  val destCRS = geotrellis.proj4.CRS.fromEpsgCode(4326)

  val extent = geoTiff.extent.reproject(sourceCRS, destCRS)
  val extentArray = Array(extent.xmin, extent.ymin, extent.xmax, extent.ymax)
  println(s"Extent array: ${extentArray.mkString(", ")}")
  extentArray
}


  def getCRSFromWKT(srsWKT:String):String = {
    // 解析WKT格式
    val crs:CoordinateReferenceSystem = org.geotools.referencing.CRS.parseWKT(srsWKT)
    // 获取 CRS
    val crsCode: String = org.geotools.referencing.CRS.lookupEpsgCode(crs, true).toString
    crsCode
//    // 定义用于转换分辨率的 MathTransform 对象
//    val transform: MathTransform = CRS.findMathTransform(crs, CRS.decode("EPSG:3857"), true)

    // 定义一个分辨率值，假设为 1 度
    //val resolution: Double = 1.0

    // 转换分辨率的单位为米
//    val resolutionInMeters: Double = resolution * transform.transform(crs.getCoordinateSystem.getAxis(1).getMinimumValue, crs.getCoordinateSystem.getAxis(0).getMinimumValue).getScaleX
  }


  def transformResolutionByCrs(crs:String, resolution:Double):Double= {
    if(crs.equals("EPSG:4326")){
      val metersPerDegree = 2 * Math.PI * 6378137 / 360
      return resolution * metersPerDegree
    }else if(crs.equals("EPSG:32650")){
      return resolution
    }else{
      return 0.0
    }
  }

//  def convertResolution(srsWKT: String, resolution: Double): Double = {
//    // Create a CoordinateReferenceSystem object for the source and target coordinate systems
//    val sourceCRS: CoordinateReferenceSystem = CRS.parseWKT(srsWKT)
//    val targetCRS: CoordinateReferenceSystem = CRS.decode("EPSG:3857")
//
//    // Create a MathTransform object for the source and target coordinate systems
//    val transform: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)
//
//    // Transform the resolution from the source to the target coordinate system
//    val pointSource: Array[Double] = Array(0, 0)
//    val pointTarget: Array[Double] = new Array[Double](2)
//    transform.transform(pointSource, 0, pointTarget, 0, 1)
//    val resolutionTransformed: Double = resolution * Math.abs(pointTarget(0) - pointSource(0))
//
//    resolutionTransformed
//  }

  def main(args: Array[String]): Unit = {
    val queryExtent = Array(73.62, 18.19, 134.7601467382, 53.54)
    // 在这里写入你的程序逻辑
    //getTileFromFile("E:\\LaoK\\data2\\GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_VC_ARD.tif", queryExtent)
    getSpatialExtent("E:\\LaoK\\data2\\GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_VC_ARD.tif")
  }

}
