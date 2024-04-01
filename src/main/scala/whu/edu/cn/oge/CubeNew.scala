package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import geotrellis.layer.SpatialKey
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{DoubleConstantNoDataCellType, Raster, Tile}
import geotrellis.vector.Extent
import io.minio.messages.Item
import io.minio.{ListObjectsArgs, MinioClient, Result}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_HEAD_SIZE
import whu.edu.cn.entity.CoverageMetadata
import whu.edu.cn.entity.cube._
import whu.edu.cn.util.MinIOUtil
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection
import whu.edu.cn.util.cube.CubePostgresqlUtil._
import whu.edu.cn.util.cube.CubeUtil.{cogHeaderBytesParse, getCubeDataType}

import java.lang
import java.sql.ResultSet
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex

object CubeNew {

  def loadCubeByTile(implicit sc: SparkContext, cubeId: String, productString: String, bandString: String, timeString: String, extentString: String, tms: String, resolution: Double): RDD[(CubeTileKey, Tile)] = {
    val product: Array[String] = productString.substring(1, productString.length - 1).split(",")
    val band: Array[String] = bandString.substring(1, bandString.length - 1).split(",")
    val time: Array[String] = timeString.substring(1, timeString.length - 1).split(",")
    val extent: Array[Double] = extentString.substring(1, extentString.length - 1).split(",").map(_.toDouble)
    loadCubeSubsetJointByTile(sc, cubeId, product, band, time, extent(0), extent(1), extent(2), extent(3), tms, resolution)
  }

  def loadCubeByImage(implicit sc: SparkContext, cubeId: String, productString: String, bandString: String, timeString: String, extentString: String, tms: String, resolution: Double): RDD[(CubeTileKey, Tile)] = {
    val product: Array[String] = productString.substring(1, productString.length - 1).split(",")
    val band: Array[String] = bandString.substring(1, bandString.length - 1).split(",")
    val time: Array[String] = timeString.substring(1, timeString.length - 1).split(",")
    val extent: Array[Double] = extentString.substring(1, extentString.length - 1).split(",").map(_.toDouble)
    loadCubeSubsetJointByImage(sc, cubeId, product, band, time, extent(0), extent(1), extent(2), extent(3), tms, resolution)
  }

  def bandRadiometricCalibration(cubeRDD: RDD[(CubeTileKey, Tile)], gain: Double, offset: Double): RDD[(CubeTileKey, Tile)] = {
    cubeRDD.map(t => {
      val tile: Tile = t._2.convert(DoubleConstantNoDataCellType)
      val newTile: Tile = tile.mapDouble(x => x * gain + offset)
      (t._1, newTile)
    })
  }

  def normalizedDifference(cubeRDD: RDD[(CubeTileKey, Tile)], bandName1: String, platform1: String, bandName2: String, platform2: String): RDD[(CubeTileKey, Tile)] = {
    val bandKey1: BandKey = new BandKey(bandName1, platform1)
    val bandKey2: BandKey = new BandKey(bandName2, platform2)
    var platformNew: String = ""
    if (platform1.equals(platform2)) {
      platformNew = platform1
    } else {
      platformNew = platform1 + "_" + platform2
    }
    val cubeProcess1: RDD[(CubeTileKey, Tile)] = cubeRDD.filter((cubeTile: (CubeTileKey, Tile)) => cubeTile._1.bandKey.equals(bandKey1) || cubeTile._1.bandKey.equals(bandKey2))
    val cubeProcess2: RDD[((String, Int, Int, Long, String, String), Iterable[(CubeTileKey, Tile)])] = cubeProcess1.groupBy(t => (t._1.spaceKey.tms, t._1.spaceKey.col, t._1.spaceKey.row, t._1.timeKey.time, t._1.productKey.productName, t._1.productKey.productType))
    val cubeProcess3: RDD[(CubeTileKey, Tile)] = cubeProcess2.map((cubeTile: ((String, Int, Int, Long, String, String), Iterable[(CubeTileKey, Tile)])) => {
      // TODO 同一个Space, time, band, product可能涉及多个瓦片！！！！因为卫星轨道有旁向和航向重叠！！！！
      val band1: Tile = cubeTile._2.filter(_._1.bandKey.equals(bandKey1)).map(_._2).localMean().convert(DoubleConstantNoDataCellType)
      val band2: Tile = cubeTile._2.filter(_._1.bandKey.equals(bandKey2)).map(_._2).localMean().convert(DoubleConstantNoDataCellType)
      val normalizedDifferenceTile: Tile = band1.localSubtract(band2).localDivide(band1.localAdd(band2))
      (new CubeTileKey(cubeTile._2.head._1.spaceKey, cubeTile._2.head._1.timeKey, cubeTile._2.head._1.productKey, new BandKey("NDVI", platformNew)), normalizedDifferenceTile)
    })
    cubeProcess3
  }

  //挑选产品和波段来进行可视化
  def visualization(cubeRDD: RDD[(CubeTileKey, Tile)]): Unit = {
    cubeRDD.map(t => ((t._1.productKey.productName, t._1.productKey.productType, t._1.bandKey.bandName, t._1.bandKey.bandPlatform, t._1.timeKey.time), (t._1.spaceKey, t._2))).groupByKey(16).foreach(t => {
      val productName: String = t._1._1
      val productType: String = t._1._2
      val bandName: String = t._1._3
      val bandPlatform: String = t._1._4
      val timestampSeconds: Long = t._1._5
      val time: String = LocalDateTime.ofEpochSecond(timestampSeconds, 0, ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      val tms: String = t._2.head._1.tms
      val coverageArray: Array[(SpatialKey, Tile)] = t._2.toArray.map(x => (new SpatialKey(x._1.col, x._1.row), x._2))
      val extentTuple4: (Double, Double, Double, Double) = t._2.map(x => (x._1.minX, x._1.minY, x._1.maxX, x._1.maxY)).reduce((x, y) => (math.min(x._1, y._1), math.min(x._2, y._2), math.max(x._3, y._3), math.max(x._4, y._4)))
      val extent: Extent = new Extent(extentTuple4._1, extentTuple4._2, extentTuple4._3, extentTuple4._4)
      val crs: CRS = tms match {
        case "WGS1984Quad" => CRS.fromName("EPSG:4326")
        case "WebMercatorQuad" => CRS.fromName("EPSG:3857")
        // TODO rHEALPixCustom的CRS如何解决？？？
        case "rHEALPixCustom" => CRS.fromName("EPSG:3785")
        case _ => CRS.fromName("EPSG:4326")
      }


      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
      val stitchedTile: Raster[Tile] = Raster(tile, extent)

      val saveFilePath: String = "D:\\组内项目\\DGGS-Cube计算优化\\DGGS\\data\\test\\" + productName + "_" + productType + "_" + bandName + "_" + bandPlatform + "_" + timestampSeconds.toString + ".tif"
      GeoTiff(stitchedTile, crs).write(saveFilePath)
    })
  }

  def createCubeFromCollectionByTile(cubeName: String, tms: String, cubeDescription: String, productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: String = null, endTime: String = null, extent: Extent = null, crs: CRS = null): Int = {
    val time1: Long = System.currentTimeMillis()
    // TODO 搞唯一键！避免多次查询！！！！


    // TODO 仅限测试！！！！把失败的表和信息全部删掉重来
    dropCubeMetaTableGroup(3)
    // 1. 创建Cube的元数据表，并获取唯一的CubeId
    val cubeId: Int = createCubeMetaTableGroupByTile(cubeName, tms, cubeDescription)
    // 2. 把OGE中的数据加载到Cube中
    // 2.1 查询OGE中的数据
    val imageMetadata: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, extent, crs)
    // 循环查到的元数据
    for (metadata <- imageMetadata) {
      // TODO 2.2 看看目前有没有转成Cube-COG
      // TODO 2.3 如果没有转成Cube-COG，那么就转成Cube-COG
      // 2.4 把Cube-COG加载到Cube中
      // 2.4.1 准备oc_product的数据
      val productName: String = metadata.getProduct
      val productType: String = metadata.getProductType
      val productDescription: String = metadata.getProductDescription
      if (!isDataInTable("oc_product_" + cubeId, Array("product_name"), Array(productName))) {
        insertDataToTable("oc_product_" + cubeId, Array("product_name", "product_type", "product_description"), Array(productName, productType, productDescription))
      }
      // 2.4.2 准备oc_band的数据
      val bandName: String = metadata.getMeasurement
      val bandPlatform: String = metadata.getPlatformName
      if (!isDataInTable("oc_band_" + cubeId, Array("band_name", "band_platform"), Array(bandName, bandPlatform))) {
        // band的其他信息可以去Postgresql中查询
        // 根据band_name和band_platform查询oc_band表，一定是有的
        val bandResultSet: ResultSet = selectDataFromTable("oc_band", Array("band_name", "band_platform"), Array(bandName, bandPlatform))
        val cubeBandMetadata: CubeBandMetadata = new CubeBandMetadata
        while (bandResultSet.next()) {
          cubeBandMetadata.setBandName(bandResultSet.getString("band_name"))
          cubeBandMetadata.setBandUnit(bandResultSet.getString("band_unit"))
          cubeBandMetadata.setBandMin(bandResultSet.getDouble("band_min"))
          cubeBandMetadata.setBandMax(bandResultSet.getDouble("band_max"))
          cubeBandMetadata.setBandScale(bandResultSet.getDouble("band_scale"))
          cubeBandMetadata.setBandOffset(bandResultSet.getDouble("band_offset"))
          cubeBandMetadata.setBandDescription(bandResultSet.getString("band_description"))
          cubeBandMetadata.setBandResolution(bandResultSet.getDouble("band_resolution"))
          cubeBandMetadata.setBandPlatform(bandResultSet.getString("band_platform"))
        }
        insertDataToTable("oc_band_" + cubeId, Array("band_name", "band_platform", "band_unit", "band_min", "band_max", "band_scale", "band_offset", "band_description", "band_resolution"), Array(bandName, bandPlatform, cubeBandMetadata.getBandUnit, cubeBandMetadata.getBandMin, cubeBandMetadata.getBandMax, cubeBandMetadata.getBandScale, cubeBandMetadata.getBandOffset, cubeBandMetadata.getBandDescription, cubeBandMetadata.getBandResolution))
      }
      // 2.4.3 准备oc_time的数据
      val timeCOG: LocalDateTime = metadata.getTime
      // 得到时间分辨率是一天的time_level_key
      val timeLevelKeyResultSet: ResultSet = selectDataFromTable("oc_time_level", Array("time_level", "resolution"), Array("D", "1"))
      var timeLevelKey: Int = 0
      while (timeLevelKeyResultSet.next()) {
        timeLevelKey = timeLevelKeyResultSet.getInt("time_level_key")
      }
      // 插入oc_time表
      val timeStamp: String = timeCOG.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      if (!isDataInTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))) {
        insertDataToTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))
      }
      // 2.4.4 准备oc_extent, oc_time和oc_tile_fact的数据，因为单个tile是和extent和time有关的
      // 2.4.4.1 先知道有哪些Cube-COG
      val imagePath: String = metadata.getPath
      val cubeImagePathPrefix: String = imagePath.replace(".tif", "") + "/" + tms
      val client: MinioClient = MinIOUtil.getMinioClient
      val iterable: lang.Iterable[Result[Item]] = client.listObjects(ListObjectsArgs.builder().bucket("oge-cube").recursive(true).prefix(cubeImagePathPrefix).build())
      val iterator: java.util.Iterator[Result[Item]] = iterable.iterator()
      val cubeImagePathList: ListBuffer[String] = ListBuffer.empty[String]
      while (iterator.hasNext) {
        val item: Item = iterator.next().get()
        val objectName: String = item.objectName()
        cubeImagePathList.append(objectName)
      }
      // 得到productKey
      val productKeyResultSet: ResultSet = selectDataFromTable("oc_product_" + cubeId, Array("product_name"), Array(productName))
      var productKey: Int = 0
      while (productKeyResultSet.next()) {
        productKey = productKeyResultSet.getInt("product_key")
      }
      // 得到bandKey
      val bandKeyResultSet: ResultSet = selectDataFromTable("oc_band_" + cubeId, Array("band_name", "band_platform"), Array(bandName, bandPlatform))
      var bandKey: Int = 0
      while (bandKeyResultSet.next()) {
        bandKey = bandKeyResultSet.getInt("band_key")
      }
      // 得到timeKey
      val timeKeyResultSet: ResultSet = selectDataFromTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))
      var timeKey: Int = 0
      while (timeKeyResultSet.next()) {
        timeKey = timeKeyResultSet.getInt("time_key")
      }

      // 2.4.4.2 循环Cube-COG，并开始准备瓦片元数据
      for (cubeImagePath <- cubeImagePathList) {
        val headerBytes: Array[Byte] = MinIOUtil.getMinioObject("oge-cube", cubeImagePath, 0, MINIO_HEAD_SIZE)
        val cubeCOGMetadata: CubeCOGMetadata = cogHeaderBytesParse(headerBytes)
        val compression: Int = cubeCOGMetadata.getCompression
        val dataType: OGECubeDataType.OGECubeDataType = getCubeDataType(cubeCOGMetadata.getSampleFormat, cubeCOGMetadata.getBitPerSample)
        // 整理所有的extent信息
        // 先通过cubeImagePath得到extent_level
        val pattern: Regex = "z(\\d+)\\.tif".r
        val matchResult: String = pattern.findFirstMatchIn(cubeImagePath).get.toString()
        val extentLevel: Int = matchResult.replace("z", "").replace(".tif", "").toInt
        // 然后通过extentLevel和tms，在oc_extent_level表得到extent_level_key
        val extentLevelKeyResultSet: ResultSet = selectDataFromTable("oc_extent_level", Array("extent_level", "tms"), Array(extentLevel.toString, tms))
        var extentLevelKey: Int = 0
        var tmsExtent: String = ""
        while (extentLevelKeyResultSet.next()) {
          extentLevelKey = extentLevelKeyResultSet.getInt("extent_level_key")
          tmsExtent = extentLevelKeyResultSet.getString("extent")
        }
        val tmsExtentJSONObject: JSONObject = JSON.parseObject(tmsExtent)
        val minXTMSExtent: Double = tmsExtentJSONObject.getDouble("min_x")
        val maxYTMSExtent: Double = tmsExtentJSONObject.getDouble("max_y")
        // 首先找到所有瓦片共用的信息
        for (rowKeyInCOG <- cubeCOGMetadata.getTileOffsets.indices) {
          for (colKeyInCOG <- cubeCOGMetadata.getTileOffsets(rowKeyInCOG).indices) {
            val tileOffset: Int = cubeCOGMetadata.getTileOffsets(rowKeyInCOG)(colKeyInCOG)
            val tileByteCount: Int = cubeCOGMetadata.getTileByteCounts(rowKeyInCOG)(colKeyInCOG)
            // 计算extent
            val geoTransform: Array[Double] = cubeCOGMetadata.getGeoTransform
            val cellScale: Array[Double] = cubeCOGMetadata.getCellScale
            val minX: Double = geoTransform(3) + colKeyInCOG * cellScale(0) * 256
            val maxY: Double = geoTransform(4) - rowKeyInCOG * cellScale(1) * 256

            val col: Int = math.round((minX - minXTMSExtent) / cellScale(0) / 256).toInt
            val row: Int = math.round((maxYTMSExtent - maxY) / cellScale(1) / 256).toInt

            val colTotal: Int = math.round(2 * -minXTMSExtent / cellScale(0) / 256).toInt
            val rowTotal: Int = math.round(2 * maxYTMSExtent / cellScale(0) / 256).toInt
            val minXExamined: Double = minXTMSExtent + 2 * -minXTMSExtent / colTotal * col
            val maxXExamined: Double = minXTMSExtent + 2 * -minXTMSExtent / colTotal * (col + 1)
            val maxYExamined: Double = maxYTMSExtent - 2 * maxYTMSExtent / rowTotal * row
            val minYExamined: Double = maxYTMSExtent - 2 * maxYTMSExtent / rowTotal * (row + 1)

            if (!isDataInTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col"), Array(extentLevelKey.toString, row.toString, col.toString))) {
              insertDataToTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col", "min_x", "min_y", "max_x", "max_y"), Array(extentLevelKey.toString, row.toString, col.toString, minXExamined.toString, minYExamined.toString, maxXExamined.toString, maxYExamined.toString))
            }
            // 得到extentKey
            val extentKeyResultSet: ResultSet = selectDataFromTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col"), Array(extentLevelKey.toString, row.toString, col.toString))
            var extentKey: Int = 0
            while (extentKeyResultSet.next()) {
              extentKey = extentKeyResultSet.getInt("extent_key")
            }
            // 插入oc_tile_fact表
            if (!isDataInTable("oc_tile_fact_" + cubeId, Array("product_key", "band_key", "extent_key", "time_key", "tile_offset", "tile_byte_count", "compression", "path"), Array(productKey.toString, bandKey.toString, extentKey.toString, timeKey.toString, tileOffset.toString, tileByteCount.toString, compression.toString, cubeImagePath))) {
              insertDataToTable("oc_tile_fact_" + cubeId, Array("product_key", "band_key", "extent_key", "time_key", "tile_offset", "tile_byte_count", "compression", "data_type", "path"), Array(productKey.toString, bandKey.toString, extentKey.toString, timeKey.toString, tileOffset.toString, tileByteCount.toString, compression.toString, dataType.toString, cubeImagePath))
            }
          }
        }
      }
    }
    val time2: Long = System.currentTimeMillis()
    println("从Collection创建Cube完毕，耗时：" + (time2 - time1) + "ms")
    cubeId
  }

  def createCubeFromCollectionByImage(cubeName: String, tms: String, cubeDescription: String, productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: String = null, endTime: String = null, extent: Extent = null, crs: CRS = null): Int = {
    val time1: Long = System.currentTimeMillis()
    // TODO 搞唯一键！避免多次查询！！！！


    // TODO 仅限测试！！！！把失败的表和信息全部删掉重来
    dropCubeMetaTableGroup(3)
    // 1. 创建Cube的元数据表，并获取唯一的CubeId
    val cubeId: Int = createCubeMetaTableGroupByImage(cubeName, tms, cubeDescription)
    // 2. 把OGE中的数据加载到Cube中
    // 2.1 查询OGE中的数据
    val imageMetadata: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, extent, crs)
    // 循环查到的元数据
    for (metadata <- imageMetadata) {
      // TODO 2.2 看看目前有没有转成Cube-COG
      // TODO 2.3 如果没有转成Cube-COG，那么就转成Cube-COG
      // 2.4 把Cube-COG加载到Cube中
      // 2.4.1 准备oc_product的数据
      val productName: String = metadata.getProduct
      val productType: String = metadata.getProductType
      val productDescription: String = metadata.getProductDescription
      if (!isDataInTable("oc_product_" + cubeId, Array("product_name"), Array(productName))) {
        insertDataToTable("oc_product_" + cubeId, Array("product_name", "product_type", "product_description"), Array(productName, productType, productDescription))
      }
      // 2.4.2 准备oc_band的数据
      val bandName: String = metadata.getMeasurement
      val bandPlatform: String = metadata.getPlatformName
      if (!isDataInTable("oc_band_" + cubeId, Array("band_name", "band_platform"), Array(bandName, bandPlatform))) {
        // band的其他信息可以去Postgresql中查询
        // 根据band_name和band_platform查询oc_band表，一定是有的
        val bandResultSet: ResultSet = selectDataFromTable("oc_band", Array("band_name", "band_platform"), Array(bandName, bandPlatform))
        val cubeBandMetadata: CubeBandMetadata = new CubeBandMetadata
        while (bandResultSet.next()) {
          cubeBandMetadata.setBandName(bandResultSet.getString("band_name"))
          cubeBandMetadata.setBandUnit(bandResultSet.getString("band_unit"))
          cubeBandMetadata.setBandMin(bandResultSet.getDouble("band_min"))
          cubeBandMetadata.setBandMax(bandResultSet.getDouble("band_max"))
          cubeBandMetadata.setBandScale(bandResultSet.getDouble("band_scale"))
          cubeBandMetadata.setBandOffset(bandResultSet.getDouble("band_offset"))
          cubeBandMetadata.setBandDescription(bandResultSet.getString("band_description"))
          cubeBandMetadata.setBandResolution(bandResultSet.getDouble("band_resolution"))
          cubeBandMetadata.setBandPlatform(bandResultSet.getString("band_platform"))
        }
        insertDataToTable("oc_band_" + cubeId, Array("band_name", "band_platform", "band_unit", "band_min", "band_max", "band_scale", "band_offset", "band_description", "band_resolution"), Array(bandName, bandPlatform, cubeBandMetadata.getBandUnit, cubeBandMetadata.getBandMin, cubeBandMetadata.getBandMax, cubeBandMetadata.getBandScale, cubeBandMetadata.getBandOffset, cubeBandMetadata.getBandDescription, cubeBandMetadata.getBandResolution))
      }
      // 2.4.3 准备oc_time的数据
      val timeCOG: LocalDateTime = metadata.getTime
      // 得到时间分辨率是一天的time_level_key
      val timeLevelKeyResultSet: ResultSet = selectDataFromTable("oc_time_level", Array("time_level", "resolution"), Array("D", "1"))
      var timeLevelKey: Int = 0
      while (timeLevelKeyResultSet.next()) {
        timeLevelKey = timeLevelKeyResultSet.getInt("time_level_key")
      }
      // 插入oc_time表
      val timeStamp: String = timeCOG.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      if (!isDataInTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))) {
        insertDataToTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))
      }
      // 2.4.4 准备oc_extent, oc_time和oc_image_fact的数据
      // 2.4.4.1 先知道有哪些Cube-COG
      val imagePath: String = metadata.getPath
      val cubeImagePathPrefix: String = imagePath.replace(".tif", "") + "/" + tms
      val client: MinioClient = MinIOUtil.getMinioClient
      val iterable: lang.Iterable[Result[Item]] = client.listObjects(ListObjectsArgs.builder().bucket("oge-cube").recursive(true).prefix(cubeImagePathPrefix).build())
      val iterator: java.util.Iterator[Result[Item]] = iterable.iterator()
      val cubeImagePathList: ListBuffer[String] = ListBuffer.empty[String]
      while (iterator.hasNext) {
        val item: Item = iterator.next().get()
        val objectName: String = item.objectName()
        cubeImagePathList.append(objectName)
      }
      // 得到productKey
      val productKeyResultSet: ResultSet = selectDataFromTable("oc_product_" + cubeId, Array("product_name"), Array(productName))
      var productKey: Int = 0
      while (productKeyResultSet.next()) {
        productKey = productKeyResultSet.getInt("product_key")
      }
      // 得到bandKey
      val bandKeyResultSet: ResultSet = selectDataFromTable("oc_band_" + cubeId, Array("band_name", "band_platform"), Array(bandName, bandPlatform))
      var bandKey: Int = 0
      while (bandKeyResultSet.next()) {
        bandKey = bandKeyResultSet.getInt("band_key")
      }
      // 得到timeKey
      val timeKeyResultSet: ResultSet = selectDataFromTable("oc_time_" + cubeId, Array("time_level_key", "time"), Array(timeLevelKey.toString, timeStamp))
      var timeKey: Int = 0
      while (timeKeyResultSet.next()) {
        timeKey = timeKeyResultSet.getInt("time_key")
      }

      // 2.4.4.2 循环Cube-COG，并开始准备瓦片元数据
      for (cubeImagePath <- cubeImagePathList) {
        val headerBytes: Array[Byte] = MinIOUtil.getMinioObject("oge-cube", cubeImagePath, 0, MINIO_HEAD_SIZE)
        val cubeCOGMetadata: CubeCOGMetadata = cogHeaderBytesParse(headerBytes)
        val compression: Int = cubeCOGMetadata.getCompression
        val dataType: OGECubeDataType.OGECubeDataType = getCubeDataType(cubeCOGMetadata.getSampleFormat, cubeCOGMetadata.getBitPerSample)
        // 整理所有的extent信息
        // 先通过cubeImagePath得到extent_level
        val pattern: Regex = "z(\\d+)\\.tif".r
        val matchResult: String = pattern.findFirstMatchIn(cubeImagePath).get.toString()
        val extentLevel: Int = matchResult.replace("z", "").replace(".tif", "").toInt
        // 然后通过extentLevel和tms，在oc_extent_level表得到extent_level_key
        val extentLevelKeyResultSet: ResultSet = selectDataFromTable("oc_extent_level", Array("extent_level", "tms"), Array(extentLevel.toString, tms))
        var extentLevelKey: Int = 0
        var tmsExtent: String = ""
        while (extentLevelKeyResultSet.next()) {
          extentLevelKey = extentLevelKeyResultSet.getInt("extent_level_key")
          tmsExtent = extentLevelKeyResultSet.getString("extent")
        }
        val tmsExtentJSONObject: JSONObject = JSON.parseObject(tmsExtent)
        val minXTMSExtent: Double = tmsExtentJSONObject.getDouble("min_x")
        val maxYTMSExtent: Double = tmsExtentJSONObject.getDouble("max_y")

        // 计算extent
        val geoTransform: Array[Double] = cubeCOGMetadata.getGeoTransform
        val cellScale: Array[Double] = cubeCOGMetadata.getCellScale
        val imageWidth: Int = cubeCOGMetadata.getImageWidth
        val imageHeight: Int = cubeCOGMetadata.getImageHeight
        val tileWidth: Int = cubeCOGMetadata.getTileWidth
        val tileHeight: Int = cubeCOGMetadata.getTileHeight
        val colImageTotal: Int = math.round(imageWidth.toDouble / tileWidth).toInt
        val rowImageTotal: Int = math.round(imageHeight.toDouble / tileHeight).toInt

        // 定义Image包含的所有extentKey的List
        val extentKeyList: ListBuffer[Int] = ListBuffer.empty[Int]
        for (colKeyInCOG <- 0 until colImageTotal) {
          for (rowKeyInCOG <- 0 until rowImageTotal) {
            val minX: Double = geoTransform(3) + colKeyInCOG * cellScale(0) * 256
            val maxY: Double = geoTransform(4) - rowKeyInCOG * cellScale(1) * 256
            val col: Int = math.round((minX - minXTMSExtent) / cellScale(0) / 256).toInt
            val row: Int = math.round((maxYTMSExtent - maxY) / cellScale(1) / 256).toInt
            val colTotal: Int = math.round(2 * -minXTMSExtent / cellScale(0) / 256).toInt
            val rowTotal: Int = math.round(2 * maxYTMSExtent / cellScale(0) / 256).toInt
            val minXExamined: Double = minXTMSExtent + 2 * -minXTMSExtent / colTotal * col
            val maxXExamined: Double = minXTMSExtent + 2 * -minXTMSExtent / colTotal * (col + 1)
            val maxYExamined: Double = maxYTMSExtent - 2 * maxYTMSExtent / rowTotal * row
            val minYExamined: Double = maxYTMSExtent - 2 * maxYTMSExtent / rowTotal * (row + 1)

            if (!isDataInTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col"), Array(extentLevelKey.toString, row.toString, col.toString))) {
              insertDataToTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col", "min_x", "min_y", "max_x", "max_y"), Array(extentLevelKey.toString, row.toString, col.toString, minXExamined.toString, minYExamined.toString, maxXExamined.toString, maxYExamined.toString))
            }
            // 得到extentKey
            val extentKeyResultSet: ResultSet = selectDataFromTable("oc_extent_" + cubeId, Array("extent_level_key", "row", "col"), Array(extentLevelKey.toString, row.toString, col.toString))
            var extentKey: Int = 0
            while (extentKeyResultSet.next()) {
              extentKey = extentKeyResultSet.getInt("extent_key")
              extentKeyList.append(extentKey)
            }
          }
        }

        // 插入oc_image_fact表
        if (!isDataInTable("oc_image_fact_" + cubeId, Array("path"), Array(cubeImagePath))) {
          insertDataToTable("oc_image_fact_" + cubeId, Array("product_key", "band_key", "time_key", "extent_key", "compression", "data_type", "path"), Array(productKey.toString, bandKey.toString, timeKey.toString, "{" + extentKeyList.mkString(",") + "}", compression.toString, dataType.toString, cubeImagePath))
        }
      }
    }
    val time2: Long = System.currentTimeMillis()
    println("从Collection创建Cube完毕，耗时：" + (time2 - time1) + "ms")
    cubeId
  }

  def createCubeFromInstance(cubeName: String, instanceId: String, tms: String): Int = {

    // 返回Cube的ID
    0
  }

  def insertCubeFromCollection(cubeName: String, product: String, band: String, time: String, extent: String, tms: String): Unit = {

  }

  def insertCubeFromInstance(cubeName: String, instanceId: String): Unit = {

  }


  def main(args: Array[String]): Unit = {
    val time1: Long = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setAppName("New Cube").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建Cube
    // createCubeFromCollectionByImage(cubeName = "test", tms = "WebMercatorQuad", cubeDescription = "test_WebMercatorQuad", productName = "LC08_L2SP_C02_T1", startTime = "2023/01/01", endTime = "2023/12/31", extent = Extent(114.16, 30.47, 114.47, 30.69))
    // 通过Tile加载Cube
    // loadCubeByTile(sc, "1", "[LC08_L2SP_C02_T1]", "[SR_B1]", "[2023-01-10 00:00:00, 2023-01-20 00:00:00]", "[113, 29, 120, 34]", "WebMercatorQuad", 30)
    // 通过Image加载Cube
    val cubeRDD1: RDD[(CubeTileKey, Tile)] = loadCubeByImage(sc, "3", "[LC08_L2SP_C02_T1]", "[SR_B3,SR_B5]", "[2023-01-01 00:00:00,2023-12-31 00:00:00]", "[114.16,30.47,114.47,30.69]", "WebMercatorQuad", 30)
    val cubeRDD2: RDD[(CubeTileKey, Tile)] = bandRadiometricCalibration(cubeRDD1, 2.8e-05, -0.2)
    val cubeRDD3: RDD[(CubeTileKey, Tile)] = normalizedDifference(cubeRDD2, "SR_B3", "Landsat 8", "SR_B5", "Landsat 8")
    visualization(cubeRDD3)
    sc.stop()

    val time2: Long = System.currentTimeMillis()
    println("总耗时：" + (time2 - time1) + "ms")
    println("Hello, World!")
  }

}
