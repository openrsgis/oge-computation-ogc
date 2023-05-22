package whu.edu.cn.application.oge

import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.render.{ColorMap, Exact}
import geotrellis.raster.resample.{Bilinear, NearestNeighbor, PointResampleMethod}
import geotrellis.raster.{ByteArrayTile, ByteCellType, ByteConstantNoDataCellType, CellType, ColorRamp, FloatArrayTile, FloatCellType, Histogram, IntArrayTile, RGBA, Raster, TargetCell, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.{FileLayerManager, FileLayerWriter}
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.geotools.geometry.jts.JTS
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.locationtech.jts.geom.{Coordinate, Geometry}
import redis.clients.jedis.Jedis
import whu.edu.cn.application.oge.Image.checkProjReso

import java.time.Instant
import scala.util.control.Breaks.{break, breakable}
import whu.edu.cn.application.oge.COGHeaderParse.{getTileBuf, tileQuery}
import whu.edu.cn.application.tritonClient.examples._
import whu.edu.cn.core.entity
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.TileMosaicImage.tileMosaic
import whu.edu.cn.util.{HttpUtil, JedisConnectionFactory, PostgresqlUtil, SystemConstants, ZCurveUtil}
//import whu.edu.cn.util.TileMosaicImage.tileMosaic
import whu.edu.cn.util.TileSerializerImage.deserializeTileData

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.postfixOps

import scala.math._

object Image {

  /**
   * load the images
   *
   * @param sc              spark context
   * @param productName     product name to query
   * @param sensorName      sensor name to query
   * @param measurementName measurement name to query
   * @param dateTime        array of start time and end time to query
   * @param geom            geom of the query window
   * @param crs             crs of the images to query
   * @return ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile])
   */
  def load(implicit sc: SparkContext, productName: String = null,
           sensorName: String = null, measurementName: String = null,
           dateTime: String = null, geom: String = null /* // TODO geom 可以去掉了 */ ,
           geom2: String = null, crs: String = null, level: Int = 0
           /* //TODO 把trigger算出来的前端瓦片编号集合传进来 */): ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile]) = {
    val zIndexStrArray: ArrayBuffer[String] = ImageTrigger.zIndexStrArray


    val dateTimeArray: Array[String] = if (dateTime != null) dateTime.replace("[", "").replace("]", "").split(",") else null
    val StartTime: String = if (dateTimeArray != null) {
      if (dateTimeArray.length == 2) dateTimeArray(0) else null
    } else null
    val EndTime: String = if (dateTimeArray != null) {
      if (dateTimeArray.length == 2) dateTimeArray(1) else null
    } else null
    var startTime = ""
    if (StartTime == null || StartTime == "") startTime = "2000-01-01 00:00:00"
    else startTime = StartTime
    var endTime = ""
    if (EndTime == null || EndTime == "") endTime = new Timestamp(System.currentTimeMillis).toString
    else endTime = EndTime


    if (geom2 != null) {
      println("geo2 = " + geom2)

      val lonLatOfBBox: ListBuffer[Double] = geom2.replace("[", "")
        .replace("]", "")
        .split(",").map(_.toDouble).to[ListBuffer]

      val queryExtent = new ArrayBuffer[Array[Double]]()


      val key: String = ImageTrigger.originTaskID + ":solvedTile:" + level
      val jedis: Jedis = JedisConnectionFactory.getJedis
      jedis.select(1)

      // 通过传进来的前端瓦片编号反算出它们各自对应的经纬度范围 V
      for (zIndexStr <- zIndexStrArray) {
        val xy: Array[Int] = ZCurveUtil.zCurveToXY(zIndexStr, level)


        val lonMinOfTile: Double = ZCurveUtil.tile2Lon(xy(0), level)
        val latMinOfTile: Double = ZCurveUtil.tile2Lat(xy(1) + 1, level)
        val lonMaxOfTile: Double = ZCurveUtil.tile2Lon(xy(0) + 1, level)
        val latMaxOfTile: Double = ZCurveUtil.tile2Lat(xy(1), level)

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
          jedis.sadd(key, zIndexStr)
          jedis.expire(key, SystemConstants.REDIS_CACHE_TTL)

        } // end if
      } // end for


      jedis.close()
      val queryExtentRDD: RDD[Array[Double]] = sc.makeRDD(queryExtent)

      val tileDataTuple: ListBuffer[(String, String, String, String, String, String,

        Array[Double])] = queryExtentRDD.map(extent => {
        val minX: Double = extent(0)
        val minY: Double = extent(1)
        val maxX: Double = extent(2)
        val maxY: Double = extent(3)

        val polygonStr: String = "POLYGON((" + minX + " " + minY + ", " +
          minX + " " + maxY + ", " + maxX + " " + maxY + ", " +
          maxX + " " + minY + "," + minX + " " + minY + "))"

        query(productName, sensorName, measurementName,
          startTime, endTime, polygonStr, crs) // 查询元数据并去重
          .map(
            metaData =>
              (metaData._1, metaData._2, metaData._3,
                metaData._4, metaData._5, metaData._6,
                extent)
          ) // 把query得到的元数据集合 和之前 与元数据集合对应的查询范围 合并到一个元组中
      }).persist() // 暂存
        .reduce(_ ++ _) // 合并成大数组
      // 元数据应当是所有范围交集并去重后的集合，要合并
      // 每一组元数据都对应一个唯一的矩形范围


      val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = sc.makeRDD(tileDataTuple)
        .map(t => { // 合并所有的元数据（追加了范围）
          val rawTiles: util.ArrayList[RawTile] = {
            tileQuery(level, t._1, t._2, t._3, t._4, t._5, t._6, productName, t._7)
          } //TODO
          println(rawTiles.size() + "aaaaaaaaafdadagadgas")
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
      (noReSlice(sc, tileRDDReP), tileRDDReP)
    }
    else { // geom2 == null，之前批处理使用的代码

      val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
        t.toDouble
      }).to[ListBuffer]


      var geom_str = ""
      if (geomReplace != null && geomReplace.length == 4) {
        val minx = geomReplace(0)
        val miny = geomReplace(1)
        val maxx = geomReplace(2)
        val maxy = geomReplace(3)
        geom_str = "POLYGON((" + minx + " " + miny + ", " + minx + " " + maxy + ", " + maxx + " " + maxy + ", " + maxx + " " + miny + "," + minx + " " + miny + "))"
      }
      val query_extent = new Array[Double](4)
      query_extent(0) = geomReplace(0)
      query_extent(1) = geomReplace(1)
      query_extent(2) = geomReplace(2)
      query_extent(3) = geomReplace(3)
      val metaData = query(productName, sensorName, measurementName, startTime, endTime, geom_str, crs)
      println("metadata.length = " + metaData.length)
      if (metaData.isEmpty) throw new RuntimeException("No data to compute!")

      val imagePathRdd: RDD[(String, String, String, String, String, String)] = sc.parallelize(metaData, metaData.length)
      val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = imagePathRdd.map(t => {
        val tiles = tileQuery(level, t._1, t._2, t._3, t._4, t._5, t._6, productName, query_extent)
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
      (noReSlice(sc, tileRDDReP), tileRDDReP)

    }


  }

  def noReSlice(implicit sc: SparkContext, tileRDDReP: RDD[RawTile]): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
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
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val tileRDD = tileRDDReP.map(t => {
      val tile = getTileBuf(t)
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
    (tileRDD, tileLayerMetadata)
  }

  def mosaic(implicit sc: SparkContext, tileRDDReP: RDD[RawTile], method: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO
    val extents = tileRDDReP.map(t => {
      (t.getLngLatBottomLeft.head, t.getLngLatBottomLeft.last,
        t.getLngLatUpperRight.head, t.getLngLatUpperRight.last)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant = tileRDDReP.map(t => {
      (t.getCol, t.getRow,
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime,
        t.getCol, t.getRow,
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2),
        min(a._3, b._3), max(a._4, b._4),
        max(a._5, b._5), max(a._6, b._6))
    })
    val firstTile = tileRDDReP.take(1)(0)
    val tl = TileLayout(Math.round((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.round((extents._4 - extents._2) / firstTile.getResolution / 256.0).toInt, 256, 256)
    println(tl)
    val extent = geotrellis.vector.Extent(extents._1, extents._4 - tl.layoutRows * 256 * firstTile.getResolution, extents._1 + tl.layoutCols * 256 * firstTile.getResolution, extents._4)
    println(extent)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDataType)
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(tl.layoutCols - 1, tl.layoutRows - 1, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    val rawTileRDD = tileRDDReP.map(t => {
      val tile = getTileBuf(t)
      t.setTile(deserializeTileData("", tile.getTileBuf, 256, tile.getDataType))
      t
    })
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rawtileArray = rawTileRDD.collect()
    val RowSum = ld.tileLayout.layoutRows
    val ColSum = ld.tileLayout.layoutCols
    val tileBox = new ListBuffer[((Extent, SpaceTimeKey), List[RawTile])]
    for (i <- 0 until ColSum) {
      for (j <- 0 until RowSum) {
        val rawTiles = new ListBuffer[RawTile]
        val tileExtent = new Extent(extents._1 + i * 256 * firstTile.getResolution, extents._4 - (j + 1) * 256 * firstTile.getResolution, extents._1 + (i + 1) * 256 * firstTile.getResolution, extents._4 - j * 256 * firstTile.getResolution)
        for (rawTile <- rawtileArray) {
          if (Extent(
            rawTile.getLngLatBottomLeft.head, // X
            rawTile.getLngLatBottomLeft.last, // Y
            rawTile.getLngLatUpperRight.head, // X
            rawTile.getLngLatUpperRight.last // Y
          ).intersects(tileExtent))
            rawTiles.append(rawTile)
        }
        if (rawTiles.nonEmpty) {
          val now = "1000-01-01 00:00:00"
          val date = sdf.parse(now).getTime
          tileBox.append(((tileExtent, SpaceTimeKey(i, j, date)), rawTiles.toList))
        }
      }
    }
    val TileRDDUnComputed = sc.parallelize(tileBox, tileBox.length)
    tileMosaic(TileRDDUnComputed, method, tileLayerMetadata, firstTile.getDataType)
  }


  def query(productName: String = null, sensorName: String = null, measurementName: String = null, startTime: String = null, endTime: String = null, geom: String = null, crs: String = null): (ListBuffer[(String, String, String, String, String, String)]) = {
    val metaData = new ListBuffer[(String, String, String, String, String, String)]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection()
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new mutable.StringBuilder
        sql ++= "select oge_image.*, oge_data_resource_product.name, oge_data_resource_product.dtype"
        sql ++= ", oge_product_measurement.band_num, oge_product_measurement.resolution"
        sql ++= " from oge_image "
        if (productName != "" && productName != null) {
          sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        }
        if (sensorName != "" && sensorName != null) {
          sql ++= " join oge_sensor on oge_data_resource_product.sensor_Key=oge_sensor.sensor_Key "
        }
        sql ++= "join oge_product_measurement on oge_product_measurement.product_key=oge_data_resource_product.id join oge_measurement on oge_product_measurement.measurement_key=oge_measurement.measurement_key "
        var t = "where"
        if (productName != "" && productName != null) {
          sql ++= t
          sql ++= " name="
          sql ++= "\'"
          sql ++= productName
          sql ++= "\'"
          t = "AND"

        }
        if (sensorName != "" && sensorName != null) {
          sql ++= t
          sql ++= " sensor_name="
          sql ++= "\'"
          sql ++= sensorName
          sql ++= "\'"
          t = "AND"
        }
        if (measurementName != "" && measurementName != null) {
          sql ++= t
          sql ++= " measurement_name="
          sql ++= "\'"
          sql ++= measurementName
          sql ++= "\'"
          t = "AND"
        }
        if (startTime != null) {
          sql ++= t
          sql ++= " phenomenon_time>="
          sql ++= "\'"
          sql ++= startTime
          sql ++= "\'"
          t = "AND"
        }
        if (endTime != null) {
          sql ++= t
          sql ++= "\'"
          sql ++= endTime
          sql ++= "\'"
          sql ++= ">=phenomenon_time"
          t = " AND"
        }
        if (crs != null && crs != "") {
          sql ++= t
          sql ++= " crs="
          sql ++= "\'"
          sql ++= crs
          sql ++= "\'"
          t = "AND"
        }
        if (geom != null && geom != "") {
          sql ++= t
          sql ++= " ST_Intersects(geom,'SRID=4326;"
          sql ++= geom
          sql ++= "')"
        }
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())


        while (extentResults.next()) {
          val path = if (measurementName != "" && measurementName != null) {
            extentResults.getString("path") + "/" +
              extentResults.getString("image_identification") +
              "_" + extentResults.getString("band_num") + ".tif"
          }
          else {
            extentResults.getString("path") + "/" + extentResults.getString("image_identification") + ".tif"
          }
          println("path = " + path)
          val time = extentResults.getString("phenomenon_time")
          val srcID = extentResults.getString("crs")
          val dtype = extentResults.getString("dtype")
          val measurement =
            if (measurementName != "" && measurementName != null)
              extentResults.getString("band_num")
            else null
          val resolution = extentResults.getString("resolution")
          metaData.append((path, time, srcID, measurement, dtype, resolution))
        }
      }
      finally {
        conn.close()
      }
    } else throw new RuntimeException("connection failed")

    metaData
  }


  def checkProjReso(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
                    image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])) = {
    val resampleTime = Instant.now.getEpochSecond
    val band1 = image1._1.first()._1.measurementName
    val image1tileRDD = image1._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val band2 = image2._1.first()._1.measurementName
    val image2tileRDD = image2._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      var reso = image1._2.cellSize.resolution
      if (image1._2.cellSize.resolution < image2._2.cellSize.resolution) {
        reso = image2._2.cellSize.resolution
      }
      var extent1 = image1._2.extent.reproject(image1._2.crs, CRS.fromEpsgCode(32650))

      var extent2 = image2._2.extent.reproject(image2._2.crs, CRS.fromEpsgCode(32650))

      val extentIntersection = extent1.intersection(extent2)
      extent1 = extentIntersection.getOrElse(extent1)
      extent2 = extentIntersection.getOrElse(extent2)
      val tl1 = TileLayout(((extent1.xmax - extent1.xmin) / (reso * 256)).toInt, ((extent1.ymax - extent1.ymin) / (reso * 256)).toInt, 256, 256)
      val ld1 = LayoutDefinition(extent1, tl1)

      val srcBounds1 = image1._2.bounds
      val newBounds1 = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds1.get.maxKey.spatialKey._1, srcBounds1.get.maxKey.spatialKey._2))

      val rasterMetaData1 = TileLayerMetadata(image1._2.cellType, image1._2.layout, image1._2.extent, image1._2.crs, newBounds1)
      val image1tileLayerRDD = TileLayerRDD(image1tileRDD, rasterMetaData1)
      val (_, image1tileRDDWithMetadata) = image1tileLayerRDD.reproject(CRS.fromEpsgCode(32650), ld1, geotrellis.raster.resample.Bilinear)
      val image1Noband = (image1tileRDDWithMetadata.mapValues(tile => tile: Tile), image1tileRDDWithMetadata.metadata)
      val newBound1 = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(image1Noband._2.tileLayout.layoutCols, image1Noband._2.tileLayout.layoutRows, resampleTime))
      val newMetadata1 = TileLayerMetadata(image1Noband._2.cellType, image1Noband._2.layout, image1Noband._2.extent, image1Noband._2.crs, newBound1)
      val image1RDD = image1Noband.map(t => {
        (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band1), t._2)
      })

      val tl2 = TileLayout(((extent2.xmax - extent2.xmin) / (reso * 256)).toInt, ((extent2.ymax - extent2.ymin) / (reso * 256)).toInt, 256, 256)
      val ld2 = LayoutDefinition(extent2, tl2)

      val srcBounds2 = image2._2.bounds
      val newBounds2 = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds2.get.maxKey.spatialKey._1, srcBounds2.get.maxKey.spatialKey._2))

      val rasterMetaData2 = TileLayerMetadata(image2._2.cellType, image2._2.layout, image2._2.extent, image2._2.crs, newBounds2)
      val image2tileLayerRDD = TileLayerRDD(image2tileRDD, rasterMetaData2)
      val (_, image2tileRDDWithMetadata) = image2tileLayerRDD.reproject(CRS.fromEpsgCode(32650), ld2, geotrellis.raster.resample.Bilinear)
      val image2Noband = (image2tileRDDWithMetadata.mapValues(tile => tile: Tile), image2tileRDDWithMetadata.metadata)
      val newBound2 = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(image2Noband._2.tileLayout.layoutCols, image2Noband._2.tileLayout.layoutRows, resampleTime))
      val newMetadata2 = TileLayerMetadata(image2Noband._2.cellType, image2Noband._2.layout, image2Noband._2.extent, image2Noband._2.crs, newBound2)
      val image2RDD = image2Noband.map(t => {
        (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band2), t._2)
      })

      ((image1RDD, newMetadata1), (image2RDD, newMetadata2))
    }
    else {
      (image1, image2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, add operation is applied between the 2 bands
   * if not, add the first value to the second for each matched pair of bands in image1 and image2
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def add(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("add bandNum1 = " + bandNum1)
    println("add bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val addRDD = image1NoBand.join(image2NoBand)
      (addRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Add"), Add(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Add(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, subtarct the second tile from the first tile for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def subtract(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("subtract bandNum1 = " + bandNum1)
    println("subtract bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val subtractRDD = image1NoBand.join(image2NoBand)
      (subtractRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Subtract"), Subtract(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Subtract(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, divide operation is applied between the 2 bands
   * if not, divide the first tile by the second tile for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def divide(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
             image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("divide bandNum1 = " + bandNum1)
    println("divide bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val divideRDD = image1NoBand.join(image2NoBand)
      (divideRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Divide"), Divide(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Divide(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, multiply operation is applied between the 2 bands
   * if not, multipliy the first tile by the second for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def multiply(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("multiply bandNum1 = " + bandNum1)
    println("multiply bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val multiplyRDD = image1NoBand.join(image2NoBand)
      (multiplyRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Multiply"), Multiply(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Multiply(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * image Binarization
   *
   * @param image     image rdd for operation
   * @param threshold threshold
   * @return
   */
  def binarization(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), threshold: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      val tilemap = t._2.map(pixel => {
        if (pixel > threshold) {
          255
        }
        else {
          0
        }
      })
      (t._1, tilemap)
    }), image._2)
  }

  /**
   * Returns 1 iff both values are non-zero for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def and(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val andRDD = image1NoBand.join(image2NoBand)
      (andRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "And"), And(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, And(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 1 iff either values are non-zero for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def or(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val orRDD = image1NoBand.join(image2NoBand)
      (orRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Or"), Or(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Or(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 0 if the input is non-zero, and 1 otherwise
   *
   * @param image the image rdd for operation
   * @return
   */
  def not(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Not(t._2))
    }), image._2)
  }

  /**
   * Computes the sine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def sin(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Sin(t._2))
    }), image._2)
  }

  /**
   * Computes the cosine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def cos(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Cos(t._2))
    }), image._2)
  }

  /**
   * Computes the tangent of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def tan(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Tan(t._2))
    }), image._2)
  }

  /**
   * Computes the hyperbolic sine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def sinh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Sinh(t._2))
    }), image._2)
  }

  /**
   * Computes the hyperbolic cosine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def cosh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Cosh(t._2))
    }), image._2)
  }

  /**
   * Computes the hyperbolic tangent of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def tanh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Tanh(t._2))
    }), image._2)
  }

  /**
   * Computes the arc sine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def asin(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Asin(t._2))
    }), image._2)
  }

  /**
   * Computes the arc cosine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def acos(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Acos(t._2))
    }), image._2)
  }

  /**
   * Computes the atan sine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def atan(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Atan(t._2))
  }), image._2)

  /**
   * Operation to get the Arc Tangent2 of values. The first raster holds the y-values, and the second holds the x values.
   * The arctan is calculated from y/x.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def atan2(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
            image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val atan2RDD = image1NoBand.join(image2NoBand)
      (atan2RDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Atan2"), Atan2(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Atan2(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Computes the smallest integer greater than or equal to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def ceil(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Ceil(t._2))
  }), image._2)

  /**
   * Computes the largest integer less than or equal to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def floor(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Floor(t._2))
  }), image._2)

  /**
   * Computes the integer nearest to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def round(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Round(t._2))
  }), image._2)

  /**
   * Computes the natural logarithm of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def log(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Log(t._2))
  }), image._2)

  /**
   * Computes the base-10 logarithm of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def log10(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Log10(t._2))
  }), image._2)

  /**
   * Computes the square root of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def sqrt(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Sqrt(t._2))
  }), image._2)

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def eq(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val eqRDD = image1NoBand.join(image2NoBand)
      (eqRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Eq"), Equal(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Equal(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 1 iff the first value is greater than the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def gt(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gtRDD = image1NoBand.join(image2NoBand)
      (gtRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Gt"), Greater(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Greater(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 1 iff the first value is equal or greater to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def gte(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gteRDD = image1NoBand.join(image2NoBand)
      (gteRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Gte"), GreaterOrEqual(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, GreaterOrEqual(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 1 iff the first value is less than the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def lt(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gtRDD = image1NoBand.join(image2NoBand)
      (gtRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Lt"), Less(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Less(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns 1 iff the first value is less or equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def lte(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gteRDD = image1NoBand.join(image2NoBand)
      (gteRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Lte"), LessOrEqual(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, LessOrEqual(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Returns an image containing all bands copied from the first input and selected bands from the second input,
   * optionally overwriting bands in the first image with the same name.
   *
   * @param image1    first image
   * @param image2    second image
   * @param names     the name of selected bands in image2
   * @param overwrite if true, overwrite bands in the first image with the same name
   * @return
   */
  def addBands(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               names: List[String], overwrite: Boolean = false): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val image1BandNames = bandNames(image1)
    val image2BandNames = bandNames(image2)
    val intersectBandNames = image1BandNames.intersect(image2BandNames)
    //select bands from image2
    val image2SelectBand: RDD[(SpaceTimeBandKey, Tile)] = image2._1.filter(t => {
      names.contains(t._1._measurementName)
    })
    if (!overwrite) {
      //image2不重写image1中相同的波段，把image2中与image1相交的波段过滤掉
      (image2SelectBand.filter(t => {
        !intersectBandNames.contains(t._1._measurementName)
      }).union(image1._1), image1._2)
    }
    else {
      //image2重写image1中相同的波段，把image1中与image2相交的波段过滤掉
      (image1._1.filter(t => {
        !intersectBandNames.contains(t._1._measurementName)
      }).union(image2SelectBand), image1._2)
    }
  }

  /**
   * get all the bands in rdd
   *
   * @param image rdd for getting bands
   * @return
   */
  def bandNames(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): List[String] = image._1.map(t => t._1._measurementName).distinct().collect().toList

  /**
   * get the target bands from original image
   *
   * @param image       the image for operation
   * @param targetBands the intersting bands
   * @return
   */
  def select(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), targetBands: List[String])
  : (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.filter(t => targetBands.contains(t._1._measurementName)), image._2)

  /**
   * Returns a map of the image's band types.
   *
   * @param image The image from which the left operand bands are taken.
   * @return
   */
  def bandTypes(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): Map[String, String] = {
    val bandTypesArray = image._1.map(t => (t._1.measurementName, t._2.cellType)).distinct().collect()
    var bandTypesMap = Map[String, String]()
    for (band <- bandTypesArray) {
      bandTypesMap = bandTypesMap + (band._1 -> band._2.toString())
    }
    bandTypesMap
  }

  /**
   * Computes the absolute value of the input.
   *
   * @param image The image to which the operation is applied.
   * @return
   */
  def abs(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, Abs(t._2))
  }), image._2)

  /**
   * Returns 1 iff the first value is not equal to the second for each matched pair of bands in image1 and image2.
   *
   * @param image1 The image from which the left operand bands are taken.
   * @param image2 The image from which the right operand bands are taken.
   * @return
   */
  def neq(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val eqRDD = image1NoBand.join(image2NoBand)
      (eqRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Ueq"), Unequal(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Unequal(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Computes the signum function (sign) of the input; zero if the input is zero, 1 if the input is greater than zero, -1 if the input is less than zero.
   *
   * @param image The image to which the operation is applied.
   * @return
   */
  def signum(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.map(u => {
      if (u > 0) 1
      else if (u < 0) -1
      else 0
    }))
  }), image._2)

  /**
   * Rename the bands of an image.Returns the renamed image.
   *
   * @param image The coverage to which to apply the operations.
   * @param name  The new names for the bands. Must match the number of bands in the Image.
   * @return
   */
  def rename(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
             name: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val nameList = name.replace("[", "").replace("]", "").split(",")
    val bandnames = bandNames(image)
    if (bandnames.length == nameList.length) {
      val namesMap = (bandnames zip nameList).toMap
      (image._1.map(t => {
        (SpaceTimeBandKey(t._1.spaceTimeKey, namesMap(t._1.measurementName)), t._2)
      }), image._2)
    }
    else image
  }

  /**
   * Raises the first value to the power of the second for each matched pair of bands in image1 and image2.
   *
   * @param image1 The image from which the left operand bands are taken.
   * @param image2 The image from which the right operand bands are taken.
   * @return
   */
  def pow(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("pow bandNum1 = " + bandNum1)
    println("pow bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val powRDD = image1NoBand.join(image2NoBand)
      (powRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Pow"), Pow(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Pow(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Selects the minimum of the first and second values for each matched pair of bands in image1 and image2.
   *
   * @param image1 The image from which the left operand bands are taken.
   * @param image2 The image from which the right operand bands are taken.
   * @return
   */
  def mini(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
           image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("min bandNum1 = " + bandNum1)
    println("min bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val minRDD = image1NoBand.join(image2NoBand)
      (minRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Min"), Min(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Min(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Selects the maximum of the first and second values for each matched pair of bands in image1 and image2.
   *
   * @param image1 The image from which the left operand bands are taken.
   * @param image2 The image from which the right operand bands are taken.
   * @return
   */
  def maxi(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
           image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var image1Reprojected = image1
    var image2Reprojected = image2
    if (image1._2.crs != image2._2.crs || image1._2.cellSize.resolution != image2._2.cellSize.resolution) {
      val tuple = checkProjReso(image1, image2)
      image1Reprojected = tuple._1
      image2Reprojected = tuple._2
    }
    val bandNum1 = bandNames(image1Reprojected).length
    val bandNum2 = bandNames(image2Reprojected).length
    println("max bandNum1 = " + bandNum1)
    println("max bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2Reprojected._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val maxRDD = image1NoBand.join(image2NoBand)
      (maxRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Max"), Max(t._2._1._2, t._2._2._2))
      }), image1Reprojected._2)
    }
    else {
      val matchRDD = image1Reprojected._1.join(image2Reprojected._1)
      (matchRDD.map(t => {
        (t._1, Max(t._2._1, t._2._2))
      }), image1Reprojected._2)
    }
  }

  /**
   * Applies a morphological mean filter to each band of an image using a named or custom kernel.
   *
   * @param image The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius The radius of the kernel to use.
   * @return
   */
  def focalMean(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    kernelType match {
      case "square" => {
        val neighborhood = focal.Square(radius)
        print(neighborhood.extent)
        val imageFocalMeaned = image._1.map(t => {
          val cellType = t._2.cellType
          (t._1, focal.Mean(t._2.convert(CellType.fromName("float32")), neighborhood, None, TargetCell.All).convert(cellType))
        })
        (imageFocalMeaned, image._2)
      }
      case "circle" => {
        val neighborhood = focal.Circle(radius)
        val imageFocalMeaned = image._1.map(t => {
          val cellType = t._2.cellType
          (t._1, focal.Mean(t._2.convert(CellType.fromName("float32")), neighborhood, None, TargetCell.All).convert(cellType))
        })
        (imageFocalMeaned, image._2)
      }
    }
  }

  /**
   * Applies a morphological median filter to each band of an image using a named or custom kernel.
   *
   * @param image      The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMedian(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                  radius: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    kernelType match {
      case "square" => {
        val neighborhood = focal.Square(radius)
        val imageFocalMedianed = image._1.map(t => {
          val cellType = t._2.cellType
          (t._1, focal.Median(t._2.convert(CellType.fromName("float32")), neighborhood, None, TargetCell.All).convert(cellType))
        })
        (imageFocalMedianed, image._2)
      }
      case "circle" => {
        val neighborhood = focal.Circle(radius)
        val imageFocalMedianed = image._1.map(t => {
          val cellType = t._2.cellType
          (t._1, focal.Median(t._2.convert(CellType.fromName("float32")), neighborhood, None, TargetCell.All).convert(cellType))
        })
        (imageFocalMedianed, image._2)
      }
    }
  }

  /**
   * Applies a morphological max filter to each band of an image using a named or custom kernel.
   *
   * @param image      The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMax(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    kernelType match {
      case "square" => {
        val neighborhood = focal.Square(radius)
        val imageFocalMaxed = image._1.map(t => {
          (t._1, focal.Max(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMaxed, image._2)
      }
      case "circle" => {
        val neighborhood = focal.Circle(radius)
        val imageFocalMaxed = image._1.map(t => {
          (t._1, focal.Max(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMaxed, image._2)
      }
    }
  }

  /**
   * Applies a morphological min filter to each band of an image using a named or custom kernel.
   *
   * @param image      The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMin(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    kernelType match {
      case "square" => {
        val neighborhood = focal.Square(radius)
        val imageFocalMined = image._1.map(t => {
          (t._1, focal.Min(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMined, image._2)
      }
      case "circle" => {
        val neighborhood = focal.Circle(radius)
        val imageFocalMined = image._1.map(t => {
          (t._1, focal.Min(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMined, image._2)
      }
    }
  }

  /**
   * Applies a morphological mode filter to each band of an image using a named or custom kernel.
   *
   * @param image      The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMode(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    kernelType match {
      case "square" => {
        val neighborhood = focal.Square(radius)
        val imageFocalMeaned = image._1.map(t => {
          (t._1, focal.Mode(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMeaned, image._2)
      }
      case "circle" => {
        val neighborhood = focal.Circle(radius)
        val imageFocalMeaned = image._1.map(t => {
          (t._1, focal.Mode(t._2, neighborhood, None, TargetCell.All))
        })
        (imageFocalMeaned, image._2)
      }
    }
  }

  /**
   * Convolves each band of an image with the given kernel.
   *
   * @param image The image to convolve.
   * @param kernel The kernel to convolve with.
   * @return
   */
  def convolve(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               kernel: focal.Kernel): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val leftNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(0, 1), t._2))
    })
    val rightNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(2, 1), t._2))
    })
    val upNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(1, 0), t._2))
    })
    val downNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(1, 2), t._2))
    })
    val leftUpNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(0, 0), t._2))
    })
    val upRightNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(2, 0), t._2))
    })
    val rightDownNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(2, 2), t._2))
    })
    val downLeftNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(0, 2), t._2))
    })
    val midNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(1, 1), t._2))
    })
    val unionRDD = leftNeighborRDD.union(rightNeighborRDD).union(upNeighborRDD).union(downNeighborRDD).union(leftUpNeighborRDD).union(upRightNeighborRDD).union(rightDownNeighborRDD).union(downLeftNeighborRDD).union(midNeighborRDD)
      .filter(t => {
        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 && t._1.spaceTimeKey.spatialKey._1 < image._2.layout.layoutCols && t._1.spaceTimeKey.spatialKey._2 < image._2.layout.layoutRows
      })
    val groupRDD = unionRDD.groupByKey().map(t => {
      val listBuffer = new ListBuffer[(SpatialKey, Tile)]()
      val list = t._2.toList
      for (key <- List(SpatialKey(0, 0), SpatialKey(0, 1), SpatialKey(0, 2), SpatialKey(1, 0), SpatialKey(1, 1), SpatialKey(1, 2), SpatialKey(2, 0), SpatialKey(2, 1), SpatialKey(2, 2))) {
        var flag = false
        breakable {
          for (tile <- list) {
            if (key.equals(tile._1)) {
              listBuffer.append(tile)
              flag = true
              break
            }
          }
        }
        if (flag == false) {
          listBuffer.append((key, ByteArrayTile(Array.fill[Byte](256 * 256)(-128), 256, 256, ByteCellType).mutable))
        }
      }
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
      (t._1, tile.crop(251, 251, 516, 516).convert(CellType.fromName("int16")))
    })

    val convolvedRDD: RDD[(SpaceTimeBandKey, Tile)] = groupRDD.map(t => {
      (t._1, focal.Convolve(t._2, kernel, None, TargetCell.All).crop(5, 5, 260, 260))
    })
    (convolvedRDD, image._2)
  }

  /**
   * Return the histogram of the image, a map of bin label value and its associated count.
   *
   * @param image The coverage to which to compute the histogram.
   * @return
   */
  def histogram(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): Map[Int, Long] = {
    val histRDD: RDD[Histogram[Int]] = image._1.map(t => {
      t._2.histogram
    })
    histRDD.reduce((a, b) => {
      a.merge(b)
    }).binCounts().toMap[Int, Long]
  }

  /**
   * Returns the projection of an Image.
   *
   * @param image The coverage to which to get the projection.
   * @return
   */
  def projection(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): String = image._2.crs.toString()

  /**
   * Force an image to be computed in a given projection
   *
   * @param image             The coverage to reproject.
   * @param newProjectionCode The code of new projection
   * @param resolution        The resolution of reprojected image
   * @return
   */
  def reproject(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
                newProjectionCode: Int, resolution: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val band = image._1.first()._1.measurementName
    val resampleTime = Instant.now.getEpochSecond
    val imagetileRDD = image._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val extent = image._2.extent.reproject(image._2.crs, CRS.fromEpsgCode(newProjectionCode))
    val tl = TileLayout(((extent.xmax - extent.xmin) / (resolution * 256)).toInt, ((extent.ymax - extent.ymin) / (resolution * 256)).toInt, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = image._2.cellType
    val srcLayout = image._2.layout
    val srcExtent = image._2.extent
    val srcCrs = image._2.crs
    val srcBounds = image._2.bounds
    val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2))
    val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val imagetileLayerRDD = TileLayerRDD(imagetileRDD, rasterMetaData);
    val (_, imagetileRDDWithMetadata) = imagetileLayerRDD.reproject(CRS.fromEpsgCode(newProjectionCode), ld, geotrellis.raster.resample.Bilinear)
    val imageNoband = (imagetileRDDWithMetadata.mapValues(tile => tile: Tile), imagetileRDDWithMetadata.metadata)
    val newBound = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(imageNoband._2.tileLayout.layoutCols - 1, imageNoband._2.tileLayout.layoutRows - 1, resampleTime))
    val newMetadata = TileLayerMetadata(imageNoband._2.cellType, imageNoband._2.layout, imageNoband._2.extent, imageNoband._2.crs, newBound)
    val imageRDD = imageNoband.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band), t._2)
    })
    (imageRDD, newMetadata)
  }

  /**
   * 自定义重采样方法，功能有局限性，勿用
   *
   * @param image        需要被重采样的图像
   * @param sourceZoom   原图像的 Zoom 层级
   * @param targetZoom   输出图像的 Zoom 层级
   * @param mode         插值方法
   * @param downSampling 是否下采样，如果sourceZoom > targetZoom，true则采样，false则不处理
   * @return 和输入图像同类型的新图像
   */
  def resampleToTargetZoom(
               image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               targetZoom: Int,
               mode: String,
               downSampling: Boolean = true
              )
  : (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val resampleMethod: PointResampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }


    val tiled: RDD[(SpatialKey, Tile)] = image._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
    val cellType: CellType = image._2.cellType
    val srcLayout: LayoutDefinition = image._2.layout
    val srcExtent: Extent = image._2.extent
    val srcCrs: CRS = image._2.crs
    val srcBounds: Bounds[SpaceTimeKey] = image._2.bounds
    val newBounds: Bounds[SpatialKey] =
      Bounds(srcBounds.get.minKey.spatialKey, srcBounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] =
      TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)

    val (sourceZoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiled, rasterMetaData)
        .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

    // 求解出原始影像的zoom



//    val myAcc2: LongAccumulator = sc.longAccumulator("myAcc2")
//    //    println("srcAcc0 = " + myAcc2.value)
//
//    reprojected.map(t => {
//
//
//      //          println("srcRows = " + t._2.rows) 256
//      //          println("srcRows = " + t._2.cols) 256
//      myAcc2.add(1)
//      t
//    }).collect()
//    println("srcNumOfTiles = " + myAcc2.value) // 234
//
//

    val level: Int = targetZoom - sourceZoom
    if (level > 0 && level < 20) {
      val imageResampled: RDD[(SpaceTimeBandKey, Tile)] = image._1.map(t => {
        (t._1, t._2.resample(t._2.cols * (1 << level), t._2.rows * (1 << level), resampleMethod))
      })
      (imageResampled,
        TileLayerMetadata(image._2.cellType, LayoutDefinition(
          image._2.extent,
          TileLayout(image._2.layoutCols, image._2.layoutRows,
            image._2.tileCols * (1 << level), image._2.tileRows * (1 << level))
        ), image._2.extent, image._2.crs, image._2.bounds))
    }
    else if (level < 0 && level > (-20) && downSampling) {
      val imageResampled: RDD[(SpaceTimeBandKey, Tile)] = image._1.map(t => {
        val tileResampled: Tile = t._2.resample(Math.ceil(t._2.cols.toDouble / (1 << -level)).toInt, Math.ceil(t._2.rows.toDouble / (1 << -level)).toInt, resampleMethod)
        (t._1, tileResampled)
      })
      println("image._2.tileCols.toDouble = " + image._2.tileCols.toDouble)
      (imageResampled,
        TileLayerMetadata(image._2.cellType, LayoutDefinition(
          image._2.extent,
          TileLayout(image._2.layoutCols, image._2.layoutRows,
            Math.ceil(image._2.tileCols.toDouble / (1 << -level)).toInt, Math.ceil(image._2.tileRows.toDouble / (1 << -level)).toInt)),
          image._2.extent, image._2.crs, image._2.bounds))
    }
    else image
  }


  /**
   * Resample the image
   *
   * @param image The coverage to resample
   * @param level The resampling level. eg:1 for up-sampling and -1 for down-sampling.
   * @param mode  The interpolation mode to use
   * @return
   */
  def resample(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), level: Int, mode: String
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO 重采样
    val resampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }
    if (level > 0 && level < 8) {
      val imageResampled = image._1.map(t => {
        (t._1, t._2.resample(t._2.cols * (level + 1), t._2.rows * (level + 1), resampleMethod))
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols * (level + 1),
        image._2.tileRows * (level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else if (level < 0 && level > (-8)) {
      val imageResampled = image._1.map(t => {
        val tileResampled = t._2.resample(t._2.cols / (-level + 1), t._2.rows / (-level + 1), resampleMethod)
        (t._1, tileResampled)
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols / (-level + 1),
        image._2.tileRows / (-level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else image
  }

  /**
   * Calculates the gradient.
   *
   * @param image The coverage to compute the gradient.
   * @return
   */
  def gradient(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val leftNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(0, 1), t._2))
    })
    val rightNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(2, 1), t._2))
    })
    val upNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(1, 0), t._2))
    })
    val downNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(1, 2), t._2))
    })
    val leftUpNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(0, 0), t._2))
    })
    val upRightNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(2, 0), t._2))
    })
    val rightDownNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(2, 2), t._2))
    })
    val downLeftNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(0, 2), t._2))
    })
    val midNeighborRDD = image._1.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(1, 1), t._2))
    })
    val unionRDD = leftNeighborRDD.union(rightNeighborRDD).union(upNeighborRDD).union(downNeighborRDD).union(leftUpNeighborRDD).union(upRightNeighborRDD).union(rightDownNeighborRDD).union(downLeftNeighborRDD).union(midNeighborRDD)
      .filter(t => {
        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 && t._1.spaceTimeKey.spatialKey._1 < image._2.layout.layoutCols && t._1.spaceTimeKey.spatialKey._2 < image._2.layout.layoutRows
      })
    val groupRDD = unionRDD.groupByKey().map(t => {
      val listBuffer = new ListBuffer[(SpatialKey, Tile)]()
      val list = t._2.toList
      for (key <- List(SpatialKey(0, 0), SpatialKey(0, 1), SpatialKey(0, 2), SpatialKey(1, 0), SpatialKey(1, 1), SpatialKey(1, 2), SpatialKey(2, 0), SpatialKey(2, 1), SpatialKey(2, 2))) {
        var flag = false
        breakable {
          for (tile <- list) {
            if (key.equals(tile._1)) {
              listBuffer.append(tile)
              flag = true
              break
            }
          }
        }
        if (flag == false) {
          listBuffer.append((key, ByteArrayTile(Array.fill[Byte](256 * 256)(-128), 256, 256, ByteCellType).mutable))
        }
      }
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
      (t._1, tile.crop(251, 251, 516, 516).convert(CellType.fromName("int16")))
    })
    val gradientRDD = groupRDD.map(t => {
      val sobelx = focal.Kernel(IntArrayTile(Array[Int](-1, 0, 1, -2, 0, 2, -1, 0, 1), 3, 3))
      val sobely = focal.Kernel(IntArrayTile(Array[Int](1, 2, 1, 0, 0, 0, -1, -2, -1), 3, 3))
      val tilex = focal.Convolve(t._2, sobelx, None, TargetCell.All).crop(5, 5, 260, 260)
      val tiley = focal.Convolve(t._2, sobely, None, TargetCell.All).crop(5, 5, 260, 260)
      (t._1, Sqrt(Add(tilex * tilex, tiley * tiley)).map(u => {
        if (u > 255) {
          255
        } else {
          u
        }
      }))
    })
    (gradientRDD, image._2)
  }

  /**
   * Clip the raster with the geometry (with the same crs).
   *
   * @param image The coverage to clip.
   * @param geom The geometry used to clip.
   * @return
   */
  def clip(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), geom: Geometry
          ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {

    val RDDExtent = image._2.extent
    val reso = image._2.cellSize.resolution
    val clipedRDD = image._1.map(t => {
      val tileExtent = Extent(RDDExtent.xmin + t._1.spaceTimeKey.col * reso * 256, RDDExtent.ymax - (t._1.spaceTimeKey.row + 1) * 256 * reso,
        RDDExtent.xmin + (t._1.spaceTimeKey.col + 1) * reso * 256, RDDExtent.ymax - t._1.spaceTimeKey.row * 256 * reso)
      val tileCliped = t._2.mask(tileExtent, geom)
      (t._1, tileCliped)
    })
    (clipedRDD, image._2)
  }

  /**
   * Clamp the raster between low and high
   *
   * @param image The coverage to clamp.
   * @param low The low value.
   * @param high The high value.
   * @return
   */
  def clamp(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), low: Int, high: Int
           ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val imageRDDClamped = image._1.map(t => {
      (t._1, t._2.map(t => {
        if (t > high) {
          high
        }
        else if (t < low) {
          low
        }
        else {
          t
        }
      }))
    })
    (imageRDDClamped, image._2)
  }

  /**
   * Transforms the image from the RGB color space to the HSV color space. Expects a 3 band image in the range [0, 1],
   * and produces three bands: hue, saturation and value with values in the range [0, 1].
   *
   * @param imageRed The Red coverage.
   * @param imageGreen The Green coverage.
   * @param imageBlue The Blue coverage.
   * @return
   */
  def rgbToHsv(imageRed: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               imageGreen: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               imageBlue: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val imageRedRDD = imageRed._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val imageGreenRDD = imageGreen._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val imageBlueRDD = imageBlue._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val joinRDD = imageRedRDD.join(imageBlueRDD).join(imageGreenRDD).map(t => {
      (t._1, (t._2._1._1, t._2._1._2, t._2._2))
    })
    val hsvRDD: RDD[List[(SpaceTimeBandKey, Tile)]] = joinRDD.map(t => {
      val hTile = t._2._1.convert(CellType.fromName("float32")).mutable
      val sTile = t._2._2.convert(CellType.fromName("float32")).mutable
      val vTile = t._2._3.convert(CellType.fromName("float32")).mutable

      for (i <- 0 to 255) {
        for (j <- 0 to 255) {
          val r: Double = t._2._1.getDouble(i, j) / 255
          val g: Double = t._2._2.getDouble(i, j) / 255
          val b: Double = t._2._3.getDouble(i, j) / 255
          val ma = max(max(r, g), b)
          val mi = min(min(r, g), b)
          if (ma == mi) {
            hTile.setDouble(i, j, 0)
          }
          else if (ma == r && g >= b) {
            hTile.setDouble(i, j, 60 * ((g - b) / (ma - mi)))
          }
          else if (ma == r && g < b) {
            hTile.setDouble(i, j, 60 * ((g - b) / (ma - mi)) + 360)
          }
          else if (ma == g) {
            hTile.setDouble(i, j, 60 * ((b - r) / (ma - mi)) + 120)
          }
          else if (ma == b) {
            hTile.setDouble(i, j, 60 * ((r - g) / (ma - mi)) + 240)
          }
          if (ma == 0) {
            sTile.setDouble(i, j, 0)
          }
          else {
            sTile.setDouble(i, j, (ma - mi) / ma)
          }
          vTile.setDouble(i, j, ma)
        }
      }

      List((SpaceTimeBandKey(t._1, "Hue"), hTile), (SpaceTimeBandKey(t._1, "Saturation"), sTile), (SpaceTimeBandKey(t._1, "Value"), vTile))
    })
    (hsvRDD.flatMap(t => t), TileLayerMetadata(CellType.fromName("float32"), imageRed._2.layout, imageRed._2.extent, imageRed._2.crs, imageRed._2.bounds))
  }

  /**
   * Transforms the image from the HSV color space to the RGB color space. Expects a 3 band image in the range [0, 1],
   * and produces three bands: red, green and blue with values in the range [0, 255].
   *
   * @param imageHue The Hue coverage.
   * @param imageSaturation The Saturation coverage.
   * @param imageValue The Value coverage.
   * @return
   */
  def hsvToRgb(imageHue: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               imageSaturation: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               imageValue: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val imageHRDD = imageHue._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val imageSRDD = imageSaturation._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val imageVRDD = imageValue._1.map(t => {
      (t._1.spaceTimeKey, t._2)
    })
    val joinRDD = imageHRDD.join(imageSRDD).join(imageVRDD).map(t => {
      (t._1, (t._2._1._1, t._2._1._2, t._2._2))
    })
    val hsvRDD: RDD[List[(SpaceTimeBandKey, Tile)]] = joinRDD.map(t => {
      val rTile = t._2._1.convert(CellType.fromName("uint8")).mutable
      val gTile = t._2._2.convert(CellType.fromName("uint8")).mutable
      val bTile = t._2._3.convert(CellType.fromName("uint8")).mutable
      for (m <- 0 to 255) {
        for (n <- 0 to 255) {
          val h: Double = t._2._1.getDouble(m, n)
          val s: Double = t._2._2.getDouble(m, n)
          val v: Double = t._2._3.getDouble(m, n)

          val i: Double = (h / 60) % 6
          val f: Double = (h / 60) - i
          val p: Double = v * (1 - s)
          val q: Double = v * (1 - f * s)
          val u: Double = v * (1 - (1 - f) * s)
          if (i == 0) {
            rTile.set(m, n, (v * 255).toInt)
            gTile.set(m, n, (u * 255).toInt)
            bTile.set(m, n, (p * 255).toInt)
          }
          else if (i == 1) {
            rTile.set(m, n, (q * 255).toInt)
            gTile.set(m, n, (v * 255).toInt)
            bTile.set(m, n, (p * 255).toInt)
          }
          else if (i == 2) {
            rTile.set(m, n, (p * 255).toInt)
            gTile.set(m, n, (v * 255).toInt)
            bTile.set(m, n, (u * 255).toInt)
          }
          else if (i == 3) {
            rTile.set(m, n, (p * 255).toInt)
            gTile.set(m, n, (q * 255).toInt)
            bTile.set(m, n, (v * 255).toInt)
          }
          else if (i == 4) {
            rTile.set(m, n, (u * 255).toInt)
            gTile.set(m, n, (p * 255).toInt)
            bTile.set(m, n, (v * 255).toInt)
          }
          else if (i == 5) {
            rTile.set(m, n, (v * 255).toInt)
            gTile.set(m, n, (p * 255).toInt)
            bTile.set(m, n, (q * 255).toInt)
          }
        }
      }
      List((SpaceTimeBandKey(t._1, "red"), rTile), (SpaceTimeBandKey(t._1, "green"), gTile), (SpaceTimeBandKey(t._1, "blue"), bTile))
    })
    (hsvRDD.flatMap(t => t), TileLayerMetadata(CellType.fromName("uint8"), imageHue._2.layout, imageHue._2.extent, imageHue._2.crs, imageHue._2.bounds))
  }

  /**
   * Casts the input value to a signed 8-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt8(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
            ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("int8")))
  }), image._2)

  /**
   * Casts the input value to a unsigned 8-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toUint8(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("uint8")))
  }), image._2)

  /**
   * Casts the input value to a signed 16-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt16(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("int16")))
  }), image._2)

  /**
   * Casts the input value to a unsigned 16-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toUint16(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("uint16")))
  }), image._2)

  /**
   * Casts the input value to a signed 32-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt32(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("int32")))
  }), image._2)

  /**
   * Casts the input value to a 32-bit float.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toFloat(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("float32")))
  }), image._2)

  /**
   * Casts the input value to a 64-bit float.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toDouble(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = (image._1.map(t => {
    (t._1, t._2.convert(CellType.fromName("float64")))
  }), image._2)


  /**
   *
   * @param image1
   * @param image2
   * @return
   */
  def classificationDLCUG(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
                        image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
                       ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {


    // 处理 image1
    // 依照 spaceTimeKey 分组
    val imageGrouped: RDD[(SpaceTimeKey, Iterable[(SpaceTimeBandKey, Tile)])] = {
      image1._1.groupBy(t => t._1.spaceTimeKey)
    }
    // 根据波段序号排序
    val imageSorted: RDD[(SpaceTimeKey, Array[(SpaceTimeBandKey, Tile)])] =
      imageGrouped.map(t => {
        val tileArray: Array[(SpaceTimeBandKey, Tile)] = t._2.toArray
        (t._1, tileArray.sortBy(x => x._1.measurementName.toInt))
      }).persist()
    val floatArrayOfImage1: RDD[(SpaceTimeKey, Array[Float])] = imageSorted.map(t => {
      (t._1,
        ModelInference.byteToFloatOPT(
          t._2(0)._2.toBytes(), t._2(1)._2.toBytes(), t._2(2)._2.toBytes(), t._2(3)._2.toBytes()
        ))
    }
    )

    // 处理image2
    val floatArrayOfImage2: RDD[(SpaceTimeKey, Array[Float])] = image2.map(t => {
      (t._1.spaceTimeKey,
        ModelInference.byteToFloatSAR(
          t._2.toBytes()
        ))
    })


    // 连接 image1 和 image2 的 Float 数组
    val floatArrayInput: RDD[(SpaceTimeKey, (Array[Float], Array[Float]))] =
      floatArrayOfImage1.join(floatArrayOfImage2)


    // 将float数组对应瓦片合二为一，进行处理并得到返回值
    val floatArrayOutput: RDD[(SpaceTimeBandKey, Tile)] =
      floatArrayInput.map(t => (t._1, ModelInference.processOneTile(t._2._1, t._2._2)))
        .mapPartitions(iter => {
          // 拆分出三个波段并生成新的 rdd
          val res = List[(SpaceTimeBandKey, Tile)]()
          while (iter.hasNext) {
            val curTuple: (SpaceTimeKey, Array[Float]) = iter.next()

            val band1 = new Array[Byte](512 * 512)
            val band2 = new Array[Byte](512 * 512)
            val band3 = new Array[Byte](512 * 512)
            val prob = new Array[Float](8)
            for (k <- 0 until 512 * 512) {
              for (l <- 0 until 8) {
                prob(l) = FloatArrayTile(curTuple._2, 512, 512, FloatCellType)(k + l * 512 * 512)
              }
              val classValue: Int = prob.zipWithIndex.maxBy(_._1)._2 // 根据值的大小进行排序并返回索引

              classValue match {
                case 0 => {
                  band1(k) = -1
                  band2(k) = -1
                  band3(k) = -1
                }
                case 1 => {
                  band1(k) = -1
                  band2(k) = -1
                  band3(k) = 127
                }
                case 2 => {
                  band1(k) = -1
                  band2(k) = 127
                  band3(k) = -1
                }
                case 3 => {
                  band1(k) = 127
                  band2(k) = -1
                  band3(k) = -1
                }
                case 4 => {
                  band1(k) = 127
                  band2(k) = 127
                  band3(k) = -1
                }
                case 5 => {
                  band1(k) = 127
                  band2(k) = -1
                  band3(k) = 127
                }
                case 6 => {
                  band1(k) = -1
                  band2(k) = 127
                  band3(k) = 127
                }
                case 7 => {
                  band1(k) = 127
                  band2(k) = 127
                  band3(k) = 127
                }
              }
            }

            // 将三个波段的 key 及其对应的tile追加进去
            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand1DL"),
              ByteArrayTile(band1, 512, 512, ByteConstantNoDataCellType))
            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand2DL"),
              ByteArrayTile(band2, 512, 512, ByteConstantNoDataCellType))
            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand3DL"),
              ByteArrayTile(band3, 512, 512, ByteConstantNoDataCellType))

          }

          // 一变三
          res.iterator
        })

    // 返回rdd和 TileLayerMetadata

    (
      // RDD
      floatArrayOutput,
      // TileLayerMetadata
      TileLayerMetadata(
        ByteConstantNoDataCellType,
        image1._2.layout,
        image1._2.extent,
        image1._2.crs,
        image1._2.bounds
      )

    )





    //    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)


  }

  //  def newSZ(image:(RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), arg0, arg1): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) ={
  //
  //    //TODO RDD转换成TIFF
  //    //TODO 调用Anaconda python环境的代码，在python中执行QGIS算法之后，返回TIFF结果
  //    //TODO TIFF再转成RDD
  //
  //
  //
  //  }

  //Precipitation Anomaly Percentage
  def PAP(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), time: String, n: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var m = n
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //val now = "1000-01-01 00:00:00"
    val date = sdf.parse(time).getTime
    val timeYear = time.substring(0, 4)
    var timeBeginYear = timeYear.toInt - n
    if (timeYear.toInt - n < 2000) {
      timeBeginYear = 2000
      m = timeYear.toInt - 2000
    }
    val timeBegin = timeBeginYear.toString + time.substring(4, 19)
    val dateBegin = sdf.parse(timeBegin).getTime


    val imageReduced = image._1.filter(t => {
      val timeResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t._1.spaceTimeKey.instant)
      timeResult.substring(5, 7) == time.substring(5, 7)
    })
    val imageAverage = imageReduced.filter(t => {
      t._1.spaceTimeKey.instant >= dateBegin && t._1.spaceTimeKey.instant < date
    }).groupBy(t => t._1.spaceTimeKey.spatialKey)
      .map(t => {
        val tiles = t._2.map(x => x._2).reduce((a, b) => Add(a, b))
        val tileAverage = tiles.mapDouble(x => {
          x / t._2.size
        })
        (t._1, tileAverage)
      })
    val imageCal = imageReduced.filter(t => t._1.spaceTimeKey.instant == date).map(t => (t._1.spaceTimeKey.spatialKey, t._2))
    val PAP = imageAverage.join(imageCal).map(t => {
      val divide = Divide(t._2._2, t._2._1)
      val PAPTile = divide.mapDouble(x => {
        x - 1.0
      })
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "PAP"), PAPTile)
    })
    (PAP, image._2)
  }

  def visualizeOnTheFly(implicit sc: SparkContext, level: Int, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, min: Int = 0, max: Int = 255,
                        palette: String = null, layerID: Int, fileName: String = null): Unit = {
    val appID = sc.applicationId
    val outputPath = "/home/geocube/oge/on-the-fly" // TODO datas/on-the-fly
    if ("timeseries".equals(method)) {
      val TMSList = new ArrayBuffer[mutable.Map[String, Any]]()
      val resampledImage: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = resampleToTargetZoom(image, level, "Bilinear", downSampling = false)
      image._1.map(t => t._2)

      val timeList = resampledImage._1.map(t => t._1.spaceTimeKey.instant).distinct().collect()
      resampledImage._1.persist()
      timeList.foreach(x => {
        val tiled = resampledImage._1.filter(m => m._1.spaceTimeKey.instant == x).map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
        val cellType = resampledImage._2.cellType
        val srcLayout = resampledImage._2.layout
        val srcExtent = resampledImage._2.extent
        val srcCrs = resampledImage._2.crs
        val srcBounds = resampledImage._2.bounds
        val newBounds = Bounds(srcBounds.get.minKey.spatialKey, srcBounds.get.maxKey.spatialKey)
        val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)

        val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          TileLayerRDD(tiled, rasterMetaData)
            .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

        val myAcc: LongAccumulator = sc.longAccumulator("myAcc")
        //        println("targetAcc0 = " + myAcc.value)
        reprojected.map(t => {
          //          println("targetRows = " + t._2.rows) 256
          //          println("targetRows = " + t._2.cols) 256
          myAcc.add(1)
          t
        }).collect()

        println("targetNumOfTiles = " + myAcc.value)

        // Create the attributes store that will tell us information about our catalog.
        val attributeStore = FileAttributeStore(outputPath)
        // Create the writer that we will use to store the tiles in the local catalog.
        val writer: FileLayerWriter = FileLayerWriter(attributeStore)

        val time = System.currentTimeMillis()
        //        val layerIDAll = appID + "-layer-" + time + "_" + palette + "-" + min + "-" + max
        val layerIDAll: String = ImageTrigger.originTaskID
        // Pyramiding up the zoom levels, write our tiles out to the local file system.



        println("zoom = " + zoom)

        Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
          if (z == level) {
            val layerId = LayerId(layerIDAll, z)
            println(layerId)
            // If the layer exists already, delete it out before writing
            if (attributeStore.layerExists(layerId)) new FileLayerManager(attributeStore).delete(layerId)
            writer.write(layerId, rdd, ZCurveKeyIndexMethod)


          }
        }


        TMSList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-tms/" + layerIDAll + "/{z}/{x}/{y}")))
      }
      )











      //TODO 回调服务

      val resultSet: Map[String, ArrayBuffer[mutable.Map[String, Any]]] = Map(
        "raster" -> TMSList,
        "vector" -> ArrayBuffer.empty[mutable.Map[String, Any]],
        "table" -> ArrayBuffer.empty[mutable.Map[String, Any]],
        "rasterCube" -> new ArrayBuffer[mutable.Map[String, Any]](),
        "vectorCube" -> new ArrayBuffer[mutable.Map[String, Any]]()
      )
      implicit val formats = DefaultFormats
      val jsonStr: String = Serialization.write(resultSet)
      val deliverWordID: String = "{\"workID\":\"" + ImageTrigger.workID + "\"}"
      HttpUtil.postResponse(SystemConstants.DAG_ROOT_URL + "/deliverUrl", jsonStr, deliverWordID)











      // 写入文件
      //      Serve.runTMS(outputPath)
      //      val writeFile = new File(fileName)
      //      val writerOutput = new BufferedWriter(new FileWriter(writeFile))
      //      val result = mutable.Map[String, Any]()
      //      result += ("raster" -> TMSList)
      //      result += ("vector" -> ArrayBuffer.empty[mutable.Map[String, Any]])
      //      result += ("table" -> ArrayBuffer.empty[mutable.Map[String, Any]])
      //      val rasterCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      //      val vectorCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      //      result += ("rasterCube" -> rasterCubeList)
      //      result += ("vectorCube" -> vectorCubeList)
      //      implicit val formats = DefaultFormats
      //      val jsonStr: String = Serialization.write(result)
      //      writerOutput.write(jsonStr)
      //      writerOutput.close()
    }

    else {
      val tiled = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      })

      //      val tiledArray = tiled.collect()
      //      val layoutd = image._2.layout
      //      val (tiledd, (_, _), (_, _)) = TileLayoutStitcher.stitch(tiledArray)
      //      val stitchedTiled = Raster(tiledd, layoutd.extent)
      //      GeoTiff(stitchedTiled, image._2.crs).write("D:\\home\\geocube\\oge\\on-the-fly\\out1.tif")

      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
      val cellType = image._2.cellType
      val srcLayout = image._2.layout
      val srcExtent = image._2.extent
      val srcCrs = image._2.crs
      val srcBounds = image._2.bounds
      val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1 - srcBounds.get.minKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2 - srcBounds.get.minKey.spatialKey._2))
      val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)

      val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
        TileLayerRDD(tiled, rasterMetaData)
          .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

      //          val tileLayerArray = reprojected.map(t => {
      //            (t._1, t._2)
      //          }).collect()
      //          val layout = reprojected.metadata.layout
      //          val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
      //          val stitchedTile = Raster(tile, reprojected.metadata.extent)
      //          stitchedTile.tile.renderPng().write("D:/on-the-fly/out.png")
      //          GeoTiff(stitchedTile, CRS.fromEpsgCode(3857)).write("D:\\home\\geocube\\oge\\on-the-fly\\out2.tif")

      // Create the attributes store that will tell us information about our catalog.
      val attributeStore = FileAttributeStore(outputPath)
      // Create the writer that we will use to store the tiles in the local catalog.
      val writer = FileLayerWriter(attributeStore)
      val time = System.currentTimeMillis()
      val layerIDAll = appID + "-layer-" + time + "_" + palette + "-" + min + "-" + max
      // Pyramiding up the zoom levels, write our tiles out to the local file system.
      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
        if (z == zoom) {
          val layerId: LayerId = LayerId(layerIDAll, z)
          // If the layer exists already, delete it out before writing
          if (attributeStore.layerExists(layerId)) new FileLayerManager(attributeStore).delete(layerId)
          writer.write(layerId, rdd, ZCurveKeyIndexMethod)
        }
      }
      val writeFile = new File(fileName)
      val writerOutput = new BufferedWriter(new FileWriter(writeFile))
      val outputString = "{\"vectorCube\":[],\"rasterCube\":[],\"table\":[], \"vector\":[], \"raster\":[{\"url\":\"http://oge.whu.edu.cn/api/oge-tms/" + layerIDAll + "/{z}/{x}/{y}\"}]}"
      writerOutput.write(outputString)
      writerOutput.close()
      //            Serve.runTMS(outputPath)
    }
  }

  def visualizeBatch(implicit sc: SparkContext, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, layerID: Int, fileName: String = null): Unit = {
    val executorOutputDir = "datas/" // TODO
    val writeFile = new File(fileName)

    if (method == null) {
      val tileLayerArray = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      }).collect() // TODO
      val layout = image._2.layout
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray) // TODO
      val stitchedTile = Raster(tile, layout.extent)
      val time = System.currentTimeMillis()
      val path = executorOutputDir + "oge_" + time + ".tif"
      val path_img = "http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_" + time + ".tif"
      GeoTiff(stitchedTile, image._2.crs).write(path)
      val writer = new BufferedWriter(new FileWriter(writeFile))
      val outputString = "{\"vectorCube\":[],\"rasterCube\":[],\"table\":[],\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\"}]}"
      writer.write(outputString)
      writer.close()
    }
    else if ("timeseries".equals(method)) {
      val result = mutable.Map[String, Any]()
      val tableList = new ArrayBuffer[mutable.Map[String, Any]]()
      val vectorList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      val vectorCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      val time = System.currentTimeMillis()

      val writer = new BufferedWriter(new FileWriter(writeFile))
      val imageTimeSeries = image._1.groupBy(t => {
        t._1.spaceTimeKey.instant
      })
      val imagePng = imageTimeSeries.map(t => {
        val tileLayerArray = t._2.map(x => (x._1.spaceTimeKey.spatialKey, x._2)).toArray
        val layout = image._2.layout
        val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
        val stitchedTile = Raster(tile, layout.extent)
        (stitchedTile, t._1)
      }).collect()
      imagePng.sortBy(t => t._2).foreach(t => {
        val path = executorOutputDir + "oge_" + time + "_" + t._2 + "_timeseries" + ".tif"
        GeoTiff(t._1, image._2.crs).write(path)
        rasterList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_" + time + "_" + t._2 + "_timeseries" + ".tif")))
      })
      result += ("table" -> tableList)
      result += ("vector" -> vectorList)
      result += ("raster" -> rasterList)
      result += ("rasterCube" -> rasterCubeList)
      result += ("vectorCube" -> vectorCubeList)
      implicit val formats = DefaultFormats
      val jsonStr: String = Serialization.write(result)
      writer.write(jsonStr)
      writer.close()
    }
  }


  def visualizeBak(implicit sc: SparkContext, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, min: Int = 0, max: Int = 255,
                   palette: String = null): Unit = {
    val executorOutputDir = "D:/"
    //    val executorOutputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogedemooutput/"
    val writeFile = new File("D:/output.txt")
    //    val writeFile = new File("/home/geocube/oge/oge-server/dag-boot/output.txt")

    if (method == null) {
      val tileLayerArray = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      }).collect()
      val layout = image._2.layout
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
      val stitchedTile = Raster(tile, layout.extent)
      val time = System.currentTimeMillis()
      val path = executorOutputDir + "oge_" + time + ".png"
      val pathReproject = executorOutputDir + "oge_" + time + "_reproject.png"
      val path_img = "http://125.220.153.26:8093/ogedemooutput/oge_" + time + ".png"
      if (palette == null) {
        val colorMap = ColorMap(
          scala.Predef.Map(
            max -> geotrellis.raster.render.RGBA(255, 255, 255, 255),
            min -> geotrellis.raster.render.RGBA(0, 0, 0, 20),
            -1 -> geotrellis.raster.render.RGBA(255, 255, 255, 255)
          ),
          ColorMap.Options(
            classBoundaryType = Exact,
            noDataColor = 0x00000000, // transparent
            fallbackColor = 0x00000000, // transparent
            strict = false
          )
        )
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else if (palette == "green") {
        val colorMap = ColorMap(
          scala.Predef.Map(
            max -> geotrellis.raster.render.RGBA(127, 255, 170, 255),
            min -> geotrellis.raster.render.RGBA(0, 0, 0, 20)
          ),
          ColorMap.Options(
            classBoundaryType = Exact,
            noDataColor = 0x00000000, // transparent
            fallbackColor = 0x00000000, // transparent
            strict = false
          )
        )
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else if ("histogram".equals(palette)) {
        //set color
        val histogram = stitchedTile.tile.histogramDouble()
        val colorMap = geotrellis.raster.render.ColorMap.fromQuantileBreaks(histogram, geotrellis.raster.render.ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else {
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
        stitchedTile.tile.renderPng(colorRamp).write(path)
      }
      val writer = new BufferedWriter(new FileWriter(writeFile))
      if ("EPSG:4326".equals(image._2.crs.toString)) {
        val pointDestLD = new Coordinate(layout.extent.xmin, layout.extent.ymin)
        val pointDestRU = new Coordinate(layout.extent.xmax, layout.extent.ymax)
        val outputString = "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + pointDestLD.x + "," + pointDestLD.y + "," + pointDestRU.x + "," + pointDestRU.y + "]}]}"
        writer.write(outputString)
        writer.close()
      }
      else {
        val crsSource = org.geotools.referencing.CRS.decode(image._2.crs.toString)
        val crsTarget = org.geotools.referencing.CRS.decode("EPSG:4326")
        val transform = org.geotools.referencing.CRS.findMathTransform(crsSource, crsTarget)
        val pointSrcLD = new Coordinate(layout.extent.xmin, layout.extent.ymin)
        val pointSrcRU = new Coordinate(layout.extent.xmax, layout.extent.ymax)
        val pointDestLD = new Coordinate()
        val pointDestRU = new Coordinate()
        JTS.transform(pointSrcLD, pointDestLD, transform)
        JTS.transform(pointSrcRU, pointDestRU, transform)
        val outputString = "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + pointDestLD.x + "," + pointDestLD.y + "," + pointDestRU.x + "," + pointDestRU.y + "]}]}"
        writer.write(outputString)
        writer.close()
      }
    }
    else if ("timeseries".equals(method)) {
      val result = mutable.Map[String, Any]()
      val vectorList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterList = new ArrayBuffer[mutable.Map[String, Any]]()
      val time = System.currentTimeMillis()

      val writer = new BufferedWriter(new FileWriter(writeFile))
      val imageTimeSeries = image._1.groupBy(t => {
        t._1.spaceTimeKey.instant
      })
      val imagePng = imageTimeSeries.map(t => {
        val tileLayerArray = t._2.map(x => (x._1.spaceTimeKey.spatialKey, x._2)).toArray
        val layout = image._2.layout
        val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
        val stitchedTile = Raster(tile, layout.extent)
        if (palette == null) {
          val colorMap = ColorMap(
            scala.Predef.Map(
              max -> geotrellis.raster.render.RGBA(255, 255, 255, 255),
              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20),
              -1 -> geotrellis.raster.render.RGBA(255, 255, 255, 255)
            ),
            ColorMap.Options(
              classBoundaryType = Exact,
              noDataColor = 0x00000000, // transparent
              fallbackColor = 0x00000000, // transparent
              strict = false
            )
          )
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else if (palette == "green") {
          val colorMap = ColorMap(
            scala.Predef.Map(
              max -> geotrellis.raster.render.RGBA(127, 255, 170, 255),
              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20)
            ),
            ColorMap.Options(
              classBoundaryType = Exact,
              noDataColor = 0x00000000, // transparent
              fallbackColor = 0x00000000, // transparent
              strict = false
            )
          )
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else if ("histogram".equals(palette)) {
          //set color
          val histogram = stitchedTile.tile.histogramDouble()
          val colorMap = geotrellis.raster.render.ColorMap.fromQuantileBreaks(histogram, geotrellis.raster.render.ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else {
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
          (stitchedTile.tile.renderPng(colorRamp), stitchedTile.extent, t._1)
        }
      }).collect()
      imagePng.sortBy(t => t._3).foreach(t => {
        val path = executorOutputDir + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"
        t._1.write(path)
        if ("EPSG:4326".equals(image._2.crs.toString)) {
          val pointDestLD = new Coordinate(t._2.xmin, t._2.ymin)
          val pointDestRU = new Coordinate(t._2.xmax, t._2.ymax)
          rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"), "extent" -> Array(pointDestLD.x, pointDestLD.y, pointDestRU.x, pointDestRU.y)))
        }
        else {
          val crsSource = org.geotools.referencing.CRS.decode(image._2.crs.toString)
          val crsTarget = org.geotools.referencing.CRS.decode("EPSG:4326")
          val transform = org.geotools.referencing.CRS.findMathTransform(crsSource, crsTarget)
          val pointSrcLD = new Coordinate(t._2.xmin, t._2.ymin)
          val pointSrcRU = new Coordinate(t._2.xmax, t._2.ymax)
          val pointDestLD = new Coordinate()
          val pointDestRU = new Coordinate()
          JTS.transform(pointSrcLD, pointDestLD, transform)
          JTS.transform(pointSrcRU, pointDestRU, transform)
          rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"), "extent" -> Array(pointDestLD.x, pointDestLD.y, pointDestRU.x, pointDestRU.y)))
        }
      })
      result += ("vector" -> vectorList)
      result += ("raster" -> rasterList)
      implicit val formats = DefaultFormats
      val jsonStr: String = Serialization.write(result)
      writer.write(jsonStr)
      writer.close()
    }
  }
}
