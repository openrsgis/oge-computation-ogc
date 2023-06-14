package whu.edu.cn.oge

import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{CellType, MultibandTile, Tile, TileLayout}
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.{FileLayerManager, FileLayerWriter}
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata, CoverageRDDAccumulator, RawTile, SpaceTimeBandKey}
import whu.edu.cn.geocube.util.HttpUtil
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageCollectionUtil.{checkMapping, makeCoverageCollectionRDD}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection
import whu.edu.cn.util.TileSerializerCoverageUtil.deserializeTileData
import whu.edu.cn.util.{GlobalConstantUtil, JedisUtil, MinIOUtil, ZCurveUtil}

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{max, min}

object CoverageCollection {
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
   * @return ((RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile])
   */
  def load(implicit sc: SparkContext, productName: String,
           sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String],
           startTime: LocalDateTime = null, endTime: LocalDateTime = null, extent: Extent = null, crs: CRS = null, level: Int = 0): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    val zIndexStrArray: ArrayBuffer[String] = Trigger.zIndexStrArray

    val key: String = Trigger.userId + Trigger.timeId + ":solvedTile:" + level
    val jedis: Jedis = new JedisUtil().getJedis
    jedis.select(1)

    // TODO lrx: 改造前端瓦片转换坐标、行列号的方式
    val unionTileExtent: geotrellis.vector.Geometry = zIndexStrArray.map(zIndexStr => {
      val xy: Array[Int] = ZCurveUtil.zCurveToXY(zIndexStr, level)
      val lonMinOfTile: Double = ZCurveUtil.tile2Lon(xy(0), level)
      val latMinOfTile: Double = ZCurveUtil.tile2Lat(xy(1) + 1, level)
      val lonMaxOfTile: Double = ZCurveUtil.tile2Lon(xy(0) + 1, level)
      val latMaxOfTile: Double = ZCurveUtil.tile2Lat(xy(1), level)
      // TODO lrx: 这里还需要存前端的实际瓦片瓦片，这里只存了编号
      jedis.sadd(key, zIndexStr)
      jedis.expire(key, GlobalConstantUtil.REDIS_CACHE_TTL)
      val geometry: geotrellis.vector.Geometry = new Extent(lonMinOfTile, latMinOfTile, lonMaxOfTile, latMaxOfTile).toPolygon()
      geometry
    }).reduce((a, b) => {
      a.union(b)
    })
    val union: geotrellis.vector.Geometry = unionTileExtent.union(extent)
    jedis.close()

    val metaList: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, union, crs)
    val metaListGrouped: Map[String, ListBuffer[CoverageMetadata]] = metaList.groupBy(t => t.getCoverageID)
    val rawTileRdd: Map[String, RDD[RawTile]] = metaListGrouped.map(t => {
      val metaListCoverage: ListBuffer[CoverageMetadata] = t._2
      val tileDataTuple: RDD[CoverageMetadata] = sc.makeRDD(metaListCoverage)
      val tileRDDFlat: RDD[RawTile] = tileDataTuple
        .map(t => {
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val client: MinioClient = new MinIOUtil().getMinioClient
            val tiles: mutable.ArrayBuffer[RawTile] = tileQuery(client, level, t, union)
            tiles
          }
          val time2: Long = System.currentTimeMillis()
          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        }).flatMap(t => t).persist()

      val tileNum: Int = tileRDDFlat.count().toInt
      println("tileNum = " + tileNum)
      tileRDDFlat.unpersist()
      val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 90))
      (t._1, tileRDDRePar.map(t => {
        val time1: Long = System.currentTimeMillis()
        val client: MinioClient = new MinIOUtil().getMinioClient
        val tile: RawTile = getTileBuf(client, t)
        val time2: Long = System.currentTimeMillis()
        println("Get Tile Time is " + (time2 - time1))
        tile
      }).cache())
    })

    makeCoverageCollectionRDD(rawTileRdd)
  }


//  def mosaic(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], method: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO
//
//    val extents = tileRDDReP.map(t => {
//      (t.getExtent.xmin, t.getExtent.ymin, t.getExtent.xmax, t.getExtent.ymax)
//    }).reduce((a, b) => {
//      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
//    })
//    val colRowInstant = tileRDDReP.map(t => {
//      (t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)), t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)))
//    }).reduce((a, b) => {
//      (min(a._1, b._1), min(a._2, b._2),
//        min(a._3, b._3), max(a._4, b._4),
//        max(a._5, b._5), max(a._6, b._6))
//    })
//    val firstTile = tileRDDReP.take(1)(0)
//    val tl = TileLayout(Math.round((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.round((extents._4 - extents._2) / firstTile.getResolution / 256.0).toInt, 256, 256)
//    println(tl)
//    val extent = geotrellis.vector.Extent(extents._1, extents._4 - tl.layoutRows * 256 * firstTile.getResolution, extents._1 + tl.layoutCols * 256 * firstTile.getResolution, extents._4)
//    println(extent)
//    val ld = LayoutDefinition(extent, tl)
//    val cellType = CellType.fromName(firstTile.getDataType.toString)
//    val crs = firstTile.getCrs
//    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(tl.layoutCols - 1, tl.layoutRows - 1, colRowInstant._6))
//    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
//
//    val rawTileRDD = tileRDDReP.map(t => {
//      val client = new MinIOUtil().getMinioClient
//      val tile = getTileBuf(client, t)
//      t.setTile(deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString))
//      t
//    })
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val rawtileArray = rawTileRDD.collect()
//    val RowSum = ld.tileLayout.layoutRows
//    val ColSum = ld.tileLayout.layoutCols
//    val tileBox = new ListBuffer[((Extent, SpaceTimeKey), List[RawTile])]
//    for (i <- 0 until ColSum) {
//      for (j <- 0 until RowSum) {
//        val rawTiles = new ListBuffer[RawTile]
//        val tileExtent = new Extent(extents._1 + i * 256 * firstTile.getResolution, extents._4 - (j + 1) * 256 * firstTile.getResolution, extents._1 + (i + 1) * 256 * firstTile.getResolution, extents._4 - j * 256 * firstTile.getResolution)
//        for (rawTile <- rawtileArray) {
//          if (Extent(
//            rawTile.getExtent.xmin, // X
//            rawTile.getExtent.ymin, // Y
//            rawTile.getExtent.xmax, // X
//            rawTile.getExtent.ymax // Y
//          ).intersects(tileExtent))
//            rawTiles.append(rawTile)
//        }
//        if (rawTiles.nonEmpty) {
//          val now = "1000-01-01 00:00:00"
//          val date = sdf.parse(now).getTime
//          tileBox.append(((tileExtent, SpaceTimeKey(i, j, date)), rawTiles.toList))
//        }
//      }
//    }
//    val TileRDDUnComputed = sc.parallelize(tileBox, tileBox.length)
//    tileMosaic(TileRDDUnComputed, method, tileLayerMetadata, firstTile.getDataType.toString)
//  }


  def map(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], baseAlgorithm: String): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {

    coverageCollection.foreach(coverage => {
      val coverageId: String = coverage._1
      val coverageRdd: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage._2
      Trigger.coverageRddList += (coverageId -> coverageRdd)
    })

    val coverageIds: Iterable[String] = coverageCollection.keys

    val coverageAfterComputation: mutable.ArrayBuffer[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))] = mutable.ArrayBuffer.empty[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))]

    for (coverageId <- coverageIds) {
      val dagChildren: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = Trigger.optimizedDagMap(baseAlgorithm)
      dagChildren.foreach(algorithm => {
        checkMapping(coverageId, algorithm)
        Trigger.coverageRddList.remove(algorithm._1)
        Trigger.func(sc, algorithm._1, algorithm._2, algorithm._3)
      })
      coverageAfterComputation.append(coverageId -> Trigger.coverageRddList(baseAlgorithm))
    }

    coverageAfterComputation.toMap
  }

  def filter(filter: String, collection: CoverageCollectionMetadata): CoverageCollectionMetadata = {
    val newCollection: CoverageCollectionMetadata = collection
    val filterGet: (String, mutable.Map[String, String]) = Trigger.lazyFunc(filter)
    Filter.func(newCollection, filter, filterGet._1, filterGet._2)
    newCollection
  }

  def visualizeOnTheFly(implicit sc:SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {

//    val outputPath = "/mnt/storage/on-the-fly" // TODO datas/on-the-fly
//    val TMSList = new ArrayBuffer[mutable.Map[String, Any]]()
//
//    val resampledCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = resampleToTargetZoom(coverage, Trigger.level, "Bilinear")
//
//    val tiled = resampledCoverage.map(t => {
//      (t._1.spaceTimeKey.spatialKey, t._2)
//    })
//    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
//    val cellType = resampledCoverage._2.cellType
//    val srcLayout = resampledCoverage._2.layout
//    val srcExtent = resampledCoverage._2.extent
//    val srcCrs = resampledCoverage._2.crs
//    val srcBounds = resampledCoverage._2.bounds
//    val newBounds = Bounds(srcBounds.get.minKey.spatialKey, srcBounds.get.maxKey.spatialKey)
//    val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
//
//    val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
//      TileLayerRDD(tiled, rasterMetaData)
//        .reproject(WebMercator, layoutScheme)
//
//    // Create the attributes store that will tell us information about our catalog.
//    val attributeStore = FileAttributeStore(outputPath)
//    // Create the writer that we will use to store the tiles in the local catalog.
//    val writer: FileLayerWriter = FileLayerWriter(attributeStore)
//
//    val layerIDAll: String = Trigger.userId + Trigger.timeId
//    // Pyramiding up the zoom levels, write our tiles out to the local file system.
//
//    println("Final Front End Level = " + Trigger.level)
//    println("Final Back End Level = " + zoom)
//
//    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
//      if (z == Trigger.level) {
//        val layerId = LayerId(layerIDAll, z)
//        println(layerId)
//        // If the layer exists already, delete it out before writing
//        if (attributeStore.layerExists(layerId)) new FileLayerManager(attributeStore).delete(layerId)
//        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
//      }
//    }
//
//    TMSList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-dag/" + layerIDAll + "/{z}/{x}/{y}.png")))
//    // 清空list
//    //    Trigger.coverageRDDList.clear()
//    //    Trigger.coverageRDDListWaitingForMosaic.clear()
//    //    Trigger.tableRDDList.clear()
//    //    Trigger.featureRDDList.clear()
//    //    Trigger.kernelRDDList.clear()
//    //    Trigger.coverageLoad.clear()
//    //    Trigger.filterEqual.clear()
//    //    Trigger.filterAnd.clear()
//    //    Trigger.cubeRDDList.clear()
//    //    Trigger.cubeLoad.clear()
//
//    //TODO 回调服务
//
//    val resultSet: Map[String, ArrayBuffer[mutable.Map[String, Any]]] = Map(
//      "raster" -> TMSList,
//      "vector" -> ArrayBuffer.empty[mutable.Map[String, Any]],
//      "table" -> ArrayBuffer.empty[mutable.Map[String, Any]],
//      "rasterCube" -> new ArrayBuffer[mutable.Map[String, Any]](),
//      "vectorCube" -> new ArrayBuffer[mutable.Map[String, Any]]()
//    )
//    implicit val formats = DefaultFormats
//    val jsonStr: String = Serialization.write(resultSet)
//    val deliverWordID: String = "{\"workID\":\"" + Trigger.workID + "\"}"
//    HttpUtil.postResponse(GlobalConstantUtil.DAG_ROOT_URL + "/deliverUrl", jsonStr, deliverWordID)
  }

  def visualizeBatch(implicit sc:SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {
  }

}
