package whu.edu.cn.oge

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile, TileLayout}
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata, CoverageRDDAccumulator, RawTile, SpaceTimeBandKey}
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageCollectionUtil.{checkMapping, makeCoverageCollectionRDD}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection
import whu.edu.cn.util.TileMosaicCoverageUtil.tileMosaic
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
   * @return ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile])
   */
  def load(implicit sc: SparkContext, productName: String,
           sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String],
           startTime: LocalDateTime = null, endTime: LocalDateTime = null, extent: Extent = null, crs: CRS = null, level: Int = 0): Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = {
    val zIndexStrArray: ArrayBuffer[String] = Trigger.zIndexStrArray

    val key: String = Trigger.originTaskID + ":solvedTile:" + level
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


  def mosaic(implicit sc: SparkContext, tileRDDReP: RDD[RawTile], method: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO
    val extents = tileRDDReP.map(t => {
      (t.getExtent.xmin, t.getExtent.ymin, t.getExtent.xmax, t.getExtent.ymax)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant = tileRDDReP.map(t => {
      (t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)), t.getSpatialKey.col, t.getSpatialKey.row, t.getTime.toEpochSecond(ZoneOffset.ofHours(0)))
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
    val cellType = CellType.fromName(firstTile.getDataType.toString)
    val crs = firstTile.getCrs
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(tl.layoutCols - 1, tl.layoutRows - 1, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    val rawTileRDD = tileRDDReP.map(t => {
      val client = new MinIOUtil().getMinioClient
      val tile = getTileBuf(client, t)
      t.setTile(deserializeTileData("", tile.getTileBuf, 256, tile.getDataType.toString))
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
            rawTile.getExtent.xmin, // X
            rawTile.getExtent.ymin, // Y
            rawTile.getExtent.xmax, // X
            rawTile.getExtent.ymax // Y
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
    tileMosaic(TileRDDUnComputed, method, tileLayerMetadata, firstTile.getDataType.toString)
  }


  def map(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])], baseAlgorithm: String): Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = {

    coverageCollection.foreach(coverage => {
      val coverageId: String = coverage._1
      val coverageRdd: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = coverage._2
      Trigger.coverageRddList += (coverageId -> coverageRdd)
    })

    val coverageIds: Iterable[String] = coverageCollection.keys

    val coverageAfterComputation: mutable.ArrayBuffer[(String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]))] = mutable.ArrayBuffer.empty[(String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]))]

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

}
