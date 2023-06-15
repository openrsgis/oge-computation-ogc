package whu.edu.cn.oge

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata, RawTile, SpaceTimeBandKey}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageCollectionUtil.{checkMapping, coverageCollectionMosaicTemplate, makeCoverageCollectionRDD}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection
import whu.edu.cn.util.{GlobalConstantUtil, JedisUtil, MinIOUtil, ZCurveUtil}

import java.time.{Instant, LocalDateTime}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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


  // TODO lrx: 检查相同的影像被写入同一个CoverageCollection
  // TODO lrx: 如果相同波段就拼（包括顺序、个数、名称），不相同就不拼
  def mosaic(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mean")
  }

  def mean(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mean")
  }

  def min(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "min")
  }

  def max(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "max")
  }

  def sum(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "sum")
  }

  def or(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "or")
  }

  def and(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "and")
  }

  def median(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "median")
  }

  def mode(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "mode")
  }


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

  def visualizeOnTheFly(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {

//    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(coverageCollection)
//
//
//
//
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

  def visualizeBatch(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {
  }

}
