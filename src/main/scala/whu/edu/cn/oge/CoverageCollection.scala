package whu.edu.cn.oge

import com.baidubce.services.bos.BosClient
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.MultibandTile
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity._
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageCollectionUtil.{checkMapping, coverageCollectionMosaicTemplate, makeCoverageCollectionRDD}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection
import whu.edu.cn.util.{COGUtil, ClientUtil, ZCurveUtil}

import java.time.LocalDateTime
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
  def load(implicit sc: SparkContext, productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: String = null, endTime: String = null, extent: Extent = null, crs: CRS = null, level: Int = 0,cloudCoverMin: Float = 0, cloudCoverMax: Float = 100): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    //    val zIndexStrArray: ArrayBuffer[String] = Trigger.zIndexStrArray
    //
    //    // TODO lrx: 改造前端瓦片转换坐标、行列号的方式
    //    val unionTileExtent: Geometry = zIndexStrArray.map(zIndexStr => {
    //      val xy: Array[Int] = ZCurveUtil.zCurveToXY(zIndexStr, level)
    //      val lonMinOfTile: Double = ZCurveUtil.tile2Lon(xy(0), level)
    //      val latMinOfTile: Double = ZCurveUtil.tile2Lat(xy(1) + 1, level)
    //      val lonMaxOfTile: Double = ZCurveUtil.tile2Lon(xy(0) + 1, level)
    //      val latMaxOfTile: Double = ZCurveUtil.tile2Lat(xy(1), level)
    //
    //      val minCoordinate = new Coordinate(lonMinOfTile, latMinOfTile)
    //      val maxCoordinate = new Coordinate(lonMaxOfTile, latMaxOfTile)
    //      val envelope: Envelope = new Envelope(minCoordinate, maxCoordinate)
    //      val geometry: Geometry = new GeometryFactory().toGeometry(envelope)
    //      geometry
    //    }).reduce((a, b) => {
    //      a.union(b)
    //    })
    //    var union: Geometry = unionTileExtent
    //    if (extent != null) {
    //      union = unionTileExtent.intersection(extent)
    //    }
    val union = extent
    val metaList: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, union, crs,cloudCoverMin, cloudCoverMax)
    val metaListGrouped: Map[String, ListBuffer[CoverageMetadata]] = metaList.groupBy(t => t.getCoverageID)
    val cogUtil: COGUtil = COGUtil.createCOGUtil(CLIENT_NAME)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val rawTileRdd: Map[String, RDD[RawTile]] = metaListGrouped.map(t => {
      val metaListCoverage: ListBuffer[CoverageMetadata] = t._2
      val tileDataTuple: RDD[CoverageMetadata] = sc.makeRDD(metaListCoverage)
      val tileRDDFlat: RDD[RawTile] = tileDataTuple
        .map(t => {
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val client = clientUtil.getClient
            val tiles: mutable.ArrayBuffer[RawTile] = cogUtil.tileQuery(client, level, t, Extent(union.getEnvelopeInternal),t.getGeom)
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
      val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 16))
      (t._1, tileRDDRePar.mapPartitions(par => {
        val client = clientUtil.getClient
        par.map(t=>{
          cogUtil.getTileBuf(client,t)
        })
      }))
    })

    makeCoverageCollectionRDD(rawTileRdd)
  }


  def mergeCoverages(coverages: List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],names: List[String]): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] ={
    if(coverages.length != names.length) {
      throw new Exception("Coverages 和 Names数量不匹配！")
    }
    val resMap = names.zip(coverages).toMap

    resMap
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

  def cat(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageCollectionMosaicTemplate(coverageCollection, "cat")
  }

  // TODO lrx: 这里要添加并行，并行之前的并行，需要写个调度
  def map(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], baseAlgorithm: String, params: Any*): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {

    coverageCollection.map(t =>{
      (t._1,Relection.reflectCall(baseAlgorithm,t._2,params))
    })
    //    coverageCollection.foreach(coverage => {
    //      val coverageId: String = coverage._1
    //      val coverageRdd: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage._2
    //      Trigger.coverageRddList += (coverageId -> coverageRdd)
    //    })
    //
    //    val coverageIds: Iterable[String] = coverageCollection.keys
    //
    //    val coverageAfterComputation: mutable.ArrayBuffer[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))] = mutable.ArrayBuffer.empty[(String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))]
    //
    //    for (coverageId <- coverageIds) {
    //      val dagChildren: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = mutable.ArrayBuffer.empty[(String, String, mutable.Map[String, String])]
    //      Trigger.optimizedDagMap(baseAlgorithm).foreach(t => {
    //        dagChildren += ((t._1, t._2, t._3.clone()))
    //      })
    //      dagChildren.foreach(algorithm => {
    //        checkMapping(coverageId, algorithm)
    //        Trigger.coverageRddList.remove(algorithm._1)
    //        Trigger.func(sc, algorithm._1, algorithm._2, algorithm._3)
    //      })
    //      coverageAfterComputation.append(coverageId -> Trigger.coverageRddList(baseAlgorithm))
    //    }
    //
    //    coverageAfterComputation.toMap
  }

  def filter(filter: String, collection: CoverageCollectionMetadata): CoverageCollectionMetadata = {
    val newCollection: CoverageCollectionMetadata = collection
    val filterGet: (String, mutable.Map[String, String]) = Trigger.lazyFunc(filter)
    Filter.func(newCollection, filter, filterGet._1, filterGet._2)
    newCollection
  }

  def visualizeOnTheFly(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], visParam: VisualizationParam): Unit = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(coverageCollection)
    COGUtil.extent = coverage._2.extent
    Coverage.visualizeOnTheFly(sc, coverage, visParam)
  }

  def visualizeBatch(implicit sc: SparkContext, coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]): Unit = {
  }

}
