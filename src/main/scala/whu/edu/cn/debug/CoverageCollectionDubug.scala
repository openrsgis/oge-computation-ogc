package whu.edu.cn.debug

import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index._
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata, RawTile, SpaceTimeBandKey}
import whu.edu.cn.oge.CoverageCollection.mosaic
import whu.edu.cn.util.CoverageCollectionUtil.makeCoverageCollectionRDD
import whu.edu.cn.util.{COGUtil, ClientUtil}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection

import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object CoverageCollectionDubug {
  def main(args: Array[String]): Unit = {
    val time1: Long = System.currentTimeMillis()


    // MOD13Q1.A2022241.mosaic.061.2022301091738.psmcrpgs_000501861676.250m_16_days_NDVI-250m_16_days
    // LC08_L1TP_124038_20181211_20181226_01_T1
    // LE07_L1TP_125039_20130110_20161126_01_T1

    ndviLandsatCollection()
    val time2: Long = System.currentTimeMillis()
    println("Total Time is " + (time2 - time1))


    println("_")

  }

  def ndviLandsatCollection(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val collectionMeta = new CoverageCollectionMetadata
    collectionMeta.setStartTime("2013-01-01 00:00:00")
    collectionMeta.setEndTime("2013-12-31 00:00:00")
    val landsatCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = loadCoverageCollection(sc, productName = "LE07_L1TP_C01_T1", startTime = collectionMeta.getStartTime, endTime = collectionMeta.getEndTime, extent = Extent(111.23, 29.31, 116.8, 31.98), level = 5)
    val landsatCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(landsatCollection)
    makeTIFF(landsatCoverage, "collection2coverage")
  }

  def loadLandsatCollection(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val collectionMeta = new CoverageCollectionMetadata
    collectionMeta.setStartTime("2013-01-01 00:00:00")
    collectionMeta.setEndTime("2013-12-31 00:00:00")
    val landsatCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = loadCoverageCollection(sc, productName = "LE07_L1TP_C01_T1", startTime = collectionMeta.getStartTime, endTime = collectionMeta.getEndTime, extent = Extent(111.23, 29.31, 116.8, 31.98), level = 5)
    val landsatCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(landsatCollection)
    makeTIFF(landsatCoverage, "collection2coverage")
  }

  def loadCoverageCollection(implicit sc: SparkContext, productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: LocalDateTime = null, endTime: LocalDateTime = null, extent: Extent = null, crs: CRS = null, level: Int = 0): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    val metaList: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime.toString, endTime.toString, extent, crs)
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
            val tiles: mutable.ArrayBuffer[RawTile] = cogUtil.tileQuery(client, level, t, extent,metaList.head.getGeom)
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
        val client = clientUtil.getClient
        val tile: RawTile = cogUtil.getTileBuf(client, t)
        //        MinIOUtil.releaseMinioClient(client)
        val time2: Long = System.currentTimeMillis()
        println("Get Tile Time1 is " + (time2 - time1))
        tile
      }))
    })

    makeCoverageCollectionRDD(rawTileRdd)
  }

  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), name: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    val writePath: String = "D:/cog/out/" + name + ".tiff"
    GeoTiff(stitchedTile, coverage._2.crs).write(writePath)
  }

  def makeTIFF(coverage: MultibandTileLayerRDD[SpatialKey], name: String): Unit = {
    val tileArray: Array[(SpatialKey, MultibandTile)] = coverage.collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage.metadata.extent)
    val writePath: String = "D:/cog/out/" + name + ".tiff"
    GeoTiff(stitchedTile, coverage.metadata.crs).write(writePath)
  }

  def makePNG(coverage: MultibandTileLayerRDD[SpatialKey], name: String): Unit = {
    val tileArray: Array[(SpatialKey, MultibandTile)] = coverage.collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage.metadata.extent)
    val writePath: String = "D:/cog/out/" + name + ".png"
    stitchedTile.tile.renderPng().write(writePath)
  }

  def makeTMS(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), layerName: String): Unit = {
    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverage._2.bounds.get.minKey.spatialKey, coverage._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage._2.cellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)
        .reproject(tmsCrs, layoutScheme)

    val outputPath: String = "/mnt/storage/on-the-fly"
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
    // Create the writer that we will use to store the tiles in the local catalog.
    val writer: FileLayerWriter = FileLayerWriter(attributeStore)

    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      val layerId: LayerId = LayerId(layerName, z)
      println(layerId)
      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        //        new FileLayerManager(attributeStore).delete(layerId)
        try {
          writer.overwrite(layerId, rdd)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
      else {
        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
    }
  }

}
