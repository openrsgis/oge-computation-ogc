package whu.edu.cn.debug

import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.{FileLayerReader, FileLayerWriter}
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index._
import geotrellis.vector.Extent
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.{CoverageCollectionMetadata, CoverageMetadata, RawTile, SpaceTimeBandKey}
import whu.edu.cn.oge.Coverage
import whu.edu.cn.oge.CoverageCollection.mosaic
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageCollectionUtil.makeCoverageCollectionRDD
import whu.edu.cn.util.CoverageUtil.makeCoverageRDD
import whu.edu.cn.util.MinIOUtil
import whu.edu.cn.util.PostgresqlServiceUtil.{queryCoverage, queryCoverageCollection}

import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import geotrellis.raster.mapalgebra.focal

object load {
  def main(args: Array[String]): Unit = {
    val time1: Long = System.currentTimeMillis()




    // MOD13Q1.A2022241.mosaic.061.2022301091738.psmcrpgs_000501861676.250m_16_days_NDVI-250m_16_days
    // LC08_L1TP_124038_20181211_20181226_01_T1
    // LE07_L1TP_125039_20130110_20161126_01_T1

    testCoverage()
    val time2: Long = System.currentTimeMillis()
    println("Total Time is " + (time2 - time1))


    println("_")

  }

  def testCoverage(): Unit={
    val NoData: Int = -2147483648
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("Test")
    val sc = new SparkContext(conf)
    val array = Array[Double](Double.NaN, -1, Double.NaN, 0.45, 5, 6, 7, 8, 10)

    var coverage1 : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage
      .makeFakeCoverage(sc,array,mutable.ListBuffer[String]("111","222","333"),3,3)
    val array2 = Array[Int](123,231,99,123,255,255,128,234,132)
    var coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage
      .makeFakeCoverage(sc, array2,mutable.ListBuffer[String]("111","222","333"), 3, 3)
    coverage2 = Coverage.entropy(coverage2,"square",5)
//    coverage1 = Coverage.focalMax(coverage1,"square",5)

    val coverage = Coverage.rgbToHsv(coverage1)
    //    println(coverage)
    for(band<-coverage2.first()._2.bands){
      val arr: Array[Double] = band.toArrayDouble()
      println(arr.mkString(","))
    }
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

  def ndviLandsat7(): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", 6)
    val coverageDouble: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.toDouble(coverage)
    val ndwi: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.normalizedDifference(coverageDouble, List("B4", "B3"))

    makeTIFF(coverage, "ls")
    makeTIFF(coverageDouble, "lsD")
    makeTIFF(ndwi, "lsNDWI")
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

  def loadModis(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val coverageModis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "MOD13Q1.A2022241.mosaic.061.2022301091738.psmcrpgs_000501861676.250m_16_days_NDVI-250m_16_days", 7)
    makeTIFF(coverageModis, "modis")
    makeTMS(sc, coverageModis, "aah")
  }

  def loadLandsat7(): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", 6)
    val coverage1Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage1, List("B1", "B2", "B3"))
    makeTIFF(coverage1Select, "c1")
    makeTMS(sc, coverage1Select, "aah")
  }

  def loadLandsat8(): Unit = {
    val time1: Long = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", 6)
    val coverage1Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage1, List("B1", "B2", "B3"))
    makeTIFF(coverage1Select, "c1")
    val coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LC08_L1TP_124039_20180109_20180119_01_T1", 5)
    val coverage2Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage2, List("B1", "B2", "B3"))
    makeTIFF(coverage2Select, "c2")


    val coverage3: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.add(coverage1Select, coverage2Select)
    makeTIFF(coverage3, "c3")

    val coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map()
    val a: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = coverageCollection + ("LE07_L1TP_125039_20130110_20161126_01_T1" -> coverage1Select)
    val b: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = a + ("LC08_L1TP_124039_20180109_20180119_01_T1" -> coverage2Select)
    val coverageMosaic: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(b)
    makeTIFF(coverageMosaic, "cMosaic")
  }

  def loadCoverage(implicit sc: SparkContext, coverageId: String, level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId)
    val queryGeometry: Geometry = metaList.head.getGeom

    //    val queryGeometry: Geometry = geotrellis.vector.io.readWktOrWkb("POLYGON((110.45709 30.26141,110.59998 30.26678,110.58066 29.94492,110.4869 29.93994,110.45709 30.26141))")

    println("bandNum is " + metaList.length)

    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .map(t => { // 合并所有的元数据（追加了范围）
        val time1: Long = System.currentTimeMillis()
        val rawTiles: mutable.ArrayBuffer[RawTile] = {
          val client: MinioClient = new MinIOUtil().getMinioClient
          val tiles: mutable.ArrayBuffer[RawTile] = tileQuery(client, level, t, queryGeometry)
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
    val rawTileRdd: RDD[RawTile] = tileRDDRePar.map(t => {
      val time1: Long = System.currentTimeMillis()
      val client: MinioClient = new MinIOUtil().getMinioClient
      val tile: RawTile = getTileBuf(client, t)
      val time2: Long = System.currentTimeMillis()
      println("Get Tile Time is " + (time2 - time1))
      tile
    })
    makeCoverageRDD(rawTileRdd)
  }

  def loadCoverageCollection(implicit sc: SparkContext, productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: LocalDateTime = null, endTime: LocalDateTime = null, extent: Extent = null, crs: CRS = null, level: Int = 0): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    val metaList: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, extent, crs)
    val metaListGrouped: Map[String, ListBuffer[CoverageMetadata]] = metaList.groupBy(t => t.getCoverageID)
    val rawTileRdd: Map[String, RDD[RawTile]] = metaListGrouped.map(t => {
      val metaListCoverage: ListBuffer[CoverageMetadata] = t._2
      val tileDataTuple: RDD[CoverageMetadata] = sc.makeRDD(metaListCoverage)
      val tileRDDFlat: RDD[RawTile] = tileDataTuple
        .map(t => {
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val client: MinioClient = new MinIOUtil().getMinioClient
            val tiles: mutable.ArrayBuffer[RawTile] = tileQuery(client, level, t, extent)
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
