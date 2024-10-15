package whu.edu.cn.debug

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.GetObjectRequest
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}
import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.ColorRamps
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
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity.{CoverageMetadata, RawTile, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.oge.{Coverage, CoverageCollection}
import whu.edu.cn.oge.CoverageCollection.{mosaic, visualizeOnTheFly}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageUtil.{checkProjResoExtent, makeCoverageRDD}
import whu.edu.cn.util.{COGUtil, ClientUtil, CoverageCollectionUtil, RDDTransformerUtil}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CoverageDubug {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val coverage = makeChangedRasterRDDFromTif(sc, "/D:/data/bug/svm/new/sagaSVMClassification_1718619217460_svm_result.tif")
    val coverage1= Coverage.addNum(coverage,1)
    makeTIFF(coverage1, "svm")
  }

  //  def ndviLandsat7(): Unit = {
  //
  //    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
  //    val sc = new SparkContext(conf)
  //
  //    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", "LE07_L1T_C01_T1")
  //    val coverageDouble: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.toDouble(coverage)
  //    val ndwi: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.normalizedDifference(coverageDouble, List("B4", "B3"))
  //
  //    makeTIFF(coverage, "ls")
  //    makeTIFF(coverageDouble, "lsD")
  //    makeTIFF(ndwi, "lsNDWI")
  //  }
  //
  //  def loadModis(): Unit = {
  //    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
  //    val sc = new SparkContext(conf)
  //    val coverageModis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "MOD13Q1.A2022241.mosaic.061.2022301091738.psmcrpgs_000501861676.250m_16_days_NDVI-250m_16_days", 7)
  //    makeTIFF(coverageModis, "modis")
  //    makeTMS(sc, coverageModis, "aah")
  //  }
  //
  //  def loadLandsat7(): Unit = {
  //
  //    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
  //    val sc = new SparkContext(conf)
  //
  //    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", "")
  //    val coverage1Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage1, List("B1", "B2", "B3"))
  //    makeTIFF(coverage1Select, "c1")
  //    makeTMS(sc, coverage1Select, "aah")
  //  }

  import sys.process._
  def test(): Unit = {
    "hostname".run
  }
  def test1(implicit sc: SparkContext):Unit={
    val c0 = Coverage.load(sc,"ASTGTM_N00E011","ASTER_GDEM_DEM30",10)
    println("Finish")
  }

  def loadLandsat8(): Unit = {
    val time1: Long = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    //    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "ASTGTM_N28E056",
    //      "ASTER_GDEM_DEM30",10)
    //    makeTIFF(coverage1,"dem")
    val filePath = "D:\\cog\\out\\dem.tiff"
    val coverage1 = RDDTransformerUtil.makeChangedRasterRDDFromTif(sc, filePath)
    //    var c = Coverage.polynomial(coverage1,List(1.0,2.0))
    val visParam: VisualizationParam = new VisualizationParam
    Trigger.level = 15
    Trigger.coverageReadFromUploadFile = true
    Coverage.visualizeOnTheFly(sc, coverage1, visParam)

    //    val coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = loadCoverage(sc, "LC08_L1TP_124039_20180109_20180119_01_T1", "LE07_L1T_C01_T1")
    //    var coverage2Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage2, List("B1", "B2", "B3"))
    ////    var coverage2Select2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage2, List("B4"))
    //
    ////    coverage2Select = Coverage.multiplyNum(coverage2Select,1000)
    ////    coverage2Select = Coverage.addBands(coverage2Select,coverage2Select2)
    //////    coverage2Select=Coverage.toInt32(coverage2Select)
    ////    coverage2Select = Coverage.removeZeroFromCoverage(coverage2Select)
    //    makeTIFF(coverage2Select, "c2")
    //
    //
    //    val coverage3: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.signum(coverage1Select)
    //    makeTIFF(coverage3, "c3")
    //
    ////    val coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map()
    ////    val a: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = coverageCollection + ("LE07_L1TP_125039_20130110_20161126_01_T1" -> coverage1Select)
    ////    val b: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = a + ("LC08_L1TP_124039_20180109_20180119_01_T1" -> coverage2Select)
    //    val coverageMosaic: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.cat(coverage1Select,coverage2Select)
    ////    coverageMosaic = Coverage.removeZeroFromCoverage(coverageMosaic)
    //    makeTIFF(coverageMosaic, "cMosaic")
  }

  def loadCoverage(implicit sc: SparkContext, coverageId: String, productKey: String, level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val time1 = System.currentTimeMillis()
    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId, productKey)
    val queryGeometry: Geometry = metaList.head.getGeom

    //    val queryGeometry: Geometry = geotrellis.vector.io.readWktOrWkb("POLYGON((110.45709 30.26141,110.59998 30.26678,110.58066 29.94492,110.4869 29.93994,110.45709 30.26141))")

    println("bandNum is " + metaList.length)

    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    val cogUtil: COGUtil = COGUtil.createCOGUtil(CLIENT_NAME)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .map(t => { // 合并所有的元数据（追加了范围）
        val time1: Long = System.currentTimeMillis()
        val rawTiles: mutable.ArrayBuffer[RawTile] = {
          val client = clientUtil.getClient
          val tiles: mutable.ArrayBuffer[RawTile] = cogUtil.tileQuery(client, level, t, queryGeometry.getEnvelopeInternal,queryGeometry)
          //          MinIOUtil.releaseMinioClient(client)
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
      val client = clientUtil.getClient
      val tile: RawTile = cogUtil.getTileBuf(client, t)
      //      MinIOUtil.releaseMinioClient(client)
      val time2: Long = System.currentTimeMillis()
      println("Get Tile Time2 is " + (time2 - time1))
      tile
    })
    println("Loading time: " + (System.currentTimeMillis() - time1))
    val time2 = System.currentTimeMillis()
    val coverage = makeCoverageRDD(rawTileRdd)
    println("Making RDD time: " + (System.currentTimeMillis() - time2))
    coverage
  }

  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), name: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    val writePath: String = "D:\\cog\\out\\" + name + ".tiff"
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

  def makePNG(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), name: String): Unit = {
    val tileLayerArray = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = coverage._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile.band(0), layout.extent)
    val writePath: String = "D:/cog/out/" + name + ".png"
    stitchedTile.tile.renderPng(ColorRamps.BlueToOrange).write(writePath)
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
