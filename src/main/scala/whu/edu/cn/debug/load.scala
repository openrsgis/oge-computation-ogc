package whu.edu.cn.debug

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark.{TileLayerRDD, _}
import geotrellis.vector.{Extent, Geometry}
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.entity.{CoverageMetadata, RawTile, SpaceTimeBandKey}
import whu.edu.cn.oge.Coverage
import whu.edu.cn.oge.CoverageCollection.mosaic
import whu.edu.cn.util.COGUtil.{getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageUtil.makeCoverageRDD
import whu.edu.cn.util.MinIOUtil
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage

import scala.collection.mutable

object load {
  def main(args: Array[String]): Unit = {
    val time1:Long = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = load(sc, "LE07_L1TP_125039_20130110_20161126_01_T1", 5)
    val coverage1Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage1, List("B1", "B2"))
    makeTIFF(coverage1Select, "c1")

    //    val a: (RDD[(SpatialKey, Tile)], TileLayerMetadata[SpatialKey]) = (coverage1Select._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2.bands(0))), TileLayerMetadata(coverage1Select._2.cellType, coverage1Select._2.layout, coverage1Select._2.extent, coverage1Select._2.crs, Bounds(coverage1Select._2.bounds.get.minKey.spatialKey, coverage1Select._2.bounds.get.maxKey.spatialKey)))
    //
    //    val tiledRdd1: TileLayerRDD[SpatialKey] = TileLayerRDD(a._1, a._2)
    //    val extent = Extent(-100.0, -9.0, 100.0, 80.0)
    //    val tl = TileLayout(100, 50, 256, 256) //tile: 0.1°×0.1°
    //    val ld = LayoutDefinition(extent, tl)
    //    val (_, b) = tiledRdd1.reproject(LatLng, ld)
    //    makeTIFF(b, "cc")


    // MOD13Q1.A2022241.mosaic.061.2022301091738.psmcrpgs_000501861676.250m_16_days_NDVI-250m_16_days
    // LC08_L1TP_124038_20181211_20181226_01_T1
    val coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = load(sc, "LC08_L1TP_124039_20180109_20180119_01_T1", 5)
    val coverage2Select: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.selectBands(coverage2, List("B1", "B2"))
    makeTIFF(coverage2Select, "c2")



    val coverage3: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.add(coverage1Select, coverage2Select)
    makeTIFF(coverage3, "c3")

    val coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map()
    val a: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = coverageCollection + ("LE07_L1TP_125039_20130110_20161126_01_T1" -> coverage1Select)
    val b: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = a + ("LC08_L1TP_124039_20180109_20180119_01_T1" -> coverage2Select)
    val coverageMosaic: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(b)
    makeTIFF(coverageMosaic, "cMosaic")


    val time2:Long = System.currentTimeMillis()
    println("Total Time is " + (time2 - time1))



    println("_")

  }

  def load(implicit sc: SparkContext, coverageId: String, level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId)
    val queryGeometry: Geometry = metaList.head.getGeom

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
    }).cache()
    makeCoverageRDD(rawTileRdd)
  }

  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), name: String): Unit = {
    val coverageSingleBand: RDD[(String, (SpatialKey, Tile))] = coverage._1.map(t => {
      val tupleArray: mutable.ListBuffer[(String, Tile)] = t._1.measurementName.zip(t._2.bands)
      tupleArray.map(x => {
        (x._1, (t._1.spaceTimeKey.spatialKey, x._2))
      })
    }).flatMap(t => t)

    val tileArrayBands: Array[(String, Iterable[(SpatialKey, Tile)])] = coverageSingleBand.groupByKey().collect()
    tileArrayBands.foreach(t => {
      val tileArray: Array[(SpatialKey, Tile)] = t._2.toArray
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
      val stitchedTile: Raster[Tile] = Raster(tile, coverage._2.extent)
      val writePath: String = "D:/cog/out/" + name + "_" + t._1 + ".tiff"
      GeoTiff(stitchedTile, coverage._2.crs).write(writePath)
    })
  }

  def makeTIFF(coverage: TileLayerRDD[SpatialKey], name: String): Unit = {

    val tileArray: Array[(SpatialKey, Tile)] = coverage.collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileArray)
    val stitchedTile: Raster[Tile] = Raster(tile, coverage.metadata.extent)
    val writePath: String = "D:/cog/out/" + name + "_" + "" + ".tiff"
    GeoTiff(stitchedTile, coverage.metadata.crs).write(writePath)
  }

}
