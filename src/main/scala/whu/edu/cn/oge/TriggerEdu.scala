package whu.edu.cn.oge

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.Coverage.reproject
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import java.nio.file.Paths

object TriggerEdu {
  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), outputPath: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    GeoTiff(stitchedTile, coverage._2.crs).write(outputPath)

  }
  def reprojectEdu(implicit sc: SparkContext, inputPath: String, outputPath: String, Crs: String, scale: Double) = {
    val epsg: Int = Crs.split(":")(1).toInt
    val crs: CRS = CRS.fromEpsgCode(epsg)
    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val coverage2 = reproject(coverage1, crs, scale)
    makeTIFF(coverage2, outputPath)
    println("SUCCESS")
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("New Coverage").setMaster("local[*]")
    val sc = new SparkContext(conf)
    reprojectEdu(sc, "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1.tif", "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1_reprojected.tif", "EPSG:3857", 100)
  }

}
