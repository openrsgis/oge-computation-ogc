package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, Point}
import whu.edu.cn.entity.{CoverageMetadata, RawTile, SpaceTimeBandKey}
import geotrellis.vector

import scala.math.{max, min}
import scala.collection.mutable

object interpolationUtils {

  def getExtent(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))])={
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    Extent(extents._1, extents._2, extents._3, extents._4)
  }

  def createPredictionPoints(extent: Extent, rows: Int, cols: Int)={
    //    //slow code
    //    val rasterExtent = RasterExtent(extent, cols, rows)
    //    val pointsRas = (for {
    //      row <- 0 until rows
    //      col <- 0 until cols
    //    } yield {
    //      val (x, y) = rasterExtent.gridToMap(col, row)
    //      vector.Point(x, y)
    //    }).toArray
    val cellWidth = (extent.xmax - extent.xmin) / cols
    val cellHeight = (extent.ymax - extent.ymin) / rows
    val pointsRas = (for {
      row <- 0 until rows
      col <- 0 until cols
    } yield {
      val x = extent.xmin + (col + 0.5) * cellWidth
      val y = extent.ymax - (row + 0.5) * cellHeight // y轴是从大到小的
      vector.Point(x, y)
    }).toArray
    pointsRas
  }

  def makeRasterOutput(predictPoint: Array[Point], predictValue: Array[Double])={
    val featureRaster = predictPoint.zipWithIndex.map(t => {
      val data = predictValue(t._2)
      val geom = t._1
      val feature = new vector.Feature[Geometry, Double](geom, data)
      //      println(geom,data)
      feature
    })
    featureRaster
  }

  def makeRasterVarOutput(predictPoint: Array[Point], predictValue: Array[(Double, Double)]) = {
    val featureRaster = predictPoint.zipWithIndex.map(t => {
      val data = predictValue(t._2)._1
      val geom = t._1
      val feature = new vector.Feature[Geometry, Double](geom, data)
      //      println(geom,data)
      feature
    })
    featureRaster
  }

  def makeTiff(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),path: String ,name: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    val writePath: String = path + name + ".tiff"
    GeoTiff(stitchedTile, coverage._2.crs).write(writePath)
  }
}
