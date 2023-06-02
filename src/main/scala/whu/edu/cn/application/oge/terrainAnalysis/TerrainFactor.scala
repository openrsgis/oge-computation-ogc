package whu.edu.cn.application.oge.terrainAnalysis

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal.{Aspect, Neighborhood, Slope, Square}
import geotrellis.raster.{CellSize, GridBounds, Tile}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

object TerrainFactor {
  var inputPath : String = _
  private var rddImage : (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = _

  def main(args: Array[String]): Unit = {
    inputPath = "/home/cgd/Documents/DEM/SAGA/rc/data/dem64.tif"
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TerrainFactor")
    val sc = new SparkContext(conf)
    rddImage = makeChangedRasterRDDFromTif(sc, inputPath)
    val slope = calSlope(rddImage)
    val aspect = calAspect(rddImage)
    saveRasterRDDToTif(slope, "/home/cgd/Documents/DEM/Output/slope.tif")
    saveRasterRDDToTif(aspect, "/home/cgd/Documents/DEM/Output/aspect.tif")
  }

  private def calSlope(input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),zFactor: Double = 1.0): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val neighborhood: Neighborhood = Square(3)
    val cellSize = CellSize(input._2.layout.cellwidth, input._2.layout.cellheight)
    val slopeRDD = input._1.map(t => (t._1, Slope(t._2, neighborhood, Some(GridBounds(t._2.cols, t._2.rows)), cellSize, zFactor)))
    (slopeRDD, input._2)
  }

  private def calAspect(input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val neighborhood: Neighborhood = Square(3)
    val cellSize = CellSize(input._2.layout.cellwidth, input._2.layout.cellheight)
    val aspectRDD = input._1.map(t => (t._1, Aspect(t._2, neighborhood, Some(GridBounds(t._2.cols, t._2.rows)), cellSize)))
    (aspectRDD, input._2)
  }

}
