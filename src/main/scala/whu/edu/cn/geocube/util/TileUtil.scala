package whu.edu.cn.geocube.util

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellType, DoubleArrayTile, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, FloatCellType, FloatCells, IntCells, MultibandTile, Raster, Tile}
import geotrellis.spark._
import geotrellis.spark.{ContextRDD, TileLayerRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.util.parsing.json.JSON
import whu.edu.cn.geocube.core.io.Input.getGeotiffTileRdd

/**
 * Provide some functions on raster tile.
 * */
object TileUtil{

  /**
   * Stitch an array of Tile into a tile.
   *
   * @param tileLayerArray A tile array with SpatialKey info
   * @param layout A tiled raster layout
   * @return
   */
  def stitch(tileLayerArray: Array[(SpatialKey, Tile)], layout: LayoutDefinition): Raster[Tile] = {
    val (tile, (kx, ky), (offsx, offsy)) = TileLayoutStitcher.stitch(tileLayerArray)
    val mapTransform = layout.mapTransform
    val nwTileEx = mapTransform(kx, ky)
    val base = nwTileEx.southEast
    val (ulx, uly) = (base.getX - offsx.toDouble * layout.cellwidth, base.getY + offsy * layout.cellheight)
    Raster(tile, geotrellis.vector.Extent(ulx, uly - tile.rows * layout.cellheight, ulx + tile.cols * layout.cellwidth, uly))
  }

  /**
   * Stitch an array of Tile into a tile.
   *
   * @param tileLayerArray A tile array with SpatialKey info
   * @param layout A tiled raster layout
   * @return
   */
  def stitchMultiband(tileLayerArray: Array[(SpatialKey, MultibandTile)], layout: LayoutDefinition): Raster[MultibandTile] = {
    val (tile, (kx, ky), (offsx, offsy)) = TileLayoutStitcher.stitch(tileLayerArray)
    val mapTransform = layout.mapTransform
    val nwTileEx = mapTransform(kx, ky)
    val base = nwTileEx.southEast
    val (ulx, uly) = (base.getX - offsx.toDouble * layout.cellwidth, base.getY + offsy * layout.cellheight)
    Raster(tile, geotrellis.vector.Extent(ulx, uly - tile.rows * layout.cellheight, ulx + tile.cols * layout.cellwidth, uly))
  }


  /**
   * Converts a JSON-formatted string into a Map format.
   *
   * @param vstr JSON-formatted string
   * @return Map.
   */
  def str2map(vstr:String): collection.immutable.Map[String, String] ={
    val vSome = JSON.parseFull(vstr)
    val vmap = vSome match {
      case Some(map:collection.immutable.Map[String, String]) => map
    }
    vmap
  }

  /**
   * Convert  generic T to a string.
   * @param v object
   * @tparam T generic T
   * @return string format.
   */
  def strConvert[T](v: Option[T]): String = {
    if (v.isDefined)
      v.get.toString
    else
      ""
  }

  /**
   * Convert two geotiffs containing (0.0, 1.0) value
   * to two new geotiffs containing (0.0, 255.0) value.
   *
   * @param sc a SparkContext
   * @param inputPath1
   * @param outputPath1
   * @param inputPath2
   * @param outputPath2
   * @param ld a layout definition
   */
  def rasterTransform(implicit sc: SparkContext,
                      inputPath1: String, outputPath1: String,
                      inputPath2: String, outputPath2: String,
                      ld: LayoutDefinition): Unit = {

    val input1Rdd: TileLayerRDD[SpatialKey] = getGeotiffTileRdd(sc, inputPath1, ld)

    val input1DoubleRdd: RDD[(SpatialKey,Tile)] = input1Rdd.map(x => {
      val srcTile: Array[Double] = x._2.toArrayDouble()
      val dstTile: Array[Double] = Array.fill(x._2.cols * x._2.rows)(0.0)
      for(i <- 0 until x._2.cols * x._2.rows){
        if(srcTile(i) == 1.0)
          dstTile(i) = 255.0
      }
      val doubleArrayTile = DoubleArrayTile(dstTile, x._2.cols, x._2.rows, DoubleConstantNoDataCellType)
      (x._1, doubleArrayTile)
    })

    val input1DoubleContextRdd: TileLayerRDD[SpatialKey] = ContextRDD(input1DoubleRdd, input1Rdd.metadata.copy(cellType = DoubleCellType))
    val stitched1: Raster[Tile] = input1DoubleContextRdd.stitch()
    GeoTiff(stitched1, input1DoubleContextRdd.metadata.crs).write(outputPath1)

    val input2Rdd: TileLayerRDD[SpatialKey] = getGeotiffTileRdd(sc, inputPath2, ld)
    val input2DoubleRdd: RDD[(SpatialKey,Tile)] = input2Rdd.map(x => {
      val srcTile: Array[Double] = x._2.toArrayDouble()
      val dstTile: Array[Double] = Array.fill(x._2.cols * x._2.rows)(0.0)
      for(i <- 0 until x._2.cols * x._2.rows){
        if(srcTile(i) == 1.0)
          dstTile(i) = 255.0
      }
      val doubleArrayTile = DoubleArrayTile(dstTile, x._2.cols, x._2.rows, DoubleConstantNoDataCellType)
      (x._1, doubleArrayTile)
    })
    val input2DoubleContextRdd: TileLayerRDD[SpatialKey] = ContextRDD(input2DoubleRdd, input2Rdd.metadata.copy(cellType = DoubleCellType))
    val stitched2: Raster[Tile] = input2DoubleContextRdd.stitch()
    GeoTiff(stitched2, input2DoubleContextRdd.metadata.crs).write(outputPath2)
  }

  /**
   * because the double or float type data maybe wrong, therefore need to correct the value of Nodata
   * @param tile Tile the input tile
   * @return Tile corrected tile
   */
  def convertNodataValue(tile: Tile):Tile = {
    var correctedTile = tile
    if(tile.cellType.name.contains("float")){
      correctedTile = tile.mapDouble(value => {
        if(value < -3.40E37){
          Float.NaN
        }else{
          value
        }
      })
    }else if(tile.cellType.name.contains("double")){
      correctedTile = tile.mapDouble(value => {
        if(value < -1.79E307){
          Double.NaN
        }else{
          value
        }
      })
    }
    correctedTile
  }
}
