package whu.edu.cn.geocube.util

import com.alibaba.fastjson.JSON
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{DoubleArrayTile, DoubleCellType, DoubleConstantNoDataCellType, MultibandTile, Raster, Tile}

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
}
