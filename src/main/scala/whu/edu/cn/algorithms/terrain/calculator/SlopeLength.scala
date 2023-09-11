package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

object SlopeLength {

  /**
   * call the Calculator
   *
   * @note the intput rddImage doesn't require padding
   * @param rddImage raster data in RDD format with metadata
   * @param radius   radius of padding, default is 16
   * @param zFactor  z position factor for calculation
   * @return result
   */
  def apply(
      rddImage: RDDImage,
      radius: Int = 16,
      zFactor: Double = 1.0
  ): RDDImage = {
    val (rddPadding, metaData) = paddingRDDImage(rddImage, radius)
    val result = rddPadding.map(t => {
      val calculator = SlopeLengthCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class SlopeLengthCalculator(tile: Tile, metaData: TileLayerMetadata[SpaceTimeKey],
                                           paddingSize: Int, zFactor: Double) {

    private val DEM: OgeTerrainTile = OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Slope: OgeTerrainTile = OgeTerrainTile.like(tile, metaData, paddingSize)
    private val Length: OgeTerrainTile = OgeTerrainTile.like(tile, metaData, paddingSize)

    def apply(): Tile = {
      val cols = DEM.nX
      val rows = DEM.nY
      val nCells = DEM.nCells

      if (DEM.setIndex()) {
        for (y <- 0 until rows) {
          for (x <- 0 until cols) {
            val (slope, _, bool) = DEM.getGradient(x, y)
            if (bool) {
              Slope.setDouble(x, y, slope)
              Length.setDouble(x, y, 0)
            } else {
              Slope.setNoData(x, y)
              Length.setNoData(x, y)
            }
          }
        }

        for (n <- 0 until nCells) {
          val (x, y, bool) = DEM.getSorted(n)
          if (bool) {
            calLength(x, y)
          }
        }
      }

      Length.refine()
    }

    private def calLength(x: Int, y: Int): Unit = {
      var i, ix, iy: Int = 0

      if (!Slope.isInGrid(x, y)) return
      i = DEM.getGradientNeighborDir(x, y)
      if (i < 0) return

      ix = DEM.calXTo(i, x)
      iy = DEM.calYTo(i, y)

      if (Slope.isInGrid(ix, iy)) {
        if (Slope.getDouble(ix, iy) > 0.5 * Slope.getDouble(x, y)) {
          val len: Double = Length.getDouble(x, y) + DEM.getLength(i)

          if (len > Length.getDouble(ix, iy)) {
            Length.setDouble(ix, iy, len)
          }
        }
      }
    }

  }

}
