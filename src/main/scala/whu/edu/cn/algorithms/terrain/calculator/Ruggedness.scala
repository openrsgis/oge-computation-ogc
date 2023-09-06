package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.{OgeTerrainTile, TileCellAddressor}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

object Ruggedness {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage raster data in RDD format with metadata
    * @param radius   radius of padding, default is 1
    * @param zFactor  z position factor for calculation
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      radius: Int = 1,
      zFactor: Double = 1.0
  ): RDDImage = {
    val (rddPadding, metaData) = paddingRDDImage(rddImage, radius)
    val result = rddPadding.map(t => {
      val calculator =
        RuggednessTRICalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class RuggednessTRICalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {

    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val TRI: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)
    private val Cells: TileCellAddressor = TileCellAddressor.create()

    def apply(): Tile = {

      //parameters
      val radius: Int = 1
      val power: Double = 2
      val bandwidth: Double = 75

      Cells.weighting.setBandWidth(radius * bandwidth / 100.0)
      val bSquare: Boolean = false
      if (!Cells.setRadius(radius, bSquare)) {}

      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          setIndex(x, y)
        }
      }

      TRI.refine()
    }

    private def setIndex(x: Int, y: Int): Boolean = {
      if (DEM.isInGrid(x, y)) {
        var i, ix, iy: Int = 0
        var z, iz, Distance, Weight, n, s: Double = 0
        val bOffset: Boolean = true

        z = DEM.getDouble(x, y)
        for (i <- 0 until Cells.kernel.nRecords) {
          ix = x
          iy = y
          val (x1, y1, distance, weight, bool) =
            Cells.getValues(i, ix, iy, Distance, Weight, bOffset)
          ix = x1
          iy = y1
          Distance = distance
          Weight = weight
          if (bool && Weight > 0 && DEM.isInGrid(ix, iy)) {
            iz = DEM.getDouble(ix, iy)
            val m: Double = (z - iz) * Weight * (z - iz) * Weight
            s += m
            n += Weight
          }

        }

        if (n > 0) {
          TRI.setDouble(x, y, math.sqrt(s / n))
          return true
        }
      }
      TRI.setNoData(x, y)
      false
    }

  }

}
