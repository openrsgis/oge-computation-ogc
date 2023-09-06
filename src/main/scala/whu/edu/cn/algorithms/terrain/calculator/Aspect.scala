package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

import scala.math.{Pi, atan2}

object Aspect {

  /**
   * call the Calculator
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
      val calculator = AspectCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class AspectCalculator(tile: Tile, metaData: TileLayerMetadata[SpaceTimeKey],
                                      paddingSize: Int, zFactor: Double) {

    private val DEM   : OgeTerrainTile = OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Aspect: OgeTerrainTile = OgeTerrainTile.like(tile, metaData, paddingSize)

    def apply(): Tile = {
      val cols = DEM.nX
      val rows = DEM.nY

      for (y <- 0 until rows) {
        for (x <- 0 until cols) {
          doFdZevenbergen(x, y)
        }
      }

      Aspect.refine()
    }

    private def doFdZevenbergen(x: Int, y: Int): Unit = {
      val zm: Array[Double] = new Array[Double](9)
      var D, E, F, G, H: Double = 0

      val cellSize = DEM.cellSize
      val _1dx2 = cellSize * cellSize
      val _4dx2 = cellSize * cellSize * 4
      val _2dx1 = cellSize * 2

      if (calSubMatrix(x, y, zm)) {
        D = ((zm(3) + zm(5)) / 2.0 - zm(4)) / _1dx2
        E = ((zm(1) + zm(7)) / 2.0 - zm(4)) / _1dx2
        F = (zm(0) - zm(2) - zm(6) + zm(8)) / _4dx2
        G = (zm(5) - zm(3)) / _2dx1
        H = (zm(7) - zm(1)) / _2dx1

        if (G != 0.0) {
          Aspect.setDouble(x, y, (Pi + atan2(H, G)).toDegrees)
        } else if (H > 0) {
          Aspect.setDouble(x, y, (Pi * 3.0 / 2.0).toDegrees)
        } else if (H < 0) {
          Aspect.setDouble(x, y, (Pi / 2.0).toDegrees)
        } else {
          Aspect.setDouble(x, y, 0)
        }
      }
    }

    private def calSubMatrix(x: Int, y: Int, submatrix: Array[Double]): Boolean = {
      val isub: Array[Int] = Array(5, 8, 7, 6, 3, 0, 1, 2)
      var ix, iy: Int = 0
      var z: Double = 0.0

      if (DEM.isNoData(x, y)) {
        Aspect.setNoData(x, y)
      } else {
        z = DEM.getDouble(x, y)
        submatrix(4) = 0.0

        for (i <- 0 until 8) {
          ix = DEM.calXTo(i, x)
          iy = DEM.calYTo(i, y)
          if (DEM.isInGrid(ix, iy)) {
            submatrix(isub(i)) = DEM.getDouble(ix, iy) - z
          } else {
            ix = DEM.calXTo(i + 4, x)
            iy = DEM.calYTo(i + 4, y)
            if (DEM.isInGrid(ix, iy)) {
              submatrix(isub(i)) = z - DEM.getDouble(ix, iy)
            } else {
              submatrix(isub(i)) = 0.0
            }
          }
        }
        return true
      }
      false
    }
  }

}
