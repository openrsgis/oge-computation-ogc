package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

import scala.math.{Pi, acos, atan, cos, sin, tan}

object HillShade {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage raster data in RDD format with metadata
    * @param radius   radius of convolution kernel
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
      val calculator = HillShadeWrapper(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class HillShadeWrapper(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {

    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Shade: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)

    def apply(): Tile = {
      val cols = DEM.nX
      val rows = DEM.nY

      calShading(bDelimit = false, bCombine = false)

      Shade.refine()
    }

    private def calShading(bDelimit: Boolean, bCombine: Boolean): Unit = {
      var Azimuth, Decline: Double = 0.0
      Azimuth = 315.0.toRadians
      Decline = 45.0.toRadians
      val sinDecline: Double = sin(Decline)
      val cosDecline: Double = cos(Decline)
      val scale: Double = 1.0

      for (y <- 0 until tile.rows) {
        for (x <- 0 until tile.cols) {
          var (slope, aspect, bool) = DEM.getGradient(x, y)
          if (!bool) {
            Shade.setNoData(x, y)
          } else {
            if (scale != 1.0) {
              slope = atan(scale * tan(slope))
            }

            var d: Double = Pi / 2.0 - slope
            d = acos(
              sin(d) * sinDecline + cos(d) * cosDecline * cos(aspect - Azimuth)
            )

            if (bDelimit && d > Pi / 2.0) {
              d = Pi / 2.0
            }

            if (bCombine) {
              d *= slope / (Pi / 2.0)
            }

            Shade.setDouble(x, y, d)

          }
        }
      }
    }

  }

}