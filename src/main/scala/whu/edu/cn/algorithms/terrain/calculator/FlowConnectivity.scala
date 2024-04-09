package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage

object FlowConnectivity {

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
        FlowConnectivityCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class FlowConnectivityCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Dir: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)
    private val Connection: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)

    def apply(): Tile = {
      Dir.noDataValue = -1
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          val i: Int = DEM.getGradientNeighborDir(x, y)
          if (i >= 0)
            Dir.setDouble(x, y, i)
          else
            Dir.setDouble(x, y, -1)
        }
      }
      Connection.noDataValue = 0.0
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          var n: Int = 0
          var i: Int = 0
          while (i < 8) {
            val ix: Int = DEM.calXFrom(i, x)
            val iy: Int = DEM.calYFrom(i, y)
            if (DEM.isInGrid(ix, iy) && i == Dir.get(ix, iy))
              n += 1
            i += 1
          }
          Connection.setDouble(x, y, n)
        }
      }
      Connection.refine()
    }

  }
}
