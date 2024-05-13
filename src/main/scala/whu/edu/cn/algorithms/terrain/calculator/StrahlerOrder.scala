package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage

object StrahlerOrder {

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
      radius: Int = 16,
      zFactor: Double = 1.0
  ): RDDImage = {
    val (rddPadding, metaData) = paddingRDDImage(rddImage, radius)
    val result = rddPadding.map(t => {
      val calculator =
        StrahlerOrderCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class StrahlerOrderCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Order: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
    private val Count: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, paddingSize, 0)

    def apply(): Tile = {
      Order.noDataValue = 0
      DEM.setIndex()
      var i, ix, iy: Int = 0
      for (n <- 0 until DEM.nCells) {
        val (x, y, bool) = DEM.getSorted(n)
        if (bool) {
          if (Count.getDouble(x, y) > 1) {
            Order.setDouble(x, y, Order.getDouble(x, y) + 1)
          }
          i = DEM.getGradientNeighborDir(x, y)
          if (i >= 0) {
            ix = DEM.calXTo(i, x)
            iy = DEM.calYTo(i, y)
            if (Order.getDouble(x, y) > Order.getDouble(ix, iy)) {
              Order.setDouble(ix, iy, Order.getDouble(x, y))
              Count.setDouble(ix, iy, 1)
            } else if (Order.getDouble(x, y) == Order.getDouble(ix, iy)) {
              Count.setDouble(ix, iy, Count.getDouble(x, y) + 1)
            }
          }

        }
      }
      Order.refine()
    }
  }
}
