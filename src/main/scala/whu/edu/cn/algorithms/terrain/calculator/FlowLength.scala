package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage

object FlowLength {

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
        FlowLengthCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class FlowLengthCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Distance: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, paddingSize, -1)

    //parameters: weights and direction of measurement
    //direction: 0 for downstream , 1 for upstream
    val Weights: OgeTerrainTile = null
    val direction: Int = 0
    def apply(): Tile = {
//      Distance.noDataValue = -1.0
      DEM.setIndex()
      if (direction == 0) {
        for (iCell <- 0 until DEM.nCells) {
          val (x, y, bool) = DEM.getSorted(iCell, bDown = false)
          if (bool) {
            var distance: Double = 0
            val i: Int = DEM.getGradientNeighborDir(x, y, bNoEdges = false)
            if (i >= 0) {
              val ix: Int = DEM.calXTo(i, x)
              val iy: Int = DEM.calYTo(i, y)
              if (Distance.isInGrid(ix, iy)) {
                var weight: Double = 0
                if (Weights != null)
                  weight = Weights.getDouble(ix, iy)
                else
                  weight = 1.0
                distance =
                  Distance.getDouble(ix, iy) + DEM.getLength(i) * weight
              }
            }
            Distance.setDouble(x, y, distance)
          }
        }
      } else {
        for (iCell <- 0 until DEM.nCells) {
          val (x, y, bool) = DEM.getSorted(iCell)
          if (bool) {
            if (Distance.isNoData(x, y))
              Distance.setDouble(x, y, 0)
            val i: Int = DEM.getGradientNeighborDir(x, y, bNoEdges = false)
            if (i >= 0) {
              val ix: Int = DEM.calXTo(i, x)
              val iy: Int = DEM.calYTo(i, y)
              if (DEM.isInGrid(ix, iy)) {
                var weight: Double = 0
                if (Weights != null)
                  weight = Weights.getDouble(x, y)
                else
                  weight = 1.0
                val distance: Double =
                  Distance.getDouble(x, y) + DEM.getLength(i) * weight
                if (
                  Distance
                    .isNoData(ix, iy) || Distance.getDouble(ix, iy) < distance
                )
                  Distance.setDouble(ix, iy, distance)

              }
            }
          }
        }

      }

      Distance.noDataValue = -1.0
      Distance.refine()
    }

  }

}
