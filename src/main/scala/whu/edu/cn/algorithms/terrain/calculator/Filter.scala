package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

object Filter {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage raster data in RDD format with metadata
    * @param min condition filter list min(include min)
    * @param max condition filter list max(include max)
    * @param zFactor  z position factor for calculation
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      min: List[Double],
      max: List[Double],
      zFactor: Double = 1.0
  ): RDDImage = {
    val metaData = rddImage._2
    val result = rddImage._1.map(t => {
      val calculator =
        FilterCalculator(t._2.band(0), metaData, min, max, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class FilterCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      min: List[Double],
      max: List[Double],
      zFactor: Double
  ) {
    private val TIF: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, 0)
    private val Filter: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, 0)

    def apply(): Tile = {
      assert(min.length == max.length, "false")
      for(y <- 0 until TIF.nY){
        for(x <- 0 until TIF.nX){
          for(i <- min.indices){
            if(TIF.getDouble(x, y) >= min(i) && TIF.getDouble(x, y) <= max(i))
              Filter.setDouble(x, y, TIF.getDouble(x, y))
          }
        }
      }
      Filter.refine()
    }
  }
}
