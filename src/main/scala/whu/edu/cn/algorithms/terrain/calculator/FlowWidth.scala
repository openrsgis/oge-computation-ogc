package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

object FlowWidth {

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
        FlowWidthCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class FlowWidthCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ){
    private val DEM: OgeTerrainTile = OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Width: OgeTerrainTile = OgeTerrainTile.like(tile, metaData, paddingSize)

    //parameters
    var TCA: OgeTerrainTile = _
    var SCA: OgeTerrainTile = _
    val method: Int = 2

    def apply(): Tile = {
      if(TCA == null)
        SCA = null
      for(y <- 0 until DEM.nY){
        for(x <- 0 until DEM.nX){
          var width: Double = 0
          method match {
            case 0 => width = getD8(x, y)
            case 1 => width = getMFD(x, y)
            case 2 | _ => width = getAspect(x, y)
          }
          if(width > 0) {
            Width.setDouble(x, y, width)
            if(SCA != null){
              if(!TCA.isNoData(x, y))
                SCA.setDouble(x, y, TCA.getDouble(x, y) / width)
              else
                SCA.setNoData(x, y)
            }
          } else{
            Width.setNoData(x, y)
            if(SCA != null){
              SCA.setNoData(x, y)
            }
          }
        }
      }

      Width.refine()
    }

    def getD8(x: Int, y: Int): Double = {
      val direction: Int = DEM.getGradientNeighborDir(x, y)
      if(direction >= 0)
        return DEM.getLength(direction)
      -1
    }

    def getMFD(x: Int, y: Int): Double = {
      if(DEM.isInGrid(x, y)){
        var width: Double = 0
        val z: Double = DEM.getDouble(x, y)

        for(i <- 0 until 8){
          val ix: Int = DEM.calXTo(i, x)
          val iy: Int = DEM.calYTo(i, y)
          if(DEM.isInGrid(ix, iy) && z > DEM.getDouble(ix, iy))
            width += 0.5 * DEM.cellSize / DEM.getUnitLength(i)
        }
        return width
      }
      -1
    }

    def getAspect(x: Int, y: Int): Double = {
      val (_, aspect, bool) = DEM.getGradient(x, y)
      if(bool)
        return (math.abs(math.sin(aspect)) + math.abs(math.cos(aspect))) * DEM.cellSize
      -1
    }

  }
}
