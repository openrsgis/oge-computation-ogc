package whu.edu.cn.algorithms.terrain.core

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile

class TileToolGrid (tile: Tile,
                    metaData: TileLayerMetadata[SpaceTimeKey],
                    paddingSize: Int){
  var pLock: OgeTerrainTile = _

  def lockCreate(): Unit = {
    if (pLock == null) {
      pLock = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
    } else {
      val pn = pLock.array.length
      for (i <- 0 until pn) {
        pLock.array(i) = 0
      }
    }
  }

  def isLocked(x: Int, y: Int): Boolean = lockGet(x, y) != 0
  def lockGet(x: Int, y: Int): Int = if(this != null && x >= 0 && x < pLock.nX && y >= 0 && y < pLock.nY) pLock.get(x, y) else 0

  def lockSet(x: Int, y: Int, value: Double = 1): Unit =
    if (this != null && x >= 0 && x < pLock.nX && y >= 0 && y < pLock.nY) pLock.setDouble(x, y, value)


}
