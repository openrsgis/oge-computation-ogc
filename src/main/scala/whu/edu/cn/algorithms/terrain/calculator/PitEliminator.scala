package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.{OgeTerrainTile, TileToolGrid}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.calculator.PitRouter.PitRouterCalculator
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

object PitEliminator {

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
        PitEliminatorCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  private case class PitEliminatorCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) extends TileToolGrid(tile, metaData, paddingSize) {
    private val DTM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private var Route: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)
    var goRoute: OgeTerrainTile = _

    //parameter
    val method: Int = 1
    val threshold: Double = -1

    val almostZero: Double = 0.001

    def apply(): Tile = {
      var bResult: Boolean = true
      var nPits: Int = 0
      val route: PitRouterCalculator =
        PitRouterCalculator(tile, metaData, paddingSize, zFactor)

      val (routeResult, n) = route.getRoutes(DTM, Route, threshold)
      Route = routeResult
      nPits = n

      if (nPits > 0) {
        createGoRoute()
        method match {
          case 0 => bResult = digChannels()
          case 1 => bResult = fillSinks()
          case _ => bResult = false
        }

      }

      DTM.refine()
    }

    def createGoRoute(): Unit = {
      goRoute = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
      for (y <- 0 until DTM.nY) {
        for (x <- 0 until DTM.nX) {
          if (!DTM.isInGrid(x, y))
            goRoute.setNoData(x, y)
          else if (Route.get(x, y) > 0)
            goRoute.setDouble(x, y, Route.get(x, y) % 8)
          else
            goRoute.setDouble(x, y, DTM.getGradientNeighborDir(x, y))
        }
      }
    }

    def digChannels(): Boolean = {
      var bPit: Boolean = true
      var i, ix, iy: Int = 0
      var z: Double = 0
      for (y <- 0 until DTM.nY) {
        for (x <- 0 until DTM.nX) {
          z = DTM.getDouble(x, y)
          while (i < 8 && bPit) {
            ix = DTM.calXTo(i, x)
            iy = DTM.calYTo(i, y)
            if (!DTM.isInGrid(ix, iy) || z > DTM.getDouble(ix, iy))
              bPit = false
            i += 1
          }

          if (bPit)
            digChannel(x, y)
        }
      }
      true
    }

    def digChannel(x: Int, y: Int): Unit = {
      var bContinue: Boolean = true
      var goDir: Int = 0
      var z: Double = DTM.getDouble(x, y)
      var ix: Int = x
      var iy: Int = y

      do {
        z -= almostZero
        goDir = goRoute.get(ix, iy)
        if (goDir < 0)
          bContinue = false
        else {
          ix = DTM.calXTo(goDir, ix)
          iy = DTM.calYTo(goDir, iy)
          if (!DTM.isInGrid(ix, iy) || z > DTM.getDouble(ix, iy))
            bContinue = false
          else
            DTM.setDouble(ix, iy, z)
        }

      } while (bContinue)
    }

    def fillSinks(): Boolean = {
      for (y <- 0 until DTM.nY) {
        for (x <- 0 until DTM.nX) {
          fillCheck(x, y)
        }
      }
      true
    }

    def fillCheck(x: Int, y: Int): Unit = {
      var bOutlet: Boolean = false
      var i, ix, iy, j: Int = 0
      var z: Double = 0

      z = DTM.getDouble(x, y)
      i = goRoute.get(x, y)
      ix = DTM.calXTo(i, x)
      iy = DTM.calYTo(i, y)

      if (!DTM.isInGrid(ix, iy) || z > DTM.getDouble(ix, iy)) {
        i = 0
        j = 4
        while (i < 8 && !bOutlet) {
          ix = DTM.calXTo(i, x)
          iy = DTM.calYTo(i, y)
          if (
            DTM.isInGrid(ix, iy) && goRoute.get(ix, iy) == j && z > DTM
              .getDouble(ix, iy)
          )
            bOutlet = true

          i += 1
          j = (j + 1) % 8
        }
        if (bOutlet) {
          lockCreate()
          lockSet(x, y)

          i = 0
          j = 4
          while (i < 8) {
            ix = DTM.calXTo(i, x)
            iy = DTM.calYTo(i, y)
            fillSink(ix, iy, j, z)
            i += 1
            j = (j + 1) % 8
          }
        }
      }
    }

    def fillSink(x: Int, y: Int, J: Int, Z: Double): Unit = {
      var j: Int = J
      var i, ix, iy: Int = 0
      var z: Double = Z
      if (DTM.isInGrid(x, y) && !isLocked(x, y) && goRoute.get(x, y) == j) {
        lockSet(x, y)
        z += almostZero * DTM.getUnitLength(j)
        if (DTM.getDouble(x, y) < z) {
          DTM.setDouble(x, y, z)
          j = 4
          while (i < 8) {
            ix = DTM.calXTo(i, x)
            iy = DTM.calYTo(i, y)
            fillSink(ix, iy, j, z)

            i += 1
            j = (j + 1) % 8
          }

        }
      }
    }

  }

}
