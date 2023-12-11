package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.{OgeTerrainTile, TileToolGrid}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

import scala.util.control.Breaks.{break, breakable}


object PitRouter {

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
        PitRouterCalculator(t._2.band(0), metaData, radius, zFactor)
      (t._1, MultibandTile(calculator()))
    })
    (result, metaData)
  }

  case class PitRouterCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) extends TileToolGrid(tile, metaData, paddingSize){
    var junction: Array[Array[Int]] = _
    var nJunctions: Array[Int] = _
    var threshold, zThr, zMax: Double = _

    private var DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private var Route: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)

    var flats: OgeTerrainTile = _
    var pits: OgeTerrainTile = _
    var route: OgeTerrainTile = _

    var flat: Array[GeoIRect] = new Array[GeoIRect](0)
    var pit: Array[Pit] = new Array[Pit](0)
    var outlets: PitOutlet = new PitOutlet

    def apply(): Tile = {
      threshold = -1
      Route = getRoutes(DEM, Route, threshold)._1

      for(y <- 0 until DEM.nY){
        for(x <- 0 until DEM.nX){
          if (DEM.isNoData(x, y))
            Route.setNoData(x, y)
        }
      }

      Route.refine()
    }

    def getRoutes(
        pDem: OgeTerrainTile,
        pRoute: OgeTerrainTile,
        Threshold: Double = -1
    ): (OgeTerrainTile, Int) = {
      DEM = pDem
      Route = pRoute
      threshold = Threshold

      var iPit, nPits, n: Int = 0
      var outlet, next: PitOutlet = null
      if (initialize()) {
        nPits = findPits()
        if (nPits > 0) {
          findOutlets(nPits)
          iPit = 0
          breakable {
            do {
              outlet = outlets
              while (outlet != null) {
                next = outlet.next
                n = findRoute(outlet)
                if (n > 0) {
                  outlet = outlets
                  iPit += n
                } else {
                  outlet = next
                }
              }

              if (iPit < nPits) {
                for (n <- 0 until nPits) {
                  if (!pit(n).bDrained) {
                    pit(n).bDrained = true
                    iPit += 1
                    break
                  }
                }
              }

            } while (iPit < nPits)
          }
        }
        if (threshold > 0) {
          nPits -= processThreshold()
        }
      }

      if (nPits > 0)
        (Route, nPits)
      else
        (Route, 0)
    }

    def initialize(): Boolean = {
      if (DEM != null && DEM.setIndex() && Route != null) {
        Route = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
        pits = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
        return true
      }
      false
    }

    def findPits(): Int = {
      var bLower, bFlat: Boolean = true
      var i, ix, iy, nFlats, nPits: Int = 0
      var z: Double = 0.0
      var pPit: Pit = null

      nFlats = 0
      nPits = 0

      for (n <- 0 until DEM.nCells) {
        val (x, y, _) = DEM.getSorted(n, bDown = false)

        if (
          x > 0 && x < DEM.nX - 1 && y > 0 && y < DEM.nY - 1
          && !DEM.isNoData(x, y) && pits.get(x, y) == 0
        ) {
          z = DEM.getDouble(x, y)
          bLower = false
          bFlat = false
          i = 0
          while (i < 8 && !bLower) {
            ix = DEM.calXTo(i, x)
            iy = DEM.calYTo(i, y)
            if (!DEM.isInGrid(ix, iy) || z > DEM.getDouble(ix, iy)) {
              bLower = true
            } else if (z == DEM.getDouble(ix, iy)) {
              bFlat = true
            }
            i += 1
          }
          if (!bLower) {
            nPits += 1
            pits.setDouble(x, y, nPits)
            pit ++= Array.tabulate[Pit](nPits - pit.length)(_ => new Pit)
            pit(nPits - 1).bDrained = false
            pit(nPits - 1).z = z
            if (bFlat) {
              nFlats += 1
              flat ++= Array.tabulate[GeoIRect](nFlats - flat.length)(_ =>
                new GeoIRect
              )
              markFlat(x, y, flat(nFlats - 1), nFlats, nPits)
            }
          }
        }
      }
      nPits
    }

    def findOutlets(nPits: Int): Int = {
      var bOutlet, bExArea, bGoExArea: Boolean = true
      var i, ix, iy, iMin, iID, j, jID: Int = 0
      val pitID: Array[Int] = new Array[Int](8)
      var z, dz, dzMin: Double = 0
      var pOutlet: PitOutlet = null

      if (nPits > 0) {
        nJunctions = new Array[Int](nPits)
        junction = Array.ofDim[Int](nPits, 0)
        for (n <- 0 until DEM.nCells) {
          val (x, y, bool) = DEM.getSorted(n, bDown = false)
          if (bool && pits.get(x, y) == 0) {
            z = DEM.getDouble(x, y)
            iMin = -1
            bOutlet = false
            bGoExArea = false

            for (i <- 0 until 8) {
              ix = DEM.calXTo(i, x)
              iy = DEM.calYTo(i, y)
              bExArea = !DEM.isInGrid(ix, iy)
              if (bExArea || z > DEM.getDouble(ix, iy)) {
                if (bExArea) {
                  pitID(i) = 0
                  iID = 0
                } else {
                  pitID(i) = pits.get(ix, iy)
                  iID = pits.get(ix, iy)
                }
                if (iID >= 0) {
                  j = 0
                  while (j < i && !bOutlet) {
                    jID = pitID(j)
                    if (jID >= 0 && !getJunction(iID, jID)) bOutlet = true
                    j += 1
                  }
                }

                if (!bGoExArea) {
                  if (bExArea) {
                    bGoExArea = true
                    iMin = i
                  } else {
                    dz = (z - DEM.getDouble(ix, iy)) / DEM.getLength(i)
                    if (iMin < 0 || dzMin < dz) {
                      iMin = i
                      dzMin = dz
                    }
                  }
                }

              } else {
                pitID(i) = -1
              }

            }

            if (bOutlet) {
              if (pOutlet != null) {
                pOutlet.next = new PitOutlet
                pOutlet.next.prev = pOutlet
                pOutlet = pOutlet.next
              } else {
                outlets = new PitOutlet
                pOutlet = outlets
                outlets.prev = null
              }

              pOutlet.next = null
              pOutlet.x = x
              pOutlet.y = y
              System.arraycopy(pitID, 0, pOutlet.pitID, 0, pitID.length)

              for (i <- 1 until 8) {
                iID = pitID(i)
                if (iID >= 0) {
                  for (j <- 0 until i) {
                    jID = pitID(j)
                    if (jID >= 0 && !getJunction(iID, jID)) {
                      addJunction(iID, jID)
                    }
                  }
                }
              }

            }

            if (iMin >= 0) {
              pits.setDouble(x, y, pitID(iMin))
            }

          }
        }
      }
      0
    }

    def findRoute(pOutlet: PitOutlet): Int = {
      var bDrained, bNotDrained: Boolean = false
      var x, y, i, ix, iy, iMin, pitID, nPitsDrained: Int = 0
      var z, dz, dzMin: Double = 0.0

      for (i <- 0 until 8) {
        pitID = pOutlet.pitID(i)
        if (pitID == 0) {
          bDrained = true
        } else if (pitID > 0) {
          if (pit(pitID - 1).bDrained) {
            bDrained = true
          } else {
            bNotDrained = true
          }
        }
      }

      if (bDrained) {
        if (bNotDrained) {
          x = pOutlet.x
          y = pOutlet.y
          z = DEM.getDouble(x, y)
          if (Route.get(x, y) == 0) {
            iMin = -1
            breakable {
              for (i <- 0 until 8) {
                ix = DEM.calXTo(i, x)
                iy = DEM.calYTo(i, y)
                if (!DEM.isInGrid(ix, iy) || Route.get(ix, iy) > 0) {
                  iMin = i
                  break
                } else {
                  pitID = pOutlet.pitID(i)
                  if (pitID == 0 || (pitID > 0 && pit(pitID - 1).bDrained)) {
                    dz = (z - DEM.getDouble(ix, iy)) / DEM.getLength(i)
                    if (iMin < 0 || dzMin < dz) {
                      iMin = i
                      dzMin = dz
                    }
                  }
                }
              }
            }
            if (iMin >= 0) {
              if (iMin > 0)
                Route.setDouble(x, y, iMin)
              else Route.setDouble(x, y, 8)
            } else throw new ArithmeticException("Routing Error")
          }

          for (i <- 0 until 8) {
            pitID = pOutlet.pitID(i)
            if (pitID > 0 && !pit(pitID - 1).bDrained) {
              pit(pitID - 1).bDrained = true
              drainPit(x, y, pitID)
              nPitsDrained += 1
            }
          }
        }

        if (pOutlet.prev != null) {
          pOutlet.prev.next = pOutlet.next
        } else {
          outlets = pOutlet.next
        }

        if (pOutlet.next != null) {
          pOutlet.next.prev = pOutlet.prev
        }
      }
      nPitsDrained
    }

    def drainPit(x: Int, y: Int, pitID: Int): Unit = {
      var x1: Int = x
      var y1: Int = y
      var i, ix, iy, iMin: Int = 0
      var z, dz, dzMin: Double = 0.0
      do {
        iMin = -1
        if (flats != null && flats.get(x1, y1) > 0) {
          drainFlat(x1, y1)
        } else {
          z = DEM.getDouble(x1, y1)
          dzMin = 0
          for (i <- 0 until 8) {
            ix = DEM.calXTo(i, x1)
            iy = DEM.calYTo(i, y1)
            if (
              DEM.isInGrid(ix, iy) && pits
                .get(ix, iy) == pitID && Route.get(ix, iy) == 0
            ) {
              dz = (z - DEM.getDouble(ix, iy)) / DEM.getLength(i)
              if (dzMin < dz) {
                iMin = i
                dzMin = dz
              }
            }
          }

          if (iMin >= 0) {
            x1 += DEM.calXTo(iMin)
            y1 += DEM.calYTo(iMin)
            i = (iMin + 4) % 8
            if (i > 0)
              Route.setDouble(x1, y1, i)
            else Route.setDouble(x1, y1, 8)
          }
        }
      } while (iMin >= 0)

    }

    def drainFlat(x: Int, y: Int): Unit = {
      var bContinue: Boolean = false
      var i, ix, iy, j, n, nPlus, flatID: Int = 0
      var pFlat: GeoIRect = null

      flatID = flats.get(x, y)
      if (flatID > 0) {
        pFlat = flat(flatID - 1)
        nPlus = -1
        flats.setDouble(x, y, nPlus)

        do {
          bContinue = false
          n = nPlus
          nPlus -= 1
          for (y <- pFlat.yMin to pFlat.yMax) {
            for (x <- pFlat.xMin to pFlat.xMax) {
              if (flats.get(x, y) == n) {
                for (i <- 0 until 8) {
                  ix = DEM.calXTo(i, x)
                  iy = DEM.calYTo(i, y)

                  if (DEM.isInGrid(ix, iy) && flatID == flats.get(ix, iy)) {
                    bContinue = true
                    j = (i + 4) % 8
                    if (j != 0) {
                      Route.setDouble(ix, iy, j)
                    } else {
                      Route.setDouble(ix, iy, 8)
                    }
                    flats.setDouble(ix, iy, nPlus)
                  }
                }
              }
            }
          }

        } while (bContinue)

        for (y <- pFlat.yMin to pFlat.yMax) {
          for (x <- pFlat.xMin to pFlat.xMax) {
            if (flats.get(x, y) < 0) {
              flats.setDouble(x, y, 0)
            }
          }
        }

      }

    }

    def getJunction(iID: Int, jID: Int): Boolean = {
      var iId: Int = iID
      var jId: Int = jID
      if (iID == jID) {
        return true
      } else {
        if (iID > jID) {
          iId = jID
          jId = iID
        }
        for (i <- 0 until nJunctions(iId)) {
          if (junction(iId)(i) == jId) {
            return true
          }
        }
      }
      false
    }

    def addJunction(iID: Int, jID: Int): Unit = {
      var iId: Int = iID
      var jId: Int = jID
      var i: Int = 0
      if (iID != jID) {
        if (iID > jID) {
          iId = jID
          jId = iID
        }
        nJunctions(iId) += 1
        i = nJunctions(iId)
        junction(iId) ++= Array.ofDim[Int](i - junction(iId).length)
        junction(iId)(i - 1) = jId
      }
    }

    def markFlat(
        x: Int,
        y: Int,
        pFlat: GeoIRect,
        flatID: Int,
        pitID: Int
    ): Unit = {
      var goStackDown: Boolean = true
      var i, ix, iy, iStart, iStack, nStcak: Int = 0
      var xMem: Array[Int] = new Array[Int](0)
      var yMem: Array[Int] = new Array[Int](0)
      var iMem: Array[Int] = new Array[Int](0)
      var z: Double = 0.0

      if (flats == null)
        flats = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
      z = DEM.getDouble(x, y)
      pFlat.xMin = x
      pFlat.xMax = x
      pFlat.yMin = y
      pFlat.yMax = y
      pits.setDouble(x, y, pitID)
      flats.setDouble(x, y, flatID)

      var x1: Int = x
      var y1: Int = y

      do {
        goStackDown = true
        i = iStart
        while (i < 8 && goStackDown) {
          ix = DEM.calXTo(i, x1)
          iy = DEM.calYTo(i, y1)
          if (
            DEM.isInGrid(ix, iy) && pits
              .get(ix, iy) == 0 && DEM.getDouble(ix, iy) == z
          ) {
            goStackDown = false
            pits.setDouble(ix, iy, pitID)
            flats.setDouble(ix, iy, flatID)
          }
          i += 1
        }

        if (goStackDown) {
          iStack -= 1
          if (iStack >= 0) {
            x1 = xMem(iStack)
            y1 = yMem(iStack)
            iStart = iMem(iStack)
          }
        } else {
          if (nStcak <= iStack) {
            nStcak = iStack + 32
            xMem ++= Array.ofDim[Int](nStcak - xMem.length)
            yMem ++= Array.ofDim[Int](nStcak - yMem.length)
            iMem ++= Array.ofDim[Int](nStcak - iMem.length)
          }
          xMem(iStack) = x1
          yMem(iStack) = y1
          iMem(iStack) = i + 1

          x1 = ix
          y1 = iy
          iStart = 0

          if (x1 < pFlat.xMin)
            pFlat.xMin = x1
          else if (x1 > pFlat.xMax)
            pFlat.xMax = x1

          if (y1 < pFlat.yMin)
            pFlat.yMin = y1
          else if (y1 > pFlat.yMax)
            pFlat.yMax = y1

          iStack += 1

        }

      } while (iStack >= 0)

    }

    def processThreshold(): Int = {
      var x, y, n, i: Int = 0
      route = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          i = Route.get(x, y)
          if (DEM.isNoData(x, y)) {
            route.setDouble(x, y, -1)
          } else if (i > 0) {
            route.setDouble(x, y, i % 8)
          } else {
            route.setDouble(x, y, DEM.getGradientNeighborDir(x, y))
          }
        }
      }
      lockCreate()
      for (i <- 0 until DEM.nCells) {
        val (x, y, bool) = DEM.getSorted(i, bDown = false)
        if (bool && pits.get(x, y) != 0) {
          zThr = DEM.getDouble(x, y) + threshold
          zMax = DEM.getDouble(x, y)
          checkThreshold(x, y)
          if (zMax > zThr) n += 1
        }
      }

      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          i = route.get(x, y)
          if (i < 0 || i == DEM.getGradientNeighborDir(x, y)) {
            Route.setDouble(x, y, 0)
          } else {
            if (i == 0)
              Route.setDouble(x, y, 8)
            else
              Route.setDouble(x, y, i)
          }
        }
      }

      n

    }

    def checkThreshold(x: Int, y: Int): Unit = {
      if(lockGet(x, y) != 0){
        return
      }
      lockSet(x, y)

      if (DEM.getDouble(x, y) > zMax) {
        zMax = DEM.getDouble(x, y)
      }
      val i: Int = route.get(x, y)
      val ix: Int = DEM.calXTo(i, x)
      val iy: Int = DEM.calYTo(i, y)

      if (DEM.isInGrid(ix, iy)) {
        if (DEM.getDouble(x, y) < DEM.getDouble(ix, iy) || zMax < zThr) {
          checkThreshold(ix, iy)
        }
      }
      if (zMax > zThr) {
        route.setDouble(x, y, (i + 4) % 8)
      }

    }

  }

  class GeoIRect() {
    var xMin, yMin, xMax, yMax: Int = _
  }
  class Pit() {
    var bDrained: Boolean = _
    var z: Double = _
  }
  class PitOutlet() {
    var x: Int = _
    var y: Int = _
    var pitID: Array[Int] = new Array[Int](8)
    var prev: PitOutlet = _
    var next: PitOutlet = _
  }

}
