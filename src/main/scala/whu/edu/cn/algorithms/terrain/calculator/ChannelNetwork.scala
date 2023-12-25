package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.TypeAliases.{RDDImage, RDDFeature}
import whu.edu.cn.algorithms.terrain.core.{OgeTerrainTile, TileToolGrid}
import com.alibaba.fastjson.JSON
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.vector.reproject.Reproject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object ChannelNetwork {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage              raster data in RDD format with metadata
    * @param dirImage              flow direction data in RDD format with metadata(Optional)
    * @param flowAccumulationImage flow accumulation data in RDD format with metadata(Optional)
    * @param zFactor               z position factor for calculation
    * @param threshold             Strahler order to begin a channel.
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      flowAccumulationImage: RDDImage,
      dirImage: RDDImage = null,
      zFactor: Double = 1.0,
      threshold: Double = 0.0
  ): RDDFeature = {
    //merge rddImage
    val sc = rddImage._1.sparkContext
    val tileLayerArray =
      rddImage._1
        .map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        .collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)

    var dirTile: MultibandTile = null
    if (dirImage != null) {
      val dirTileLayerArray =
        dirImage._1
          .map(t => {
            (t._1.spaceTimeKey.spatialKey, t._2)
          })
          .collect()
      val (dir, (_, _), (_, _)) = TileLayoutStitcher.stitch(dirTileLayerArray)
      dirTile = dir
    }

    val flowAccumulationLayerArray =
      flowAccumulationImage._1
        .map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        .collect()
    val (flowAccumulation, (_, _), (_, _)) =
      TileLayoutStitcher.stitch(flowAccumulationLayerArray)

    var calculator: ChannelNetworkCalculator = null
    if (dirTile == null) {
      calculator = ChannelNetworkCalculator(
        tile.band(0),
        null,
        flowAccumulation.band(0),
        rddImage._2,
        zFactor,
        threshold
      )
    } else {
      calculator = ChannelNetworkCalculator(
        tile.band(0),
        dirTile.band(0),
        flowAccumulation.band(0),
        rddImage._2,
        zFactor,
        threshold
      )
    }

    val result = calculator()

    val rddResult: RDD[(String, (Geometry, mutable.Map[String, Any]))] =
      sc.parallelize(result)
    rddResult

  }

  private case class ChannelNetworkCalculator(
      tile: Tile,
      direction: Tile,
      flowAccumulation: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      zFactor: Double,
      threshold: Double
  ) extends TileToolGrid(tile, metaData, 0) {
    private val DEM: OgeTerrainTile = OgeTerrainTile.from(tile, metaData, 0)
    private val Direction: OgeTerrainTile =
      if (direction != null) OgeTerrainTile.from(direction, metaData, 0)
      else null
    private val Accumulation: OgeTerrainTile =
      OgeTerrainTile.from(flowAccumulation, metaData, 0)
    private var Channels: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)
    private val ChannelRoute: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)
    private val Start: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)

    val Shapes: ListBuffer[(String, (Geometry, mutable.Map[String, Any]))] =
      ListBuffer()
    val geometryFactory = new GeometryFactory()

    //parameters
    val minLength: Int = 10
    val maxDivCells: Int = -1
    val initMethod: Int = 2

    def apply(): ListBuffer[(String, (Geometry, mutable.Map[String, Any]))] = {
      if (!DEM.setIndex())
        throw new Exception("index creation failed!")

      //Flow Direction
      val traceMethod: Int = 0
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          if (Direction != null) {
            val id: Int = Direction.get(x, y)
            if (id >= 1 && id <= 8) {
              Channels.setDouble(x, y, id)
            } else {
              traceMethod match {
                case 0 | _ => setRouteStandard(x, y)
              }
            }
          } else {
            traceMethod match {
              case 0 | _ => setRouteStandard(x, y)
            }
          }
        }
      }

      //Initiation
      initMethod match {
        case 0 =>
          for (n <- 0 until DEM.nCells) {
            if (Accumulation.getDouble(n) <= threshold) Start.setDouble(n, 1)
          }
        case 1 =>
          for (n <- 0 until DEM.nCells) {
            if (Accumulation.getDouble(n) == threshold) Start.setDouble(n, 1)
          }
        case 2 =>
          for (n <- 0 until DEM.nCells) {
            if (Accumulation.getDouble(n) >= threshold) Start.setDouble(n, 1)
          }
      }

      //Trace Channel Routes
      for (n <- 0 until DEM.nCells) {
        val (x, y, bool) = DEM.getSorted(n)
        if (bool)
          setChannelRoute(x, y)
      }

      Channels = OgeTerrainTile.fill(tile, metaData, 0, 0)

      //pass 4
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          setChannelOrder(x, y)
        }
      }

      //pass 5
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          setChannelMouth(x, y)
        }
      }

      lockCreate()
      var id: Int = 1
      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          setVector(x, y, id)
          id += 1
        }
      }

      for (n <- 0 until DEM.nCells) {
        if (Channels.getDouble(n) == 0) {
          Channels.setNoData(n)
          ChannelRoute.setNoData(n)
        }
      }

      Shapes

    }

    def setRouteStandard(x: Int, y: Int): Unit = {
      var i, ix, iy, iMin: Int = 0
      val z: Double = DEM.getDouble(x, y)
      var dz, dzMin: Double = 0

      breakable {
        for (i <- 1 to 8) {
          ix = DEM.calXTo(i, x)
          iy = DEM.calYTo(i, y)
          if (!DEM.isInGrid(ix, iy)) {
            iMin = i
            break
          } else {
            dz = (z - DEM.getDouble(ix, iy)) / DEM.getLength(i)
            if (iMin <= 0 || dzMin < dz) {
              iMin = i
              dzMin = dz
            }
          }
        }
      }
      Channels.setDouble(x, y, iMin)
    }

    def setChannelRoute(X: Int, Y: Int): Unit = {
      var x: Int = X
      var y: Int = Y
      val xStart: Int = x
      val yStart: Int = y
      var ix, iy: Int = 0
      var goDir, n, nDiv: Int = 0
      var z, dz, dzMin, length: Double = 0
      val dir: ArrayBuffer[Int] = ArrayBuffer()

      if (Start.get(x, y) != 0 && ChannelRoute.get(x, y) == 0) {
        lockCreate()
        do {
          goDir = 0
          z = DEM.getDouble(x, y)
          for (i <- 1 to 8) {
            ix = DEM.calXTo(i, x)
            iy = DEM.calYTo(i, y)
            if (
              DEM.isInGrid(ix, iy) && !isLocked(ix, iy) && ChannelRoute.get(
                ix,
                iy
              ) != 0
            ) {
              dz = (z - DEM.getDouble(ix, iy)) / DEM.getLength(i)
              if (goDir <= 0 || dzMin < dz) {
                goDir = i
                dzMin = dz
              }
            }
          }

          if (goDir <= 0)
            goDir = Channels.get(x, y)
          if (goDir > 0) {
            lockSet(x, y)
            x = DEM.calXTo(goDir, x)
            y = DEM.calYTo(goDir, y)
            length += DEM.getUnitLength(goDir)
            dir += goDir
          }

        } while (goDir > 0 && DEM.isInGrid(x, y) && !isLocked(
          x,
          y
        ) && ChannelRoute.get(x, y) == 0)

        n = dir.size

        if (length >= minLength) {
          x = xStart
          y = yStart

          if (goDir < 0)
            n -= nDiv
          for (m <- 0 until n) {
            goDir = dir(m)
            ChannelRoute.setDouble(x, y, goDir)
            for (i <- 0 until 8) {
              ix = DEM.calXTo(i, x)
              iy = DEM.calYTo(i, y)
              if (DEM.isInGrid(ix, iy)) {
                Start.setDouble(ix, iy, 0)
              }
            }
            x = DEM.calXTo(goDir, x)
            y = DEM.calYTo(goDir, y)
          }
        }
      }

    }

    def setChannelOrder(X: Int, Y: Int): Unit = {
      var x: Int = X
      var y: Int = Y
      var i, ix, iy, j, n: Int = 0
      if (ChannelRoute.get(x, y) > 0) {
        j = 4
        for (i <- 0 until 8) {
          ix = DEM.calXTo(i, x)
          iy = DEM.calYTo(i, y)
          if (
            DEM.isInGrid(ix, iy) && ChannelRoute.get(
              ix,
              iy
            ) != 0 && j == ChannelRoute.get(ix, iy) % 8
          )
            n += 1
          j = (j + 1) % 8
        }

        if (n == 0) {
          lockCreate() // 是否重置
          do {
            lockSet(x, y)
            Channels.setDouble(x, y, Channels.getDouble(x, y) + 1)
            i = ChannelRoute.get(x, y)
            if (i > 0) {
              x = DEM.calXTo(i, x)
              y = DEM.calYTo(i, y)
            }
          } while (DEM.isInGrid(x, y) && i > 0 && !isLocked(x, y))
        }
      }
    }

    def setChannelMouth(x: Int, y: Int): Unit = {
      val order: Int = Channels.get(x, y)
      if (order > 0) {
        val goDir: Int = ChannelRoute.get(x, y)
        if (goDir > 0) {
          val ix: Int = DEM.calXTo(goDir, x)
          val iy: Int = DEM.calYTo(goDir, y)
          if (
            !DEM.isInGrid(ix, iy) || (Channels.get(
              ix,
              iy
            ) > 0 && order != Channels.get(ix, iy))
          ) {
            Channels.setDouble(x, y, -1)
          }
        } else {
          Channels.setDouble(x, y, -1)
        }
      }
    }

    def setVector(X: Int, Y: Int, ID: Int): Unit = {
      var x: Int = X
      var y: Int = Y
      var id: Int = ID
      var bContinue: Boolean = false
      var i, ix, iy, j: Int = 0
      val order: Int = Channels.get(x, y)
      var length: Double = 0

      val coordinates = ArrayBuffer.empty[Coordinate]

      if (order > 0) {
        bContinue = true
        j = 4
        while (i < 8 && bContinue) {
          ix = DEM.calXTo(i, x)
          iy = DEM.calYTo(i, y)
          if (
            DEM
              .isInGrid(ix, iy) && Channels.get(ix, iy) == order && ChannelRoute
              .get(ix, iy) != 0 && j == ChannelRoute.get(ix, iy) % 8
          ) { bContinue = false }
          i += 1
          j = (j + 1) % 8
        }

        if (bContinue) {
          id = 1
          do {
            bContinue = false
            length = 0
            lockSet(x, y)
            coordinates += addCoordinate(x, y)
            i = ChannelRoute.get(x, y)
            if (i > 0) {
              ix = DEM.calXTo(i, x)
              iy = DEM.calYTo(i, y)
              length += DEM.getLength(i)
              if (DEM.isInGrid(ix, iy)) {
                if (
                  !isLocked(ix, iy) && (Channels.get(
                    ix,
                    iy
                  ) == order || Channels.get(ix, iy) < 0)
                ) {
                  x = ix
                  y = iy
                  bContinue = true
                } else {
                  coordinates += addCoordinate(ix, iy)
                }
              }
            }
          } while (bContinue)

          val properties: String =
            f"{'SegmentID' : '$id', 'order' : '$order', 'length' : '$length'}"

          val lineString =
            geometryFactory.createLineString(coordinates.toArray)
          lineString.setSRID(4326)
          val shape: (String, (Geometry, mutable.Map[String, Any])) = (
            UUID.randomUUID().toString,
            (lineString, getMapFromJsonStr(properties))
          )
          Shapes += shape
        }

      }

    }

    def addCoordinate(x: Int, y: Int): Coordinate = {
      val crs: CRS = metaData.crs
      val xMin = metaData.extent.xmin
      val yMax = metaData.extent.ymax
      //原始投影坐标点
      val xCoordinate = xMin + (x + 0.5) * metaData.cellSize.width
      val yCoordinate = yMax - (DEM.nY - y - 0.5) * metaData.cellSize.width

      val pixelPoint = geotrellis.vector.Point(xCoordinate, yCoordinate)
      val reprojectedPoint = Reproject(pixelPoint, crs, LatLng)

      val coordinate: Coordinate =
        new Coordinate(reprojectedPoint.getX, reprojectedPoint.getY)
      coordinate
    }

    private def getMapFromJsonStr(json: String): mutable.Map[String, Any] = {
      val map = mutable.Map.empty[String, Any]
      if (StringUtils.isNotEmpty(json) && !json.equals("")) {
        val jsonObject = JSON.parseObject(json)
        val sIterator = jsonObject.keySet.iterator
        while (sIterator.hasNext) {
          val key = sIterator.next()
          val value = jsonObject.getString(key)
          map += (key -> value)
        }
      }
      map
    }

  }
}
