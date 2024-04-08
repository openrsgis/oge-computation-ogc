package whu.edu.cn.algorithms.terrain.calculator

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import whu.edu.cn.algorithms.terrain.core.{OgeTerrainTile, TileToolGrid}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage

import java.text.SimpleDateFormat
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object WatershedBasins {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage              raster data in RDD format with metadata
    * @param flowAccumulationImage flow accumulation data in RDD format with metadata(Optional)
    * @param zFactor               z position factor for calculation
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      flowAccumulationImage: RDDImage,
      zFactor: Double = 1.0
  ): RDDImage = {
    //merge rddImage
    val sc = rddImage._1.sparkContext
    val tileLayerArray =
      rddImage._1
        .map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        .collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)

    val flowAccumulationLayerArray =
      flowAccumulationImage._1
        .map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        .collect()
    val (flowAccumulation, (_, _), (_, _)) =
      TileLayoutStitcher.stitch(flowAccumulationLayerArray)

    val calculator: WatershedBasinsCalculator =
      WatershedBasinsCalculator(
        tile.band(0),
        flowAccumulation.band(0),
        rddImage._2,
        0,
        zFactor
      )
    val resultTile = MultibandTile(calculator())

    val metadata = rddImage._2
    val mergedTile = resultTile
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime

    // 定义切分后的小瓦片大小
    val tileWidth = 256
    val tileHeight = 256

    // 计算整体瓦片的列数和行数
    val totalCols = metadata.layout.cols
    val totalRows = metadata.layout.rows

    // 计算切分后的小瓦片的列数和行数
    val subTileCols = (totalCols.toDouble / tileWidth).ceil.toInt
    val subTileRows = (totalRows.toDouble / tileHeight).ceil.toInt

    // 创建新的布局定义对象
    val newLayoutDefinition = metadata.layout

    val subTiles = resultTile.split(newLayoutDefinition.tileLayout)
    val tiledSplit = subTiles.zipWithIndex.map { case (subTile, index) =>
      val col = index % newLayoutDefinition.layoutCols
      val row = index / newLayoutDefinition.layoutCols
      val newKey = SpaceTimeBandKey(SpaceTimeKey(col, row, date), ListBuffer("Aspect"))
      (newKey, subTile)
    }
    val tiledSplitRDD = sc.parallelize(tiledSplit)

    (tiledSplitRDD, metadata)

  }

  private case class WatershedBasinsCalculator(
      tile: Tile,
      flowAccumulation: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) extends TileToolGrid(tile, metaData, 0) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Accumulation: OgeTerrainTile =
      OgeTerrainTile.from(flowAccumulation, metaData, paddingSize)
    private var Direction: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)
    private var Channels: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)
    private val ChannelRoute: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)
    private val Start: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, 0, 0)
    private var Basins: OgeTerrainTile =
      OgeTerrainTile.like(tile, metaData, paddingSize)

    val minLength: Int = 10
    val minSize: Int = 0
    var nBasins: Int = 0

    def apply(): Tile = {
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

      for (n <- 0 until DEM.nCells) {
        if (Accumulation.getDouble(n) >= 0) Start.setDouble(n, 1)
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

      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          setChannelMouth(x, y)
        }
      }

      for (n <- 0 until DEM.nCells) {
        if (Channels.getDouble(n) == 0) {
          Channels.setNoData(n)
          ChannelRoute.setNoData(n)
        }
      }

      Direction = OgeTerrainTile.like(tile, metaData, paddingSize)
      Direction.noDataValue = -1
      Basins.noDataValue = -1
      Basins = OgeTerrainTile.like(tile, metaData, paddingSize)
      var n: Int = 0
      var nbasins, nCells: Int = 0

      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          if (DEM.isNoData(x, y))
            Direction.setNoData(x, y)
          else {
            n = DEM.getGradientNeighborDir(x, y)
            Direction.setDouble(x, y, if (n < 0) -1 else (n + 4) % 8)
          }
        }
      }

      nBasins = 0
      for (n <- 0 until DEM.nCells) {
        val (x, y, _) = DEM.getSorted(n, bCheckNoData = false)
        if (!Channels.isNoData(x, y) && Channels.get(x, y) < 0) {
          nBasins += 1
          nCells = getBasin(x, y)
          if (nCells < minSize) {
            nbasins = nBasins - 1
            nBasins = -1
            getBasin(x, y)
            nBasins = nbasins
          }
        }
      }
      Basins.refine()
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

    def getBasin(x: Int, y: Int): Int = {
      var ix, iy: Int = 0
      var nCells: Int = 1
      if (Basins.isNoData(x, y) && !Direction.isNoData(x, y)) {
        Basins.setDouble(x, y, nBasins)
        for (i <- 0 until 8) {
          ix = DEM.calXTo(i, x)
          iy = DEM.calYTo(i, y)
          if (DEM.isInGrid(ix, iy) && Direction.get(ix, iy) == i)
            nCells += getBasin(ix, iy)
        }
        return nCells
      }
      -1
    }
  }
}
