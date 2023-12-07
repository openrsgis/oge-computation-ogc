package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.RDDImage
import geotrellis.layer.stitch.TileLayoutStitcher
import whu.edu.cn.entity.SpaceTimeBandKey
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.math.Pi

object FlowAccumulation {

  /** call the Calculator
    *
    * @note the intput rddImage doesn't require padding
    * @param rddImage raster data in RDD format with metadata
    * @param zFactor  z position factor for calculation
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      zFactor: Double = 1.0
  ): RDDImage = {

    val sc = rddImage._1.sparkContext
    val tileLayerArray =
      rddImage._1
        .map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        .collect()
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val calculator =
      FlowAccumulationCalculator(tile.band(0), rddImage._2, 0, zFactor)
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

  private case class FlowAccumulationCalculator(
      tile: Tile,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double
  ) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val Flow: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
    private val flow: Array[OgeTerrainTile] = new Array[OgeTerrainTile](8)

    //parameters
    val update: Int = 0
    val method: Int = 2
    val convergence: Double = 1.1

    def apply(): Tile = {
      if (initialize()) {
        var bChanged: Boolean = false
        do {
          bChanged = false
          for (y <- 0 until DEM.nY) {
            for (x <- 0 until DEM.nX) {
              if (getFlow(x, y))
                bChanged = true
            }
          }
        } while (bChanged)
      }

      for(y <- 0 until DEM.nY){
        for(x <- 0 until DEM.nX){
          Flow.setDouble(x, y, Flow.getDouble(x, y) / 1e6)
        }
      }

//      Flow.noDataValue = -99999
//
//      // real rows cols
//      val nx = ((metaData.extent.xmax - metaData.extent.xmin) / metaData.cellSize.width).round.toInt
//      val ny = ((metaData.extent.ymax - metaData.extent.ymin) / metaData.cellSize.width).round.toInt
//
//      for (y <- 0 until DEM.nY) {
//        for (x <- 0 until DEM.nX) {
//          if (Flow.isNoData(x, y)) {
//            Flow.setDouble(x, y, 0)
//          }
//          if (x >= nx || y < DEM.nY - ny) {
//            Flow.setNoData(x, y)
//          }
//        }
//      }

      Flow.refine()
    }

    def initialize(): Boolean = {
      Flow.noDataValue = 0.0
      for (i <- 0 until 8) {
        flow(i) = OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
      }
      val c: Double = convergence

      for (y <- 0 until DEM.nY) {
        for (x <- 0 until DEM.nX) {
          if (!DEM.isNoData(x, y)) {
            method match {
              case 0 => setD8(x, y)
              case 1 => setDinf(x, y)
              case 2 => setMFD(x, y, c)
            }
          }
        }
      }
      true

    }

    def setD8(x: Int, y: Int): Boolean = {
      val i: Int = DEM.getGradientNeighborDir(x, y)
      if (i < 0)
        return false
      flow(i).setDouble(x, y, 1.0)
      true
    }

    def setDinf(x: Int, y: Int): Boolean = {
      val (s, a, bool) = DEM.getGradient(x, y)
      if (bool && a >= 0) {
        val i: Array[Int] = new Array[Int](2)
        i(0) = (a / (Pi / 4)).toInt
        i(1) = i(0) + 1
        val ix0: Int = DEM.calXTo(i(0), x)
        val iy0: Int = DEM.calYTo(i(0), y)
        val ix1: Int = DEM.calXTo(i(1), x)
        val iy1: Int = DEM.calYTo(i(1), y)

        if (
          DEM
            .isInGrid(ix0, iy0) && DEM.getDouble(ix0, iy0) < DEM.getDouble(x, y)
          && DEM.isInGrid(ix1, iy1) && DEM
            .getDouble(ix1, iy1) < DEM.getDouble(x, y)
        ) {
          val d: Double = (a % (Pi / 4)) / (Pi / 4)
          flow(i(0) % 8).setDouble(x, y, 1 - d)
          flow(i(1) % 8).setDouble(x, y, d)
          return true
        }
      }
      setD8(x, y)
    }

    def setMFD(x: Int, y: Int, convergence: Double): Boolean = {
      val dz: Array[Double] = new Array[Double](8)
      var dzSum: Double = 0
      val z: Double = DEM.getDouble(x, y)
      for (i <- 0 until 8) {
        val ix: Int = DEM.calXTo(i, x)
        val iy: Int = DEM.calYTo(i, y)
        if (DEM.isInGrid(ix, iy)) {
          dz(i) = DEM.getDouble(ix, iy)
          if (dz(i) < z) {
            dz(i) = math.pow((z - dz(i)) / DEM.getLength(i), convergence)
            dzSum += dz(i)
          } else
            dz(i) = 0.0
        } else
          dz(i) = 0.0
      }

      if (dzSum > 0) {
        for (i <- 0 until 8) {
          if (dz(i) > 0) {
            val ix = DEM.calXTo(i, x)
            val iy = DEM.calYTo(i, y)
            flow(i).setDouble(x, y, dz(i) / dzSum)
          }
        }
      }
      true
    }

    def getFlow(x: Int, y: Int): Boolean = {
      if (!Flow.isNoData(x, y) || DEM.isNoData(x, y))
        return false
      var flow0: Double = DEM.cellArea
//      var flow0: Double = metaData.cellSize.width * metaData.cellSize.width
      for (i <- 0 until 8) {
        val ix: Int = DEM.calXFrom(i, x)
        val iy: Int = DEM.calYFrom(i, y)
        if (flow(i).isInGrid(ix, iy) && flow(i).getDouble(ix, iy) > 0) {
          if (Flow.isNoData(ix, iy))
            return false
          flow0 += flow(i).getDouble(ix, iy) * Flow.getDouble(ix, iy)
        }

      }

      Flow.setDouble(x, y, flow0)
      true
    }

  }

}
