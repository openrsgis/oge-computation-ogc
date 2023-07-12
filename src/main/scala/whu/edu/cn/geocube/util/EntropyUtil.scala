package whu.edu.cn.geocube.util

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.{CellwiseCalculation, DoubleArrayTileResult, FocalCalculation, Square}

import scala.collection.mutable

// 交叉熵处理类
object EntropyUtil {
  def calculation(tile: Tile,
                  n: Neighborhood,
                  bounds: Option[GridBounds[Int]],
                  target: TargetCell = TargetCell.All)
  : FocalCalculation[Tile] = {
    n match {
      case Square(ext) =>
        new CellEntropyCalcDouble(tile, n, bounds, ext, target)
      case _ =>
        throw new RuntimeException("这个非正方形的还没写！")
    }
  }

  def apply(tile: Tile,
            n: Neighborhood,
            bounds: Option[GridBounds[Int]],
            target: TargetCell = TargetCell.All)
  : Tile =
    calculation(tile, n, bounds, target).execute()

}


class CellEntropyCalcDouble(r: Tile,
                            n: Neighborhood,
                            bounds: Option[GridBounds[Int]],
                            extent: Int,
                            target: TargetCell)
  extends CellwiseCalculation[Tile](r, n, bounds, target)
    with DoubleArrayTileResult {

  println(s"Calc extent = ${extent}")

  // map[灰度值, 出现次数]
  val greyMap = new mutable.HashMap[Int, Int]()
  var currRangeMax = 0


  private final def isIllegalData(v: Int): Boolean = {
    isNoData(v) && (v < 0 || v > 255)
  }


  def add(r: Tile, x: Int, y: Int): Unit = {
    val v: Int = r.get(x, y)
    // assert v isData
    if (isIllegalData(v)) {
      //      assert(false)
    } else if (!greyMap.contains(v)) {
      greyMap.put(v, 0)
      currRangeMax += 1
    } else {
      greyMap(v) += 1
      currRangeMax += 1
    }
  }

  def remove(r: Tile, x: Int, y: Int): Unit = {
    val v: Int = r.get(x, y)
    if (isIllegalData(v)) {
      //      assert(false)
    } else if (greyMap.contains(v) &&
      greyMap(v) > 0 &&
      currRangeMax > 0) {
      greyMap(v) -= 1
      currRangeMax -= 1
    }
  }

  def setValue(x: Int, y: Int): Unit = {
    resultTile.setDouble(x, y,
      if (currRangeMax == 0) NODATA else {

        var sum = 0.0
        for (kv <- greyMap) if (kv._2 > 0) {
          val p: Double = kv._2.toDouble / currRangeMax.toDouble
          sum -= p * math.log10(p) / math.log10(2)
        }
        sum
      })
  }

  def reset(): Unit = {
    greyMap.clear()
    currRangeMax = 0
  }

}