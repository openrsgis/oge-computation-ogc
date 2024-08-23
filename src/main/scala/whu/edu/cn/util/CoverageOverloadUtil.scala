package whu.edu.cn.util

import geotrellis.raster.{ArrayTile, DI, Dimensions, GeoAttrsError, NODATA, Tile, d2i, i2d, isData, isNoData}
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import spire.syntax.cfor.cfor
import scala.collection.mutable

object CoverageOverloadUtil {

  // Returns 1 iff both values are non-zero
  object And extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int): Int = {
      if (isNoData(z1) || isNoData(z2)) NODATA
      else if (z1 == 0 || z2 == 0) 0
      else 1
    }

    def combine(z1: Double, z2: Double): Double = {
      if (isNoData(z1) || isNoData(z2)) Double.NaN
      else if (z1.equals(0.0) || z2.equals(0.0)) 0 // 已被重载
      else 1
    }
  }

  object Cat extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int): Int = {
      if (isNoData(z1)&&isNoData(z2)) NODATA
      else if(isNoData(z1)) z2
      else z1
    }

    def combine(z1: Double, z2: Double): Double = {
      if (isNoData(z1) && isNoData(z2)) Double.NaN
      else if (isNoData(z1)) z2
      else z1
    }
  }

  // Returns 1 iff either values are non-zero
  object Or extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int): Int = {
      if (isNoData(z1) || isNoData(z2)) NODATA
      else if (z1 == 0 && z2 == 0) 0
      else 1
    }

    def combine(z1: Double, z2: Double): Double = {
      if (isNoData(z1) || isNoData(z2)) Double.NaN
      else if (z1.equals(0.0) && z2.equals(0.0)) 0
      else 1
    }
  }


  // Returns 0 if the input is non-zero, and 1 otherwise
  object Not extends Serializable {
    def apply(r: Tile): Tile =
      r.mapDouble {
        z =>
          if (isNoData(z)) z
          else if (z.equals(0.0)) 1
          else 0
      }
  }

  //以下是为CoverageCollection运算新增的重载
  object Min extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int): Int = {
      if (isNoData(z1)) {
        if(isNoData(z2)) NODATA else z2
      }
      else {
        if(isNoData(z2)) z1 else math.min(z1, z2)
      }
    }

    def combine(z1: Double, z2: Double): Double = {
      if (isNoData(z1)) {
        if(isNoData(z2)) Double.NaN else z2
      }
      else {
        if(isNoData(z2)) z1 else math.min(z1, z2)
      }
    }
  }
  object Max extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int): Int = {
      if (isNoData(z1)) {
        if(isNoData(z2)) NODATA else z2
      }
      else {
        if(isNoData(z2)) z1 else math.max(z1, z2)
      }
    }

    def combine(z1: Double, z2: Double): Double = {
      if (isNoData(z1)) {
        if(isNoData(z2)) Double.NaN else z2
      }
      else {
        if(isNoData(z2)) z1 else math.max(z1, z2)
      }
    }
  }
  object Add extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int) =
      if (isNoData(z1) && isNoData(z2)) NODATA
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else z1 + z2

    def combine(z1: Double, z2: Double) =
      if (isNoData(z1) && isNoData(z2)) Double.NaN
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else z1 + z2
  }
  object OrCollection extends LocalTileBinaryOp {
    def combine(z1: Int,z2: Int) =
      if (isNoData(z1) && isNoData(z2)) NODATA
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else z1 | z2

    def combine(z1: Double,z2: Double) =
      if (isNoData(z1) && isNoData(z2)) Double.NaN
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else i2d(d2i(z1) | d2i(z2))
  }
  object AndCollection extends LocalTileBinaryOp {
    def combine(z1: Int, z2: Int) =
      if (isNoData(z1) && isNoData(z2)) NODATA
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else z1 & z2

    def combine(z1: Double, z2: Double) =
      if (isNoData(z1) && isNoData(z2)) Double.NaN
      else if(isNoData(z1)) z2
      else if(isNoData(z2)) z1
      else i2d(d2i(z1) & d2i(z2))
  }
  object Median extends Serializable {

    def apply(rs: Tile*): Tile =
      apply(rs)

    def apply(rs: Traversable[Tile])(implicit d: DI): Tile = {
      // TODO: Replace all of these with rs.assertEqualDimensions
      if(Set(rs.map(_.dimensions)).size != 1) {
        val dimensions = rs.map(_.dimensions).toSeq
        throw new GeoAttrsError("Cannot combine rasters with different dimensions." +
          s"$dimensions are not all equal")
      }

      val layerCount = rs.toSeq.length
      if(layerCount == 0) {
        sys.error(s"Can't compute median of empty sequence")
      } else {
        val newCellType = rs.map(_.cellType).reduce(_.union(_))
        val Dimensions(cols, rows) = rs.head.dimensions
        val tile = ArrayTile.alloc(newCellType, cols, rows)

        if(newCellType.isFloatingPoint) {
          val counts = mutable.ListBuffer[Double]()

          cfor(0)(_ < rows, _ + 1) { row =>
            cfor(0)(_ < cols, _ + 1) { col =>
              counts.clear
              for(r <- rs) {
                val v = r.getDouble(col, row)
                if(isData(v)) {
                  counts.append(v)
                }
              }
              val sorted =
                counts
                  .toList
                  .sortBy { k => k }
              val len = sorted.length
              val m =
                if(len==0) Double.NaN
                else if(len%2==0) (sorted(len/2-1)+sorted(len/2))/2.0
                else sorted(len/2)
              tile.setDouble(col, row, m)
            }
          }
        } else {
          val counts = mutable.ListBuffer[Int]()

          for(col <- 0 until cols) {
            for(row <- 0 until rows) {
              counts.clear
              for(r <- rs) {
                val v = r.get(col, row)
                if(isData(v)) {
                  counts.append(v)
                }
              }

              val sorted =
                counts
                  .toList
                  .sortBy { k => k }
              val len = sorted.length
              val m =
                if(len==0) NODATA
                else if(len%2==0) (sorted(len/2-1)+sorted(len/2))/2
                else sorted(len/2)
              tile.set(col, row, m)
            }
          }
        }
        tile
      }
    }
  }


}