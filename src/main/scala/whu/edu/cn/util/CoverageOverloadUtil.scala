package whu.edu.cn.util

import geotrellis.raster.{NODATA, Tile, d2i, i2d, isNoData}
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp

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


}