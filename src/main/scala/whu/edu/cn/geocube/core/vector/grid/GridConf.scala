package whu.edu.cn.geocube.core.vector.grid

import geotrellis.vector.Extent

/**
 * A config for grid tessellation.
 *
 * */
case class GridConf(_gridDimX: Long, _gridDimY: Long, _extent: Extent) extends Serializable {
  val gridDimX = _gridDimX // column num
  val gridDimY = _gridDimY // row num
  val extent = _extent // spatial extent
  val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble //grid x-resolution
  val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble // grid y-resolution

  override def equals(o: Any): Boolean =
    o match {
      case other: GridConf => (gridDimX == other.gridDimX && gridDimY == other.gridDimY && extent.equals(other.extent))
      case _ => false
    }

  override def toString: String = "GridDim(" + gridDimX + ", " + gridDimY + "), " + extent.toString() + ", GridSize(" + gridSizeX + ", " + gridSizeY + ")"
}
