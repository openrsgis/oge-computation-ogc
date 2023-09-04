package whu.edu.cn.entity

import geotrellis.layer.SpatialKey

import scala.beans.BeanProperty

/**
 * Extend Geotrellis SpaceTimeKey to contain measurement dimension.
 *
 */
case class SpaceBandKey(_spatialKey: SpatialKey, _measurementName: String) extends Serializable {
  @BeanProperty
  var spatialKey: SpatialKey = _spatialKey
  @BeanProperty
  var measurementName: String = _measurementName

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceBandKey =>
        this.spatialKey.row == obj.spatialKey.row &&
          this.spatialKey.col == obj.spatialKey.col &&
          this.measurementName.equals(obj.measurementName)
      case _ => false
    }
  }
}
