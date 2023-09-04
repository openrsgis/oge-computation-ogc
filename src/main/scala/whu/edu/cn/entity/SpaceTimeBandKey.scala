package whu.edu.cn.entity

import geotrellis.layer.SpaceTimeKey

import scala.beans.BeanProperty
import scala.collection.mutable

/**
 * Extend Geotrellis SpaceTimeKey to contain measurement dimension.
 *
 */
case class SpaceTimeBandKey(_spaceTimeKey: SpaceTimeKey, _measurementName: mutable.ListBuffer[String]) extends Serializable {
  @BeanProperty
  var spaceTimeKey: SpaceTimeKey = _spaceTimeKey
  @BeanProperty
  var measurementName:  mutable.ListBuffer[String] = _measurementName

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceTimeBandKey =>
        this.spaceTimeKey.row == obj.spaceTimeKey.row &&
          this.spaceTimeKey.col == obj.spaceTimeKey.col &&
          this.spaceTimeKey.instant == obj.spaceTimeKey.instant &&
          this.measurementName.equals(obj.measurementName)
      case _ => false
    }
  }
}
