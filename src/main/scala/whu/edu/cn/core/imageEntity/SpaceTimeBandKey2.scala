package whu.edu.cn.core.imageEntity

import geotrellis.layer.SpaceTimeKey

import scala.beans.BeanProperty

/**
 * Extend Geotrellis SpaceTimeKey to contain measurement dimension.
 *
 */
case class SpaceTimeBandKey2(_spaceTimeKey: SpaceTimeKey, _measurementName: String){
  @BeanProperty
  var spaceTimeKey = _spaceTimeKey
  @BeanProperty
  var measurementName = _measurementName
}
