package whu.edu.cn.geocube.core.entity

import geotrellis.layer.SpaceTimeKey

//import java.beans.BeanProperty
import scala.beans.BeanProperty

/**
 * Extend Geotrellis SpaceTimeKey to contain measurement dimension.
 *
 */
case class SpaceTimeBandKey (_spaceTimeKey: SpaceTimeKey, _measurementName: String, _additionalDimensions: Array[GcDimension] = null){
  @BeanProperty
  var spaceTimeKey = _spaceTimeKey
  @BeanProperty
  var measurementName = _measurementName
  @BeanProperty
  var additionalDimensions = _additionalDimensions
}
