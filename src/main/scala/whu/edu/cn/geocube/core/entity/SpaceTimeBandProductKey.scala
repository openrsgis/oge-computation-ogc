package whu.edu.cn.geocube.core.entity

import geotrellis.layer.SpaceTimeKey

import scala.beans.BeanProperty

case class SpaceTimeBandProductKey (_spaceTimeKey: SpaceTimeKey, _measurementName: String, _productName: String) {
  @BeanProperty
  var spaceTimeKey = _spaceTimeKey
  @BeanProperty
  var measurementName = _measurementName
  @BeanProperty
  var productName = _productName

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceTimeBandProductKey =>
        this.spaceTimeKey.row == obj.spaceTimeKey.row &&
          this.spaceTimeKey.col == obj.spaceTimeKey.col &&
          this.spaceTimeKey.instant == obj.spaceTimeKey.instant &&
          this.measurementName.equals(obj.measurementName) &&
          this.productName.equals(obj.productName)
      case _ => false
    }
  }
}
