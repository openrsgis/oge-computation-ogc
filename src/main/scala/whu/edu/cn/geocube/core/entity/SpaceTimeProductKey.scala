package whu.edu.cn.geocube.core.entity

import geotrellis.layer.SpaceTimeKey

import scala.beans.BeanProperty

case class SpaceTimeProductKey(_spaceTimeKey: SpaceTimeKey, _productName: String){
  @BeanProperty
  var spaceTimeKey = _spaceTimeKey
  @BeanProperty
  var productName = _productName

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceTimeBandProductKey =>
        this.spaceTimeKey.row == obj.spaceTimeKey.row &&
          this.spaceTimeKey.col == obj.spaceTimeKey.col &&
          this.spaceTimeKey.instant == obj.spaceTimeKey.instant &&
          this.productName.equals(obj.productName)
      case _ => false
    }
  }
}
