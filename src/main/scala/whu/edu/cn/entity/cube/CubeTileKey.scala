package whu.edu.cn.entity.cube

import geotrellis.layer.SpaceTimeKey


class CubeTileKey(_spaceKey: SpaceKey, _timeKey: TimeKey, _productKey: ProductKey, _bandKey: BandKey) extends Serializable {
  var spaceKey: SpaceKey = _spaceKey
  var timeKey: TimeKey = _timeKey
  var productKey: ProductKey = _productKey
  var bandKey: BandKey = _bandKey

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: CubeTileKey =>
        this.spaceKey.equals(obj.spaceKey) &&
          this.timeKey.equals(obj.timeKey) &&
          this.productKey.equals(obj.productKey) &&
          this.bandKey.equals(obj.bandKey)
      case _ => false
    }
  }
}
