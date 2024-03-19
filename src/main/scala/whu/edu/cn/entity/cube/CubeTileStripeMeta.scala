package whu.edu.cn.entity.cube

class CubeTileStripeMeta(_cubeTileKey: CubeTileKey, _tilePath: String, _stripe: Int, _tileStripeOffset: Long, _tileByteCount: Long, _dataType: String, _tileCompression: Int) extends Serializable {
  var cubeTileKey: CubeTileKey = _cubeTileKey
  var tilePath: String = _tilePath
  var stripe: Int = _stripe
  var tileStripeOffset: Long = _tileStripeOffset
  var tileByteCount: Long = _tileByteCount
  var dataType: String = _dataType
  var tileCompression: Int = _tileCompression

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: CubeTileStripeMeta =>
        this.cubeTileKey.equals(obj.cubeTileKey) &&
          this.tilePath.equals(obj.tilePath) &&
          this.stripe == obj.stripe &&
          this.tileStripeOffset == obj.tileStripeOffset &&
          this.tileByteCount == obj.tileByteCount &&
          this.dataType.equals(obj.dataType) &&
          this.tileCompression == obj.tileCompression
      case _ => false
    }
  }
}
