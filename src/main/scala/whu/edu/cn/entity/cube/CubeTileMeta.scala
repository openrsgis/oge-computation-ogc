package whu.edu.cn.entity.cube

class CubeTileMeta(_cubeTileKey: CubeTileKey, _tilePath: String, _tileOffset: Long, _tileByteCount: Long, _dataType: String, _tileCompression: Int) extends Serializable {
  var cubeTileKey: CubeTileKey = _cubeTileKey
  var tilePath: String = _tilePath
  var tileOffset: Long = _tileOffset
  var tileByteCount: Long = _tileByteCount
  var dataType: String = _dataType
  var tileCompression: Int = _tileCompression

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: CubeTileMeta =>
        this.cubeTileKey.equals(obj.cubeTileKey) &&
          this.tilePath.equals(obj.tilePath) &&
          this.tileOffset == obj.tileOffset &&
          this.tileByteCount == obj.tileByteCount &&
          this.dataType.equals(obj.dataType) &&
          this.tileCompression == obj.tileCompression
      case _ => false
    }
  }
}
