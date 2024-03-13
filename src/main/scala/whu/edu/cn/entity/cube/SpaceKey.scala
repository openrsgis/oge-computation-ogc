package whu.edu.cn.entity.cube

class SpaceKey(_row: Int, _col: Int, _minX: Double, _maxX: Double, _minY: Double, _maxY: Double) extends Serializable {
  var row: Int = _row
  var col: Int = _col
  var minX: Double = _minX
  var maxX: Double = _maxX
  var minY: Double = _minY
  var maxY: Double = _maxY

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceKey =>
        this.row == obj.row &&
          this.col == obj.col
      case _ => false
    }
  }
}
