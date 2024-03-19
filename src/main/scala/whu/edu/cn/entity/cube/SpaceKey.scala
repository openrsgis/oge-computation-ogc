package whu.edu.cn.entity.cube

class SpaceKey(_tms: String, _col: Int, _row: Int, _minX: Double, _maxX: Double, _minY: Double, _maxY: Double) extends Serializable {
  var tms: String = _tms
  var col: Int = _col
  var row: Int = _row
  var minX: Double = _minX
  var maxX: Double = _maxX
  var minY: Double = _minY
  var maxY: Double = _maxY

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceKey =>
        this.tms == obj.tms &&
          this.col == obj.col &&
          this.row == obj.row
      case _ => false
    }
  }
}
