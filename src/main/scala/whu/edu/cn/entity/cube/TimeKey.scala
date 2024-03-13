package whu.edu.cn.entity.cube

class TimeKey(_time: Long) extends Serializable {
  var time: Long = _time

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: TimeKey =>
        this.time == obj.time
      case _ => false
    }
  }
}
