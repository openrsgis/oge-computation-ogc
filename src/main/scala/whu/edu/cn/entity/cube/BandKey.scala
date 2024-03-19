package whu.edu.cn.entity.cube

import scala.beans.BeanProperty

class BandKey(_bandName: String, _bandPlatform: String) extends Serializable {
  var bandName: String = _bandName
  var bandPlatform: String = _bandPlatform

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: BandKey =>
        this.bandName.equals(obj.bandName) &&
          this.bandPlatform.equals(obj.bandPlatform)
      case _ => false
    }
  }
}
