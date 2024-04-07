package whu.edu.cn.algorithms.terrain.core

import geotrellis.layer.SpaceTimeKey

case class SpaceTimeBandKey(spaceTimeKey: SpaceTimeKey, measurementName: String) {

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: SpaceTimeBandKey =>
        this.spaceTimeKey.row == obj.spaceTimeKey.row &&
        this.spaceTimeKey.col == obj.spaceTimeKey.col &&
        this.spaceTimeKey.instant == obj.spaceTimeKey.instant &&
        this.measurementName.equals(obj.measurementName)
      case _ => false
    }
  }

}
