package whu.edu.cn.geocube.core.cube.tabular

import scala.beans.BeanProperty

class TabularRecord(_id: String,
                    _attributes: Map[String, String]) extends Serializable {
  @BeanProperty
  var id: String = _id
  @BeanProperty
  var attributes: Map[String, String] = _attributes
}
