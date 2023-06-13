package whu.edu.cn.geocube.core.cube.tabular

import java.io.Serializable

import scala.collection.mutable.ArrayBuffer

class TabularCollection extends Serializable {
  var tabularCollection: ArrayBuffer[Map[String, String]] = new ArrayBuffer[Map[String, String]]()
  def this(tabularCollection: ArrayBuffer[Map[String, String]]) {
    this()
    this.tabularCollection = tabularCollection
  }
}
