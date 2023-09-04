package whu.edu.cn.geocube.core.entity

import scala.beans.BeanProperty

/**
 * A fact for vector data.
 */
class VectorGridFact extends Serializable {
  @BeanProperty
  var factId: Int = -1
  @BeanProperty
  var factKey: Int = -1
  @BeanProperty
  var productKey: Int = -1
  @BeanProperty
  var extentKey: Int = -1

  var uuids: String = ""

  def this(factId: Int, factKey: Int, productKey: Int, extentKey: Int, uuids: String){
    this()
    this.factId = factId
    this.factKey = factKey
    this.productKey = productKey
    this.extentKey = extentKey
    this.uuids = uuids
  }

  def getUUIDs: String = uuids
  def setUUIDs(_uuids: String):Unit  = uuids = _uuids
}
