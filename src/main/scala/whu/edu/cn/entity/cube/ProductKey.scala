package whu.edu.cn.entity.cube

class ProductKey(_productName: String, _productType: String) extends Serializable {
  var productName: String = _productName
  var productType: String = _productType

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: ProductKey =>
        this.productName.equals(obj.productName) &&
          this.productType.equals(obj.productType)
      case _ => false
    }
  }
}
