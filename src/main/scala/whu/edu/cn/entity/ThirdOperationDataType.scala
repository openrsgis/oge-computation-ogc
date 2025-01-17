package whu.edu.cn.entity

/**
 * 第三方算子处理数据类型
 */
object ThirdOperationDataType extends Enumeration {
  type ThirdOperationDataType = Value
  val SHP, GEOJSON, GEOPKG, TIF, RDD, STRING, INT, DOUBLE, JSONOBJECT, OTHER = Value

  // 大小写不敏感的withName方法
  def withNameInsensitive(name: String): ThirdOperationDataType = {
    values.find(_.toString.equalsIgnoreCase(name)).getOrElse(throw new NoSuchElementException)
  }
}
