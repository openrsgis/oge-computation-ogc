package whu.edu.cn.entity

/**
 * 第三方算子处理数据类型
 */
object ThirdOperationDataType extends Enumeration {
  type ThirdOperationDataType = Value
  val SHP, GEOJSON, GEOPKG, TIF, RDD, OTHER = Value
}
