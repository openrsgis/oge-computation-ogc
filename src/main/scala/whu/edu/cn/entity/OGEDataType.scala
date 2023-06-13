package whu.edu.cn.entity

object OGEDataType extends Enumeration with Serializable {
  type OGEDataType = Value
  val int8raw, uint8raw, int8, uint8, int16raw, uint16raw, int16, uint16, int32raw, int32, float32raw, float32, float64raw, float64 = Value
}
