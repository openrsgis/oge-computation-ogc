package whu.edu.cn.entity

object OGEDataType extends Enumeration with Serializable {
  type OGEDataType = Value

  // 定义一个包含优先级的类
  case class OGEDataValue(priority: Int) extends super.Val

  // 创建枚举成员，并为每个成员设置优先级
  val int8raw: OGEDataValue = OGEDataValue(1)
  val uint8raw: OGEDataValue = OGEDataValue(1)
  val int8: OGEDataValue = OGEDataValue(1)
  val uint8: OGEDataValue = OGEDataValue(1)
  val int16raw: OGEDataValue = OGEDataValue(1)
  val uint16raw: OGEDataValue = OGEDataValue(1)
  val int16: OGEDataValue = OGEDataValue(1)
  val uint16: OGEDataValue = OGEDataValue(1)
  val int32raw: OGEDataValue = OGEDataValue(1)
  val int32: OGEDataValue = OGEDataValue(1)
  val float32raw: OGEDataValue = OGEDataValue(2)
  val float32: OGEDataValue = OGEDataValue(2)
  val float64raw: OGEDataValue = OGEDataValue(3)
  val float64: OGEDataValue = OGEDataValue(3)

  // 定义一个比较函数来比较两个成员的优先级，并根据优先级返回相应的数据类型
  def compareAndGetType(member1: OGEDataType, member2: OGEDataType): OGEDataType = {
    val priority1: Int = member1.asInstanceOf[OGEDataValue].priority
    val priority2: Int = member2.asInstanceOf[OGEDataValue].priority

    if (priority1 == 1 && priority2 == 1) int32raw
    else if (priority1 <= 2 && priority2 <= 2) float32raw
    else float64raw
  }
}
