package whu.edu.cn.entity.cube

object OGECubeDataType extends Enumeration with Serializable {
  type OGECubeDataType = Value

  // 定义一个包含优先级的类
  case class OGECubeDataValue(priority: Int, bit: Int) extends super.Val

  // 创建枚举成员，并为每个成员设置优先级

  val uint8: OGECubeDataValue = OGECubeDataValue(1, 8)
  val uint16: OGECubeDataValue = OGECubeDataValue(2, 16)
  val uint32: OGECubeDataValue = OGECubeDataValue(3, 32)
  val uint64: OGECubeDataValue = OGECubeDataValue(4, 64)
  val int8: OGECubeDataValue = OGECubeDataValue(5, 8)
  val int16: OGECubeDataValue = OGECubeDataValue(6, 16)
  val int32: OGECubeDataValue = OGECubeDataValue(7, 32)
  val int64: OGECubeDataValue = OGECubeDataValue(8, 64)
  val float32: OGECubeDataValue = OGECubeDataValue(9, 32)
  val float64: OGECubeDataValue = OGECubeDataValue(10, 64)

  // 定义一个比较函数来比较两个成员的优先级，并根据优先级返回相应的数据类型
  def compareAndGetType(member1: OGECubeDataType, member2: OGECubeDataType): OGECubeDataType = {
    val priority1: Int = member1.asInstanceOf[OGECubeDataValue].priority
    val bit1: Int = member1.asInstanceOf[OGECubeDataValue].bit
    val priority2: Int = member2.asInstanceOf[OGECubeDataValue].priority
    val bit2: Int = member2.asInstanceOf[OGECubeDataValue].bit
    if (priority1 >= 9 || priority2 >= 9) {
      if (priority1 < priority2) {
        member2
      }
      else {
        member1
      }
    }
    else if (priority1 >= 5 && priority2 >= 5) {
      if (priority1 < priority2) {
        member2
      }
      else {
        member1
      }
    }
    else if (priority1 <= 4 && priority2 <= 4) {
      if (priority1 < priority2) {
        member2
      }
      else {
        member1
      }
    }
    else {
      if (priority1 < priority2) {
        if (bit1 <= bit2) {
          member2
        }
        else {
          // 找到在优先级是[5,8]中bit是bit1的，因为此时bit1比bit2大
          findMemberWithBit(OGECubeDataType.values.filter(m => m.asInstanceOf[OGECubeDataValue].priority >= 5 &&
            m.asInstanceOf[OGECubeDataValue].priority <= 8), bit1)
        }
      }
      else {
        if (bit1 >= bit2) {
          member1
        }
        else {
          // 找到在优先级是[5,8]中bit是bit2的，因为此时bit2比bit1大
          findMemberWithBit(OGECubeDataType.values.filter(m => m.asInstanceOf[OGECubeDataValue].priority >= 5 &&
            m.asInstanceOf[OGECubeDataValue].priority <= 8), bit2)
        }
      }
    }
  }

  // 辅助函数，根据 bit 查找符合条件的成员
  def findMemberWithBit(members: Iterable[OGECubeDataType], bit: Int): OGECubeDataType = {
    members.find(m => m.asInstanceOf[OGECubeDataValue].bit == bit).getOrElse(uint8)
  }
}

