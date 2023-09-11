package whu.edu.cn.geocube.core.entity

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

class GcDimension extends Serializable {
  @BeanProperty
  var id: Int = 0
  @BeanProperty
  var dimensionName: String = ""
  @BeanProperty
  var dimensionType: String = ""
  @BeanProperty
  var dimensionTableName: String = ""
  @BeanProperty
  var memberType: String = ""
  @BeanProperty
  var step: Double = 0.0
  @BeanProperty
  var description: String = ""
  @BeanProperty
  var unit: String = ""
  @BeanProperty
  var dimensionTableColumnName: String = ""
  @BeanProperty
  var coordinates: ArrayBuffer[String] = new ArrayBuffer[String]()
  @BeanProperty
  var value: Object = ""

  override def equals(other: Any): Boolean = other match {
    case that: GcDimension =>
      this.id == that.id &&
        this.dimensionName == that.dimensionName &&
        this.dimensionType == that.dimensionType &&
        this.dimensionTableName == that.dimensionTableName &&
        this.memberType == that.memberType &&
        this.step == that.step &&
        this.description == that.description &&
        this.unit == that.unit &&
        this.dimensionTableColumnName == that.dimensionTableColumnName &&
        this.coordinates == that.coordinates &&
        this.value == that.value
    case _ => false
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + id
    result = prime * result + dimensionName.hashCode
    result = prime * result + dimensionType.hashCode
    result = prime * result + dimensionTableName.hashCode
    result = prime * result + memberType.hashCode
    result = prime * result + step.hashCode
    result = prime * result + description.hashCode
    result = prime * result + unit.hashCode
    result = prime * result + dimensionTableColumnName.hashCode
    result = prime * result + coordinates.hashCode
    result = prime * result + value.hashCode
    result
  }

//  def setId(id: Int): Unit = {
//    this.id = id
//  }
//
//  def setDimensionName(dimensionName: String): Unit = {
//    this.dimensionName = dimensionName
//  }
//
//  def setDimensionType(dimensionType: String): Unit = {
//    this.dimensionType = dimensionType
//  }
//
//  def setDimensionTableName(dimensionTableName: String): Unit = {
//    this.dimensionTableName = dimensionTableName
//  }
//
//  def setStep(step: Double): Unit = {
//    this.step = step
//  }
//
//  def setDescription(description: String): Unit = {
//    this.description = description
//  }
//
//  def setUnit(unit: String): Unit = {
//    this.unit = unit
//  }
//
//  def setDimensionTableColumnName(dimensionTableColumnName: String): Unit = {
//    this.dimensionTableColumnName = dimensionTableColumnName
//  }
//
//  def setMemberType(memberType: String): Unit = {
//    this.memberType = memberType
//  }
//
//  def setValue(value: Object): Unit = {
//    this.value = value
//  }
//
//  def setCoordinates(coordinates: ArrayBuffer[String]): Unit = {
//    this.coordinates = coordinates
//  }
//
//  def getId: Int = {
//    this.id
//  }
//
//  def getDimensionName: String = {
//    this.dimensionName
//  }
//
//  def getDimensionType: String = {
//    this.dimensionType
//  }
//
//  def getDimensionTableName: String = {
//    this.dimensionTableName
//  }
//
//  def getMemberType: String = {
//    this.memberType
//  }
//
//  def getStep: Double = {
//    this.step
//  }
//
//  def getDescription: String = {
//    this.description
//  }
//
//  def getUnit: String = {
//    this.unit
//  }
//
//  def getDimensionTableColumnName: String = {
//    this.dimensionTableColumnName
//  }
//
//  def getValue: Object = {
//    this.value
//  }
//
//  def getCoordinates: ArrayBuffer[String] = {
//    this.coordinates
//  }

  //  def convertDataType: Unit = {
  //    if (this.value != null) {
  //      if (this.value.isInstanceOf[String]) {
  //        if (this.memberType.contains("int")) {
  //          this.value = this.value.toString.toInt
  //        } else if (this.memberType.contains("float")) {
  //          this.value = this.value.toString.toFloat
  //        } else if (this.memberType.contains("double")) {
  //          this.value = this.value.toString.toDouble
  //        }
  //      }
  //    }
  //  }
  def convertDataType: Unit = {
    if (this.value != null && this.value.isInstanceOf[String]) {
      val stringValue = this.value.asInstanceOf[String]
      val convertedValue = this.memberType match {
        case t if t.contains("int")    => stringValue.toInt
//        case t if t.contains("float")  => stringValue.toFloat
//        case t if t.contains("double") => stringValue.toDouble
        case t if t.contains("float")  => BigDecimal(stringValue).toFloat
        case t if t.contains("double") => BigDecimal(stringValue).toDouble
        case _                         => stringValue // 默认情况下，保持原始值不变
      }
      this.value = convertedValue.asInstanceOf[Object]
    }
  }

}
