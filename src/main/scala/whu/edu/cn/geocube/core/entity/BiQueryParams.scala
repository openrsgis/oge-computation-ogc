package whu.edu.cn.geocube.core.entity

import scala.collection.mutable.ArrayBuffer

class BiQueryParams extends Serializable {
  var locationNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var genders: ArrayBuffer[String] = new ArrayBuffer[String]()
  var times: ArrayBuffer[String] = new ArrayBuffer[String]()
  var timeType: String = "discrete"

  def getLocationNames: ArrayBuffer[String] = this.locationNames
  def getGenders:  ArrayBuffer[String] = this.genders
  def getTimes: ArrayBuffer[String] = this.times
  def getTimeType: String = this.timeType

  def setLocationNames(locationNames: Array[String]):Unit = this.locationNames = locationNames.toBuffer.asInstanceOf[ArrayBuffer[String]]
  def setGenders(genders: Array[String]):Unit = this.genders = genders.toBuffer.asInstanceOf[ArrayBuffer[String]]
  def setTimes(times: Array[String]): Unit = this.times = times.toBuffer.asInstanceOf[ArrayBuffer[String]]
  def setTimeType(timeType: String): Unit = {
    if (timeType == "discrete" || timeType == "continuous") this.timeType = timeType
    else
      throw new RuntimeException("timeType must be discrete or continuous")
  }
}
