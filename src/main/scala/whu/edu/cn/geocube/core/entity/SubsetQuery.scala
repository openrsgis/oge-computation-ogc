package whu.edu.cn.geocube.core.entity

import com.fasterxml.jackson.databind.annotation.JsonSerialize

class SubsetQuery extends Serializable {

  @JsonSerialize
  var axisName: Option[String] = None
  @JsonSerialize
  var interval: Option[Boolean] = None
  @JsonSerialize
  var lowPoint: Option[Object] = None
  @JsonSerialize
  var highPoint: Option[Object] = None
  @JsonSerialize
  var point: Option[Object] = None
  @JsonSerialize
  var isNumber: Option[Boolean] = None

  def this(axisName: Option[String], interval: Option[Boolean], lowPoint: Option[Object],
           highPoint: Option[Object], point: Option[Object], isNumber: Option[Boolean]) {
    this()
    this.axisName = axisName
    this.interval = interval
    this.lowPoint = lowPoint
    this.highPoint = highPoint
    this.point = point
    this.isNumber = isNumber
  }

  def setAxisName(axisName: String): Unit = {
    this.axisName = Some(axisName)
  }

  def setInterval(interval: Boolean): Unit = {
    this.interval = Some(interval)
  }

  def setLowerPoint(lowPoint: Object): Unit = {
    this.lowPoint = Some(lowPoint)
  }

  def setHighPoint(highPoint: Object): Unit = {
    this.highPoint = Some(highPoint)
  }

  def setPoint(point: Object): Unit = {
    this.point = Some(point)
  }

  def setIsNumber(isNumber: Boolean): Unit = {
    this.isNumber = Some(isNumber)
  }

  def getAxisName: Option[String] = {
    this.axisName
  }

  def getInterval: Option[Boolean] = {
    this.interval
  }

  def getLowPoint: Option[Object] = {
    this.lowPoint
  }

  def getHighPoint: Option[Object] = {
    this.highPoint
  }

  def getPoint: Option[Object] = {
    this.point
  }

  def getIsNumber: Option[Boolean] = {
    this.isNumber;
  }


}
