package whu.edu.cn.entity

import geotrellis.proj4.CRS
import geotrellis.vector.Extent

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable
import scala.util.control.Breaks

// TODO lrx: 用户输入不正确时添加错误检查机制
class CoverageCollectionMetadata extends Serializable {
  var productName: String = _
  var sensorName: String = _
  var measurementName: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
  var startTime: LocalDateTime = _
  var endTime: LocalDateTime = _
  var extent: Extent = _
  var crs: CRS = _
  var cloudCoverMin: Float = 0
  var cloudCoverMax: Float = 100

  def setCloudCoverMin(cloudLevel: Float): Unit ={
    cloudCoverMin = cloudLevel
  }

  def setCloudCoverMax(cloudLevel: Float): Unit = {
    cloudCoverMax = cloudLevel
  }

  def getCloudCoverMin(): Float = {
    cloudCoverMin
  }

  def getCloudCoverMax(): Float ={
    cloudCoverMax
  }

  def getProductName: String = {
    this.productName
  }

  def setProductName(productName: String): Unit = {
    this.productName = productName
  }

  def getSensorName: String = {
    this.sensorName
  }

  def setSensorName(sensorName: String): Unit = {
    this.sensorName = sensorName
  }

  def getMeasurementName: mutable.ArrayBuffer[String] = {
    this.measurementName
  }

  def setMeasurementName(measurementName: String): Unit = {
    val measurement: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    if (measurementName.head == '[' && measurementName.last == ']') {
      measurement ++= measurementName.replace("[", "").replace("]", "").split(",")
    }
    else {
      measurement.append(measurementName)
    }
    if (this.measurementName.nonEmpty) {
      this.measurementName.clear()
    }
    this.measurementName = measurement
  }

  def getStartTime: LocalDateTime = {
    this.startTime
  }

  def setStartTime(startTime: String): Unit = {

    val formatPatterns = new mutable.ArrayBuffer[String] // 日期格式模式列表
    formatPatterns.append("yyyy-MM-dd HH:mm:ss")
    formatPatterns.append("yyyy/MM/dd HH:mm:ss")
    formatPatterns.append("yyyy-MM-dd")
    formatPatterns.append("yyyy/MM/dd")

    var localDateTime: LocalDateTime = null
    var localDate: LocalDate = null
    val loop = new Breaks
    loop.breakable {
      for (pattern <- formatPatterns) {
        try {
          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
          localDateTime = LocalDateTime.parse(startTime, formatter)
          loop.break()
          // 匹配成功，退出循环
        } catch {
          case _: DateTimeParseException =>
            try {
              val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
              localDate = LocalDate.parse(startTime, formatter)
              loop.break()
              // 匹配成功，退出循环
            } catch {
              case _: DateTimeParseException =>
              // 匹配失败，继续尝试下一个模式
            }
        }
      }
    }

    if (localDateTime != null) {
      this.startTime = localDateTime
      println(localDateTime)
    }
    else if (localDate != null) {
      this.startTime = localDate.atStartOfDay()
      println(localDate.atStartOfDay)
    }
    else {
      System.out.println("无法识别的日期格式")
    }

  }

  def getEndTime: LocalDateTime = {
    this.endTime
  }

  def setEndTime(endTime: String): Unit = {

    val formatPatterns = new mutable.ArrayBuffer[String] // 日期格式模式列表
    formatPatterns.append("yyyy-MM-dd HH:mm:ss")
    formatPatterns.append("yyyy/MM/dd HH:mm:ss")
    formatPatterns.append("yyyy-MM-dd")
    formatPatterns.append("yyyy/MM/dd")

    var localDateTime: LocalDateTime = null
    var localDate: LocalDate = null
    val loop = new Breaks
    loop.breakable {
      for (pattern <- formatPatterns) {
        try {
          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
          localDateTime = LocalDateTime.parse(endTime, formatter)
          loop.break()
          // 匹配成功，退出循环
        } catch {
          case _: DateTimeParseException =>
            try {
              val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
              localDate = LocalDate.parse(endTime, formatter)
              loop.break()
              // 匹配成功，退出循环
            } catch {
              case _: DateTimeParseException =>
              // 匹配失败，继续尝试下一个模式
            }
        }
      }
    }

    if (localDateTime != null) {
      this.endTime = localDateTime
      println(localDateTime)
    }
    else if (localDate != null) {
      this.endTime = localDate.atStartOfDay()
      println(localDate.atStartOfDay)
    }
    else {
      System.out.println("无法识别的日期格式")
    }
  }

  def getExtent: Extent = {
    this.extent
  }

  def setExtent(extent: String): Unit = {
    val extentArray: Array[String] = extent.replace("[", "").replace("]", "").split(",")
    val extentDefinition = new Extent(extentArray.head.toDouble, extentArray(1).toDouble, extentArray(2).toDouble, extentArray(3).toDouble)
    this.extent = extentDefinition
  }

  def getCrs: CRS = {
    this.crs
  }

  def setCrs(crs: String): Unit = {
    val crsDefinition: CRS = CRS.fromName(crs)
    this.crs = crsDefinition
  }

}
