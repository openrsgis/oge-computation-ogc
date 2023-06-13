package whu.edu.cn.geocube.view

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Primary info of product for generating a html page.
 *
 * @param _path
 * @param _instant
 * @param _header
 */
case class Info(_path:String, _instant: Long, _header: String) {
  def this(_path:String, _instant: Long, _header: String, _value:Double){
    this(_path, _instant, _header)
    this.value = _value
  }

  var path = _path
  var instant = _instant
  var header = _header
  var value:Double = 0.0

  def getValue() = value
  def setValue(value: Double) = {
    this.value = value
  }

  def getPath() = path
  def setPath(path: String) = {
    this.path = path
  }
  def getInstant():String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date(instant)
    sdf.format(date)
  }
  def setInstant(instant: Long) = {
    this.instant = instant
  }
  def getHeader() = header
  def setHeader(header: String) = {
    this.header = header
  }
}
