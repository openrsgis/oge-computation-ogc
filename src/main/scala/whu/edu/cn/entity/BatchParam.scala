package whu.edu.cn.entity

import geotrellis.proj4.CRS

class BatchParam {
  var userId: String = _
  var dagId: String = _
  var crs: CRS = _
  var scale: Double = _
  var folder: String = _
  var fileName: String = _
  var format: String = _

  def getUserId: String = {
    this.userId
  }

  def setUserId(userId: String): Unit = {
    this.userId = userId
  }

  def getDagId: String = {
    this.dagId
  }

  def setDagId(dagId: String): Unit = {
    this.dagId = dagId
  }

  def getCrs: CRS = {
    this.crs
  }

  def setCrs(crs: String): Unit = {
    val crsDefinition: CRS = CRS.fromName(crs)
    this.crs = crsDefinition
  }

  def getScale: Double = {
    this.scale
  }

  def setScale(scale: String): Unit = {
    this.scale = string2Double(scale)
  }

  def getFolder: String = {
    this.folder
  }

  def setFolder(folder: String): Unit = {
    this.folder = folder
  }

  def getFileName: String = {
    this.fileName
  }

  def setFileName(fileName: String): Unit = {
    this.fileName = fileName
  }

  def getFormat: String = {
    this.format
  }

  def setFormat(format: String): Unit = {
    this.format = format
  }


  def string2Double(string: String): Double = {
    try {
      string.toDouble
    }
    catch {
      case _: Exception =>
        throw new IllegalArgumentException("无法将String转为Double")
    }
  }

}
