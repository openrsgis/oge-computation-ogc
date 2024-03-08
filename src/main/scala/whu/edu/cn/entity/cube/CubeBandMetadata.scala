package whu.edu.cn.entity.cube

class CubeBandMetadata {
  var bandName: String = _
  var bandUnit: String = _
  var bandMin: Double = _
  var bandMax: Double = _
  var bandScale: Double = _
  var bandOffset: Double = _
  var bandDescription: String = _
  var bandResolution: Double = _
  var bandPlatform: String = _

  def getBandName: String = {
    this.bandName
  }

  def setBandName(bandName: String): Unit = {
    this.bandName = bandName
  }

  def getBandUnit: String = {
    this.bandUnit
  }

  def setBandUnit(bandUnit: String): Unit = {
    this.bandUnit = bandUnit
  }

  def getBandMin: Double = {
    this.bandMin
  }

  def setBandMin(bandMin: Double): Unit = {
    this.bandMin = bandMin
  }

  def getBandMax: Double = {
    this.bandMax
  }

  def setBandMax(bandMax: Double): Unit = {
    this.bandMax = bandMax
  }

  def getBandScale: Double = {
    this.bandScale
  }

  def setBandScale(bandScale: Double): Unit = {
    this.bandScale = bandScale
  }

  def getBandOffset: Double = {
    this.bandOffset
  }

  def setBandOffset(bandOffset: Double): Unit = {
    this.bandOffset = bandOffset
  }

  def getBandDescription: String = {
    this.bandDescription
  }

  def setBandDescription(bandDescription: String): Unit = {
    this.bandDescription = bandDescription
  }

  def getBandResolution: Double = {
    this.bandResolution
  }

  def setBandResolution(bandResolution: Double): Unit = {
    this.bandResolution = bandResolution
  }

  def getBandPlatform: String = {
    this.bandPlatform
  }

  def setBandPlatform(bandPlatform: String): Unit = {
    this.bandPlatform = bandPlatform
  }

  override def toString: String = {
    "CubeBandMetadata{" + "bandName='" + bandName + '\'' + ", bandUnit='" + bandUnit + '\'' + ", bandMin=" + bandMin + ", bandMax=" + bandMax + ", bandScale=" + bandScale + ", bandOffset=" + bandOffset + ", bandDescription='" + bandDescription + '\'' + ", bandResolution=" + bandResolution + ", bandPlatform='" + bandPlatform + '\'' + '}'
  }

  def this(bandName: String, bandUnit: String, bandMin: Double, bandMax: Double, bandScale: Double, bandOffset: Double, bandDescription: String, bandResolution: Double, bandPlatform: String) {
    this()
    this.bandName = bandName
    this.bandUnit = bandUnit
    this.bandMin = bandMin
    this.bandMax = bandMax
    this.bandScale = bandScale
    this.bandOffset = bandOffset
    this.bandDescription = bandDescription
    this.bandResolution = bandResolution
    this.bandPlatform = bandPlatform
  }

}
