package whu.edu.cn.entity

// TODO lrx: 每个参数设置用户乱输入的Exception，如：min输入字符串
class VisualizationParam {
  var bands: List[String] = List.empty[String]
  var min: List[Double] = List.empty[Double]
  var max: List[Double] = List.empty[Double]
  var gain: List[Double] = List.empty[Double]
  var bias: List[Double] = List.empty[Double]
  var gamma: List[Double] = List.empty[Double]
  var palette: List[String] = List.empty[String]
  var opacity: Double = 1.0
  var format: String = "png"

  def getFirstThreeBands: List[String] = {
    this.bands.take(3)
  }

  def getBands: List[String] = {
    this.bands
  }

  def setBands(bands: String): Unit = {
    if (bands.head == '[' && bands.last == ']') {
      this.bands ++= string2Array(bands)
    }
    else {
      this.bands :+= bands
    }
  }

  def getMin: List[Double] = {
    this.min
  }

  def setMin(min: String): Unit = {
    if (min.head == '[' && min.last == ']') {
      this.min ++= string2DoubleArray(min)
    }
    else {
      this.min :+= string2Double(min)
    }
  }

  def getMax: List[Double] = {
    this.max
  }

  def setMax(max: String): Unit = {
    if (max.head == '[' && max.last == ']') {
      this.max ++= string2DoubleArray(max)
    }
    else {
      this.max :+= string2Double(max)
    }

  }

  def getGain: List[Double] = {
    this.gain
  }

  def setGain(gain: String): Unit = {
    if (gain.head == '[' && gain.last == ']') {
      this.gain ++= string2DoubleArray(gain)
    }
    else {
      this.gain :+= string2Double(gain)
    }
  }

  def getBias: List[Double] = {
    this.bias
  }

  def setBias(bias: String): Unit = {
    if (bias.head == '[' && bias.last == ']') {
      this.bias ++= string2DoubleArray(bias)
    }
    else {
      this.bias :+= string2Double(bias)
    }
  }

  def getGamma: List[Double] = {
    this.gamma
  }

  def setGamma(gamma: String): Unit = {
    if (gamma.head == '[' && gamma.last == ']') {
      this.gamma ++= string2DoubleArray(gamma)
    }
    else {
      this.gamma :+= string2Double(gamma)
    }
  }

  def getPalette: List[String] = {
    this.palette
  }

  def setPalette(palette: String): Unit = {
    if (palette.head == '[' && palette.last == ']') {
      this.palette ++= string2Array(palette)
    }
    else {
      this.palette :+= palette
    }
  }

  def getOpacity: Double = {
    this.opacity
  }

  def setOpacity(opacity: String): Unit = {
    this.opacity = string2Double(opacity)
    if (this.opacity > 1.0 || this.opacity < 0.0) {
      throw new IllegalArgumentException(s"输入的opcity必须是0-1之间的Double，您输入了$opacity")
    }
  }

  def getFormat: String = {
    this.format
  }

  def setFormat(format: String): Unit = {
    if (format != "png" || format != "jpg") {
      throw new IllegalArgumentException("不合格的参数：format")
    }
    else {
      this.format = format
    }
  }

  def string2Array(string: String): Array[String] = {
    try {
      string.substring(1, string.length - 1).split(",")
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException("无法转换字符串为字符串数组")
    }
  }

  def string2DoubleArray(string: String): Array[Double] = {
    try {
      string.substring(1, string.length - 1).split(",").map(t => t.toDouble)
    }
    catch {
      case _: Exception =>
        throw new IllegalArgumentException("无法转换字符串为Double数组")
    }
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

  def setAllParam(bands: String = null, gain: String = null, bias: String = null, min: String = null, max: String = null, gamma: String = null, opacity: String = null, palette: String = null, format: String = null): Unit = {
    if (bands != null) {
      this.setBands(bands)
    }
    if (gain != null) {
      this.setGain(gain)
    }
    if (bias != null) {
      this.setBias(bias)
    }
    if (min != null) {
      this.setMin(min)
    }
    if (max != null) {
      this.setMax(max)
    }
    if (gamma != null) {
      this.setGamma(gamma)
    }
    if (opacity != null) {
      this.setOpacity(opacity)
    }
    if (palette != null) {
      this.setPalette(palette)
    }
    if (format != null) {
      this.setFormat(format)
    }
  }
}
