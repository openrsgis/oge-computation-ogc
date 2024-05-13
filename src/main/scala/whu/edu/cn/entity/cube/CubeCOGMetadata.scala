package whu.edu.cn.entity.cube

class CubeCOGMetadata extends Serializable {
  var imageWidth: Int = _
  var imageHeight: Int = _
  var bitPerSample: Int = _
  var compression: Int = _
  var bandCount: Int = _
  var tileWidth: Int = _
  var tileHeight: Int = _
  var tileOffsets: Array[Array[Int]] = Array.ofDim[Int](0, 0)
  var tileByteCounts: Array[Array[Int]] = Array.ofDim[Int](0, 0)
  var sampleFormat: Int = _
  var cellScale: Array[Double] = Array[Double]()
  var geoTransform: Array[Double] = Array[Double]()
  var crs = ""
  var gdalMetadata = ""
  var gdalNodata = ""

  def getImageWidth: Int = {
    this.imageWidth
  }

  def setImageWidth(imageWidth: Int): Unit = {
    this.imageWidth = imageWidth
  }

  def getImageHeight: Int = {
    this.imageHeight
  }

  def setImageHeight(imageHeight: Int): Unit = {
    this.imageHeight = imageHeight
  }

  def getBitPerSample: Int = {
    this.bitPerSample
  }

  def setBitPerSample(bitPerSample: Int): Unit = {
    this.bitPerSample = bitPerSample
  }

  def getCompression: Int = {
    this.compression
  }

  def setCompression(compression: Int): Unit = {
    this.compression = compression
  }

  def getBandCount: Int = {
    this.bandCount
  }

  def setBandCount(bandCount: Int): Unit = {
    this.bandCount = bandCount
  }

  def getTileWidth: Int = {
    this.tileWidth
  }

  def setTileWidth(tileWidth: Int): Unit = {
    this.tileWidth = tileWidth
  }

  def getTileHeight: Int = {
    this.tileHeight
  }

  def setTileHeight(tileHeight: Int): Unit = {
    this.tileHeight = tileHeight
  }

  def getTileOffsets: Array[Array[Int]] = {
    this.tileOffsets
  }

  def setTileOffsets(tileOffsets: Array[Array[Int]]): Unit = {
    this.tileOffsets = tileOffsets
  }

  def getTileByteCounts: Array[Array[Int]] = {
    this.tileByteCounts
  }

  def setTileByteCounts(tileByteCounts: Array[Array[Int]]): Unit = {
    this.tileByteCounts = tileByteCounts
  }

  def getSampleFormat: Int = {
    this.sampleFormat
  }

  def setSampleFormat(sampleFormat: Int): Unit = {
    this.sampleFormat = sampleFormat
  }

  def getCellScale: Array[Double] = {
    this.cellScale
  }

  def setCellScale(cellScale: Array[Double]): Unit = {
    this.cellScale = cellScale
  }

  def getGeoTransform: Array[Double] = {
    this.geoTransform
  }

  def setGeoTransform(geoTransform: Array[Double]): Unit = {
    this.geoTransform = geoTransform
  }

  def getCrs: String = {
    this.crs
  }

  def setCrs(crs: String): Unit = {
    this.crs = crs
  }

  def getGdalMetadata: String = {
    this.gdalMetadata
  }

  def setGdalMetadata(gdalMetadata: String): Unit = {
    this.gdalMetadata = gdalMetadata
  }

  def getGdalNodata: String = {
    this.gdalNodata
  }

  def setGdalNodata(gdalNodata: String): Unit = {
    this.gdalNodata = gdalNodata
  }

}
