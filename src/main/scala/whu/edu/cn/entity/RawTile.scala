package whu.edu.cn.entity

import geotrellis.layer.SpatialKey
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import whu.edu.cn.entity.OGEDataType.OGEDataType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class RawTile extends Serializable {
  var path: String = _
  var time: LocalDateTime = _
  var measurement: String = _
  var measurementRank: Int = _
  var product: String = _
  var coverageId: String = _
  var extent: Extent = _
  var spatialKey: SpatialKey = _
  var offset: Long = _
  var byteCount: Long = _
  var tileBuf: Array[Byte] = Array.empty[Byte]
  var rotation: Double = _
  var resolutionCol: Double = _
  var resolutionRow: Double = _
  var crs: CRS = _
  var dataType: OGEDataType = _
  var tile: Tile = _

  def getPath: String = {
    this.path
  }

  def setPath(path: String): Unit = {
    this.path = path
  }

  def getTime: LocalDateTime = {
    this.time
  }

  def setTime(time: String): Unit = {
    val formatPatterns = new mutable.ArrayBuffer[String]
    formatPatterns.append("yyyy-MM-dd HH:mm:ss")

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val localDateTime: LocalDateTime = LocalDateTime.parse(time, formatter)

    if (localDateTime != null) {
      this.time = localDateTime
      println(localDateTime)
    }
    else {
      System.out.println("无法识别的日期格式")
    }
  }

  def setTime(time: LocalDateTime): Unit = {
    this.time = time
  }

  def getMeasurement: String = {
    this.measurement
  }

  def setMeasurement(measurement: String): Unit = {
    this.measurement = measurement
  }

  def getMeasurementRank: Int = {
    this.measurementRank
  }

  def setMeasurementRank(measurementRank: Int): Unit = {
    this.measurementRank = measurementRank
  }

  def getProduct: String = {
    this.product
  }

  def setProduct(product: String): Unit = {
    this.product = product
  }

  def getCoverageId: String = {
    this.coverageId
  }

  def setCoverageId(coverageId: String): Unit = {
    this.coverageId = coverageId
  }

  def getExtent: Extent = {
    this.extent
  }

  def setExtent(extent: Extent): Unit = {
    this.extent = extent
  }

  def getSpatialKey: SpatialKey = {
    this.spatialKey
  }

  def setSpatialKey(spatialKey: SpatialKey): Unit = {
    this.spatialKey = spatialKey
  }

  def getOffset: Long = {
    this.offset
  }

  def setOffset(offset: Long): Unit = {
    this.offset = offset
  }

  def getByteCount: Long = {
    this.byteCount
  }

  def setByteCount(byteCount: Long): Unit = {
    this.byteCount = byteCount
  }

  def getTileBuf: Array[Byte] = {
    this.tileBuf
  }

  def setTileBuf(tileBuf: Array[Byte]): Unit = {
    this.tileBuf = tileBuf
  }

  def getRotation: Double = {
    this.rotation
  }

  def setRotation(rotation: Double): Unit = {
    this.rotation = rotation
  }

  def getResolutionCol: Double = {
    this.resolutionCol
  }

  def setResolutionCol(resolutionCol: Double): Unit = {
    this.resolutionCol = resolutionCol
  }

  def getResolutionRow: Double = {
    this.resolutionRow
  }

  def setResolutionRow(resolutionRow: Double): Unit = {
    this.resolutionRow = resolutionRow
  }

  def getCrs: CRS = {
    this.crs
  }

  def setCrs(crs: String): Unit = {
    this.crs = CRS.fromName(crs)
  }

  def setCrs(crs: CRS): Unit = {
    this.crs = crs
  }

  def getDataType: OGEDataType = {
    this.dataType
  }

  def setDataType(dataType: String): Unit = {
    this.dataType = OGEDataType.withName(dataType)
  }

  def setDataType(dataType: OGEDataType): Unit = {
    this.dataType = dataType
  }

  def getTile: Tile = {
    this.tile
  }

  def setTile(tile: Tile): Unit = {
    this.tile = tile
  }
}