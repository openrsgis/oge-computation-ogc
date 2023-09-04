package whu.edu.cn.util

import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{ByteArrayTile, ByteCellType, ByteConstantNoDataCellType, CellType, DoubleArrayTile, DoubleCellType, DoubleConstantNoDataCellType, FloatArrayTile, FloatCellType, FloatConstantNoDataCellType, IntArrayTile, IntCellType, IntConstantNoDataCellType, ShortArrayTile, ShortCellType, ShortConstantNoDataCellType, Tile, UByteArrayTile, UByteCellType, UByteConstantNoDataCellType, UShortArrayTile, UShortCellType, UShortConstantNoDataCellType}

import scala.collection.mutable.ArrayBuffer

/**
 * A class for tile data serialization and deserialization.
 *
 * Here is a match list between data type and cell type in the GeoTrellis.
 *
 * case "int8raw" => ByteCellType
 * case "uint8raw" => UByteCellType
 * case "int16raw" => ShortCellType
 * case "uint16raw" => UShortCellType
 * case "int32raw" => IntCellType
 * case "float32raw" => FloatCellType
 * case "float64raw" => DoubleCellType
 * case "int8" => ByteConstantNoDataCellType
 * case "uint8" => UByteConstantNoDataCellType
 * case "int16" => ShortConstantNoDataCellType
 * case "uint16" => UShortConstantNoDataCellType
 * case "int32" => IntConstantNoDataCellType
 * case "float32" => FloatConstantNoDataCellType
 * case "float64" => DoubleConstantNoDataCellType
 */

object TileSerializerCoverageUtil {
  /**
   * Deserialize tile bytes to Tile object.
   *
   * @param platform  Satellite platform
   * @param tileBytes Tile data in Array[Byte]
   * @param tileSize  The size of tile, e.g. 4000*4000
   * @param dataType  Type of data
   * @return
   */
    // TODO lrx: 这里后面要定义成OGEDataType
  def deserializeTileData(platform: String, tileBytes: Array[Byte], tileSize: Int, dataType: String): Tile = {
    dataType match {
//      case "int8raw" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "uint8raw" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "int8" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "uint8" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "int16raw" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "uint16raw" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "int16" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "uint16" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "int32raw" => deserialize2IntType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "int32" => deserialize2IntType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "float32raw" => deserialize2FloatType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "float32" => deserialize2FloatType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "float64raw" => deserialize2DoubleType(tileBytes, tileSize, CellType.fromName(dataType))
//      case "float64" => deserialize2DoubleType(tileBytes, tileSize, CellType.fromName(dataType))


      case "int8raw" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName("int8raw"))
      case "uint8raw" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName("uint8raw"))
      case "int8" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName("int8raw"))
      case "uint8" => deserialize2ByteType(tileBytes, tileSize, CellType.fromName("uint8raw"))
      case "int16raw" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName("int16raw"))
      case "uint16raw" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName("uint16raw"))
      case "int16" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName("int16raw"))
      case "uint16" => deserialize2ShortType(tileBytes, tileSize, CellType.fromName("uint16raw"))
      case "int32raw" => deserialize2IntType(tileBytes, tileSize, CellType.fromName("int32raw"))
      case "int32" => deserialize2IntType(tileBytes, tileSize, CellType.fromName("int32raw"))
      case "float32raw" => deserialize2FloatType(tileBytes, tileSize, CellType.fromName("float32raw"))
      case "float32" => deserialize2FloatType(tileBytes, tileSize, CellType.fromName("float32raw"))
      case "float64raw" => deserialize2DoubleType(tileBytes, tileSize, CellType.fromName("float64raw"))
      case "float64" => deserialize2DoubleType(tileBytes, tileSize, CellType.fromName("float64raw"))
      case _ => throw new RuntimeException("Not support for " + dataType)
    }
  }

  /**
   * Deserialize to int8raw, uint8raw, int8 or uint8 tile.
   *
   * @param tileBytes
   * @param tileSize
   * @param cellType
   * @return a ByteArrayTile of int8raw, uint8raw, int8 or uint8 type
   */
  def deserialize2ByteType(tileBytes: Array[Byte], tileSize: Int, cellType: CellType): Tile = {
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Byte](tileSize * tileSize)
    for (i <- index) {
      cell(i) = tileBytes(i)
    }
    cellType match {
      case ByteCellType => ByteArrayTile(cell, tileSize, tileSize, ByteCellType)
      case UByteCellType => UByteArrayTile(cell, tileSize, tileSize, UByteCellType)
      case ByteConstantNoDataCellType => ByteArrayTile(cell, tileSize, tileSize, ByteConstantNoDataCellType)
      case UByteConstantNoDataCellType => UByteArrayTile(cell, tileSize, tileSize, UByteConstantNoDataCellType)
    }
  }

  /**
   * Deserialize to short16raw, ushort16raw, short16 or ushort16 tile.
   *
   * @param tileBytes
   * @param tileSize
   * @param cellType
   * @return a ShortArrayTile of short16raw, ushort16raw, short16 and ushort16 type
   */
  def deserialize2ShortType(tileBytes: Array[Byte], tileSize: Int, cellType: CellType): Tile = {
    val subFirst = ArrayBuffer.range(0, tileSize * tileSize * 2, 2)
    val subSecond = ArrayBuffer.range(1, tileSize * tileSize * 2 + 1, 2)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Short](tileSize * tileSize)
    for (i <- index) {
      val bytesArray: Array[Byte] = new Array[Byte](2)
      bytesArray(0) = tileBytes(subFirst(i))
      bytesArray(1) = tileBytes(subSecond(i))
      cell(i) = ((bytesArray(0) & 0xff) | ((bytesArray(1) & 0xff) << 8)).toShort
    }

    cellType match {
      case ShortCellType => ShortArrayTile(cell, tileSize, tileSize, ShortCellType)
      case UShortCellType => UShortArrayTile(cell, tileSize, tileSize, UShortCellType)
      case ShortConstantNoDataCellType => ShortArrayTile(cell, tileSize, tileSize, ShortConstantNoDataCellType)
      case UShortConstantNoDataCellType => UShortArrayTile(cell, tileSize, tileSize, UShortConstantNoDataCellType)
    }
  }

  /**
   * Deserialize to int32raw or int32 tile.
   *
   * @param tileBytes
   * @param tileSize
   * @param cellType
   * @return a IntArrayTile of int32raw or int32 type
   */
  def deserialize2IntType(tileBytes: Array[Byte], tileSize: Int, cellType: CellType): Tile = {
    val subFirst = ArrayBuffer.range(0, tileSize * tileSize * 4, 4)
    val subSecond = ArrayBuffer.range(1, tileSize * tileSize * 4 + 1, 4)
    val subThird = ArrayBuffer.range(2, tileSize * tileSize * 4 + 2, 4)
    val subFourth = ArrayBuffer.range(3, tileSize * tileSize * 4 + 3, 4)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Int](tileSize * tileSize)
    for (i <- index) {
      val _array: Array[Byte] = new Array[Byte](4)
      _array(0) = tileBytes(subFirst(i))
      _array(1) = tileBytes(subSecond(i))
      _array(2) = tileBytes(subThird(i))
      _array(3) = tileBytes(subFourth(i))
      cell(i) = (_array(0) & 0xff) | ((_array(1) & 0xff) << 8) | ((_array(2) & 0xff) << 16) | ((_array(3) & 0xff) << 24)
    }
    cellType match {
      case IntCellType => IntArrayTile(cell, tileSize, tileSize, IntCellType)
      case IntConstantNoDataCellType => IntArrayTile(cell, tileSize, tileSize, IntConstantNoDataCellType)
    }
  }

  /**
   * Deserialize to float32raw or float32 tile.
   *
   * @param tileBytes
   * @param tileSize
   * @param cellType
   * @return a FloatArrayTile of float32raw or float32 type
   */
  def deserialize2FloatType(tileBytes: Array[Byte], tileSize: Int, cellType: CellType): Tile = {
    val subFirst = ArrayBuffer.range(0, tileSize * tileSize * 4, 4)
    val subSecond = ArrayBuffer.range(1, tileSize * tileSize * 4 + 1, 4)
    val subThird = ArrayBuffer.range(2, tileSize * tileSize * 4 + 2, 4)
    val subFourth = ArrayBuffer.range(3, tileSize * tileSize * 4 + 3, 4)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Float](tileSize * tileSize)
    for (i <- index) {
      if (i < tileBytes.length / 4) {
        val _array: Array[Byte] = new Array[Byte](4)
        _array(0) = tileBytes(subFirst(i))
        _array(1) = tileBytes(subSecond(i))
        _array(2) = tileBytes(subThird(i))
        _array(3) = tileBytes(subFourth(i))
        val asInt: Int = (_array(0) & 0xFF) | ((_array(1) & 0xFF) << 8) | ((_array(2) & 0xFF) << 16) | ((_array(3) & 0xFF) << 24)
        cell(i) = java.lang.Float.intBitsToFloat(asInt)
      }
      else {
        cell(i) = 0
      }
    }
    cellType match {
      case FloatCellType => FloatArrayTile(cell, tileSize, tileSize, FloatCellType)
      case FloatConstantNoDataCellType => FloatArrayTile(cell, tileSize, tileSize, FloatConstantNoDataCellType)
    }
  }

  /**
   * Deserialize to float64raw or float64 tile.
   *
   * @param tileBytes
   * @param tileSize
   * @param cellType
   * @return a DoubleArrayTile of float64raw or float64 type
   */
  def deserialize2DoubleType(tileBytes: Array[Byte], tileSize: Int, cellType: CellType): Tile = {
    val subFirst = ArrayBuffer.range(0, tileSize * tileSize * 8, 8)
    val subSecond = ArrayBuffer.range(1, tileSize * tileSize * 8 + 1, 8)
    val subThird = ArrayBuffer.range(2, tileSize * tileSize * 8 + 2, 8)
    val subFourth = ArrayBuffer.range(3, tileSize * tileSize * 8 + 3, 8)
    val subFixth = ArrayBuffer.range(3, tileSize * tileSize * 8 + 4, 8)
    val subSixth = ArrayBuffer.range(3, tileSize * tileSize * 8 + 5, 8)
    val subSeventh = ArrayBuffer.range(3, tileSize * tileSize * 8 + 6, 8)
    val subEighth = ArrayBuffer.range(3, tileSize * tileSize * 8 + 7, 8)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Double](tileSize * tileSize)
    for (i <- index) {
      val _array: Array[Byte] = new Array[Byte](8)
      _array(0) = tileBytes(subFirst(i))
      _array(1) = tileBytes(subSecond(i))
      _array(2) = tileBytes(subThird(i))
      _array(3) = tileBytes(subFourth(i))
      _array(4) = tileBytes(subFixth(i))
      _array(5) = tileBytes(subSixth(i))
      _array(6) = tileBytes(subSeventh(i))
      _array(7) = tileBytes(subEighth(i))
      val asLong: Long = (_array(0) & 0xFF) | ((_array(1) & 0xFF) << 8) | ((_array(2) & 0xFF) << 16) | ((_array(3) & 0xFF) << 24) |
        ((_array(4) & 0xFF) << 32) | ((_array(5) & 0xFF) << 40) | ((_array(6) & 0xFF) << 48) | ((_array(7) & 0xFF) << 56)
      cell(i) = java.lang.Double.longBitsToDouble(asLong)
    }

    cellType match {
      case DoubleCellType => DoubleArrayTile(cell, tileSize, tileSize, DoubleCellType)
      case DoubleConstantNoDataCellType => DoubleArrayTile(cell, tileSize, tileSize, DoubleConstantNoDataCellType)
    }
  }

  /**
   * Transform a Tile object to png bytes.
   *
   * @param tile
   * @return png bytes array
   */
  def tile2PngBytes(tile: Tile): Array[Byte] = {
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val png = tile.renderPng(colorRamp)
    png
  }

}
