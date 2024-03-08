package whu.edu.cn.util.cube

import whu.edu.cn.entity.cube.OGECubeDataType.OGECubeDataType
import whu.edu.cn.entity.cube.{CubeCOGMetadata, OGECubeDataType}

import java.io.ByteArrayOutputStream
import java.util.zip.Inflater
import scala.collection.mutable.ArrayBuffer

object CubeUtil {

  private final val typeArray: Array[Int] = Array( //"???",
    0, //
    1, // byte //8-bit unsigned integer
    1, // ascii//8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
    2, // short",2),//16-bit (2-byte) unsigned integer.
    4, // long",4),//32-bit (4-byte) unsigned integer.
    8, // rational",8),//Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
    1, // sbyte",1),//An 8-bit signed (twos-complement) integer
    1, // undefined",1),//An 8-bit byte that may contain anything, depending on the definition of the field
    2, // sshort",1),//A 16-bit (2-byte) signed (twos-complement) integer.
    4, // slong",1),// A 32-bit (4-byte) signed (twos-complement) integer.
    8, // srational",1),//Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
    4, // float",4),//Single precision (4-byte) IEEE format
    8 // double",8)//Double precision (8-byte) IEEE format
  )


  def cogHeaderBytesParse(headerBytes: Array[Byte]): CubeCOGMetadata = {
    val cubeCOGMetadata: CubeCOGMetadata = new CubeCOGMetadata
    var ifh: Int = getIntII(headerBytes, 4, 4)
    while (ifh != 0) {
      val deCount: Int = getIntII(headerBytes, ifh, 2)
      ifh += 2
      for (_ <- 0 until deCount) {
        val tagIndex: Int = getIntII(headerBytes, ifh, 2)
        val typeIndex: Int = getIntII(headerBytes, ifh + 2, 2)
        val count: Int = getIntII(headerBytes, ifh + 4, 4)
        var pData: Int = ifh + 8
        val totalSize: Int = typeArray(typeIndex) * count
        if (totalSize > 4) {
          pData = getIntII(headerBytes, pData, 4)
        }
        val typeSize: Int = typeArray(typeIndex)
        tagIndex match {
          case 256 => cubeCOGMetadata.setImageWidth(getIntII(headerBytes, pData, typeSize))
          case 257 => cubeCOGMetadata.setImageHeight(getIntII(headerBytes, pData, typeSize))
          case 258 => cubeCOGMetadata.setBitPerSample(getIntII(headerBytes, pData, typeSize))
          case 259 => cubeCOGMetadata.setCompression(getIntII(headerBytes, pData, typeSize))
          case 277 => cubeCOGMetadata.setBandCount(getIntII(headerBytes, pData, typeSize))
          case 322 => cubeCOGMetadata.setTileWidth(getIntII(headerBytes, pData, typeSize))
          case 323 => cubeCOGMetadata.setTileHeight(getIntII(headerBytes, pData, typeSize))
          case 324 => cubeCOGMetadata.setTileOffsets(getBytesArray(pData, typeSize, headerBytes, cubeCOGMetadata.getImageWidth, cubeCOGMetadata.getImageHeight, cubeCOGMetadata.getBandCount))
          case 325 => cubeCOGMetadata.setTileByteCounts(getBytesArray(pData, typeSize, headerBytes, cubeCOGMetadata.getImageWidth, cubeCOGMetadata.getImageHeight, cubeCOGMetadata.getBandCount))
          case 339 => cubeCOGMetadata.setSampleFormat(getIntII(headerBytes, pData, typeSize))
          case 33550 => cubeCOGMetadata.setCellScale(getDoubleCell(pData, typeSize, count, headerBytes))
          case 33922 => cubeCOGMetadata.setGeoTransform(getDoubleTrans(pData, typeSize, count, headerBytes))
          case 34737 => cubeCOGMetadata.setCrs(getString(headerBytes, pData, typeSize * count - 1))
          case 42112 => cubeCOGMetadata.setGdalMetadata(getString(headerBytes, pData, typeSize * count - 1))
          case 42113 => cubeCOGMetadata.setGdalNodata(getString(headerBytes, pData, typeSize * count - 1))
          case _ =>
        }
        ifh += 12
      }
      ifh = getIntII(headerBytes, ifh, 4)
    }
    cubeCOGMetadata
  }

  def getCubeDataType(sampleFormat: Int, bitPerSample: Int): OGECubeDataType = {
    sampleFormat match {
      case 1 =>
        bitPerSample match {
          case 8 => OGECubeDataType.uint8
          case 16 => OGECubeDataType.uint16
          case 32 => OGECubeDataType.uint32
          case 64 => OGECubeDataType.uint64
          case _ => null
        }
      case 2 =>
        bitPerSample match {
          case 8 => OGECubeDataType.int8
          case 16 => OGECubeDataType.int16
          case 32 => OGECubeDataType.int32
          case 64 => OGECubeDataType.int64
          case _ => null
        }
      case 3 =>
        bitPerSample match {
          case 32 => OGECubeDataType.float32
          case 64 => OGECubeDataType.float64
          case _ => null
        }
      case _ => null
    }
  }


  private def getIntII(pd: Array[Byte], start_pos: Int, length: Int): Int = {
    var value = 0
    for (i <- 0 until length) {
      value |= (pd(start_pos + i) & 0xFF) << i * 8
      if (value < 0) {
        value += 256 << i * 8
      }
    }
    value
  }

  private def getBytesArray(start_pos: Int, type_size: Int, header: Array[Byte], image_width: Int,
                            image_height: Int, band_count: Int): Array[Array[Int]] = {
    val strip_offsets: ArrayBuffer[Array[Int]] = ArrayBuffer[Array[Int]]()

    val tile_count_x: Int = (image_width + 255) / 256
    val tile_count_y: Int = (image_height + 255) / 256

    for (_ <- 0 until band_count) {
      val offsets: ArrayBuffer[Int] = ArrayBuffer[Int]()
      for (i <- 0 until tile_count_y) {
        offsets.clear()
        for (j <- 0 until tile_count_x) {
          val v: Long = getLong(header, start_pos + (i * tile_count_x + j) * type_size, type_size)
          offsets.append(v.toInt)
        }
        strip_offsets.append(offsets.toArray)
      }
    }
    strip_offsets.toArray
  }

  private def getLong(header_bytes: Array[Byte], start: Int, length: Int): Long = {
    var value = 0L
    for (i <- 0 until length) {
      value |= (header_bytes(start + i) & 0xFF).toLong << (8 * i)
      if (value < 0) {
        value += 256L << (i * 8)
      }
    }
    value
  }

  private def getDoubleCell(start_pos: Int, type_size: Int, count: Int, header: Array[Byte]): Array[Double] = {
    val cell: ArrayBuffer[Double] = ArrayBuffer[Double]()
    for (i <- 0 until count) {
      val v = getDouble(header, start_pos + i * type_size, type_size)
      cell.append(v)
    }
    cell.toArray
  }

  private def getDouble(pd: Array[Byte], start_pos: Int, length: Int): Double = {
    var value = 0L
    for (i <- 0 until length) {
      value |= (pd(start_pos + i) & 0xFF).toLong << (8 * i)
      if (value < 0) {
        value += 256L << i * 8
      }
    }
    java.lang.Double.longBitsToDouble(value)
  }

  private def getDoubleTrans(start_pos: Int, type_size: Int, count: Int, header: Array[Byte]): Array[Double] = {
    val geo_trans: ArrayBuffer[Double] = ArrayBuffer[Double]()
    for (i <- 0 until count) {
      val v: Double = getDouble(header, start_pos + i * type_size, type_size)
      geo_trans.append(v)
    }
    geo_trans.toArray
  }

  private def getString(pd: Array[Byte], start_pos: Int, length: Int): String = {
    val str_get: Array[Byte] = pd.slice(start_pos, start_pos + length)
    new String(str_get, "UTF-8")
  }

  def decompress(data: Array[Byte]): Array[Byte] = {
    val inflater: Inflater = new Inflater
    inflater.setInput(data)
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val buffer: Array[Byte] = new Array[Byte](1024)
    while (!inflater.finished()) {
      val count: Int = inflater.inflate(buffer)
      out.write(buffer, 0, count)
    }
    out.toByteArray
  }

}
