//package whu.edu.cn.util
//
//import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
//import geotrellis.raster._
//import geotrellis.vector.Extent
//import org.apache.spark.rdd.RDD
//import whu.edu.cn
//import whu.edu.cn.entity.{RawTile, SpaceTimeBandKey}
//
//object TileMosaicCoverageUtil {
//
//  // TODO lrx: 这里后面要定义成OGEDataType
//  def dataTypeSet(dataType: String, method: String): MutableArrayTile = {
//    dataType match {
//      case "int8raw" => setByteType(CellType.fromName(dataType), method)
//      case "uint8raw" => setByteType(CellType.fromName(dataType), method)
//      case "int8" => setByteType(CellType.fromName(dataType), method)
//      case "uint8" => setByteType(CellType.fromName(dataType), method)
//      case "int16raw" => setShortType(CellType.fromName(dataType), method)
//      case "uint16raw" => setShortType(CellType.fromName(dataType), method)
//      case "int16" => setShortType(CellType.fromName(dataType), method)
//      case "uint16" => setShortType(CellType.fromName(dataType), method)
//      case "int32raw" => setIntType(CellType.fromName(dataType), method)
//      case "int32" => setIntType(CellType.fromName(dataType), method)
//      case "float32raw" => setFloatType(CellType.fromName(dataType), method)
//      case "float32" => setFloatType(CellType.fromName(dataType), method)
//      case "float64raw" => setDoubleType(CellType.fromName(dataType), method)
//      case "float64" => setDoubleType(CellType.fromName(dataType), method)
//      case _ => throw new RuntimeException("Not support for " + dataType)
//    }
//  }
//
//  def getTileValue(tile: Tile, i: Int, j: Int, dataType: String): Any = {
//    dataType match {
//      case "int8raw" => tile.get(j, i).toByte
//      case "uint8raw" => tile.get(j, i).toByte
//      case "int8" => tile.get(j, i).toByte
//      case "uint8" => tile.get(j, i).toByte
//      case "int16raw" => tile.get(j, i).toShort
//      case "uint16raw" => tile.get(j, i).toShort
//      case "int16" => tile.get(j, i).toShort
//      case "uint16" => tile.get(j, i).toShort
//      case "int32raw" => tile.get(j, i)
//      case "int32" => tile.get(j, i)
//      case "float32raw" => tile.getDouble(j, i).toFloat
//      case "float32" => tile.getDouble(j, i).toFloat
//      case "float64raw" => tile.getDouble(j, i)
//      case "float64" => tile.getDouble(j, i)
//      case _ => throw new RuntimeException("Not support for " + dataType)
//    }
//  }
//
//  def setByteType(cellType: CellType, method: String): MutableArrayTile = {
//    var typeSet: Byte = 0
//    method match {
//      case "min" => typeSet = Byte.MaxValue
//      case "max" => typeSet = Byte.MinValue
//      case _ => typeSet = 0
//    }
//    cellType match {
//      case ByteCellType => ByteArrayTile(Array.fill[Byte](256 * 256)(typeSet), 256, 256, ByteCellType).mutable
//      case UByteCellType => UByteArrayTile(Array.fill[Byte](256 * 256)(typeSet), 256, 256, UByteCellType).mutable
//      case ByteConstantNoDataCellType => ByteArrayTile(Array.fill[Byte](256 * 256)(typeSet), 256, 256, ByteConstantNoDataCellType).mutable
//      case UByteConstantNoDataCellType => UByteArrayTile(Array.fill[Byte](256 * 256)(typeSet), 256, 256, UByteConstantNoDataCellType).mutable
//    }
//  }
//
//  def setShortType(cellType: CellType, method: String): MutableArrayTile = {
//    var typeSet: Short = 0
//    method match {
//      case "min" => typeSet = Short.MaxValue
//      case "max" => typeSet = Short.MinValue
//      case _ => typeSet = 0
//    }
//    cellType match {
//      case ShortCellType => ShortArrayTile(Array.fill[Short](256 * 256)(typeSet), 256, 256, ShortCellType).mutable
//      case UShortCellType => UShortArrayTile(Array.fill[Short](256 * 256)(typeSet), 256, 256, UShortCellType).mutable
//      case ShortConstantNoDataCellType => ShortArrayTile(Array.fill[Short](256 * 256)(typeSet), 256, 256, ShortConstantNoDataCellType).mutable
//      case UShortConstantNoDataCellType => UShortArrayTile(Array.fill[Short](256 * 256)(typeSet), 256, 256, UShortConstantNoDataCellType).mutable
//    }
//  }
//
//  def setIntType(cellType: CellType, method: String): MutableArrayTile = {
//    var typeSet: Int = 0
//    method match {
//      case "min" => typeSet = Int.MaxValue
//      case "max" => typeSet = Int.MinValue
//      case _ => typeSet = 0
//    }
//    cellType match {
//      case IntCellType => IntArrayTile(Array.fill[Int](256 * 256)(typeSet), 256, 256, IntCellType).mutable
//      case IntConstantNoDataCellType => IntArrayTile(Array.fill[Int](256 * 256)(typeSet), 256, 256, IntConstantNoDataCellType).mutable
//    }
//  }
//
//  def setFloatType(cellType: CellType, method: String): MutableArrayTile = {
//    var typeSet: Float = 0
//    method match {
//      case "min" => typeSet = Float.MaxValue
//      case "max" => typeSet = Float.MinValue
//      case _ => typeSet = 0
//    }
//    cellType match {
//      case FloatCellType => FloatArrayTile(Array.fill[Float](256 * 256)(typeSet), 256, 256, FloatCellType).mutable
//      case FloatConstantNoDataCellType => FloatArrayTile(Array.fill[Float](256 * 256)(typeSet), 256, 256, FloatConstantNoDataCellType).mutable
//    }
//  }
//
//  def setDoubleType(cellType: CellType, method: String): MutableArrayTile = {
//    var typeSet: Double = 0
//    method match {
//      case "min" => typeSet = Double.MaxValue
//      case "max" => typeSet = Double.MinValue
//      case _ => typeSet = 0
//    }
//    cellType match {
//      case DoubleCellType => DoubleArrayTile(Array.fill[Double](256 * 256)(typeSet), 256, 256, DoubleCellType).mutable
//      case DoubleConstantNoDataCellType => DoubleArrayTile(Array.fill[Double](256 * 256)(typeSet), 256, 256, DoubleConstantNoDataCellType).mutable
//    }
//  }
//
//  def tileMosaic(TileRDDUnComputed: RDD[((Extent, SpaceTimeKey), List[RawTile])], method: String, tileLayerMetadata: TileLayerMetadata[SpaceTimeKey], dType: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//    method match {
//      case "min" => {
//        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
//          val size = t._2(0).getResolution
//          val measurementName = t._2(0).getMeasurement
//          val mutableArrayTile = dataTypeSet(dType, method)
//          for (rawtile <- t._2) {
//            for (i <- 0 to 255) {
//              for (j <- 0 to 255) {
//                val y = Math.round((t._1._1.ymax - (rawtile.getExtent.ymax - size * i)) / size).toInt
//                val x = Math.round((rawtile.getExtent.xmin + size * j - t._1._1.xmin) / size).toInt
//                val value = getTileValue(rawtile.getTile, i, j, dType)
//                if (x >= 0 && x < 256 && y >= 0 && y < 256 && value.toString.toDouble != 0 && value.toString.toDouble < mutableArrayTile.get(x, y)) {
//                  value match {
//                    case _: Byte =>
//                      mutableArrayTile.set(x, y, value.toString.toByte)
//                    case _: Short =>
//                      val a = value.toString.toShort
//                      mutableArrayTile.set(x, y, value.toString.toShort)
//                    case _: Int =>
//                      mutableArrayTile.set(x, y, value.toString.toInt)
//                    case _: Float =>
//                      mutableArrayTile.setDouble(x, y, value.toString.toFloat)
//                    case _: Double =>
//                      mutableArrayTile.setDouble(x, y, value.toString.toDouble)
//                    case _ => throw new RuntimeException("Data Type Error!")
//                  }
//                }
//              }
//            }
//          }
//          (SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
//        })
//        (TileRDDComputed, tileLayerMetadata)
//      }
//      case "max" => {
//        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
//          val size = t._2(0).getResolution
//          val measurementName = t._2(0).getMeasurement
//          val mutableArrayTile = dataTypeSet(dType, method)
//          for (rawtile <- t._2) {
//            for (i <- 0 to 255) {
//              for (j <- 0 to 255) {
//                val y = Math.round((t._1._1.ymax - (rawtile.getExtent.ymax - size * i)) / size).toInt
//                val x = Math.round((rawtile.getExtent.xmin + size * j - t._1._1.xmin) / size).toInt
//                val value = getTileValue(rawtile.getTile, i, j, dType)
//                if (x >= 0 && x < 256 && y >= 0 && y < 256 && value.toString.toDouble != 0 && value.toString.toDouble > mutableArrayTile.get(x, y)) {
//                  value match {
//                    case _: Byte =>
//                      mutableArrayTile.set(x, y, value.toString.toByte)
//                    case _: Short =>
//                      mutableArrayTile.set(x, y, value.toString.toShort)
//                    case _: Int =>
//                      mutableArrayTile.set(x, y, value.toString.toInt)
//                    case _: Float =>
//                      mutableArrayTile.setDouble(x, y, value.toString.toFloat)
//                    case _: Double =>
//                      mutableArrayTile.setDouble(x, y, value.toString.toDouble)
//                    case _ => throw new RuntimeException("Data Type Error!")
//                  }
//                }
//              }
//            }
//          }
//          (SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
//        })
//        (TileRDDComputed, tileLayerMetadata)
//      }
//      case _ => {
//        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
//          val assignTimes = Array.fill[Byte](256, 256)(0)
//          val size = t._2(0).getResolution
//          val measurementName = t._2(0).getMeasurement
//          val mutableArrayTile = dataTypeSet(dType, method)
//          for (rawtile <- t._2) {
//            for (i <- 0 to 255) {
//              for (j <- 0 to 255) {
//                val y = Math.round((t._1._1.ymax - (rawtile.getExtent.ymax - size * i)) / size).toInt
//                val x = Math.round((rawtile.getExtent.xmin + size * j - t._1._1.xmin) / size).toInt
//                val value = getTileValue(rawtile.getTile, i, j, dType)
//                if (x >= 0 && x < 256 && y >= 0 && y < 256 && value.toString.toDouble != 0) {
//                  value match {
//                    case _: Byte =>
//                      mutableArrayTile.set(x, y, 1 / (assignTimes(y)(x) + 1) * value.toString.toByte + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.get(x, y).toByte)
//                      assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
//                    case _: Short =>
//                      mutableArrayTile.set(x, y, 1 / (assignTimes(y)(x) + 1) * value.toString.toShort + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.get(x, y).toShort)
//                      assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
//                    case _: Int =>
//                      mutableArrayTile.set(x, y, 1 / (assignTimes(y)(x) + 1) * value.toString.toInt + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.get(x, y))
//                      assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
//                    case _: Float =>
//                      mutableArrayTile.setDouble(x, y, 1 / (assignTimes(y)(x) + 1) * value.toString.toFloat + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.getDouble(x, y))
//                      assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
//                    case _: Double =>
//                      mutableArrayTile.setDouble(x, y, 1 / (assignTimes(y)(x) + 1) * value.toString.toDouble + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.getDouble(x, y))
//                      assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
//                    case _ => throw new RuntimeException("Data Type Error!")
//                  }
//                }
//              }
//            }
//          }
//          (SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
//        })
//        (TileRDDComputed, tileLayerMetadata)
//      }
//    }
//  }
//}
