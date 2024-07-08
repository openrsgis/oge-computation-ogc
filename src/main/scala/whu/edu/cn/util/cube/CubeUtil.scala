package whu.edu.cn.util.cube

import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.cube.OGECubeDataType.OGECubeDataType
import whu.edu.cn.entity.cube.{BandKey, CubeCOGMetadata, CubeTileKey, OGECubeDataType, ProductKey, SpaceKey, TimeKey}

import java.io.ByteArrayOutputStream
import java.time.Instant
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

  def checkProjResoExtent(cube1: (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey]), cube2: (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])): ((RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey]), (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])) = {
    val tile1: (CubeTileKey, Tile) = cube1._1.first()
    val tile2: (CubeTileKey, Tile) = cube2._1.first()
    val time1: Long = cube1._1.collect().head._1.timeKey.time
    val time2: Long = cube2._1.collect().head._1.timeKey.time
    val productkey1: ProductKey = cube1._1.collect().head._1.productKey
    val productkey2: ProductKey = cube2._1.collect().head._1.productKey
    val band1: BandKey = cube1._1.collect().head._1.bandKey
    val band2: BandKey = cube2._1.collect().head._1.bandKey
    val cube1tileRDD: RDD[(SpatialKey, Tile)] = cube1._1.map(t => {
      (new SpatialKey(t._1.spaceKey.col, t._1.spaceKey.row), t._2)
    })
    val cube2tileRDD: RDD[(SpatialKey, Tile)] = cube2._1.map(t => {
      (new SpatialKey(t._1.spaceKey.col, t._1.spaceKey.row), t._2)
    })

    if (cube1._2.crs != cube2._2.crs || cube1._2.cellSize.resolution != cube2._2.cellSize.resolution || cube1._2.extent != cube2._2.extent) {
      var reso: Double = 0.0
      var crs: CRS = null
      var tms: String = null
      var resoRatio: Double = 0.0
      // flag是针对影像分辨率的，flag=1，代表第1张影像分辨率低
      var flag: Int = 0

      if (cube1._2.crs != cube2._2.crs) {
        // 这里投影到统一的坐标系下只是为了比较分辨率大小
        val reso1: Double = cube1._2.cellSize.resolution
        // 把第二张影像投影到第一张影像的坐标系下，然后比较分辨率
        val reso2: Double = tile2._2.reproject(cube2._2.layout.mapTransform(new SpatialKey(tile2._1.spaceKey.col, tile2._1.spaceKey.row)), cube2._2.crs, cube1._2.crs).cellSize.resolution
        if (reso1 < reso2) {
          reso = reso1
          resoRatio = reso2 / reso1
          flag = 2
          crs = cube1._2.crs
          tms = cube1._1.first()._1.spaceKey.tms
        }
        else {
          reso = reso2
          resoRatio = reso1 / reso2
          flag = 1
          crs = cube2._2.crs
          tms = cube2._1.first()._1.spaceKey.tms
        }
      }
      else {
        crs = cube1._2.crs
        tms = cube1._1.first()._1.spaceKey.tms
        if (cube1._2.cellSize.resolution < cube2._2.cellSize.resolution) {
          reso = cube1._2.cellSize.resolution
          resoRatio = cube2._2.cellSize.resolution / cube1._2.cellSize.resolution
          flag = 2
        }
        else {
          reso = cube2._2.cellSize.resolution
          resoRatio = cube1._2.cellSize.resolution / cube2._2.cellSize.resolution
          flag = 1
        }
      }

      val extent1: Extent = cube1._2.extent.reproject(cube1._2.crs, crs)
      val extent2: Extent = cube2._2.extent.reproject(cube2._2.crs, crs)

      if (extent1.intersects(extent2)) {
        // 这里一定不会null，但是API要求orNull
        var extent: Extent = extent1.intersection(extent2).orNull

        // 先重投影，重投影到原始范围重投影后的范围、这个范围除以256, 顺势进行裁剪
        val layoutCols: Int = math.max(math.ceil((extent.xmax - extent.xmin) / reso / 256.0).toInt, 1)
        val layoutRows: Int = math.max(math.ceil((extent.ymax - extent.ymin) / reso / 256.0).toInt, 1)
        val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
        // Extent必须进行重新计算，因为layoutCols和layoutRows加了黑边，导致范围变化了
        val newExtent: Extent = new Extent(extent.xmin, extent.ymin, extent.xmin + reso * 256.0 * layoutCols, extent.ymin + reso * 256.0 * layoutRows)
        extent = newExtent
        val ld: LayoutDefinition = LayoutDefinition(extent, tl)

        // 这里是coverage1开始进行重投影
        val cube1Rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = TileLayerRDD(cube1tileRDD, cube1._2)

        var cube1tileLayerRdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = cube1Rdd
        var newTileSize: Int = 256
        // 如果flag=1， 代表第一张影像的分辨率较低
        if (flag == 1) {
          val tileRatio1: Int = (math.log(resoRatio) / math.log(2)).toInt
          if (tileRatio1 != 0) {
            // 对影像进行瓦片大小重切分
            newTileSize = 256 / math.pow(2, tileRatio1).toInt
            val cube1Retiled: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = cube1Rdd.regrid(newTileSize)
            // 进行裁剪
            val cropExtent1: Extent = extent.reproject(crs, cube1Retiled.metadata.crs)
            cube1tileLayerRdd = cube1Retiled.crop(cropExtent1)
          }
        }


        val (_, reprojectedRdd1): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          cube1tileLayerRdd.reproject(crs, ld)

        // Filter配合extent的强制修改，达到真正裁剪到我们想要的Layout的目的
        val reprojFilter1: RDD[(SpatialKey, Tile)] = reprojectedRdd1.filter(layer => {
          val key: SpatialKey = layer._1
          val extentR: Extent = ld.mapTransform(key)
          extentR.xmin >= extent.xmin && extentR.xmax <= extent.xmax && extentR.ymin >= extent.ymin && extentR.ymax <= extent.ymax
        })

        val srcMetadata1: TileLayerMetadata[SpatialKey] = reprojectedRdd1.metadata
        val newProjBounds1: Bounds[SpatialKey] = srcMetadata1.bounds
        val newMetadata1: TileLayerMetadata[SpatialKey] = TileLayerMetadata(srcMetadata1.cellType, srcMetadata1.layout, extent, srcMetadata1.crs, newProjBounds1)

        // 这里LayerMetadata直接使用reprojectRdd1的，尽管SpatialKey有负值也不影响
        val newCube1: (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey]) = (reprojFilter1.map(t => {
          (new CubeTileKey(new SpaceKey(tms, t._1.col, t._1.row, newTileSize * t._1.col * reso, newTileSize * (t._1.col + 1) * reso, newTileSize * t._1.row * reso, newTileSize * (t._1.row + 1) * reso), new TimeKey(time1), productkey1, band1), t._2)
        }), newMetadata1)

        // 这里是coverage2开始进行重投影
        val cube2Rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = TileLayerRDD(cube2tileRDD, cube2._2)


        var cube2tileLayerRdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = cube2Rdd
        // 如果flag=2， 代表第二张影像的分辨率较低
        if (flag == 2) {
          val tileRatio2: Int = (math.log(resoRatio) / math.log(2)).toInt
          if (tileRatio2 != 0) {
            // 对影像进行瓦片大小重切分
            val newTileSize2: Int = 256 / math.pow(2, tileRatio2).toInt
            val cube2Retiled: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = cube2Rdd.regrid(newTileSize2)
            // 进行裁剪
            val cropExtent2: Extent = extent.reproject(crs, cube2Retiled.metadata.crs)
            cube2tileLayerRdd = cube2Retiled.crop(cropExtent2)
          }
        }

        val (_, reprojectedRdd2): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          cube2tileLayerRdd.reproject(crs, ld)

        val reprojFilter2: RDD[(SpatialKey, Tile)] = reprojectedRdd2.filter(layer => {
          val key: SpatialKey = layer._1
          val extentR: Extent = ld.mapTransform(key)
          extentR.xmin >= extent.xmin && extentR.xmax <= extent.xmax && extentR.ymin >= extent.ymin && extentR.ymax <= extent.ymax
        })

        // metadata需要添加time
        val srcMetadata2: TileLayerMetadata[SpatialKey] = reprojectedRdd2.metadata
        val newProjBounds2: Bounds[SpatialKey] = srcMetadata2.bounds
        val newMetadata2: TileLayerMetadata[SpatialKey] = TileLayerMetadata(srcMetadata2.cellType, srcMetadata2.layout, extent, srcMetadata2.crs, newProjBounds2)

        // 这里LayerMetadata直接使用reprojectRdd2的，尽管SpatialKey有负值也不影响
        val newCube2: (RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey]) = (reprojFilter2.map(t => {
          (new CubeTileKey(new SpaceKey(tms, t._1.col, t._1.row, newTileSize * t._1.col * reso, newTileSize * (t._1.col + 1) * reso, newTileSize * t._1.row * reso, newTileSize * (t._1.row + 1) * reso), new TimeKey(time1), productkey1, band1), t._2)
        }), newMetadata2)

        (newCube1, newCube2)
      }
      else {
        (cube1, cube2)
      }
    }
    else {
      (cube1, cube2)
    }
  }

  def cubeTemplate(cube1: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], cube2: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], func: (Tile, Tile) => Tile): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    var cellType: CellType = null
    val resampleTime: Long = Instant.now.getEpochSecond
    if (cube1.length == cube2.length){
      var tolcube: ArrayBuffer[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = ArrayBuffer()
      for (i <- cube1.indices) {
        for (j <- cube2.indices) {
          if (cube1(i)._1.first()._1.bandKey.bandName == cube2(j)._1.first()._1.bandKey.bandName) {
            val cubeRDD1 = cube1(i)
            val cubeRDD2 = cube2(j)
            val (cube1Reprojected, cube2Reprojected) = checkProjResoExtent(cubeRDD1, cubeRDD2)
            val tms = cube1Reprojected._1.first()._1.spaceKey.tms
            val cellSize: Double = cube1Reprojected._2.cellSize.height
            val reso: Double = cube1Reprojected._2.cellSize.resolution
            val productType = cube1Reprojected._1.first()._1.productKey.productType
            val bandName: String = cube1Reprojected._1.first()._1.bandKey.bandName
            val bandPlatform: String = cube1Reprojected._1.first()._1.bandKey.bandPlatform
            val cube1NoTime: RDD[(SpatialKey, Tile)] = cube1Reprojected._1.map(t => (new SpatialKey(t._1.spaceKey.col, t._1.spaceKey.row), t._2))
            val cube2NoTime: RDD[(SpatialKey, Tile)] = cube2Reprojected._1.map(t => (new SpatialKey(t._1.spaceKey.col, t._1.spaceKey.row), t._2))
            val rdd: RDD[(SpatialKey, (Tile, Tile))] = cube1NoTime.join(cube2NoTime)
            cellType = func(rdd.first()._2._1, rdd.first()._2._2).cellType
            tolcube.append((rdd.map(t => {
              (new CubeTileKey(new SpaceKey(tms, t._1.col, t._1.row, cellSize * t._1.col * reso, cellSize * (t._1.col + 1) * reso, cellSize * t._1.row * reso, cellSize * (t._1.row + 1) * reso), new TimeKey(resampleTime), new ProductKey("OGE", productType), new BandKey(bandName, bandPlatform)), func(t._2._1, t._2._2))
            }), TileLayerMetadata(cellType, cube1Reprojected._2.layout, cube1Reprojected._2.extent, cube1Reprojected._2.crs, cube1Reprojected._2.bounds)))
          }
        }
      }
      tolcube.toArray
    }
    else {
      throw new IllegalArgumentException("Error: 波段数量不相等")
    }
  }

  def cubeTemplate(cube: Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])], func: Tile => Tile): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    cube.map(rdd => {
      (rdd._1.map(t => (t._1, func(t._2))), rdd._2)
    })
  }

}
