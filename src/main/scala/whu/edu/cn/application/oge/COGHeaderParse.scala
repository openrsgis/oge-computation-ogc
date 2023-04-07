package whu.edu.cn.application.oge


import io.minio.{GetObjectArgs, MinioClient}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Coordinate
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.crs.ProjectedCRS
import org.opengis.referencing.operation.MathTransform
import org.opengis.referencing.operation.TransformException
import whu.edu.cn.util.SystemConstants._

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util
import scala.util.control.Breaks


object COGHeaderParse {
  var nearestZoom = 0
  private val TypeArray = Array( //"???",
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
    8) // double",8)//Double precision (8-byte) IEEE format)

  /**
   * 根据元数据查询 tile
   *
   * @param level        JSON中的level字段，前端层级
   * @param in_path      获取tile的路径
   * @param time
   * @param crs
   * @param measurement  影像的测量方式
   * @param dType
   * @param resolution
   * @param productName  产品名
   * @param query_extent 查询瓦片的矩形范围
   * @param bandCount    多波段
   * @param tileSize     瓦片尺寸
   * @return 后端瓦片
   */
  def tileQuery(level: Int, in_path: String,
                time: String, crs: String,
                measurement: String, dType: String,
                resolution: String, productName: String,
                query_extent: Array[Double],
                bandCount: Int = 1,
                tileSize: Int = 256): util.ArrayList[RawTile] = {

    val imageSize: Array[Int] = Array[Int](0, 0) // imageLength & imageWidth
    val tileByteCounts = new util.ArrayList[util.ArrayList[util.ArrayList[Integer]]]
    val geoTrans = new util.ArrayList[Double] //TODO 经纬度
    val cell = new util.ArrayList[Double]
    val tileOffsets = new util.ArrayList[util.ArrayList[util.ArrayList[Integer]]]


    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_URL)
      .credentials(MINIO_KEY, MINIO_PWD)
      .build()
    // 获取指定offset和length的"myObject"的输入流。

    val inputStream: InputStream = minioClient.getObject(
      GetObjectArgs.builder()
        .bucket("oge")
        .`object`(in_path)
        .offset(0L)
        .length(HEAD_SIZE.toLong).build()
    )
    try {
      val outStream = new ByteArrayOutputStream
      val buffer = new Array[Byte](HEAD_SIZE) //16383
      var len: Int = 0
      while ( {
        len = inputStream.read(buffer)
        len != -1
      }) outStream.write(buffer, 0, len)

      val headerByte: Array[Byte] = outStream.toByteArray
      inputStream.close()
      outStream.close()
      parse(headerByte, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount, tileSize)
      System.out.println(cell)

      getTiles(level, query_extent, crs, in_path, time, measurement, dType, resolution, productName, tileOffsets, cell, geoTrans, tileByteCounts, bandCount, tileSize)

    } finally if (inputStream != null) inputStream.close()

  }


  /**
   * 获取 tile 影像本体
   *
   * @param tile tile相关数据
   * @return
   */
  def getTileBuf(tile: RawTile): RawTile = {

    val minioClient: MinioClient = MinioClient.builder()
      .endpoint(MINIO_URL)
      .credentials(MINIO_KEY, MINIO_PWD)
      .build()

    val inputStream: InputStream = minioClient.getObject(
      GetObjectArgs.builder()
        .bucket("oge")
        .`object`(tile.getPath)
        .offset(tile.getOffset()(0))
        .length(tile.getOffset()(1) - tile.getOffset()(0)).build()
    )

    try {
      val length: Int = Math.toIntExact(tile.getOffset()(1) - tile.getOffset()(0))
      val outStream = new ByteArrayOutputStream
      val buffer = new Array[Byte](length)
      var len: Int = -1
      while ( {
        len = inputStream.read(buffer)
        len != -1
      }) outStream.write(buffer, 0, len)
      tile.setTileBuf(outStream.toByteArray)
      inputStream.close()
      outStream.close()

      tile
    } finally if (inputStream != null) inputStream.close()

  }

  /**
   * 获取 Tile 相关的一些数据，不包含tile影像本体
   *
   * @param l          json里的 level 字段，表征前端 Zoom
   * @param query_extent
   * @param crs
   * @param in_path
   * @param time
   * @param measurement
   * @param dType
   * @param resolution 数据库中的原始影像分辨率
   * @param productName
   * @return
   */
  private def getTiles(l: Int, query_extent: Array[Double],
                       crs: String, in_path: String,
                       time: String, measurement: String,
                       dType: String, resolution: String,
                       productName: String,
                       tileOffsets: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                       cell: util.ArrayList[Double],
                       geoTrans: util.ArrayList[Double],
                       tileByteCounts: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                       bandCount: Int,
                       tileSize: Int): util.ArrayList[RawTile] = {
    var level = 0
    var resolutionTMS = 0.0
    val resolutionTMSArray: Array[Double] =
      Array(
        156543.033928, /* 地图 zoom 为0时的分辨率，以下按zoom递增 */
        78271.516964,
        39135.758482,
        19567.879241,
        9783.939621,
        4891.969810,
        2445.984905,
        1222.992453,
        611.496226,
        305.748113,
        152.874057,
        76.437028,
        38.218514,
        19.109257,
        9.554629,
        4.777314,
        2.388657,
        1.194329,
        0.597164,
        0.298582,
        0.149291
      )
    val resolutionOrigin: Double = resolution.toDouble
    System.out.println("resolutionOrigin = " + resolutionOrigin) //460

    if (l == -1) level = 0
    else {
      resolutionTMS = resolutionTMSArray(l)
      System.out.println(l)
      level = Math.ceil(Math.log(resolutionTMS / resolutionOrigin) / Math.log(2)).toInt + 1
      var maxZoom = 0
      val loop = new Breaks
      loop.breakable {
        for (i <- resolutionTMSArray.indices) {
          if (Math.ceil(Math.log(resolutionTMSArray(i) / resolutionOrigin)
            / Math.log(2)).toInt + 1 == 0) {
            maxZoom = i
            loop.break()

          }
        }
      }
      System.out.println("maxZoom = " + maxZoom)
      System.out.println("java.level = " + level)
      System.out.println("tileOffsets.size() = " + tileOffsets.size) // 后端瓦片数

      // 正常情况下的换算关系
      COGHeaderParse.nearestZoom = ImageTrigger.level
      //TODO 这里我们认为数据库中金字塔的第0层对应了前端 zoom 的第10级
      //                        0 10
      //                        1 9
      //                        2 8
      //                        ...
      //                        6 4
      if (level > tileOffsets.size - 1) {
        level = tileOffsets.size - 1
        assert(maxZoom > level)
        COGHeaderParse.nearestZoom = maxZoom - level
        // throw new RuntimeException("Level is too small!");
      }
      if (level < 0) {
        level = 0
        COGHeaderParse.nearestZoom = maxZoom
        // throw new RuntimeException("Level is too big!");
      }
    }
    val lowerLeftLongitude: Double = query_extent(0)
    val lowerLeftLatitude: Double = query_extent(1)
    val upperRightLongitude: Double = query_extent(2)
    val upperRightLatitude: Double = query_extent(3)
    val pointLower = new Coordinate
    val pointUpper = new Coordinate
    pointLower.setX(lowerLeftLatitude)
    pointLower.setY(lowerLeftLongitude)
    pointUpper.setX(upperRightLatitude)
    pointUpper.setY(upperRightLongitude)
    var pointLowerReprojected = new Coordinate
    var pointUpperReprojected = new Coordinate
    var flag = false
    var flagReader = false
    try if ("EPSG:4326" == crs) {
      pointLowerReprojected = pointLower
      pointUpperReprojected = pointUpper
    }
    else {
      val crsSource: CoordinateReferenceSystem = CRS.decode("EPSG:4326")
      //System.out.println("是否为投影坐标"+(crsSource instanceof ProjectedCRS));
      //System.out.println("是否为经纬度坐标"+(crsSource instanceof GeographicCRS));
      val crsTarget: CoordinateReferenceSystem = CRS.decode(crs)
      if (crsTarget.isInstanceOf[ProjectedCRS]) flag = true
      val transform: MathTransform = CRS.findMathTransform(crsSource, crsTarget)
      JTS.transform(pointLower, pointLowerReprojected, transform)
      JTS.transform(pointUpper, pointUpperReprojected, transform)
    }
    catch {
      case e@(_: FactoryException | _: TransformException) =>
        e.printStackTrace()
    }
    val pMin = new Array[Double](2)
    val pMax = new Array[Double](2)
    pMin(0) = pointLowerReprojected.getX
    pMin(1) = pointLowerReprojected.getY
    pMax(0) = pointUpperReprojected.getX
    pMax(1) = pointUpperReprojected.getY
    // 图像范围
    // 东西方向空间分辨率  --->像素宽度
    val w_src: Double = cell.get(0)
    // 南北方向空间分辨率 ---> 像素高度 // TODO
    val h_src: Double = cell.get(1)
    // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
    var xMin = .0
    var yMax = .0
    if (flag) {
      xMin = geoTrans.get(3)
      yMax = geoTrans.get(4)
    }
    else {
      xMin = geoTrans.get(4)
      yMax = geoTrans.get(3)
    }
    val tileSearch = new util.ArrayList[RawTile]
    //int l = 0;
    /*        if ("MOD13Q1_061".equals(productName)) {
                flagReader = true;
            }
            if ("LJ01_L2".equals(productName)) {
                flagReader = true;
            }
            if ("ASTER_GDEM_DEM30".equals(productName)) {
                flagReader = true;
            }
            if ("GPM_Precipitation_China_Month".equals(productName)) {
                flagReader = true;
            }*/


    productName match {
      case "MOD13Q1_061" =>
      case "LJ01_L2" =>
      case "ASTER_GDEM_DEM30" =>
      case "GPM_Precipitation_China_Month" =>
        flagReader = true
    }
    //if ("LC08_L1TP_C01_T1".equals(productName)) {
    //    l = 2;
    //}
    //if ("LE07_L1TP_C01_T1".equals(productName)) {
    //    l = 0;
    //计算目标影像的左上和右下图上坐标
    var p_left = 0
    var p_right = 0
    var p_lower = 0
    var p_upper = 0
    if (flag) {
      p_left = ((pMin(0) - xMin) / (tileSize * w_src * Math.pow(2, level).toInt)).toInt
      p_right = ((pMax(0) - xMin) / (tileSize * w_src * Math.pow(2, level).toInt)).toInt
      p_lower = ((yMax - pMax(1)) / (tileSize * h_src * Math.pow(2, level).toInt)).toInt
      p_upper = ((yMax - pMin(1)) / (tileSize * h_src * Math.pow(2, level).toInt)).toInt
    }
    else {
      p_lower = ((pMin(1) - yMax) / (tileSize * w_src * Math.pow(2, level).toInt)).toInt
      p_upper = ((pMax(1) - yMax) / (tileSize * w_src * Math.pow(2, level).toInt)).toInt
      p_left = ((xMin - pMax(0)) / (tileSize * h_src * Math.pow(2, level).toInt)).toInt
      p_right = ((xMin - pMin(0)) / (tileSize * h_src * Math.pow(2, level).toInt)).toInt
    }
    var pCoordinate: Array[Int] = null
    var srcSize: Array[Double] = null
    var pRange: Array[Double] = null
    if (!flagReader) {
      pCoordinate = Array[Int](p_lower, p_upper, p_left, p_right)
      srcSize = Array[Double](w_src, h_src)
      pRange = Array[Double](xMin, yMax)
    }
    else {
      srcSize = Array[Double](h_src, w_src)
      pRange = Array[Double](yMax, xMin)
      pCoordinate = Array[Int](p_left, p_right, p_lower, p_upper)
    }
    for (i <- Math.max(pCoordinate(0), 0) to (
      if (pCoordinate(1) >= tileOffsets.get(level).size / bandCount)
        tileOffsets.get(level).size / bandCount - 1
      else pCoordinate(1))
         ) {
      for (j <- Math.max(pCoordinate(2), 0) to (
        if (pCoordinate(3) >= tileOffsets.get(level).get(i).size)
          tileOffsets.get(level).get(i).size - 1
        else pCoordinate(3))
           ) {
        for (k <- 0 until bandCount) {
          val t = new RawTile
          t.setOffset(
            tileOffsets.get(level).get(i).get(j).toLong,
            tileByteCounts.get(level).get(i).get(j) + t.getOffset()(0)
          )


          t.setLngLatBottomLeft(
            j * (tileSize * srcSize(0) * Math.pow(2, level).toInt) + pRange(0),
            (i + 1) * (tileSize * -srcSize(1) * Math.pow(2, level).toInt) + pRange(1)
          )
          t.setLngLatUpperRight(
            (j + 1) * (tileSize * srcSize(0) * Math.pow(2, level).toInt) + pRange(0),
            i * (tileSize * -srcSize(1) * Math.pow(2, level).toInt) + pRange(1)
          )
          t.setRotation(geoTrans.get(5))
          t.setResolution(w_src * Math.pow(2, level).toInt)
          t.setRowCol(i, j)
          t.setLevel(level)
          t.setPath(in_path)
          t.setTime(time)
          t.setMeasurement(measurement)
          t.setCrs(crs.replace("EPSG:", "").toInt)
          t.setDataType(dType)
          t.setProduct(productName)
          tileSearch.add(t)
        }
      }
    }
    tileSearch
  }

  /**
   * 解析参数，并修改一些数据
   *
   * @param header
   * @param tileOffsets
   * @param cell
   * @param geoTrans
   * @param tileByteCounts
   * @param imageSize 图像尺寸
   */
  private def parse(header: Array[Byte],
                    tileOffsets: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                    cell: util.ArrayList[Double],
                    geoTrans: util.ArrayList[Double],
                    tileByteCounts: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                    imageSize: Array[Int],
                    bandCount: Int,
                    tileSize: Int): Unit = {
    var pIFD: Int = getIntII(header, 4, 4, tileSize) // DecodeIFH
    // DecodeIFD
    while ( {
      pIFD != 0
    }) {
      val DECount: Int = getIntII(header, pIFD, 2, tileSize)
      pIFD += 2
      for (i <- 0 until DECount) { // DecodeDE
        val TagIndex: Int = getIntII(header, pIFD, 2, tileSize)
        val TypeIndex: Int = getIntII(header, pIFD + 2, 2, tileSize)
        val Count: Int = getIntII(header, pIFD + 4, 4, tileSize)
        //先找到数据的位置
        var pData: Int = pIFD + 8
        val totalSize: Int = TypeArray(TypeIndex) * Count
        if (totalSize > 4) pData = getIntII(header, pData, 4, tileSize)
        //再根据Tag把值读出并存起来，GetDEValue
        getDEValue(TagIndex, TypeIndex, Count, pData, header, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount, tileSize)
        // 之前的
        pIFD += 12

        pIFD = getIntII(header, pIFD, 4, tileSize)
      } // end for
    } // end while
  }

  private def getDEValue(tagIndex: Int, typeIndex: Int,
                         count: Int, pData: Int, header: Array[Byte],
                         tileOffsets: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                         cell: util.ArrayList[Double],
                         geoTrans: util.ArrayList[Double],
                         tileByteCounts: util.ArrayList[util.ArrayList[util.ArrayList[Integer]]],
                         imageSize: Array[Int],
                         bandCount: Int,
                         tileSize: Int): Unit = {
    val typeSize: Int = TypeArray(typeIndex)
    tagIndex match {
      case tileSize => //ImageWidth

        imageSize(1) = getIntII(header, pData, typeSize, tileSize)

      case 257 => //ImageLength

        imageSize(0) = getIntII(header, pData, typeSize, tileSize)

      case 258 =>
        val BitPerSample: Int = getIntII(header, pData, typeSize, tileSize)

      case 286 => //XPosition

        val xPosition: Int = getIntII(header, pData, typeSize, tileSize)

      case 287 => //YPosition

        val yPosition: Int = getIntII(header, pData, typeSize, tileSize)

      case 324 => //tileOffsets

        // getOffsetArray(pData, typeSize, header, tileOffsets, imageSize, bandCount)

        val StripOffsets = new util.ArrayList[util.ArrayList[Integer]]
        for (k <- 0 until bandCount) {
          for (i <- 0 until (imageSize(0) / tileSize) + 1) {
            val Offsets = new util.ArrayList[Integer]
            for (j <- 0 until (imageSize(1) / tileSize) + 1) {
              Offsets.add(
                getIntII(header,
                  pData + (i * ((imageSize(1) / tileSize) + 1) + j) * typeSize, typeSize, tileSize)
              )
            }
            StripOffsets.add(Offsets)
          }
        }
        tileOffsets.add(StripOffsets)

      case 325 => //tileByteCounts

        //  getTileBytesArray(pData, typeSize, header, tileByteCounts, imageSize, bandCount)
        val stripBytes = new util.ArrayList[util.ArrayList[Integer]]
        for (k <- 0 until bandCount) {
          for (i <- 0 until (imageSize(0) / tileSize) + 1) {
            val tileBytes = new util.ArrayList[Integer]
            for (j <- 0 until (imageSize(1) / tileSize) + 1) {
              tileBytes.add(
                getIntII(header,
                  pData + (i * ((imageSize(1) / tileSize) + 1) + j) * typeSize, typeSize, tileSize)
              )
            }
            stripBytes.add(tileBytes)
          }
        }
        tileByteCounts.add(stripBytes)


      case 33550 => //  cellWidth

        // getDoubleCell
        for (i <- 0 until count)
          cell.add(getDouble(header, pData + i * typeSize, typeSize, tileSize))


      case 33922 => //geoTransform

        // getDoubleTrans
        for (i <- 0 until count)
          geoTrans.add(getDouble(header, pData + i * typeSize, typeSize, tileSize))

      case 34737 => //Spatial reference

        val crs: String = getString(header, pData, typeSize * count)


    }
  }

  private def getIntII(pd: Array[Byte], startPos: Int, Length: Int, tileSize: Int): Int = {
    var value = 0
    for (i <- 0 until Length) {
      value |= pd(startPos + i) << i * 8
      if (value < 0) value += tileSize << i * 8
    }
    value
  }

  private def getDouble(pd: Array[Byte], startPos: Int, Length: Int, tileSize: Int): Double = {
    var value = 0
    for (i <- 0 until Length) {
      value |= (pd(startPos + i) & 0xff).toLong << (8 * i)
      if (value < 0) value += tileSize.toLong << i * 8
    }
    java.lang.Double.longBitsToDouble(value)
  }

  private def getString(pd: Array[Byte], startPos: Int, Length: Int): String = {
    val dd = new Array[Byte](Length)
    System.arraycopy(pd, startPos, dd, 0, Length)
    new String(dd)
  }


}


