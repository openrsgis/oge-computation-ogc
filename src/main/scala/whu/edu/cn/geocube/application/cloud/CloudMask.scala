package whu.edu.cn.geocube.application.cloud

import geotrellis.raster.{ByteArrayTile, DoubleArrayTile, DoubleCellType, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import scala.io.StdIn

/**
 * Cloud mask for Gaofen-1.
 * Under developing.
 * */
object CloudMask {
  def main(args:Array[String]):Unit = {
    cloudMask("E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\GeometryCorrection\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125_REPROJECT2.tiff",
      "E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\cloud.tiff")

    /*cloudMask(Array("E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\B1_GDAL_SpecCal_GeoCor_GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125_REPROJECT2.tiff",
      "E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\B2_GDAL_SpecCal_GeoCor_GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125_REPROJECT2.tiff",
      "E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\B3_GDAL_SpecCal_GeoCor_GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125_REPROJECT2.tiff",
      "E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\B4_GDAL_SpecCal_GeoCor_GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125_REPROJECT2.tiff"),
      "E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125\\cloud.tiff")*/

    println("Hit to exit!")
    StdIn.readLine()
  }

  def cloudMask(inputPath:String, outputPath:String): Unit = {
    val geoTiff: MultibandGeoTiff = GeoTiffReader.readMultiband(inputPath)
    val dataBand: Array[Array[Double]] = Array(Array(), Array(), Array(), Array())
    for ( band <- 0 to 3 ){
      dataBand(band) = geoTiff.tile.band(band).convert(DoubleCellType).toArrayDouble()
    }

    val rows = geoTiff.rows
    val cols = geoTiff.cols

    val maskData:Array[Array[Byte]] = Array.fill(rows, cols)(0)

    var DVGreenRed:Double = 0.0
    var DVMaxMin4:Double = 0.0
    var DVMaxMin:Double = 0.0
    var DVMaxMinRGB = 0.0
    val imgSize = cols * rows

    for(index <- 0 until imgSize){
      val i = index / cols
      val j = index % cols
      if (dataBand(2)(index) > 0 && dataBand(0)(index) > 0){
        DVGreenRed = NDIndex (dataBand(1)(index), dataBand(2)(index))
        DVMaxMin4 = Max(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMin = Max(0.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(1023.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMinRGB = Max(0.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))/Min(1023.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))
      }
      if ( dataBand(0)(index).isNaN ) maskData(i)(j) = Byte.MinValue
      else if ( (dataBand(0)(index) > dataBand(1)(index))
        && (dataBand(1)(index) > dataBand(2)(index))
        && (dataBand(1)(index) > dataBand(3)(index))
        && (-0.038 < NDIndex(dataBand(3)(index), dataBand(2)(index)) && NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.0683)
        && (-0.0245 < NDIndex(dataBand(1)(index), dataBand(3)(index)) && NDIndex(dataBand(1)(index), dataBand(3)(index)) < 0.1054) ) maskData(i)(j) = 1
      else if ( (DVMaxMin4 < 1.15 || (1.15 <= DVMaxMin4 && DVMaxMin < 1.09 && (-0.025 < DVGreenRed && DVGreenRed < 0.04) && DVMaxMinRGB < 1.28))
        && !(dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && !(dataBand(0)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand(2)(index) && dataBand(1)(index) > dataBand(3)(index)) ) maskData(i)(j) = 2
      else if ( (dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && (dataBand(0)(index) > 0.40)
        && (NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.057)
        && (NDIndex(dataBand(3)(index), dataBand(0)(index)) < 0.136)
        && (NDIndex(dataBand(1)(index), dataBand(0)(index)) < 0.04)) maskData(i)(j) = 4
      else maskData(i)(j) = 0
    }

    var band0 = 0.0
    var band1 = 0.0
    var band2 = 0.0
    var band3 = 0.0
    var countCloud = 0
    var k = 0
    val cloudWindow = Array.ofDim[Byte](25)

    for(i <- 0 until rows; j <- 0 until cols){
      band0 = dataBand(0)(i * cols + j)
      band1 = dataBand(1)(i * cols + j)
      band2 = dataBand(2)(i * cols + j)
      band3 = dataBand(3)(i * cols + j)
      if (maskData(i)(j) == 0
        && (band3 > band2 && band2 > band1 && band1 > band0)
        && Brightness(band0, band1, band2, band3) > 0.25
        && NDIndex(band3, band2) < 0.06) {
        k = 0
        countCloud = 0
        for (x <- -2 to 2; y <- -2 to 2) {
          if ( (i + x >= 0) && (i + x) < rows &&(j + y) >= 0 && (j + y) < cols)
            cloudWindow(k) = maskData(x+i)(y+j)
          k = k + 1
        }
        cloudWindow.foreach( a => {
          if (a > 0)
            countCloud = countCloud + 1
        })
        if (countCloud >= 9) maskData(i)(j) = 3
        else maskData(i)(j) = Byte.MinValue
      }
    }

    val tiffMaskFinalData = Array.ofDim[Byte](rows * cols)

    for(i <- 0 until rows; j <- 0 until cols){
      band0 = dataBand(0)(i * cols + j)
      band1 = dataBand(1)(i * cols + j)
      band2 = dataBand(2)(i * cols + j)
      band3 = dataBand(3)(i * cols + j)
      k = 0
      countCloud = 0
      for (x <- -2 to 2; y <- -2 to 2) {
        if ((i + x >= 0) && (i + x) < rows && (j + y) >= 0 && (j + y) < cols)
          cloudWindow(k) = maskData(x+i)(y+j)
        k = k + 1
      }
      cloudWindow.foreach(a => {
        if (a > 0)
          countCloud = countCloud + 1
      })
      if (countCloud >= 10) tiffMaskFinalData(i * cols + j) = 1
      else tiffMaskFinalData(i * cols + j) = 0
    }

    val Tiff = ByteArrayTile(tiffMaskFinalData, cols, rows)
    SinglebandGeoTiff(Tiff, geoTiff.extent, geoTiff.crs).write(outputPath)
  }
  
  def cloudMask(inputPath:Array[String], outputPath:String): Unit = {
    val geoTiff = GeoTiffReader.readSingleband(inputPath(0))
    val rows = geoTiff.rows
    val cols = geoTiff.cols
    val dataBand: Array[Array[Double]] = Array(Array(), Array(), Array(), Array())
    for ( band <- 0 to 3 ){
      dataBand(band) = GeoTiffReader.readSingleband(inputPath(band)).tile.convert(DoubleCellType).toArrayDouble()
    }

    val maskData:Array[Array[Double]] = Array.fill(rows, cols)(0)

    var DVGreenRed:Double = 0.0
    var DVMaxMin4:Double = 0.0
    var DVMaxMin:Double = 0.0
    var DVMaxMinRGB = 0.0
    val imgSize = cols * rows

    for(index <- 0 until imgSize){
      val i = index / cols
      val j = index % cols
      if (dataBand(2)(index) > 0 && dataBand(0)(index) > 0){
        DVGreenRed = NDIndex (dataBand(1)(index), dataBand(2)(index))
        DVMaxMin4 = Max(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMin = Max(0.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(1023.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMinRGB = Max(0.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))/Min(1023.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))
      }
      if ( dataBand(0)(index).isNaN ) maskData(i)(j) = -9999
      else if ( (dataBand(0)(index) > dataBand(1)(index))
        && (dataBand(1)(index) > dataBand(2)(index))
        && (dataBand(1)(index) > dataBand(3)(index))
        && (-0.038 < NDIndex(dataBand(3)(index), dataBand(2)(index)) && NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.0683)
        && (-0.0245 < NDIndex(dataBand(1)(index), dataBand(3)(index)) && NDIndex(dataBand(1)(index), dataBand(3)(index)) < 0.1054) ) maskData(i)(j) = 1
      else if ( (DVMaxMin4 < 1.15 || (1.15 <= DVMaxMin4 && DVMaxMin < 1.09 && (-0.025 < DVGreenRed && DVGreenRed < 0.04) && DVMaxMinRGB < 1.28))
        && !(dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && !(dataBand(0)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand(2)(index) && dataBand(1)(index) > dataBand(3)(index)) ) maskData(i)(j) = 2
      else if ( (dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && (dataBand(0)(index) > 0.40)
        && (NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.057)
        && (NDIndex(dataBand(3)(index), dataBand(0)(index)) < 0.136)
        && (NDIndex(dataBand(1)(index), dataBand(0)(index)) < 0.04)) maskData(i)(j) = 4
      else maskData(i)(j) = 0
    }

    var band0 = 0.0
    var band1 = 0.0
    var band2 = 0.0
    var band3 = 0.0
    var countCloud = 0
    var k = 0
    val cloudWindow = Array.ofDim[Double](25)

    for(i <- 0 until rows; j <- 0 until cols){
      band0 = dataBand(0)(i * cols + j)
      band1 = dataBand(1)(i * cols + j)
      band2 = dataBand(2)(i * cols + j)
      band3 = dataBand(3)(i * cols + j)
      if (maskData(i)(j) == 0
        && (band3 > band2 && band2 > band1 && band1 > band0)
        && Brightness(band0, band1, band2, band3) > 0.25
        && NDIndex(band3, band2) < 0.06) {
        k = 0
        countCloud = 0
        for (x <- -2 to 2; y <- -2 to 2) {
          if ( (i + x >= 0) && (i + x) < rows &&(j + y) >= 0 && (j + y) < cols)
            cloudWindow(k) = maskData(x+i)(y+j)
          k = k + 1
        }
        cloudWindow.foreach( a => {
          if (a > 0)
            countCloud = countCloud + 1
        })
        if (countCloud >= 9) maskData(i)(j) = 3
        else maskData(i)(j) = -9999
      }
    }

    val tiffMaskFinalData = Array.ofDim[Double](rows * cols)

    for(i <- 0 until rows; j <- 0 until cols){
      band0 = dataBand(0)(i * cols + j)
      band1 = dataBand(1)(i * cols + j)
      band2 = dataBand(2)(i * cols + j)
      band3 = dataBand(3)(i * cols + j)
      k = 0
      countCloud = 0
      for (x <- -2 to 2; y <- -2 to 2) {
        if ((i + x >= 0) && (i + x) < rows && (j + y) >= 0 && (j + y) < cols)
          cloudWindow(k) = maskData(x+i)(y+j)
        k = k + 1
      }
      cloudWindow.foreach(a => {
        if (a > 0)
          countCloud = countCloud + 1
      })
      if (countCloud >= 10) tiffMaskFinalData(i * cols + j) = 1
      else tiffMaskFinalData(i * cols + j) = 0
    }

    val Tiff = DoubleArrayTile(tiffMaskFinalData, cols, rows).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiff.extent, geoTiff.crs).write(outputPath)
  }



  def NDIndex (B1:Double, B2: Double): Float = {
    ((B1-B2)/(B1+B2)).toFloat
  }

  def Min (B1:Double, B2: Double, B3:Double, B4: Double):Double = {
    var min = B1
    if (B2 < min) min = B2
    if (B3 < min) min = B3
    if (B4 < min) min = B4
    min
  }

  def Max (B1:Double, B2: Double, B3:Double, B4: Double):Double = {
    var max = B1
    if (max < B2) max = B2
    if (max < B3) max = B3
    if (max < B4) max = B4
    max
  }

  def Brightness (B1:Double, B2: Double, B3:Double, B4: Double):Float = {
    ((B1 + B2 + B3 + B4)/4).toFloat
  }


}

