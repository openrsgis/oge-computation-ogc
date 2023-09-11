package whu.edu.cn.geocube.core.raster.ard.gaofen1

import java.io.File
import java.lang.RuntimeException
import scala.util.control.Breaks._
import geotrellis.raster.{FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType}
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import org.apache.spark.SparkContext
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import sys.process._

/**
 * Preprocessing for Gaofen-1 ARD.
 * Now provide RPC geometric correction, radiometric calibration and atmospheric correction.
 */
object Preprocessing{
  /**
   * Encompass RPC geometric correction, Radiometric calibration and Atmospheric correction.
   * Using GDAL is faster than using Geotrellis.
   * DstImage nodata DN value is 65535.
   *
   * @param sc A SparkContext
   * @param inputDirs Input Gaofen-1 datasets directory
   */
  def run(implicit sc: SparkContext,
                  inputDirs:Array[String]):Unit = {
    for(inputDir <- inputDirs){

      //absolute path of raw image, RPC correction image, and raw image xml file
      //for windows system
//      val getImageXMLPath = (dir: String) => {
//        val arrStr = dir.split("\\\\")
//        if(dir.contains("PMS1")) {
//          Array(dir + "\\GeoCor_" + arrStr(arrStr.length - 1) + "-MSS1.tiff",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-MSS1.xml",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-MSS1.tiff",
//            dir + "\\GeoCor_" + arrStr(arrStr.length - 1) + "-PAN1.tiff",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-PAN1.xml",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-PAN1.tiff"
//          )
//        }else if (dir.contains("PMS2")){
//          Array(dir + "\\GeoCor_" + arrStr(arrStr.length - 1) + "-MSS2.tiff",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-MSS2.xml",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-MSS2.tiff",
//            dir + "\\GeoCor_" + arrStr(arrStr.length - 1) + "-PAN2.tiff",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-PAN2.xml",
//            dir + "\\" + arrStr(arrStr.length - 1) + "-PAN2.tiff"
//          )
//        } else if (dir.contains("WFV")){
//          Array(dir + "\\GeoCor_" + arrStr(arrStr.length - 1) + ".tiff",
//            dir + "\\" + arrStr(arrStr.length - 1) + ".xml",
//            dir + "\\" + arrStr(arrStr.length - 1) + ".tiff"
//          )
//        } else {
//          throw new RuntimeException("Wrong instruments for Gaofen-1!")
//        }
//
//      }

      //for linux system
      val getImageXMLPath = (dir: String) => {
        val arrStr = dir.split("/")
        if (dir.contains("PMS2")){
          Array(dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-MSS2.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-MSS2.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-MSS2.tiff",
            dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-PAN2.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN2.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN2.tiff"
          )
        }
        else if(dir.contains("PMS1")) {
          Array(dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-MSS1.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-MSS1.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-MSS1.tiff",
            dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-PAN1.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN1.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN1.tiff"
          )
        }else if(dir.contains("PMS")) {
          Array(dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-MUX.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-MUX.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-MUX.tiff",
            dir + "/GeoCor_" + arrStr(arrStr.length - 1) + "-PAN.tiff",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN.xml",
            dir + "/" + arrStr(arrStr.length - 1) + "-PAN.tiff"
          )
        } else if (dir.contains("WFV")){
          Array(dir + "/GeoCor_" + arrStr(arrStr.length - 1) + ".tiff",
            dir + "/" + arrStr(arrStr.length - 1) + ".xml",
            dir + "/" + arrStr(arrStr.length - 1) + ".tiff"
          )
        } else {
          throw new RuntimeException("Wrong instruments for Gaofen-1!")
        }
      }

      //path of RPC correction image, xml, and raw image
      val datasetPath = getImageXMLPath(inputDir)(0)
      val xmlPath = getImageXMLPath(inputDir)(1)
      val rawDatasetPath = getImageXMLPath(inputDir)(2)
      println(datasetPath)
      println(rawDatasetPath)
      //RPC correction using gdal scripts to perform and nodata value is set as 65535
      val rpcCommand = "gdalwarp -dstnodata 65535 -overwrite -rpc " + rawDatasetPath + " " + datasetPath
      rpcCommand.!

      println(rawDatasetPath + ": ARD product is being generated ...")

      //get metadata from xml file
      val parser = Parser(xmlPath)
      parser.collectMetadata()
      val gainParams = parser.gainParams
      val offsetParams = parser.offsetParams
      val esun = parser.esun
      val date = parser.date
      val sz = parser.solarZenith
      val distance = earthSunDistance(julianDay(date(0), date(1), date(2)))
      val (minDN, maxDN) = (0.0f, 1023.0f)

      //Radiometric calibration and Atmospheric correction
      gdal.AllRegister()
      val srcDataset = gdal.Open(datasetPath, gdalconstConstants.GA_ReadOnly)
      if (srcDataset == null) {
        System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo())
        System.err.println(gdal.GetLastErrorMsg())
        System.exit(1)
      }
      val driver = srcDataset.GetDriver()
      val imgWidth = srcDataset.getRasterXSize
      val imgHeight = srcDataset.getRasterYSize
      val nBandCount = srcDataset.getRasterCount
      val imgProjection = srcDataset.GetProjectionRef
      val imgGeoTrans = new Array[Double](6)
      srcDataset.GetGeoTransform(imgGeoTrans)

      val tmpValue:Array[Float] = Array.fill(imgWidth * imgHeight)(Float.NaN)

      val dstDataSet = driver.Create(inputDir + "/GDAL_SpecCal_" + new File(datasetPath).getName, imgWidth, imgHeight, 4, 6)
      dstDataSet.SetGeoTransform(imgGeoTrans)
      dstDataSet.SetProjection(imgProjection)
      for(i <- 0 until nBandCount){
        /*val dstDataSet = driver.Create(inputDir + "/B"  + (i + 1) + "_GDAL_SpecCal_" + new File(datasetPath).getName, imgWidth, imgHeight, 1, 6)
        dstDataSet.SetGeoTransform(imgGeoTrans)
        dstDataSet.SetProjection(imgProjection)*/

        srcDataset.GetRasterBand(i + 1).ReadRaster(0, 0, imgWidth, imgHeight, tmpValue)
        //val (minDN, maxDN) = calMinMaxDN(tmpValue)
        val lhazel = calLhazel(minDN, maxDN, gainParams(i), offsetParams(i), sz, esun(i), distance)

        for(index <- 0 until imgWidth * imgHeight){
          tmpValue(index) = tmpValue(index) * gainParams(i) + offsetParams(i)
          tmpValue(index) = (Math.PI * Math.pow(distance, 2) * (tmpValue(index) - lhazel) / esun(i) * Math.pow(Math.cos(sz), 2)).toFloat
        }

        dstDataSet.GetRasterBand(i + 1).WriteRaster(0, 0, imgWidth, imgHeight, tmpValue)
        val nodataValue = Math.PI * Math.pow(distance, 2) * (65535.0 * gainParams(i) + offsetParams(i) - lhazel) / esun(i) * Math.pow(Math.cos(sz), 2)
        dstDataSet.GetRasterBand(i + 1).SetNoDataValue(nodataValue)

        //dstDataSet.delete()
      }
      dstDataSet.delete()
      srcDataset.delete()

      if(getImageXMLPath(inputDir).length > 3) {
        val panDatasetPath = getImageXMLPath(inputDir)(3)
        val panXmlPath = getImageXMLPath(inputDir)(4)
        val panRawDatasetPath = getImageXMLPath(inputDir)(5)

        val rpcCommand = "gdalwarp -dstnodata 65535 -overwrite -rpc " + panRawDatasetPath + " " + panDatasetPath
        rpcCommand.!

        println(panRawDatasetPath + ": ARD product is being generated ...")
        val parser = Parser(panXmlPath)
        parser.collectMetadata()
        val gainParams = parser.gainParams
        val offsetParams = parser.offsetParams
        val esun = parser.esun
        val date = parser.date
        val sz = parser.solarZenith
        val distance = earthSunDistance(julianDay(date(0), date(1), date(2)))
        val (minDN, maxDN) = (0.0f, 1023.0f)

        gdal.AllRegister()
        val srcDataset = gdal.Open(panDatasetPath, gdalconstConstants.GA_ReadOnly)
        if (srcDataset == null) {
          System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo())
          System.err.println(gdal.GetLastErrorMsg())
          System.exit(1)
        }
        val driver = srcDataset.GetDriver()
        val imgWidth = srcDataset.getRasterXSize
        val imgHeight = srcDataset.getRasterYSize
        val nBandCount = srcDataset.getRasterCount
        val imgProjection = srcDataset.GetProjectionRef
        val imgGeoTrans = new Array[Double](6)
        srcDataset.GetGeoTransform(imgGeoTrans)


        val tmpValue:Array[Float] = Array.fill(imgWidth * imgHeight)(Float.NaN)

        val dstDataSet = driver.Create(inputDir + "/GDAL_SpecCal_" + new File(panDatasetPath).getName, imgWidth, imgHeight, 1, 6)
        dstDataSet.SetGeoTransform(imgGeoTrans)
        dstDataSet.SetProjection(imgProjection)
        for(i <- 0 until nBandCount){
          /*val dstDataSet = driver.Create(inputDir + "/B"  + (i + 1) + "_GDAL_SpecCal_" + new File(datasetPath).getName, imgWidth, imgHeight, 1, 6)
          dstDataSet.SetGeoTransform(imgGeoTrans)
          dstDataSet.SetProjection(imgProjection)*/

          srcDataset.GetRasterBand(i + 1).ReadRaster(0, 0, imgWidth, imgHeight, tmpValue)
          //val (minDN, maxDN) = calMinMaxDN(tmpValue)
          val lhazel = calLhazel(minDN, maxDN, gainParams(i), offsetParams(i), sz, esun(i), distance)

          for(index <- 0 until imgWidth * imgHeight){
            tmpValue(index) = tmpValue(index) * gainParams(i) + offsetParams(i)
            tmpValue(index) = (Math.PI * Math.pow(distance, 2) * (tmpValue(index) - lhazel) / esun(i) * Math.pow(Math.cos(sz), 2)).toFloat
          }

          dstDataSet.GetRasterBand(i + 1).WriteRaster(0, 0, imgWidth, imgHeight, tmpValue)
          val nodataValue = Math.PI * Math.pow(distance, 2) * (65535.0 * gainParams(i) + offsetParams(i) - lhazel) / esun(i) * Math.pow(Math.cos(sz), 2)
          dstDataSet.GetRasterBand(i + 1).SetNoDataValue(nodataValue)

          //dstDataSet.delete()
        }
        dstDataSet.delete()
        srcDataset.delete()
      }
      //gdal.GDALDestroyDriverManager()
    }

  }

  //get julian day
  def julianDay(year: Float, month: Float, day: Float): Float = {
    day - 32075 + (1461 * (year + 4800 + (month - 14) / 12) / 4) + (367 * ((month - 2 - (month - 14) / 12) / 12)) - ((3 * (year + 4900 + (month - 14) / 12) / 100) / 4)
  }

  //calculate distance between earth and sun
  def earthSunDistance(julianDay: Float): Float = (1 - 0.01674f * Math.cos(0.9856f * (julianDay - 4) * Math.PI / 180)).toFloat

  //calculate minimum and maximum raw DN value
  def calMinMaxDN(srcValue:Array[Float]):(Float, Float) = {
    var (minDN, maxDN) = ( Float.MaxValue, Float.MinValue)
    srcValue.foreach { x =>
      breakable{
        if (x == 65535) break()
        if (x > maxDN) maxDN = x
        if (x < minDN) minDN = x
      }
    }
    (minDN, maxDN)
  }

  //get hazel
  def calLhazel(minDN: Float, maxDN: Float, gain: Float, bias: Float, sz: Float, esun: Float, distance: Float): Float = {
    val lmin = bias + gain * minDN
    val lmax = bias + gain * maxDN
    val lhazel = (lmin + minDN * (lmax - lmin) / maxDN) - (0.01f * esun * Math.pow(Math.cos(sz), 2) / (Math.PI * Math.pow(distance, 2)))
    lhazel.toFloat
  }
}

