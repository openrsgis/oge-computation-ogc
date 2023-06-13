package whu.edu.cn.geocube.core.raster.ard.landsat45

import java.io.File
import java.util.regex.Pattern

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType}
import org.apache.spark.SparkContext
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants

/**
 * RadiometricCalibration + AtmosphericCorrection
 */
object SpectralCalibration{
  /**
   * Paralleling spectralCalibration using GDAL, the parallel granularity is single image.
   * Faster than using geotrellis.
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param earthSunDistance Earth sun distance, obtained from meta txt
   * @param solarZenith, sun elevation can be obtained from meta txt, and solarZenith = 90 - sunElevation
   * @param esun Obtained from RS Authorized Institution
   * @param outputDir Output directory
   */
  def runWithGdal(implicit sc: SparkContext,
          datasetWithRescalingParams:Array[(String, Array[Float])],
          earthSunDistance:Float,
          solarZenith: Float,
          esun:Array[Float],
          outputDir:String):Unit = {
    val rdd = sc.parallelize(datasetWithRescalingParams, datasetWithRescalingParams.length)
    rdd.foreach { x =>
      val inputPath = x._1
      //Get band index
      val array = new File(inputPath).getName.split("_")
      val str = array(array.length - 1)
      val pattern = Pattern.compile("\\d+")
      val matcher = pattern.matcher(str)
      var index = 0
      while (matcher.find())
        index = matcher.group(0).toInt

      //There is no esun parameters for band6
      if(index != 6 ){
        val multParam = x._2(0)
        val addParam = x._2(1)
        val minDN = x._2(2)
        val maxDN = x._2(3)

        val distance = earthSunDistance
        val sz = (solarZenith * Math.PI / 180).toFloat

        if(inputPath.contains("B5.TIF")){
          gdal.AllRegister()
          val srcDataset = gdal.Open(inputPath, gdalconstConstants.GA_ReadOnly)
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

          val dstDataSet = driver.Create(outputDir + "\\GDAL_" + new File(inputPath).getName, imgWidth, imgHeight, nBandCount, 6)
          dstDataSet.SetGeoTransform(imgGeoTrans)
          dstDataSet.SetProjection(imgProjection)

          val srcValue:Array[Int] = Array.fill(imgWidth * imgHeight)(0)

          srcDataset.GetRasterBand(1).ReadRaster(0, 0, imgWidth, imgHeight, srcValue)
          //val (minDN, maxDN) = calMaxMinDN(srcValue)
          val lhazel = calLhazel(minDN, maxDN, multParam, addParam, sz, esun(index - 1), distance)

          val dstValue = srcValue.map{x =>
            if(x != 0){
              val lλ_RadiometricCalibration = x * multParam + addParam
              val ρ_AtmosphericCorrection = Math.PI * Math.pow(distance, 2) * (lλ_RadiometricCalibration - lhazel) / esun(index - 1) * Math.pow(Math.cos(sz), 2)
              ρ_AtmosphericCorrection
            }
            else
              Float.NaN
          }
          dstDataSet.GetRasterBand(1).WriteRaster(0, 0, imgWidth, imgHeight,dstValue)
          val nodataValue = Math.PI * Math.pow(distance, 2) * (0.0 * multParam + addParam - lhazel) / esun(index - 1) * Math.pow(Math.cos(sz), 2)
          dstDataSet.GetRasterBand(1).SetNoDataValue(Float.NaN)

          dstDataSet.delete()
          srcDataset.delete()
          gdal.GDALDestroyDriverManager()
        }
      }


    }
  }

  /**
   * Paralleling spectralCalibration using Geotrellis, the parallel granularity is single image.
   * Slower than using gdal.
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param earthSunDistance Earth sun distance, which can be obtained from meta txt
   * @param solarZenith Sun elevation can be obtained from meta txt, and solarZenith = 90 - sunElevation
   * @param esun Obtained from RS Authorized Institution
   * @param outputDir Output directory
   */
  def runWithGeotrellis(implicit sc: SparkContext,
          datasetWithRescalingParams:Array[(String, Array[Float])],
          earthSunDistance:Float,
          solarZenith: Float,
          esun:Array[Float],
          outputDir:String):Unit = {
    val rdd = sc.parallelize(datasetWithRescalingParams, datasetWithRescalingParams.length)
    rdd.foreach { x =>
      val inputPath = x._1
      //Get band index
      val array = new File(inputPath).getName.split("_")
      val str = array(array.length - 1)
      val pattern = Pattern.compile("\\d+")
      val matcher = pattern.matcher(str)
      var index = 0
      while (matcher.find())
        index = matcher.group(0).toInt

      //There is no esun parameters for band10 and band11
      if(index != 6){
        val multParam = x._2(0)
        val addParam = x._2(1)
        val minDN = x._2(2)
        val maxDN = x._2(3)
        val distance = earthSunDistance
        val sz = (solarZenith * Math.PI / 180).toFloat

        if(inputPath.contains("B5.TIF")){
          val geotiff = GeoTiffReader.readSingleband(inputPath)
          //val geotiff = SinglebandGeoTiff.streaming(inputPath)
          //val (minDN, maxDN) = geotiffTile.findMinMax
          val lhazel = calLhazel(minDN, maxDN, multParam, addParam, sz, esun(index - 1), distance)
          val nodataValue = Math.PI * Math.pow(distance, 2) * (0.0 * multParam + addParam - lhazel) / esun(index - 1) * Math.pow(Math.cos(sz), 2)
          val geotiffTile = geotiff.tile
            .convert(FloatUserDefinedNoDataCellType(0.0f))
            //.convert(FloatUserDefinedNoDataCellType(nodataValue.toFloat))
            .convert(FloatConstantNoDataCellType)

          val calibratedTile = geotiffTile.mapIfSetDouble{ x =>
            val lλ_RadiometricCalibration = x * multParam + addParam
            val ρ_AtmosphericCorrection = Math.PI * Math.pow(distance, 2) * (lλ_RadiometricCalibration - lhazel) / esun(index - 1) * Math.pow(Math.cos(sz), 2)
            ρ_AtmosphericCorrection
          }

          GeoTiff(calibratedTile, geotiff.extent, geotiff.crs).write(outputDir + "\\Geotrellis_" + new File(inputPath).getName)
        }
      }


    }
  }


  def calMaxMinDN(srcValue:Array[Int]):(Int, Int) = {
    var (minDN, maxDN) = ( Int.MaxValue, Int.MinValue)
    srcValue.foreach{x =>
      if (x > maxDN) maxDN = x
      if (x < minDN) minDN = x
    }
    (minDN, maxDN)
  }

  def calLhazel(minDN: Float, maxDN: Float, gain: Float, bias: Float, sz: Float, esun: Float, distance: Float): Float = {
    val lmin = bias + gain * minDN
    val lmax = bias + gain * maxDN
    val lhazel = (lmin + minDN * (lmax - lmin) / maxDN) - (0.01f * esun * Math.pow(Math.cos(sz), 2) / (Math.PI * Math.pow(distance, 2)))
    lhazel.toFloat
  }

  def julianDay(year: Float, month: Float, day: Float): Float = {
    val results = day - 32075 + (1461 * (year + 4800 + (month - 14) / 12) / 4) + (367 * ((month - 2 - (month - 14) / 12) / 12)) - ((3 * (year + 4900 + (month - 14) / 12) / 100) / 4)
    results
  }

  def earthSunDistance(julianDay: Float): Float = (1 - 0.01674f * Math.cos(0.9856f * (julianDay - 4) * Math.PI / 180)).toFloat


}

class SpectralCalibration {

}


