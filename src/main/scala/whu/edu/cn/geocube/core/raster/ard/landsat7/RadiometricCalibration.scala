package whu.edu.cn.geocube.core.raster.ard.landsat7

import java.io.File
import java.util.regex.Pattern

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType}
import org.apache.spark.SparkContext
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants

object RadiometricCalibration{
  /**
   * Paralleling all images using GDAL, the parallel granularity is image.
   * Faster than using geotrellis
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param outputDir Output directory
   */
  def runWithGdal(implicit sc: SparkContext,
          datasetWithRescalingParams:Array[(String, Array[Float])],
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

      //For B6_VCID1 and B6_VCID2
      if(index >= 7) index += 1
      if(str.equals("1.TIF")) index = 6
      if(str.equals("2.TIF")) index = 7

      //There is no esun parameters for band6_VCID_1 and band6_VCID_2, so they will not be processed
      if(index == 6 || index == 7){
        val multParam = x._2(0)
        val addParam = x._2(1)
        val minDN = x._2(2)
        val maxDN = x._2(3)

        if(index == 6){
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

          //val dstValue = srcValue.map( _ * multParam + addParam)
          val dstValue = srcValue.map{x=>
            if(x!=0)
              x * multParam + addParam
            else
              Float.NaN
          }
          dstDataSet.GetRasterBand(1).WriteRaster(0, 0, imgWidth, imgHeight,dstValue)

          val nodataValue = 0.0 * multParam + addParam
          dstDataSet.GetRasterBand(1).SetNoDataValue(Float.NaN)

          dstDataSet.delete()
          srcDataset.delete()
          gdal.GDALDestroyDriverManager()
        }
      }

    }
  }

  /**
   * Paralleling imagesets using Geotrellis, the parallel granularity is single image
   * Slower than using gdal
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param outputDir Output directory
   */
  def runWithGeotrellis(implicit sc: SparkContext,
          datasetWithRescalingParams:Array[(String, Array[Float])],
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

      //For B6_VCID1 and B6_VCID2
      if(index >= 7) index += 1
      if(str.equals("1.TIF")) index = 6
      if(str.equals("2.TIF")) index = 7

      //There is no esun parameters for band6_VCID_1 and band6_VCID_2, so they will not be processed
      if(index == 6 || index == 7){
        val multParam = x._2(0)
        val addParam = x._2(1)
        val minDN = x._2(2)
        val maxDN = x._2(3)

        if(index == 6){
          val geotiff = GeoTiffReader.readSingleband(inputPath)
          //val geotiff = SinglebandGeoTiff.streaming(inputPath)
          val calibratedTile = geotiff.tile
            .convert(FloatUserDefinedNoDataCellType(0.0f))
            .convert(FloatConstantNoDataCellType)
            .mapIfSetDouble(_ * multParam + addParam)

          GeoTiff(calibratedTile, geotiff.extent, geotiff.crs).write(outputDir + "\\Geotrellis_" + new File(inputPath).getName)
        }
      }

    }
  }

}

class RadiometricCalibration {

}

