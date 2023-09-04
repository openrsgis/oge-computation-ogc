package whu.edu.cn.geocube.core.raster.ard.landsat7

import java.io.File
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.geocube.core.raster.ard.landsat8.Parser

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

/**
 * Under developing.
 *
 * Generate analysis ready data.
 * Handle ETM+ sensors of Landsat-7, ignoring the atmospheric correction of high-gain band6H and low-gain band6L, the rest bands are
 * "band_name": ["blue", "green", "red", "NIR", "SWIR1", "SWIR2", "PAN"],
 * "band_index": [1, 2, 3, 4, 5, 7, 8],
 */

object ARDProduct {

  //landsat7
  val inputDir:String = "E:\\SatelliteImage\\Landsat7\\LE71140352017120EDC00"
  val outputRadiometricCalibration:String = "E:\\SatelliteImage\\Landsat7\\LE71140352017120EDC00\\RadiometricCalibration"
  val outputSpectralCalibration:String = "E:\\SatelliteImage\\Landsat7\\LE71140352017120EDC00\\SpectralCalibration"

  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ARDProduct")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    try{
      //There is no esun parameters for band6_VCID_1 and band6_VCID_2
      val ardProduct = ARDProduct(9)
      val (datasetWithRescalingParams, parser) = ardProduct.loadDataset(inputDir)

      ardProduct.radiometricCalibration(sc, datasetWithRescalingParams, outputRadiometricCalibration)

      /**
       * note: solarZenith and earthSunDistance is obtained from meta txt here,
       * earthSunDistance also can be calculated by acquired date.
       * esun is custom data, not from RS Authorized Institution
       */
      ardProduct.spectralCalibration(sc,
        datasetWithRescalingParams,
        parser.earthSunDistance,
        90.0f - parser.sunElevation,
        parser.esun.toArray,
        outputSpectralCalibration
      )

      println("Hit enter to exit")
      StdIn.readLine()
    }finally {
      sc.stop()
    }

  }

}

case class ARDProduct(_nSubdataset: Int) {
  val nSubdataset: Int = _nSubdataset

  /**
   * Acquire the metadata of landsat dataset, not load data actually.
   *
   * @param inputDir Input directory of landsat8
   *
   * @return Band path with gain, bias, minDN, maxDN, and a whole metadata handler
   */
  def loadDataset(inputDir:String): (Array[(String, Array[Float])], Parser) = {
    val pattern =  """[0-9].TIF""".r
    val dir = new File(inputDir)

    //There is no esun parameters for band6_VCID_1 and band6_VCID_2
    val files = dir.listFiles()
      .filter(x => pattern.findAllIn(x.getName).hasNext)
      /*.filter{x =>
        val str = x.getName
        (!str.contains("B6_VCID_1.TIF")) && (!str.contains("B6_VCID_2.TIF"))
      }*/

    assert(files.length == nSubdataset)
    println("Absolute Path of Datasets")
    for(i <- 0 until nSubdataset)
      println(files(i).getAbsolutePath)

    val metaFile = dir.listFiles()
      .filter(_.getName.contains("MTL"))

    val parser = Parser(metaFile(0).getAbsolutePath, nSubdataset)

    val (multParams, addParams) = parser.getRadiometricRescalingParams()
    println("GROUP = RADIOMETRIC_RESCALING")
    println(" RADIANCE_MULT_BAND")
    multParams.foreach(x => println("  " + x))
    println(" RADIANCE_ADD_BAND")
    addParams.foreach(x => println("  " + x))

    val (minDN, maxDN) = parser.getQuantizeCalibratedPixel()
    println(" QUANTIZE_CAL_MIN_BAND")
    minDN.foreach(x => println("  " + x))
    println(" QUANTIZE_CAL_MAX_BAND")
    maxDN.foreach(x => println("  " + x))

    val datasetWithRescalingParams = new ArrayBuffer[(String, Array[Float])]()
    files.foreach{x =>
      val array = x.getName.split("_")
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


      datasetWithRescalingParams.append((x.getAbsolutePath,
        Array(multParams(index - 1), addParams(index - 1), minDN(index - 1), maxDN(index - 1))))
    }

    parser.collectMetadata("Landsat7_ETM+")
    (datasetWithRescalingParams.toArray, parser)
  }

  /**
   * Paralleling radiometricCalibration.
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param outputDir Output directory
   */
  def radiometricCalibration(implicit sc: SparkContext,
                             datasetWithRescalingParams:Array[(String, Array[Float])],
                             outputDir:String):Unit = {
    RadiometricCalibration.runWithGdal(sc, datasetWithRescalingParams, outputDir)
  }

  /**
   * Paralleling spectralCalibration.
   *
   * @param sc A SparkContext
   * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
   * @param earthSunDistance Earth sun distance, which can be obtained from meta txt
   * @param solarZenith Sun elevation can be obtained from meta txt, and solarZenith = 90 - sunElevation
   * @param esun Obtained from RS Authorized Institution
   * @param outputDir Output directory
   */
  def spectralCalibration(implicit sc: SparkContext,
                          datasetWithRescalingParams:Array[(String, Array[Float])],
                          earthSunDistance:Float,
                          solarZenith: Float,
                          esun:Array[Float],
                          outputDir:String): Unit = {
    SpectralCalibration.runWithGdal(sc, datasetWithRescalingParams, earthSunDistance, solarZenith, esun, outputDir)
  }

}
