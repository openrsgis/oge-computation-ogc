package whu.edu.cn.geocube.core.raster.ard.landsat8

import java.io.File
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._


/**
 * Landsat-8 ARD product generation from L1TP-level, which supports OLI instrument containing
 * ["coastal","blue", "green", "red", "NIR", "SWIR1", "SWIR2", "PAN", "CIRRUS"].
 * "TIRS1" and "TIRS2" of TIRS instrument are not supported.
 *
 * Provided preprocessing functions including:
 * (1) Radiometric calibration (辐射定标)
 * (2) Atmospheric correction (暗通道去雾大气校正)
 *
 * Will provide more accurate atmospheric correction
 * */

object ARDProduct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ARDProduct")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    val inputDir = args(0)
    val inputDirFile = new File(inputDir)
    val inputPaths = inputDirFile.listFiles().map(x => x.getAbsolutePath)
    try {
      inputPaths.foreach { inputPath =>
        println("##########" + inputPath + " preprocessing is running...")
        val outputSpectralCalibration = inputPath + "/SpectralCalibration"

        //there is no esun parameters for band10 and band11, but still has 11 bands
        val ardProduct = ARDProduct(11)

        //get metadata
        val (datasetWithRescalingParams, parser) = ardProduct.extractMetadata(inputPath)

        //perform radiometric calibration and atmospheric correction
        ardProduct.generate(sc,
          datasetWithRescalingParams,
          parser.earthSunDistance,
          90.0f - parser.sunElevation,
          parser.esun.toArray,
          outputSpectralCalibration
        )

        //remove raw images
        val sceneFile = new File(inputPath)
        val sceneImgPaths = sceneFile.listFiles().map(x => x.getAbsolutePath).filter(x => x.contains(".TIF") && (!x.contains("BQA.TIF")))
        sceneImgPaths.foreach { sceneImgPath =>
          val deleteCommand = "rm -rf " + sceneImgPath
          deleteCommand.!
        }

        //upload to hdfs
        val arrStr = inputDir.split("/")
        val length = arrStr.length
        val upLoadCommand = "/home/geocube/hadoop/bin/hdfs dfs -put " + inputPath + " /data/Landsat8/Hubei/" + arrStr(length - 2) + "/" + arrStr(length - 1) + "/"
        upLoadCommand.!
      }
    } finally {
      sc.stop()
    }
  }

}

/**
 * ARD product for Landsat-8.
 * @param _nSubdataset band num
 */
case class ARDProduct(_nSubdataset: Int) {
val nSubdataset: Int = _nSubdataset

/**
 * Extract the metadata of landsat dataset, not load data actually.
 *
 * @param inputDir Input directory of landsat8
 * @return Band path with gain, bias, minDN, maxDN, and a whole metadata handler
 */
def extractMetadata(inputDir:String): (Array[(String, Array[Float])], Parser) = {
  val pattern =  """[0-9].TIF""".r
  val dir = new File(inputDir)
  //There is no esun parameters for band10 and band11
  val files = dir.listFiles()
    .filter(x => pattern.findAllIn(x.getName).hasNext)
    /*.filter{x =>
      val str = x.getName
      (!str.contains("B10.TIF")) && (!str.contains("B11.TIF"))
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

    datasetWithRescalingParams.append((x.getAbsolutePath,
      Array(multParams(index - 1), addParams(index - 1), minDN(index - 1), maxDN(index - 1))))
  }

  parser.collectMetadata("Landsat8_OLI")
  (datasetWithRescalingParams.toArray, parser)
}

/**
 * Perform radiometric calibration and atmospheric correction
 *
 * @param sc A SparkContext
 * @param datasetWithRescalingParams Input band path with gain, bias, minDN, maxDN
 * @param earthSunDistance Earth sun distance, which can be obtained from meta txt
 * @param solarZenith Sun elevation can be obtained from meta txt, and solarZenith = 90 - sunElevation
 * @param esun Obtained from RS Authorized Institution
 * @param outputDir Output directory
 */
def generate(implicit sc: SparkContext,
             datasetWithRescalingParams:Array[(String, Array[Float])],
             earthSunDistance:Float,
             solarZenith: Float,
             esun:Array[Float],
             outputDir:String): Unit = {
  Preprocessing.run(sc, datasetWithRescalingParams, earthSunDistance, solarZenith, esun, outputDir)
}

}

