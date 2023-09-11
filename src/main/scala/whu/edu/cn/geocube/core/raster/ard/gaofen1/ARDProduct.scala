package whu.edu.cn.geocube.core.raster.ard.gaofen1

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.StdIn

/**
 * Gaofen-1 ARD product generation from L1A-level, which supports both WFV and PMS instruments.
 *
 * Provided preprocessing functions including:
 * (1) RPC geometric correction (RPC几何校正)
 * (2) Radiometric calibration (辐射定标)
 * (3) Atmospheric correction (暗通道去雾大气校正)
 *
 * Will provide more functions including:
 * (1) Orthorectification (DEM正射校正)
 * (2) Incorporating GCP for geometric correction (控制点几何校正)
 * */
object ARDProduct {
  //val inputDirs: Array[String] = Array("E:\\SatelliteImage\\GF1\\GF1_WFV3_E115.7_N28.9_20170729_L1A0002514125")
//  val inputDirs: Array[String] = Array("E:\\SatelliteImage\\GF1\\GF1_PMS1_E109.6_N19.4_20160426_L1A0001540952")
//  val inputDirs: Array[String] = Array("E:\\项目\\Geocube\\dataStorage\\GF1_PMS1_E113.5_N30.3_20180417_L1A0003127123")
  val inputDirs: Array[String] = Array("/home/geocube/environment_test/geocube_ogc/data/custom/GF1_PMS1_E113.5_N30.3_20180417_L1A0003127123")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
//      .setMaster("spark://125.220.153.26:7077")
      .setAppName("Gaofen-1 ARDProduct")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    println("test spark-submit through scala file")
    val sc = new SparkContext(conf)

    try {
      val ardProduct = ARDProduct(1)
      ardProduct.generate(sc, inputDirs)

      println("Hit enter to exit")
      StdIn.readLine()
    } finally {
      sc.stop()
    }

  }
}

/**
 * ARD product for Gaofen-1.
 *
 * @param _nSubdataset Input Gaofen-1 datasets num
 */
case class ARDProduct(_nSubdataset: Int) {
  val nSubdataset: Int = _nSubdataset

  /**
   * Encompass RPC geometric correction, Radiometric calibration and Atmospheric correction
   *
   * @param sc A SparkContext
   * @param inputDirs Input Gaofen-1 datasets directory
   */
  def generate(implicit sc: SparkContext,
                          inputDirs:Array[String]): Unit = {
    assert(nSubdataset == inputDirs.length)
    Preprocessing.run(sc, inputDirs)
  }
}

