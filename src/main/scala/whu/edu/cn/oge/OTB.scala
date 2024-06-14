package whu.edu.cn.oge

import java.io.{BufferedReader, BufferedWriter, FileWriter, InputStreamReader, OutputStreamWriter, PrintWriter}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal.ZFactor
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bouncycastle.util.StringList
import org.locationtech.jts.geom.{Geometry, _}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.{dagId, runMain, workTaskJson}
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._
import whu.edu.cn.oge.Feature._
import whu.edu.cn.config.GlobalConfig

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.io.Source

object OTB {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)

  }

  val algorithmData = GlobalConfig.OTBConf.OTB_DATA
  val algorithmCode = GlobalConfig.OTBConf.OTB_ALGORITHMCODE
  val host = GlobalConfig.OTBConf.OTB_HOST
  val userName = GlobalConfig.OTBConf.OTB_USERNAME
  val password = GlobalConfig.OTBConf.OTB_PASSWORD
  val port = GlobalConfig.OTBConf.OTB_PORT

  /**
   * This application computes edge features on a selected channel of the input.It uses different filters such as gradient, Sobel and Touzi.
   *
   * @param sc        Alias object for SparkContext.
   * @param input     The input image on which the features are computed.
   * @param channel   The selected channel index.
   * @param filter    Choice of edge feature, which contains gradient, sobel, touzi.
   * @return          Output image containing the edge features.
   */
  def otbEdgeExtraction(implicit sc: SparkContext,
                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                        channel : Int = 1,
                        filter: String = "gradient"):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbEdgeExtraction_" + time + ".tif"
    val writePath = algorithmData + "otbEdgeExtraction_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_edgeextraction.py --input "$outputTiffPath" --channel $channel --filter "$filter" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * Performs dimensionality reduction on input image. PCA,NA-PCA,MAF,ICA methods are available. It is also possible to compute the inverse transform to reconstruct the image and to optionally export the transformation matrix to a text file.
   *
   * @param sc      Alias object for SparkContext.
   * @param input   The input image to apply dimensionality reduction.
   * @param method  Selection of the reduction dimension method.
   * @param rescale Enable rescaling of the reduced output image.
   * @return Output output image. Components are ordered by decreasing eigenvalues.
   */
  def otbDimensionalityReduction(implicit sc: SparkContext,
                        input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                 method: String = "pca",
                                 rescale: String = "no"):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbDimensionalityReduction_" + time + ".tif"
    val writePath = algorithmData + "otbDimensionalityReduction_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_edgeextraction.py --input "$outputTiffPath" --method $method --rescale "$rescale" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * Computes Structural Feature Set textures on every pixel of the input image selected channel.
   *
   * @param sc                Alias object for SparkContext.
   * @param input             The input image to compute the features on.
   * @param channel           The selected channel index.
   * @param spectralThreshold Spectral Threshold.
   * @param spatialThreshold  Spatial Threshold.
   * @param numberOfDirection Number of Direction.
   * @param alpha             Alpha.
   * @param ratioMaximumConsiderationNumber Number of Direction.
   * @return Output output image. Components are ordered by decreasing eigenvalues.
   */
  def otbSFSTextureExtraction(implicit sc: SparkContext,
                                 input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              channel:Int = 1,
                              spectralThreshold: Double = 50,
                              spatialThreshold: Int = 100,
                              numberOfDirection: Int = 20,
                              alpha: Double = 1,
                              ratioMaximumConsiderationNumber: Int = 5):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbSFSTextureExtraction_" + time + ".tif"
    val writePath = algorithmData + "otbSFSTextureExtraction_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_edgeextraction.py --input "$outputTiffPath" --channel $channel --spectral-threshold $spectralThreshold --spatial-threshold $spatialThreshold --number-of-direction $numberOfDirection --alpha $alpha --ratio-maximum-consideration-number $ratioMaximumConsiderationNumber --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * Change detection by Multivariate Alteration Detector (MAD) algorithm.
   *
   * @param sc            Alias object for SparkContext.
   * @param inputBefore   Multiband image of the scene before perturbations.
   * @param inputAfter    Mutliband image of the scene after perturbations.
   * @return Multiband image containing change maps.
   */
  def otbMultivariateAlterationDetector(implicit sc: SparkContext,
                              inputBefore: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              inputAfter: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPathBefore = algorithmData + "otbMultivariateAlterationDetectorBefore_" + time + ".tif"
    val outputTiffPathAfter = algorithmData + "otbMultivariateAlterationDetectorAfter_" + time + ".tif"
    val writePath = algorithmData + "otbMultivariateAlterationDetector_" + time + "_out.tif"
    saveRasterRDDToTif(inputBefore, outputTiffPathBefore)
    saveRasterRDDToTif(inputAfter, outputTiffPathAfter)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_multivariatealterationdetector.py --input-before "$outputTiffPathBefore"  --input-after "$outputTiffPathAfter" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * This application computes radiometric indices using the relevant channels of the input image. The output is a multi band image into which each channel is one of the selected indices.
   *
   * @param input       Input image
   * @param channelsBlue  Blue channel index
   * @param channelsGreen  Green channel index
   * @param channelsRed  Red channel index
   * @param channelsNir  NIR channel index
   * @param channelsMir  Mir channel index
   * @param indicesList     List of available radiometric indices with their relevant channels in brackets:
   * @param ram      Available memory for processing (in MB).
   * @return Radiometric indices output image
   */
  def otbRadiometricIndices(implicit sc: SparkContext,
                            input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            channelsBlue: Int = 1,
                            channelsGreen: Int = 1,
                            channelsRed: Int = 1,
                            channelsNir : Int = 1,
                            channelsMir : Int = 1,
                            ram : Int = 256,
                            indicesList : List[String] = List("Vegetation:NDVI")):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbRadiometricIndices_" + time + ".tif"
    val writePath = algorithmData + "otbRadiometricIndices_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)
    val indices: String = indicesList.mkString(" ")

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_radiometricindices.py --input "$outputTiffPath"  --channels-blue $channelsBlue --channels-green $channelsGreen --channels-red $channelsRed --channels-nir $channelsNir --channels-mir $channelsMir --list $indices --ram $ram --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * UTM zone determination from a geographic point.
   *
   * @param sc   Alias object for SparkContext.
   * @param lat  Latitude value of desired point.
   * @param lon  Longitude value of desired point.
   */
  def otbObtainUTMZoneFromGeoPoint(implicit sc: SparkContext,
                                   lat : Double,
                                   lon : Double): Unit= {
    val time = System.currentTimeMillis()

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_obtainUTMzonefromgeopoint.py --lat $lat --lon $lon """.stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }


}


