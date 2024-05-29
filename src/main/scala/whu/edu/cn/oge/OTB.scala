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
import scala.collection.mutable.{ListBuffer, Map}
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
        raw"""conda activate otb;${algorithmCode}python otb_dimensionalityreduction.py --input "$outputTiffPath" --method $method --rescale "$rescale" --output "$writePath"""".stripMargin

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
        raw"""conda activate otb;${algorithmCode}python otb_SFStextureextraction.py --input "$outputTiffPath" --channel $channel --spectral-threshold $spectralThreshold --spatial-threshold $spatialThreshold --number-of-direction $numberOfDirection --alpha $alpha --ratio-maximum-consideration-number $ratioMaximumConsiderationNumber --output "$writePath"""".stripMargin

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
  /**
    * This application chains together the 4 steps of the MeanShift framework, that is the MeanShiftSmoothing, the LSMSSegmentation, the LSMSSmallRegionsMerging and the LSMSVectorization.In this mode, the application will produce a vector file or database and compute field values for each region.
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param  spatialr Radius of the spatial neighborhood for averaging.
    * @param ranger    Threshold on spectral signature euclidean distance (expressed in radiometry unit) to consider neighborhood pixel for averaging.
    * @param minsize   Minimum Segment Size.
    * @param tilesizex Size of tiles along the X-axis for tile-wise processing.
    * @param tilesizey Size of tiles along the Y-axis for tile-wise processing.
    * @return          The output GIS vector file, representing the vectorized version of the segmented image where the features of the polygons are the radiometric means and variances.
    */
  def otbLargeScaleMeanShiftVector(implicit sc: SparkContext,
                                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                   spatialr : Int = 5,
                                   ranger : Float = 15,
                                   minsize : Int = 50,
                                   tilesizex : Int = 500,
                                   tilesizey : Int = 500):
  (RDD[(String, (Geometry, mutable.Map[String, Any]))]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbLargeScaleMeanShiftVector_" + time + ".tif"
    val writePath = algorithmData + "otbLargeScaleMeanShiftVector_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_largescalemeanshiftvector.py --input "$outputTiffPath" --spatialr $spatialr --ranger $ranger --minsize $minsize --tilesizex $tilesizex --tilesizey $tilesizey  --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }
  /**
    * This application chains together the 4 steps of the MeanShift framework, that is the MeanShiftSmoothing, the LSMSSegmentation, the LSMSSmallRegionsMerging and the LSMSVectorization.In this mode, the application will produce a standard labeled raster.
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param spatialr Radius of the spatial neighborhood for averaging.
    * @param ranger    Threshold on spectral signature euclidean distance (expressed in radiometry unit) to consider neighborhood pixel for averaging.
    * @param minsize   Minimum Segment Size.
    * @param tilesizex Size of tiles along the X-axis for tile-wise processing.
    * @param tilesizey Size of tiles along the Y-axis for tile-wise processing.
    * @return It corresponds to the output of the small region merging step.
    */
  def otbLargeScaleMeanShiftRaster(implicit sc: SparkContext,
                                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                   spatialr : Int = 5,
                                   ranger : Float = 15,
                                   minsize : Int = 50,
                                   tilesizex : Int = 500,
                                   tilesizey : Int = 500):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbLargeScaleMeanShiftRaster_" + time + ".tif"
    val writePath = algorithmData + "otbLargeScaleMeanShiftRaster_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_largescalemeanshiftraster.py --input "$outputTiffPath" --spatialr $spatialr --ranger $ranger --minsize $minsize --tilesizex $tilesizex --tilesizey $tilesizey  --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
    * This application smooths an image using the MeanShift algorithm.
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param spatialr  Radius of the spatial neighborhood for averaging.
    * @param ranger    Threshold on spectral signature euclidean distance (expressed in radiometry unit) to consider neighborhood pixel for averaging.
    * @param thres     Algorithm will stop if update of average spectral signature and spatial position is below this threshold.
    * @param maxiter   Algorithm will stop if convergence threshold is not met after the maximum number of iterations.
    * @param rangeramp Vary the range radius linearly with the central pixel intensity (experimental).
    * @return It contains the final average spectral signatures of each pixel and the 2D displacement between the input pixel spatial position and the final position after convergence.
    */
  def otbMeanShiftSmoothing(implicit sc: SparkContext,
                            input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            spatialr : Int = 5,
                            ranger : Float = 15,
                            thres : Float = 0.1.toFloat,
                            maxiter : Int = 100,
                            rangeramp : Float = 0)= {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbMeanShiftSmoothing_" + time + ".tif"
    val writePath1 = algorithmData + "otbMeanShiftSmoothing_" + time + "_out.tif"
    val writePath2 = algorithmData + "otbMeanShiftSmoothing_" + time + "_out.tif"

    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_otb_meanshiftsmoothing.py --input "$outputTiffPath" --spatialr $spatialr --ranger $ranger --thres $thres --maxiter $maxiter --rangeramp $rangeramp  --output1 "$writePath1" --output2 "$writePath2"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val RDDtif1= makeChangedRasterRDDFromTif(sc, writePath1)
    val RDDtif2= makeChangedRasterRDDFromTif(sc, writePath2)
    //    mutable.Map("1"->RDDtif1,"2"->RDDtif2)
    val listRDD:List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = List(RDDtif1,RDDtif2)
    val listname:List[String]=List("1","2")
    CoverageCollection.mergeCoverages(listRDD,listname)
  }

  /**
    * Computes local statistical moments on every pixel in the selected channel of the input image
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param channel The selected channel index (1 based)
    * @param radius    The computational window radius.
    * @return Output image containing the local statistical moments.
    */
  def otbLocalStatisticExtraction(implicit sc: SparkContext,
                                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                  channel : Int = 1,
                                  radius : Int = 3):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otblocalstatisticextraction_" + time + ".tif"
    val writePath = algorithmData + "otblocalstatisticextraction_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_localstatisticextraction.py --input "$outputTiffPath" --channel $channel --radius $radius --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
    * Performs segmentation of an image, and output a raster file.OTB implementation of the Mean-Shift algorithm (multi-threaded).
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param spatialr Spatial radius of the neighborhood.
    * @param ranger    Range radius defining the radius (expressed in radiometry unit) in the multispectral space
    * @param thres Algorithm iterative scheme will stop if mean-shift vector is below this threshold or if iteration number reached maximum number of iterations.
    * @param maxiter Algorithm iterative scheme will stop if convergence hasn’t been reached after the maximum number of iterations
    * @param minsize Minimum size of a region (in pixel unit) in segmentation. Smaller clusters will be merged to the neighboring cluster with the closest radiometry. If set to 0 no pruning is done.
    * @return The output labeled image.
    */
  def otbSegmentationMeanshiftRaster(implicit sc: SparkContext,
                                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                     spatialr : Int = 5,
                                     ranger : Float = 15,
                                     thres : Float = 0.1.toFloat,
                                     maxiter : Int = 100,
                                     minsize : Int = 100):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentationmeanshiftraster_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentationmeanshiftraster_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationmeanshiftraster.py --input "$outputTiffPath" --spatialr $spatialr --ranger $ranger --thres $thres --maxiter $maxiter --minsize $minsize --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }
  /**
    * Performs segmentation of an image, and output a vector file.OTB implementation of the Mean-Shift algorithm (multi-threaded).
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param spatialr Spatial radius of the neighborhood.
    * @param ranger    Range radius defining the radius (expressed in radiometry unit) in the multispectral space
    * @param thres Algorithm iterative scheme will stop if mean-shift vector is below this threshold or if iteration number reached maximum number of iterations.
    * @param maxiter Algorithm iterative scheme will stop if convergence hasn’t been reached after the maximum number of iterations
    * @param minsize Minimum size of a region (in pixel unit) in segmentation. Smaller clusters will be merged to the neighboring cluster with the closest radiometry. If set to 0 no pruning is done.
    * @param neighbor Activate 8-Neighborhood connectivity (default is 4).
    * @param stitch Scan polygons on each side of tiles and stitch polygons which connect by more than one pixel.
    * @param v_minsize Objects whose size is below the minimum object size (area in pixels) will be ignored during vectorization.
    * @param simplify Simplify polygons according to a given tolerance (in pixel). This option allows reducing the size of the output file or database.
    * @param tilesize User defined tiles size for tile-based segmentation. Optimal tile size is selected according to available RAM if null.
    * @param startlabel Starting value of the geometry index field
    * @return The output vector file or database (name can be anything understood by OGR)
    */
  def otbSegmentationMeanshiftVector(implicit sc: SparkContext,
                                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                     spatialr : Int = 5,
                                     ranger : Float = 15,
                                     thres : Float = 0.1.toFloat,
                                     maxiter : Int = 100,
                                     minsize : Int = 100,
                                     neighbor : Boolean = false,
                                     stitch : Boolean = false,
                                     v_minsize: Int = 1,
                                     simplify : Float = 0.1.toFloat,
                                     tilesize : Int = 1024,
                                     startlabel : Int = 1):
  (RDD[(String, (Geometry, mutable.Map[String, Any]))]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentationmeanshiftvector_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentationmeanshiftvector_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationmeanshiftvector.py --input "$outputTiffPath" --spatialr $spatialr --ranger $ranger --thres $thres --maxiter $maxiter --minsize $minsize --output "$writePath" --neighbor $neighbor --stitch $stitch --v_minsize $v_minsize --simplify $simplify --tilesize $tilesize --startlabel $startlabel""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }
  /**
    * Performs segmentation of an image, and output a raster file.The traditional watershed algorithm. The height function is the gradient magnitude of the amplitude (square root of the sum of squared bands).
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param threshold Depth threshold Units in percentage of the maximum depth in the image.
    * @param level     flood level for generating the merge tree from the initial segmentation (between 0 and 1)
    * @return The output labeled image.
    */
  def otbSegmentationWatershedRaster(implicit sc: SparkContext,
                                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                     threshold : Float = 0.01.toFloat,
                                     level : Float = 0.1.toFloat):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentationwatershedraster_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentationwatershedraster_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationwatershedraster.py --input "$outputTiffPath" --threshold $threshold --level $level --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }
  /**
    * Performs segmentation of an image, and output a vector file.The traditional watershed algorithm. The height function is the gradient magnitude of the amplitude (square root of the sum of squared bands).
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param threshold Depth threshold Units in percentage of the maximum depth in the image.
    * @param level     flood level for generating the merge tree from the initial segmentation (between 0 and 1)
    * @param neighbor Activate 8-Neighborhood connectivity (default is 4).
    * @param stitch Scan polygons on each side of tiles and stitch polygons which connect by more than one pixel.
    * @param v_minsize Objects whose size is below the minimum object size (area in pixels) will be ignored during vectorization.
    * @param simplify Simplify polygons according to a given tolerance (in pixel). This option allows reducing the size of the output file or database.
    * @param tilesize User defined tiles size for tile-based segmentation. Optimal tile size is selected according to available RAM if null.
    * @param startlabel Starting value of the geometry index field
    * @return The output vector file or database (name can be anything understood by OGR)
    */
  def otbSegmentationWatershedVector(implicit sc: SparkContext,
                                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                     threshold : Float = 0.01.toFloat,
                                     level : Float = 0.1.toFloat,
                                     neighbor : Boolean = false,
                                     stitch : Boolean = false,
                                     v_minsize: Int = 1,
                                     simplify : Float = 0.1.toFloat,
                                     tilesize : Int = 1024,
                                     startlabel : Int = 1):
  (RDD[(String, (Geometry, mutable.Map[String, Any]))]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentationwatershedvector_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentationwatershedvector_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationwatershedvector.py --input "$outputTiffPath" --threshold $threshold --level $level --output "$writePath" --neighbor $neighbor --stitch $stitch --v_minsize $v_minsize --simplify $simplify --tilesize $tilesize --startlabel $startlabel""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }
  /**
    * Performs segmentation of an image, and output a raster file.Segmentation based on morphological profiles.
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param size      Size of the profiles
    * @param start     Initial radius of the structuring element (in pixels)
    * @param step     Radius step along the profile (in pixels
    * @param sigma     Profiles values under the threshold will be ignored.
    * @return The output labeled image.
    */
  def otbSegmentationMprofilesdRaster(implicit sc: SparkContext,
                                      input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                      size : Int = 5,
                                      start : Int = 1,
                                      step : Int = 1,
                                      sigma : Float = 1):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentationmprofilesdraster_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentationmprofilesdraster_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationmprofilesdraster.py --input "$outputTiffPath" --size $size --start $start --step $step --sigma $sigma --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
    * Performs segmentation of an image, and output a vector file.Segmentation based on morphological profiles
    *
    * @param sc        Alias object for SparkContext.
    * @param input     The input image on which the features are computed.
    * @param size      Size of the profiles
    * @param start     Initial radius of the structuring element (in pixels)
    * @param step     Radius step along the profile (in pixels
    * @param sigma     Profiles values under the threshold will be ignored.
    * @param neighbor Activate 8-Neighborhood connectivity (default is 4).
    * @param stitch Scan polygons on each side of tiles and stitch polygons which connect by more than one pixel.
    * @param v_minsize Objects whose size is below the minimum object size (area in pixels) will be ignored during vectorization.
    * @param simplify Simplify polygons according to a given tolerance (in pixel). This option allows reducing the size of the output file or database.
    * @param tilesize User defined tiles size for tile-based segmentation. Optimal tile size is selected according to available RAM if null.
    * @param startlabel Starting value of the geometry index field
    * @return The output vector file
    */
  def otbSegmentationMprofilesVector(implicit sc: SparkContext,
                                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                     size : Int = 5,
                                     start : Int = 1,
                                     step : Int = 1,
                                     sigma : Float = 1,
                                     neighbor : Boolean = false,
                                     stitch : Boolean = false,
                                     v_minsize: Int = 1,
                                     simplify : Float = 0.1.toFloat,
                                     tilesize : Int = 1024,
                                     startlabel : Int = 1):
  (RDD[(String, (Geometry, mutable.Map[String, Any]))]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbsegmentatiomprofilesvector_" + time + ".tif"
    val writePath = algorithmData + "otbsegmentatiomprofilesvector_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_segmentationmprofilesvector.py --input "$outputTiffPath" --size $size --start $start --step $step --sigma $sigma  --output "$writePath" --neighbor $neighbor --stitch $stitch --v_minsize $v_minsize --simplify $simplify --tilesize $tilesize --startlabel $startlabel""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }
  /**
    * Train a regression model from multiple triplets of feature images, predictor images.This group of parameters allows setting SVM classifier parameters.
    *
    * @param sc            Alias object for SparkContext.
    * @param input_predict A list of input predictor images.
    * @param input_label  A list of input label images
    * @param ratio  Ratio between training and validation samples.
    * @param kernel SVM Kernel Type.
    * @param model Type of SVM formulation
    * @param costc SVM models have a cost parameter C (1 by default) to control the trade-off between training errors and forcing rigid margins.
    * @param gamma Set gamma parameter in poly/rbf/sigmoid kernel function
    * @param coefficient Set coef0 parameter in poly/sigmoid kernel function
    * @param degree Set polynomial degree in poly kernel function
    * @param costnu Cost parameter Nu, in the range 0..1, the larger the value, the smoother the decision.
    * @param opt SVM parameters optimization flag.
    * @param prob Probability estimation flag.
    * @param epsilon Probability estimation flag.
    * @return Output file containing the model estimated (.txt format).
    */
  def otbTrainImagesRegressionLibSvm(implicit sc: SparkContext,
                                     input_predict : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                     input_label : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                     ratio : Float = 0.5.toFloat,
                                     kernel : String = "linear",
                                     model : String = "epssvr",
                                     costc : Float = 1,
                                     gamma : Float = 1,
                                     coefficient : Float = 0,
                                     degree : Int = 3,
                                     costnu : Float = 0.5.toFloat,
                                     opt : Boolean = false,
                                     prob : Boolean = false,
                                     epsilon : Float = 0.001.toFloat) = {
    var outputTiffPath_predict: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_label: ListBuffer[String] = ListBuffer.empty
    for (img <- input_predict) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_predict = algorithmData + "otbTrainImagesRegressionLibsvmPredict_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_predict)
      outputTiffPath_predict = outputTiffPath_predict :+ path_predict
      outputTiffPath_predict = outputTiffPath_predict :+ " "
    }
    for (img <- input_label) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_label = algorithmData + "otbTrainImagesRegressionLibsvmLabel_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_label)
      outputTiffPath_label = outputTiffPath_label :+ path_label
      outputTiffPath_label = outputTiffPath_label :+ " "
    }
    val time = System.currentTimeMillis()
    val writePath = algorithmData + "otbTrainImagesRegressionLibsvm_" + time + "_out.txt"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_trainimagesregressionlibsvm.py --input_predict "$outputTiffPath_predict" --input_label "$outputTiffPath_label" --ratio $ratio --kernel "$kernel" --model "$model" --costc $costc --gamma $gamma --coefficient $coefficient --degree $degree --costnu $costnu --opt $opt --prob $prob --epsilon $epsilon --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val Str = Source.fromFile(writePath).mkString

    Str
  }
  /**
    * Train a regression model from multiple triplets of feature images, predictor images.This group of parameters allows setting Decision Tree classifier parameters.
    *
    * @param sc            Alias object for SparkContext.
    * @param input_predict A list of input predictor images.
    * @param input_label  A list of input label images
    * @param ratio  Ratio between training and validation samples.
    * @param max The training algorithm attempts to split each node while its depth is smaller than the maximum possible depth of the tree. The actual depth may be smaller if the other termination criteria are met, and/or if the tree is pruned.
    * @param min If the number of samples in a node is smaller than this parameter, then this node will not be split.
    * @param ra If all absolute differences between an estimated value in a node and the values of the train samples in this node are smaller than this regression accuracy parameter, then the node will not be split further.
    * @param cat Cluster possible values of a categorical variable into K <= cat clusters to find a suboptimal split.
    * @param r If true, then a pruning will be harsher. This will make a tree more compact and more resistant to the training data noise but a bit less accurate
    * @param t If true, then pruned branches are physically removed from the tree.
    * @return Output file containing the model estimated (.txt format).
    */
  def otbTrainImagesRegressionDt(implicit sc: SparkContext,
                                 input_predict : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                 input_label : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                 ratio : Float = 0.5.toFloat,
                                 max : Int = 10,
                                 min : Int = 10,
                                 ra : Float = 0.01.toFloat,
                                 cat : Int = 10,
                                 r : Boolean = false,
                                 t :Boolean = false) = {
    var outputTiffPath_predict: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_label: ListBuffer[String] = ListBuffer.empty
    for(img <-input_predict) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_predict= algorithmData + "otbTrainImagesRegressionDtPredict_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_predict)
      outputTiffPath_predict=outputTiffPath_predict:+path_predict
      outputTiffPath_predict=outputTiffPath_predict:+" "
    }
    for(img <-input_label) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_label= algorithmData + "otbTrainImagesRegressionDtLabel_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_label)
      outputTiffPath_label=outputTiffPath_label:+path_label
      outputTiffPath_label=outputTiffPath_label:+" "
    }
    val time = System.currentTimeMillis()
    val writePath = algorithmData + "otbTrainImagesRegressionDt_" + time + "_out.txt"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_trainimagesregressiondt.py --input_predict "$outputTiffPath_predict" --input_label "$outputTiffPath_label" --ratio $ratio --max $max --min $min --ra $ra --cat $cat --r $r --t $t --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val Str = Source.fromFile(writePath).mkString

    Str
  }
  /**
    * Train a regression model from multiple triplets of feature images, predictor images.This group of parameters allows setting Random forests classifier parameters.
    *
    * @param sc            Alias object for SparkContext.
    * @param input_predict A list of input predictor images.
    * @param input_label  A list of input label images
    * @param ratio  Ratio between training and validation samples.
    * @param max The depth of the tree. A low value will likely underfit and conversely a high value will likely overfit. The optimal value can be obtained using cross validation or other suitable methods
    * @param min If the number of samples in a node is smaller than this parameter, then the node will not be split. A reasonable value is a small percentage of the total data e.g. 1 percent.
    * @param ra If all absolute differences between an estimated value in a node and the values of the train samples in this node are smaller than this regression accuracy parameter, then the node will not be split.
    * @param cat Cluster possible values of a categorical variable into K <= cat clusters to find a suboptimal split.
    * @param var_  The size of the subset of features, randomly selected at each tree node, that are used to find the best split(s). If you set it to 0, then the size will be set to the square root of the total number of features.
    * @param nbtrees The maximum number of trees in the forest. Typically, the more trees you have, the better the accuracy. However, the improvement in accuracy generally diminishes and reaches an asymptote for a certain number of trees. Also to keep in mind, increasing the number of trees increases the prediction time linearly.
    * @param acc Sufficient accuracy (OOB error).
    * @return Output file containing the model estimated (.txt format).
    */
  def otbTrainImagesRegressionRf(implicit sc: SparkContext,
                                 input_predict : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                 input_label : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                 ratio : Float = 0.5.toFloat,
                                 max : Int = 5,
                                 min : Int = 10,
                                 ra : Float = 0,
                                 cat : Int = 10,
                                 var_ : Int = 0,
                                 nbtrees : Int = 100,
                                 acc : Float = 0.01.toFloat) = {
    var outputTiffPath_predict: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_label: ListBuffer[String] = ListBuffer.empty
    for(img <-input_predict) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_predict= algorithmData + "otbTrainImagesRegressionRfPredict_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_predict)
      outputTiffPath_predict=outputTiffPath_predict:+path_predict
      outputTiffPath_predict=outputTiffPath_predict:+" "
    }
    for(img <-input_label) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_label= algorithmData + "otbTrainImagesRegressionRfLabel_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_label)
      outputTiffPath_label=outputTiffPath_label:+path_label
      outputTiffPath_label=outputTiffPath_label:+" "
    }
    val time = System.currentTimeMillis()
    val writePath = algorithmData + "otbTrainImagesRegressionRf_" + time + "_out.txt"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_trainimagesregressionrf.py --input_predict "$outputTiffPath_predict" --input_label "$outputTiffPath_label" --ratio $ratio --max $max --min $min --ra $ra --cat $cat --var_ $var_ --nbtrees $nbtrees --acc $acc --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val Str = Source.fromFile(writePath).mkString

    Str
  }
  /**
    * Train a regression model from multiple triplets of feature images, predictor images.This group of parameters allows setting KNN classifier parameters.
    *
    * @param sc            Alias object for SparkContext.
    * @param input_predict A list of input predictor images.
    * @param input_label  A list of input label images
    * @param ratio  Ratio between training and validation samples.
    * @param number The number of neighbors to use.
    * @param rule Decision rule for regression output [mean|median]
    * @return Output file containing the model estimated (.txt format).
    */
  def otbTrainImagesRegressionKnn(implicit sc: SparkContext,
                                  input_predict : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                  input_label : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                  ratio : Float = 0.5.toFloat,
                                  number : Int = 32,
                                  rule : String = "mean") = {
    var outputTiffPath_predict: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_label: ListBuffer[String] = ListBuffer.empty
    for(img <-input_predict) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_predict= algorithmData + "otbTrainImagesRegressionKnnPredict_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_predict)
      outputTiffPath_predict=outputTiffPath_predict:+path_predict
      outputTiffPath_predict=outputTiffPath_predict:+" "
    }
    for(img <-input_label) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_label= algorithmData + "otbTrainImagesRegressionKnnLabel_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_label)
      outputTiffPath_label=outputTiffPath_label:+path_label
      outputTiffPath_label=outputTiffPath_label:+" "
    }
    val time = System.currentTimeMillis()
    val writePath = algorithmData + "otbTrainImagesRegressionKnn_" + time + "_out.txt"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_trainimagesregressionknn.py --input_predict "$outputTiffPath_predict" --input_label "$outputTiffPath_label" --ratio $ratio --number $number --rule $rule --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val Str = Source.fromFile(writePath).mkString

    Str
  }
  /**
    * Train a regression model from multiple triplets of feature images, predictor images.This group of parameters allows setting Shark Random forests classifier parameters.
    *
    * @param sc            Alias object for SparkContext.
    * @param input_predict A list of input predictor images.
    * @param input_label  A list of input label images
    * @param ratio  Ratio between training and validation samples.
    * @param nbtrees The maximum number of trees in the forest. Typically, the more trees you have, the better the accuracy. However, the improvement in accuracy generally diminishes and reaches an asymptote for a certain number of trees. Also to keep in mind, increasing the number of trees increases the prediction time linearly.
    * @param nodesize If the number of samples in a node is smaller than this parameter, then the node will not be split. A reasonable value is a small percentage of the total data e.g. 1 percent.
    * @param mtry The number of features (variables) which will be tested at each node in order to compute the split. If set to zero, the square root of the number of features is used.
    * @param oobr Set the fraction of the original training dataset to use as the out of bag sample.A good default value is 0.66.
    * @return Output file containing the model estimated (.txt format).
    */
  def otbTrainImagesRegressionSharkrf(implicit sc: SparkContext,
                                      input_predict : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                      input_label : Predef.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                      ratio : Float = 0.5.toFloat,
                                      nbtrees : Int = 100,
                                      nodesize : Int = 25,
                                      mtry : Int = 0,
                                      oobr : Float = 0.66.toFloat) = {
    var outputTiffPath_predict: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_label: ListBuffer[String] = ListBuffer.empty
    for(img <-input_predict) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_predict= algorithmData + "otbTrainImagesRegressionSharkrfPredict_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_predict)
      outputTiffPath_predict=outputTiffPath_predict:+path_predict
      outputTiffPath_predict=outputTiffPath_predict:+" "
    }
    for(img <-input_label) {
      val time = System.currentTimeMillis()
      Thread.sleep(10)
      val path_label= algorithmData + "otbTrainImagesRegressionSharkrfLabel_" + time + ".tif"
      saveRasterRDDToTif(img._2, path_label)
      outputTiffPath_label=outputTiffPath_label:+path_label
      outputTiffPath_label=outputTiffPath_label:+" "
    }
    val time = System.currentTimeMillis()
    val writePath = algorithmData + "otbTrainImagesRegressionSharkrf_" + time + "_out.txt"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_trainimagesregressionsharkrf.py --input_predict "$outputTiffPath_predict" --input_label "$outputTiffPath_label" --ratio $ratio --nbtrees $nbtrees --nodesize $nodesize --mtry $mtry --oobr $oobr --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val Str = Source.fromFile(writePath).mkString

    Str
  }

}


