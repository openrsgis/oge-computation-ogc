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
import whu.edu.cn.trigger.Trigger.{dagId, runMain, userId, workTaskJson}
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._
import whu.edu.cn.oge.Feature._
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.oge.Coverage.loadTxtFromUpload

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
  val algorithmDockerData = GlobalConfig.OTBConf.OTB_DOCKERDATA
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

  /**
   * Compute statistics of the features in a set of OGR Layers.
   *
   * @param sc    Alias object for SparkContext.
   * @param inshp Vector Data,Name of the input shapefile.
   * @param feat  List of features to consider for statistics.
   * @return XML file containing mean and variance of each feature.
   */
  def otbComputeOGRLayersFeaturesStatistics(implicit sc: SparkContext,
                                            inshp: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                            feat: List[String]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = algorithmData + "otbComputeOGRLayersFeaturesStatistics_" + time + ".shp"
    val writePath = algorithmData + "otbComputeOGRLayersFeaturesStatistics_" + time + "_out.xml"
    saveFeatureRDDToShp(inshp, outputShpPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_computeogrlayersfeaturesstatistics.py --inshp "$outputShpPath" --outstats $writePath --feat "$feat" """.stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    //    makeFeatureRDDFromShp(sc, writePath)
    //    val readXMLData = XML.load(writePath)
    //    val readXMLDatastr: String = readXMLData.toString
  }

  /**
   * Unsupervised KMeans image classification.
   *
   * @param sc    Alias object for SparkContext.
   * @param in    Input image filename.
   * @param nc    Number of modes, which will be used to generate class membership.
   * @param ts    Size of the training set (in pixels).
   * @param maxit Maximum number of iterations for the learning step. If this parameter is set to 0, the KMeans algorithm will not stop until convergence.
   * @return Output image containing class labels.
   */
  def otbKMeansClassification(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              nc: Int = 5, ts: Int = 100, maxit: Int = 1000, centroidsIn: String, centroidsOut: String, ram: Int = 256,
                              sampler: List[String] = List("periodic"), samplerPeriodicJitter: Int = 0,
                              vm: String, nodatalabel: Int = 0, cleanup: Boolean = true, rand: Int):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbKMeansClassification_" + time + ".tif"
    val writePath = algorithmData + "otbKMeansClassification_" + time + "_out.tif"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_kmeansclassification.py --in "$outputTiffPath" --ts $ts --nc $nc --maxit $maxit
          --centroids.in "$centroidsIn" --centroids.out "$centroidsOut" --ram $ram --sampler "$sampler" --sampler.periodic.jitter $samplerPeriodicJitter
          --vm "$vm" --nodatalabel $nodatalabel  --cleanup $cleanup  --rand $rand
          --out "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * SOM image classification
   *
   * @param sc   Alias object for SparkContext.
   * @param in   Input image to classify.
   * @param vm   Validity mask (only pixels corresponding to a mask value greater than 0 will be used for learning).
   * @param tp   Probability for a sample to be selected in the training set.
   * @param ts   Maximum training set size (in pixels).
   * @param som  Output image containing the Self-Organizing Map.
   * @param sx   X size of the SOM map.
   * @param sy   Y size of the SOM map.
   * @param nx   X size of the initial neighborhood in the SOM map.
   * @param ny   Y size of the initial neighborhood in the SOM map.
   * @param ni   Number of iterations for SOM learning.
   * @param bi   Initial learning coefficient.
   * @param bf   Final learning coefficient.
   * @param iv   Maximum initial neuron weight.
   * @param rand Set a specific random seed with integer value.
   * @param ram  Available memory for processing (in MB).
   * @return Output classified image (each pixel contains the index of its corresponding vector in the SOM).
   */
  def otbSOMClassification(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                           vm: String, tp: Float = 1f, ts: Int, som: String, sx: Int = 32,
                           sy: Int = 32, nx: Int = 10, ny: Int = 10, ni: Int = 5, bi: Float = 0.1f,
                           bf: Float = 0.1f, iv: Float = 0f, rand: Int, ram: Int = 256
                          ):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbSOMClassification_" + time + ".tif"
    val writePath = algorithmData + "otbSOMClassification_" + time + "_out.tif"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_somclassification.py --in "$outputTiffPath" --vm "$vm" --tp $tp --ts $ts --som $som
        --sx "$sx" --sy "$sy" --nx $nx --ny "$ny" --ni $ni
        --bi "$bi" --bf $bf  --iv $iv  --rand $rand --ram $ram
        --out "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * The application extracts samples values from animage using positions contained in a vector data file.
   *
   * @param in                 Support image
   * @param vec                Vector data file containing samplingpositions. (OGR format)
   * @param outfield           Choice between naming method for output fields
   * @param outfieldPrefixName Prefix used to form the field names thatwill contain the extracted values.
   * @param outfieldListNames  Full list of output field names.
   * @param field              Name of the field carrying the class name in the input vectors.
   * @param layer              Layer index to read in the input vector file.
   * @param ram                Available memory for processing (in MB).
   * @return Output vector data file storing samplevalues (OGR format). If not given, the input vector data file is updated
   */
  def otbSampleExtraction(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                          vec: RDD[(String, (Geometry, mutable.Map[String, Any]))], outfield: List[String] = List("prefix"), outfieldPrefixName: String = "value_",
                          outfieldListNames: List[String], field: String, layer: Int = 0, ram: Int = 256
                         ):(RDD[(String, (Geometry, mutable.Map[String, Any]))])
  = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbSOMClassification_" + time + ".tif"
    val writePath = algorithmData + "otbSOMClassification_" + time + "_out.shp"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_sampleextraction.py --in $outputTiffPath --vec $vec --outfield $outfield
        --outfield.prefix.name $outfieldPrefixName --outfield.list.names $outfieldListNames --field $field --layer $layer --ram $ram --out "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   * The application selects a set of samples from geometries intended for training (they should have a field giving the associated class).
   *
   * @param in                    Support image that will be classified
   * @param mask                  Validity mask (only pixels corresponding to a mask value greater than 0 will be used for statistics)
   * @param vec                   Input geometries to analyse
   * @param instats               Input file storing statistics (XML format)
   * @param outrates              Output rates (CSV formatted)
   * @param sampler               Type of sampling (periodic, pattern based, random)
   * @param samplerPeriodicJitter Jitter amplitude added during sample selection (0 = no jitter)
   * @param strategy              [Set samples count for each class|Set the same samples counts for all classes|
   *                              Use a percentage of the samples available for each class|Set the total number of samples to generate, and use class proportions.
   *                              |Set the same number of samples for all classes, with the smallest class fully sampled|Use all samples]
   * @param strategyByclassIn     Number of samples by class (CSV format with class name in 1st column and required samples in the 2nd.
   * @param strategyConstantNb    Number of samples for all classes
   * @param strategyPercentP      The percentage to use
   * @param strategyTotalV        The number of samples to generate
   * @param field                 Name of the field carrying the class name in the input vectors.
   * @param layer                 Layer index to read in the input vector file.
   * @param elevDem               This parameter allows selecting a directory containing Digital Elevation Model files. Note that this directory should contain only DEM files. Unexpected behaviour might occurs if other images are found in this directory. Input DEM tiles should be in a raster format supported by GDAL.
   * @param elevGeoid             Use a geoid grid to get the height above the ellipsoid in case there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles. A version of the geoid can be found on the OTB website (egm96.grd and egm96.grd.hdr at).
   * @param elevDefault           This parameter allows setting the default height above ellipsoid when there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles, and no geoid file has been set. This is also used by some application as an average elevation value.
   * @param rand                  Set a specific random seed with integer value.
   * @param ram                   Available memory for processing (in MB).
   * @return Output resampled geometries
   */

  def otbSampleSelection(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), mask: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                         vec: RDD[(String, (Geometry, mutable.Map[String, Any]))], instats: String, outrates: String, sampler: List[String] = List("periodic"), samplerPeriodicJitter: Int = 0,
                         strategy: String = "smallest", strategyByclassIn: String, strategyConstantNb: Int, strategyPercentP: Float = 0.5f, strategyTotalV: Int = 1000, field: String, layer: Int = 0,
                         elevDem: String, elevGeoid: String, elevDefault: Float = 0, rand: Int, ram: Int = 256
                        ):(RDD[(String, (Geometry, mutable.Map[String, Any]))]) = {
    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbSOMClassification_" + time + ".tif"
    val writePath = algorithmData + "otbSOMClassification_" + time + "_out.shp"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_sampleselection.py --in $outputTiffPath --mask $mask --vec $vec --instats $instats --outrates $outrates
        --sampler $sampler --sampler.periodic.jitter $samplerPeriodicJitter --strategy $strategy --strategy.byclass.in $strategyByclassIn --strategy.constant.nb $strategyConstantNb
        --strategy.percent.p $strategyPercentP --strategy.total.v $strategyTotalV --field $field --layer $layer --elev.dem $elevDem --elev.geoid $elevGeoid --elev.default $elevDefault
         --rand $rand --ram $ram --out "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * The application allows converting pixel values from DN (for Digital Numbers) to reflectance. Calibrated values are called surface reflectivity and its values lie in the range [0, 1].
   * The first level is called Top Of Atmosphere (TOA) reflectivity. It takes into account the sensor gain, sensor spectral response and the solar illuminations.
   * The second level is called Top Of Canopy (TOC) reflectivity. In addition to sensor gain and solar illuminations, it takes into account the optical thickness of the atmosphere, the atmospheric pressure, the water vapor amount, the ozone amount, as well as the composition and amount of aerosol gasses.
   * It is also possible to indicate an AERONET file which contains atmospheric parameters (version 1 and version 2 of Aeronet file are supported. Note that computing TOC reflectivity will internally compute first TOA and then TOC reflectance.
   *
   * @param in                      Input image filename
   * @param level                   Image to Top Of Atmosphere reflectance/TOA reflectance to Image/Image to Top Of Canopy reflectance (atmospheric corrections)
   * @param milli                   Flag to use milli-reflectance instead of reflectance. This allows saving the image with integer pixel type (in the range [0, 1000] instead of floating point in the range [0, 1]. In order to do that, use this option and set the output pixel type (-out filename double for example)
   * @param clamp                   Clamping in the range [0, 1]. It can be useful to preserve area with specular reflectance.
   * @param acqui                   .minute  Minute (0-59)
   * @param acquiHour               Hour (0-23)
   * @param acquiDay                Day (1-31)
   * @param acquiMonth              Month (1-12)
   * @param acquiYear               Year
   * @param acquiFluxnormcoeff      Flux Normalization Coefficient
   * @param acquiSolardistance      Solar distance (in AU)
   * @param acquiSunElev            Sun elevation angle (in degrees)
   * @param acquiSunAzim            Sun azimuth angle (in degrees)
   * @param acquiViewElev           Viewing elevation angle (in degrees)
   * @param acquiViewAzim           Viewing azimuth angle (in degrees)
   * @param acquiGainbias           A text file containing user defined gains and biases
   * @param acquiSolarilluminations Solar illuminations (one value per band, in W/m^2/micron)
   * @param ram                     Available memory for processing (in MB).
   * @return Output calibrated image filename
   */
  def otbOpticalCalibration(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            level: String = "toa", milli: Boolean = false, clamp: Boolean = true, acquiMinute: Int = 0, acquiHour: Int = 12,
                            acquiDay: Int = 1, acquiMonth: Int = 1,
                            acquiYear: Int = 2000, acquiFluxnormcoeff: Float, acquiSolardistance: Float,
                            acquiSunElev: Float = 90, acquiSunAzim: Float = 0, acquiViewElev: Float = 90,
                            acquiViewAzim: Float = 0, acquiGainbias: String, acquiSolarilluminations: String,
                            ram: Int = 256, userId: String, dagId: String
                           ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbOpticalCalibration_" + time + ".tif"
    val writePath = algorithmData + "otbOpticalCalibration_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    // docker路径
    val dockerTiffPath = algorithmDockerData + "otbOpticalCalibration_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "otbOpticalCalibration_" + time + "_out.tif"

    val getacquiGainbiasPath = loadTxtFromUpload(acquiGainbias, userId, dagId, "otb")
    val getacquiSolarilluminationsPath = loadTxtFromUpload(acquiSolarilluminations, userId, dagId, "otb")

    try {
      versouSshUtil(host, userName, password, port)

      val st = raw"""docker start serene_black; docker exec serene_black otbcli_OpticalCalibration -in "$dockerTiffPath" -level "$level" -milli $milli -clamp $clamp -acqui.minute $acquiMinute -acqui.hour $acquiHour -acqui.day $acquiDay -acqui.month $acquiMonth -acqui.year $acquiYear  -acqui.sun.elev $acquiSunElev  -acqui.sun.azim $acquiSunAzim -acqui.view.elev $acquiViewElev -acqui.view.azim $acquiViewAzim -acqui.gainbias $getacquiGainbiasPath  -acqui.solarilluminations  $getacquiSolarilluminationsPath  -ram $ram -out $writeDockerPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The application allows converting pixel values from DN (for Digital Numbers) to reflectance. Calibrated values are called surface reflectivity and its values lie in the range [0, 1].
   * The first level is called Top Of Atmosphere (TOA) reflectivity. It takes into account the sensor gain, sensor spectral response and the solar illuminations.
   * The second level is called Top Of Canopy (TOC) reflectivity. In addition to sensor gain and solar illuminations, it takes into account the optical thickness of the atmosphere, the atmospheric pressure, the water vapor amount, the ozone amount, as well as the composition and amount of aerosol gasses.
   * It is also possible to indicate an AERONET file which contains atmospheric parameters (version 1 and version 2 of Aeronet file are supported. Note that computing TOC reflectivity will internally compute first TOA and then TOC reflectance.
   *
   * @param in                      Input image filename
   * @param level                   Image to Top Of Atmosphere reflectance/TOA reflectance to Image/Image to Top Of Canopy reflectance (atmospheric corrections)
   * @param acquiGainbias           A text file containing user defined gains and biases
   * @param acquiSolarilluminations Solar illuminations (one value per band, in W/m^2/micron)
   * @param atmoAerosol             No Aerosol Model/Continental/Maritime/Urban/Desertic
   * @param atmoOz                  Stratospheric ozone layer content (in cm-atm)
   * @param atmoWa                  Total water vapor content over vertical atmospheric column (in g/cm2)
   * @param atmoPressure            Atmospheric Pressure (in hPa)
   * @param atmoOpt                 Aerosol Optical Thickness (unitless)
   * @param atmoAeronet             Aeronet file containing atmospheric parameters
   * @param atmoRsr                 Sensor relative spectral response file By default the application gets this information in the metadata
   * @param atmoRadius              Window radius for adjacency effects correctionsSetting this parameters will enable the correction ofadjacency effects
   * @param atmoPixsize             Pixel size (in km) used tocompute adjacency effects, it doesn’t have tomatch the image spacing
   * @param ram                     Available memory for processing (in MB).
   * @return Output calibrated image filename
   */
  def otbOpticalAtmospheric(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            acquiGainbias: String, acquiSolarilluminations: String,
                            atmoAerosol: String = "noaersol", atmoOz: Float = 0, atmoWa: Float = 2.5f
                            , atmoPressure: Float = 1030, atmoOpt: Float = 0.2f, atmoAeronet: String,
                            atmoRsr: String, atmoRadius: Int = 2, atmoPixsize: Float = 1, level: String = "toc", ram: Int = 256, userId: String, dagId: String
                           ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbOpticalAtmospheric_" + time + ".tif"
    val writePath = algorithmData + "otbOpticalAtmospheric_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    // docker路径
    val dockerTiffPath = algorithmDockerData + "otbOpticalAtmospheric_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "otbOpticalAtmospheric_" + time + "_out.tif"

    val getacquiGainbiasPath = loadTxtFromUpload(acquiGainbias, userId, dagId, "otb")
    val getacquiSolarilluminationsPath = loadTxtFromUpload(acquiSolarilluminations, userId, dagId, "otb")


    try {
      versouSshUtil(host, userName, password, port)

      val st = raw"""docker start serene_black; docker exec serene_black otbcli_OpticalCalibration -in "$dockerTiffPath" -level "$level"  -acqui.gainbias $getacquiGainbiasPath  -acqui.solarilluminations  $getacquiSolarilluminationsPath -atmo.aerosol "$atmoAerosol" -atmo.oz $atmoOz -atmo.wa $atmoWa -atmo.pressure $atmoPressure -atmo.opt $atmoOpt  -atmo.radius $atmoRadius -atmo.pixsize $atmoPixsize -ram $ram -out $writeDockerPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }
  /**
   * This application uses inverse sensor modelling combined with a choice of interpolation functions to resample a sensor geometry image into a ground geometry regular grid. The ground geometry regular grid is defined with respect to a map projection (see map parameter). The application offers several modes to estimate the output grid parameters (origin and ground sampling distance), including automatic estimation of image size, ground sampling distance, or both, from image metadata, user-defined ROI corners, or another ortho-image.A digital Elevation Model along with a geoid file can be specified to account for terrain deformations.In case of SPOT5 images, the sensor model can be approximated by an RPC model in order to speed-up computation.
   *
   * @param ioIn                  The input image to ortho-rectify
   * @param map                   Defines the map projection to be used.
   * @param mapUtmZone            The zone number ranges from 1 to 60 and allows defining the transverse mercator projection (along with the hemisphere)
   * @param mapUtmNorthhem        The transverse mercator projections are defined by their zone number as well as the hemisphere. Activate this parameter if your image is in the northern hemisphere.
   * @param mapEpsgCode           See www.spatialreference.org to find which EPSG code is associated to your projection
   * @param outputsMode           User Defined|Automatic Size from Spacing|Automatic Spacing from Size|Automatic Size from Spacing and output corners|Fit to ortho
   * @param outputsUlx            Cartographic X coordinate of upper-left corner (meters for cartographic projections, degrees for geographic ones)
   * @param outputsUly            Cartographic Y coordinate of the upper-left corner (meters for cartographic projections, degrees for geographic ones)
   * @param outputsSizex          Size of projected image along X (in pixels)
   * @param outputsSizey          Size of projected image along Y (in pixels)
   * @param outputsSpacingx       Size of each pixel along X axis (meters for cartographic projections, degrees for geographic ones)
   * @param outputsSpacingy       Size of each pixel along Y axis (meters for cartographic projections, degrees for geographic ones)
   * @param outputsLrx            Cartographic X coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)
   * @param outputsLry            Cartographic Y coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)
   * @param outputsOrtho          A model ortho-image that can be used to compute size, origin and spacing of the output
   * @param outputsIsotropic      Default spacing (pixel size) values are estimated from the sensor modeling of the image. It can therefore result in a non-isotropic spacing. This option allows you to force default values to be isotropic (in this case, the minimum of spacing in both direction is applied. Values overridden by user are not affected by this option.
   * @param outputsDefault        Default value to write when outside of input image.
   * @param elevDem               This parameter allows selecting a directory containing Digital Elevation Model files. Note that this directory should contain only DEM files. Unexpected behaviour might occurs if other images are found in this directory. Input DEM tiles should be in a raster format supported by GDAL.
   * @param elevGeoid             Use a geoid grid to get the height above the ellipsoid in case there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles. A version of the geoid can be found on the OTB website (egm96.grd and egm96.grd.hdr at).
   * @param elevDefault           This parameter allows setting the default height above ellipsoid when there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles, and no geoid file has been set. This is also used by some application as an average elevation value.
   * @param interpolator          This group of parameters allows one to define how the input image will be interpolated during resampling.
   * @param interpolatorBcoRadius This parameter allows one to control the size of the bicubic interpolation filter. If the target pixel size is higher than the input pixel size, increasing this parameter will reduce aliasing artifacts.
   * @param optRpc                Enabling RPC modeling allows one to speed-up SPOT5 ortho-rectification. Value is the number of control points per axis for RPC estimation
   * @param optRam                This allows setting the maximum amount of RAM available for processing. As the writing task is time consuming, it is better to write large pieces of data, which can be achieved by increasing this parameter (pay attention to your system capabilities)
   * @param optGridspacing        Resampling is done according to a coordinate mapping deformation grid, whose pixel size is set by this parameter, and expressed in the coordinate system of the output image The closer to the output spacing this parameter is, the more precise will be the ortho-rectified image,but increasing this parameter will reduce processing time.
   * @return The ortho-rectified output image
   */
  def otbOrthoRectification(implicit sc: SparkContext,
                            ioIn: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            map: String = "utm", mapUtmZone: Int = 31, mapUtmNorthhem: Boolean = false,
                            mapEpsgCode: Int = 4326, outputsMode: String = "auto",
                            outputsUlx: Double, outputsUly: Double, outputsSizex: Int,
                            outputsSizey: Int, outputsSpacingx: Double, outputsSpacingy: Double,
                            outputsLrx: Double, outputsLry: Double, outputsOrtho: String,
                            outputsIsotropic: Boolean = true, outputsDefault: Double = 0,
                            elevDem: String, elevGeoid: String, elevDefault: Float = 0,
                            interpolator: String = "bco", interpolatorBcoRadius: Int = 2,
                            optRpc: Int = 10, optRam: Int = 256,
                            optGridspacing: Double = 4): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbOrthoRectification_" + time + ".tif"
    val writePath = algorithmData + "otbOrthoRectification_" + time + "_out.tif"
    saveRasterRDDToTif(ioIn, outputTiffPath)
    // docker路径
    val dockerTiffPath = algorithmDockerData + "otbOrthoRectification_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "otbOrthoRectification_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)
      val interpolatorParams = if (interpolator == "bco") {
        s"-interpolator '$interpolator' -interpolatorBcoRadius $interpolatorBcoRadius"
      } else {
        s"--interpolator '$interpolator'"
      }

      val mapParams = if (map == "epsg") {
        s" -map.utm.zone $mapUtmZone -map.utm.northhem $mapUtmNorthhem -map.epsg.code $mapEpsgCode"
      } else {
        s" -map.utm.zone $mapUtmZone -map.utm.northhem $mapUtmNorthhem"
      }
      val st = if (outputsUlx == 0 && outputsUly == 0 && outputsSizex == 0 && outputsSizey == 0 && outputsSpacingx == 0 && outputsSpacingy == 0 && outputsLrx == 0 && outputsLry == 0) {
        raw"""docker start serene_black; docker exec serene_black otbcli_OrthoRectification -io.in $dockerTiffPath  -io.out "$writeDockerPath"""".stripMargin
      } else {
//        raw"""docker start serene_black; docker exec serene_black otbcli_OrthoRectification -io.in $dockerTiffPath -map "$map" $mapParams -outputs.mode "$outputsMode" -outputs.ulx $outputsUlx -outputs.uly $outputsUly -outputs.sizex $outputsSizex -outputs.sizey $outputsSizey -outputs.spacingx $outputsSpacingx -outputs.spacingy $outputsSpacingy  -outputs.lrx $outputsLrx -outputs.lry $outputsLry  -outputs.ortho "$outputsOrtho"  -outputs.isotropic $outputsIsotropic  -outputs.default $outputsDefault  -elev.dem "$elevDem"  -elev.geoid "$elevGeoid"  -elev.default $elevDefault  $interpolatorParams  -opt.rpc $optRpc  -opt.ram $optRam  -opt.gridspacing $optGridspacing  -io.out "$writeDockerPath"""".stripMargin
        raw"""docker start serene_black; docker exec serene_black otbcli_OrthoRectification -io.in $dockerTiffPath -map "$map" $mapParams -outputs.mode "$outputsMode" -outputs.ulx $outputsUlx -outputs.uly $outputsUly -outputs.sizex $outputsSizex -outputs.sizey $outputsSizey -outputs.spacingx $outputsSpacingx -outputs.spacingy $outputsSpacingy  -outputs.lrx $outputsLrx -outputs.lry $outputsLry  -outputs.ortho "$outputsOrtho"  -outputs.isotropic $outputsIsotropic  -outputs.default $outputsDefault   -elev.default $elevDefault  $interpolatorParams  -opt.rpc $optRpc  -opt.ram $optRam  -opt.gridspacing $optGridspacing  -io.out "$writeDockerPath"""".stripMargin

      }

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * This application performs P+XS pansharpening. The default mode use Pan and XS sensor models to estimate the transformation to superimpose XS over Pan before the fusion (“default mode”). The application provides also a PHR mode for Pleiades images which does not use sensor models as Pan and XS products are already coregistered but only estimate an affine transformation to superimpose XS over the Pan.Note that this option is automatically activated in case Pleiades images are detected as input.
   *
   * @param inp                   Input panchromatic image.
   * @param inxs                  Input XS image.
   * @param elevDem               This parameter allows selecting a directory containing Digital Elevation Model files. Note that this directory should contain only DEM files. Unexpected behaviour might occurs if other images are found in this directory. Input DEM tiles should be in a raster format supported by GDAL.
   * @param elevGeoid             Use a geoid grid to get the height above the ellipsoid in case there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles. A version of the geoid can be found on the OTB website (egm96.grd and egm96.grd.hdr at).
   * @param elevDefault           This parameter allows setting the default height above ellipsoid when there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles, and no geoid file has been set. This is also used by some application as an average elevation value.
   * @param mode                  Superimposition mode
   * @param method                Selection of the pan-sharpening method.
   * @param methodRcsRadiusx      Set the x radius of the sliding window.
   * @param methodRcsRadiusy      Set the y radius of the sliding window.
   * @param methodLmvmRadiusx     Set the x radius of the sliding window.
   * @param methodLmvmRadiusy     Set the y radius of the sliding window.
   * @param methodBayesLambda     Set the weighting value.
   * @param methodBayesS          Set the S coefficient.
   * @param lms                   Spacing of the deformation field. Default is 10 times the PAN image spacing.
   * @param interpolator          This group of parameters allows defining how the input image will be interpolated during resampling.
   * @param interpolatorBcoRadius This parameter allows controlling the size of the bicubic interpolation filter. If the target pixel size is higher than the input pixel size, increasing this parameter will reduce aliasing artifacts.
   * @param fv                    Fill value for area outside the reprojected image
   * @param ram                   Available memory for processing (in MB).
   * @return Output image.
   */
  def otbBundleToPerfectSensor(implicit sc: SparkContext, inp: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),

                               inxs: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), elevDem: String, elevGeoid: String, elevDefault: Float = 0, mode: String = "default", method: String = "rcs", methodRcsRadiusx: Int = 9,
                               methodRcsRadiusy: Int = 9, methodLmvmRadiusx: Int = 3,
                               methodLmvmRadiusy: Int = 3, methodBayesLambda: Float = 0.9999f, methodBayesS: Float = 1, lms: Float = 4, interpolator: String = "bco", interpolatorBcoRadius: Int = 2, fv: Float = 0, ram: Int = 256)
                               : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {


    val time = System.currentTimeMillis()

    val outputTiffPath_p = algorithmData + "otbBundleToPerfectSensorP_" + time + ".tif"
    val outputTiffPath_in = algorithmData + "otbBundleToPerfectSensorIN_" + time + ".tif"
    val writePath = algorithmData + "otbBundleToPerfectSensor_" + time + "_out.tif"
    saveRasterRDDToTif(inp, outputTiffPath_p)
    saveRasterRDDToTif(inxs, outputTiffPath_in)
    // docker路径
    val dockerTiffPath_p = algorithmDockerData + "otbBundleToPerfectSensorP_" + time + ".tif"
    val dockerTiffPath_in = algorithmDockerData + "otbBundleToPerfectSensorIN_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "otbBundleToPerfectSensor_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)
      val methodParams = if (method == "rcs") {
        s" -method $method -method.rcs.radiusx $methodRcsRadiusx -method.rcs.radiusy $methodRcsRadiusy"
      } else if (method == "lmvm") {
        s" -method $method -method.lmvm.radiusx $methodLmvmRadiusx -method.lmvm.radiusy $methodLmvmRadiusy"
      } else {
        s" -method $method -method.bayes.lambda $methodBayesLambda -method.bayes.s $methodBayesS"
      }

      val st = interpolator match {
        case "bco" => raw"""docker start serene_black; docker exec serene_black otbcli_BundleToPerfectSensor -inp "$dockerTiffPath_p" -inxs "$dockerTiffPath_in"  -elev.default $elevDefault -mode "$mode" $methodParams  -lms $lms -interpolator "$interpolator" -interpolator.bco.radius $interpolatorBcoRadius -fv $fv -ram $ram -out "$writeDockerPath"""".stripMargin
        case "nn" => raw"""docker start serene_black; docker exec serene_black otbcli_BundleToPerfectSensor -inp "$dockerTiffPath_p" -inxs "$dockerTiffPath_in"  -elev.default $elevDefault -mode "$mode" $methodParams  -lms $lms -interpolator "$interpolator"  -fv $fv -ram $ram -out "$writeDockerPath"""".stripMargin
        case "linear" => raw"""docker start serene_black; docker exec serene_black otbcli_BundleToPerfectSensor -inp "$dockerTiffPath_p" -inxs "$dockerTiffPath_in" -elev.default $elevDefault -mode "$mode" $methodParams  -lms $lms -interpolator "$interpolator"  -fv $fv -ram $ram -out "$writeDockerPath"""".stripMargin
      }
println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }
}

