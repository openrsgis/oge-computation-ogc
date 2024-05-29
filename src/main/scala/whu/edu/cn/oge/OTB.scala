package whu.edu.cn.oge

import java.io.{BufferedReader, BufferedWriter, FileWriter, InputStreamReader, OutputStreamWriter, PrintWriter}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bouncycastle.util.StringList
import org.locationtech.jts.geom.{Geometry, _}
import org.sparkproject.dmg.pmml.False
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.{dagId, runMain, stringList, workTaskJson}
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._
import whu.edu.cn.oge.Feature._
import whu.edu.cn.config.GlobalConfig

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import scala.xml.XML

object OTB {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    print("ok")

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
   * Compute statistics of the features in a set of OGR Layers.
   *
   * @param sc      Alias object for SparkContext.
   * @param inshp   Vector Data,Name of the input shapefile.
   * @param feat  List of features to consider for statistics.
   * @return XML file containing mean and variance of each feature.
   */
  def otbComputeOGRLayersFeaturesStatistics(implicit sc: SparkContext,
                        inshp: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                            feat: List[String]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = algorithmData + "otbComputeOGRLayersFeaturesStatistics_" + time + ".shp"
    val writePath = algorithmData + "otbComputeOGRLayersFeaturesStatistics_" + time + "_out.xml"
    saveFeatureRDDToShp(inshp,outputShpPath)

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
    val readXMLData=XML.load(writePath)
    val readXMLDatastr:String=readXMLData.toString
  }

  /**
   * Unsupervised KMeans image classification.
   *
   * @param sc      Alias object for SparkContext.
   * @param in   Input image filename.
   * @param nc Number of modes, which will be used to generate class membership.
   * @param ts  Size of the training set (in pixels).
   * @param maxit  Maximum number of iterations for the learning step. If this parameter is set to 0, the KMeans algorithm will not stop until convergence.
   * @return Output image containing class labels.
   */
  def otbKMeansClassification(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              nc: Int = 5, ts: Int = 100,maxit:Int=1000,centroidsIn:String,centroidsOut:String,ram:Int=256,
                              sampler:List[String]=List("periodic"),samplerPeriodicJitter:Int=0,
                              vm:String,nodatalabel:Int=0,cleanup:Boolean=true,rand:Int):
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
   * @param sc    Alias object for SparkContext.
   * @param in    Input image to classify.
   * @param vm    Validity mask (only pixels corresponding to a mask value greater than 0 will be used for learning).
   * @param tp    Probability for a sample to be selected in the training set.
   * @param ts    Maximum training set size (in pixels).
   * @param som   Output image containing the Self-Organizing Map.
   * @param sx    X size of the SOM map.
   * @param sy    Y size of the SOM map.
   * @param nx    X size of the initial neighborhood in the SOM map.
   * @param ny    Y size of the initial neighborhood in the SOM map.
   * @param ni    Number of iterations for SOM learning.
   * @param bi    Initial learning coefficient.
   * @param bf Final learning coefficient.
   * @param iv Maximum initial neuron weight.
   * @param rand Set a specific random seed with integer value.
   * @param ram Available memory for processing (in MB).
   * @return Output classified image (each pixel contains the index of its corresponding vector in the SOM).
   */
  def otbSOMClassification(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                           vm: String, tp: Float = 1f, ts: Int, som: String, sx: Int=32,
                           sy: Int = 32, nx: Int = 10, ny: Int = 10,ni: Int = 5,bi: Float = 0.1f,
                           bf: Float = 0.1f,iv: Float = 0f,rand: Int,ram: Int = 256
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
   *  The application extracts samples values from animage using positions contained in a vector data file.
   *
   * @param  in  Support image
   * @param  vec  Vector data file containing samplingpositions. (OGR format)
   * @param  outfield  Choice between naming method for output fields
   * @param  outfieldPrefixName  Prefix used to form the field names thatwill contain the extracted values.
   * @param  outfieldListNames  Full list of output field names.
   * @param  field  Name of the field carrying the class name in the input vectors.
   * @param  layer  Layer index to read in the input vector file.
   * @param  ram  Available memory for processing (in MB).
   * @return Output vector data file storing samplevalues (OGR format). If not given, the input vector data file is updated
   */
  def otbSampleExtraction(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                          vec: RDD[(String, (Geometry, mutable.Map[String, Any]))], outfield: List[String] = List("prefix"), outfieldPrefixName:String = "value_",
  outfieldListNames: List[String], field:String,layer:Int=0,ram:Int=256
                          ):
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
   * @param in       Support image that will be classified
   * @param mask     Validity mask (only pixels corresponding to a mask value greater than 0 will be used for statistics)
   * @param vec      Input geometries to analyse
   * @param instats  Input file storing statistics (XML format)
   * @param outrates Output rates (CSV formatted)
   * @param sampler  Type of sampling (periodic, pattern based, random)
   * @param samplerPeriodicJitter  Jitter amplitude added during sample selection (0 = no jitter)
   * @param strategy [Set samples count for each class|Set the same samples counts for all classes|
   *                 Use a percentage of the samples available for each class|Set the total number of samples to generate, and use class proportions.
   *                 |Set the same number of samples for all classes, with the smallest class fully sampled|Use all samples]
   * @param strategyByclassIn  Number of samples by class (CSV format with class name in 1st column and required samples in the 2nd.
   * @param strategyConstantNb  Number of samples for all classes
   * @param strategyPercentP The percentage to use
   * @param strategyTotalV  The number of samples to generate
   * @param field    Name of the field carrying the class name in the input vectors.
   * @param layer    Layer index to read in the input vector file.
   * @param elevDem  This parameter allows selecting a directory containing Digital Elevation Model files. Note that this directory should contain only DEM files. Unexpected behaviour might occurs if other images are found in this directory. Input DEM tiles should be in a raster format supported by GDAL.
   * @param elevGeoid  Use a geoid grid to get the height above the ellipsoid in case there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles. A version of the geoid can be found on the OTB website (egm96.grd and egm96.grd.hdr at).
   * @param elevDefault  This parameter allows setting the default height above ellipsoid when there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles, and no geoid file has been set. This is also used by some application as an average elevation value.
   * @param rand     Set a specific random seed with integer value.
   * @param ram      Available memory for processing (in MB).
   * @return Output resampled geometries
   */

  def otbSampleSelection(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), mask: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                         vec: RDD[(String, (Geometry, mutable.Map[String, Any]))],instats:String,outrates:String, sampler:List[String]=List("periodic"),samplerPeriodicJitter:Int=0,
                         strategy:String="smallest",strategyByclassIn:String,strategyConstantNb:Int, strategyPercentP:Float=0.5f,strategyTotalV:Int=1000,field:String,layer:Int=0,
                         elevDem:String,elevGeoid:String,elevDefault:Float=0,rand:Int,ram:Int=256
                          )= {
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
   * @param in    Input image filename
   * @param level  Image to Top Of Atmosphere reflectance/TOA reflectance to Image/Image to Top Of Canopy reflectance (atmospheric corrections)
   * @param milli  Flag to use milli-reflectance instead of reflectance. This allows saving the image with integer pixel type (in the range [0, 1000] instead of floating point in the range [0, 1]. In order to do that, use this option and set the output pixel type (-out filename double for example)
   * @param clamp Clamping in the range [0, 1]. It can be useful to preserve area with specular reflectance.
   * @param acqui.minute  Minute (0-59)
   * @param acquiHour  Hour (0-23)
   * @param acquiDay  Day (1-31)
   * @param acquiMonth  Month (1-12)
   * @param acquiYear  Year
   * @param acquiFluxnormcoeff  Flux Normalization Coefficient
   * @param acquiSolardistance  Solar distance (in AU)
   * @param acquiSunElev  Sun elevation angle (in degrees)
   * @param acquiSunAzim  Sun azimuth angle (in degrees)
   * @param acquiViewElev  Viewing elevation angle (in degrees)
   * @param acquiViewAzim  Viewing azimuth angle (in degrees)
   * @param acquiGainbias  A text file containing user defined gains and biases
   * @param acquiSolarilluminations  Solar illuminations (one value per band, in W/m^2/micron)
   * @param atmoAerosol    No Aerosol Model/Continental/Maritime/Urban/Desertic
   * @param atmoOz  Stratospheric ozone layer content (in cm-atm)
   * @param atmoWa  Total water vapor content over vertical atmospheric column (in g/cm2)
   * @param atmoPressure  Atmospheric Pressure (in hPa)
   * @param atmoOpt  Aerosol Optical Thickness (unitless)
   * @param atmoAeronet  Aeronet file containing atmospheric parameters
   * @param atmoRsr  Sensor relative spectral response file By default the application gets this information in the metadata
   * @param atmoRadius  Window radius for adjacency effects correctionsSetting this parameters will enable the correction ofadjacency effects
   * @param atmoPixsize  Pixel size (in km) used tocompute adjacency effects, it doesn’t have tomatch the image spacing
   * @param ram    Available memory for processing (in MB).
   * @return Output calibrated image filename
   */
  def otbOpticalCalibration(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            level: String="toa", milli: Boolean = false, clamp: Boolean= true, acquiMinute: Int= 0, acquiHour: Int= 12, acquiDay: Int= 1, acquiMonth: Int= 1,
                           acquiYear: Int= 2000, acquiFluxnormcoeff: Float, acquiSolardistance: Float, acquiSunElev: Float= 90, acquiSunAzim: Float= 0, acquiViewElev: Float= 90,
                           acquiViewAzim: Float= 0, acquiGainbias: String, acquiSolarilluminations: String, atmoAerosol: String= "noaersol", atmoOz: Float= 0, atmoWa: Float= 2.5f
                         , atmoPressure: Float= 1030, atmoOpt: Float= 0.2f, atmoAeronet: String, atmoRsr: String, atmoRadius: Int= 2, atmoPixsize: Float= 1, ram: Int= 256
                          ):(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbOpticalCalibration_" + time + ".tif"
    val writePath = algorithmData + "otbOpticalCalibration_" + time + "_out.tif"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_opticalcalibration.py --in $outputTiffPath --level $level --milli $milli --clamp $clamp
             --acqui.minute $acquiMinute --acqui.hour $acquiHour --acqui.day $acquiDay --acqui.month $acquiMonth --acqui.year $acquiYear
             --acqui.fluxnormcoeff $acquiFluxnormcoeff --acqui.solardistance $acquiSolardistance --acqui.sun.elev $acquiSunElev
             --acqui.sun.azim $acquiSunAzim --acqui.view.elev $acquiViewElev --acqui.view.azim $acquiViewAzim --acqui.gainbias $acquiGainbias --acqui.solarilluminations $acquiSolarilluminations
              --atmo.aerosol $atmoAerosol --atmo.oz $atmoOz --atmo.wa $atmoWa --atmo.pressure $atmoPressure --atmo.opt $atmoOpt --atmo.aeronet $atmoAeronet
              --atmo.rsr $atmoRsr --atmo.radius $atmoRadius --atmo.pixsize $atmoPixsize --ram $ram --out "$writePath"""".stripMargin


      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }
  /**
   *  This application uses inverse sensor modelling combined with a choice of interpolation functions to resample a sensor geometry image into a ground geometry regular grid. The ground geometry regular grid is defined with respect to a map projection (see map parameter). The application offers several modes to estimate the output grid parameters (origin and ground sampling distance), including automatic estimation of image size, ground sampling distance, or both, from image metadata, user-defined ROI corners, or another ortho-image.A digital Elevation Model along with a geoid file can be specified to account for terrain deformations.In case of SPOT5 images, the sensor model can be approximated by an RPC model in order to speed-up computation.
   *
   * @param  ioIn  The input image to ortho-rectify
   * @param  map  Defines the map projection to be used.
   * @param  mapUtmZone  The zone number ranges from 1 to 60 and allows defining the transverse mercator projection (along with the hemisphere)
   * @param  mapUtmNorthhem  The transverse mercator projections are defined by their zone number as well as the hemisphere. Activate this parameter if your image is in the northern hemisphere.
   * @param  mapEpsgCode  See www.spatialreference.org to find which EPSG code is associated to your projection
   * @param  outputsMode  User Defined|Automatic Size from Spacing|Automatic Spacing from Size|Automatic Size from Spacing and output corners|Fit to ortho
   * @param  outputsUlx  Cartographic X coordinate of upper-left corner (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsUly  Cartographic Y coordinate of the upper-left corner (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsSizex  Size of projected image along X (in pixels)
   * @param  outputsSizey  Size of projected image along Y (in pixels)
   * @param  outputsSpacingx  Size of each pixel along X axis (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsSpacingy  Size of each pixel along Y axis (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsLrx  Cartographic X coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsLry  Cartographic Y coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)
   * @param  outputsOrtho  A model ortho-image that can be used to compute size, origin and spacing of the output
   * @param  outputsIsotropic  Default spacing (pixel size) values are estimated from the sensor modeling of the image. It can therefore result in a non-isotropic spacing. This option allows you to force default values to be isotropic (in this case, the minimum of spacing in both direction is applied. Values overridden by user are not affected by this option.
   * @param  outputsDefault  Default value to write when outside of input image.
   * @param  elevDem  This parameter allows selecting a directory containing Digital Elevation Model files. Note that this directory should contain only DEM files. Unexpected behaviour might occurs if other images are found in this directory. Input DEM tiles should be in a raster format supported by GDAL.
   * @param  elevGeoid  Use a geoid grid to get the height above the ellipsoid in case there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles. A version of the geoid can be found on the OTB website (egm96.grd and egm96.grd.hdr at).
   * @param  elevDefault  This parameter allows setting the default height above ellipsoid when there is no DEM available, no coverage for some points or pixels with no_data in the DEM tiles, and no geoid file has been set. This is also used by some application as an average elevation value.
   * @param  interpolator  This group of parameters allows one to define how the input image will be interpolated during resampling.
   * @param  interpolatorBcoRadius  This parameter allows one to control the size of the bicubic interpolation filter. If the target pixel size is higher than the input pixel size, increasing this parameter will reduce aliasing artifacts.
   * @param  optRpc  Enabling RPC modeling allows one to speed-up SPOT5 ortho-rectification. Value is the number of control points per axis for RPC estimation
   * @param  optRam  This allows setting the maximum amount of RAM available for processing. As the writing task is time consuming, it is better to write large pieces of data, which can be achieved by increasing this parameter (pay attention to your system capabilities)
   * @param  optGridspacing  Resampling is done according to a coordinate mapping deformation grid, whose pixel size is set by this parameter, and expressed in the coordinate system of the output image The closer to the output spacing this parameter is, the more precise will be the ortho-rectified image,but increasing this parameter will reduce processing time.
   * @return The ortho-rectified output image
   */
  def otbOrthoRectification(implicit sc: SparkContext, in: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            map: String = "utm", mapUtmZone: Int = 31, mapUtmNorthhem: Boolean = false, mapEpsgCode: Int = 4326, outputsMode: String = "auto",
                            outputsUlx: Double, outputsUly: Double, outputsSizex: Int, outputsSizey: Int, outputsSpacingx: Double, outputsSpacingy: Double,
                            outputsLrx: Double, outputsLry: Double, outputsOrtho: String, outputsIsotropic: Boolean = true, outputsDefault: Double = 0,
                            elevDem: String, elevGeoid: String, elevDefault: Float = 0, interpolator: String = "bco", interpolatorBcoRadius: Int = 2, optRpc: Int = 10, optRam: Int = 256,
                            optGridspacing: Double = 4): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbOrthoRectification_" + time + ".tif"
    val writePath = algorithmData + "otbOrthoRectification_" + time + "_out.tif"
    saveRasterRDDToTif(in, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_orthorectification.py --io.in $outputTiffPath --map $map --map.utm.zone $mapUtmZone --map.utm.northhem $mapUtmNorthhem
             |--map.epsg.code $mapEpsgCode --outputs.mode $outputsMode --outputs.ulx $outputsUlx --outputs.uly $outputsUly --outputs.sizex $outputsSizex --outputs.sizey $outputsSizey
             |--outputs.spacingx $outputsSpacingx --outputs.spacingy $outputsSpacingy --outputs.lrx $outputsLrx --outputs.lry $outputsLry --outputs.ortho $outputsOrtho
             |--outputs.isotropic $outputsIsotropic --outputs.default $outputsDefault --elev.dem $elevDem --elev.geoid $elevGeoid --elev.default $elevDefault --interpolator $interpolator
             |--interpolator.bco.radius $interpolatorBcoRadius --opt.rpc $optRpc --opt.ram $optRam --opt.gridspacing $optGridspacing --io.out "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }
  /**
   *  This application performs P+XS pansharpening. Pansharpening is a process of merging high-resolution panchromatic and lower resolution multispectral imagery to create a single high-resolution color image. Algorithms available in the applications are: RCS, bayesian fusion and Local Mean and Variance Matching(LMVM).
   *
   * @param  inp               Input panchromatic image.
   * @param  inxs              Input XS image.
   * @param  method            Selection of the pan-sharpening method.
   * @param  methodRcsRadiusx  Set the x radius of the sliding window.
   * @param  methodRcsRadiusy  Set the y radius of the sliding window.
   * @param  methodLmvmRadiusx Set the x radius of the sliding window.
   * @param  methodLmvmRadiusy Set the y radius of the sliding window.
   * @param  methodBayesLambda Set the weighting value.
   * @param  methodBayesS      Set the S coefficient.
   * @param  ram               Available memory for processing (in MB).
   * @return Output image.
   */
  def otbPansharpening(implicit sc: SparkContext, inp: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                       inxs: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),method:String="rcs",
                       methodRcsRadiusx:Int=9,methodRcsRadiusy:Int=9,methodLmvmRadiusx:Int=3,methodLmvmRadiusy:Int=3,methodBayesLambda:Float=0.9999f,methodBayesS:Float=1f,ram:Int=256)
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath_p = algorithmData + "otbPansharpeningP_" + time + ".tif"
    val outputTiffPath_in = algorithmData + "otbPansharpeningIN_" + time + ".tif"
    val writePath = algorithmData + "otbPansharpening_" + time + "_out.tif"
    saveRasterRDDToTif(inp, outputTiffPath_p)
    saveRasterRDDToTif(inxs, outputTiffPath_in)
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_pansharpening.py --inp $outputTiffPath_p --inxs $outputTiffPath_in    --method $method --method.rcs.radiusx $methodRcsRadiusx --method.rcs.radiusy
             |$methodRcsRadiusy --method.lmvm.radiusx $methodLmvmRadiusx --method.lmvm.radiusy $methodLmvmRadiusy --method.bayes.lambda
             |$methodBayesLambda --method.bayes.s $methodBayesS --ram $ram --out "$writePath"""".stripMargin


      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }

}
