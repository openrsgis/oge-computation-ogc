//package whu.edu.cn.oge
//
//import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
//import geotrellis.raster.{MultibandTile, Tile}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.locationtech.jts.geom._
//import whu.edu.cn.entity.SpaceTimeBandKey
//import whu.edu.cn.util.RDDTransformerUtil._
//import whu.edu.cn.util.SSHClientUtil._
//
//import scala.collection.mutable.Map
//
//object QGIS {
//  def main(args: Array[String]): Unit = {
//
//  }
//
//  /**
//   *
//   * Calculated slope direction
//   *
//   * @param sc      Alias object for SparkContext
//   * @param input   Digital Terrain Model raster layer
//   * @param zFactor Vertical exaggeration. This parameter is useful when the Z units differ from the X and Y units, for example feet and meters. You can use this parameter to adjust for this. The default is 1 (no exaggeration).
//   * @return The output aspect raster layer
//   */
//
//  def nativeAspect(implicit sc: SparkContext,
//                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                   zFactor: Double = 1.0):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/nativeAspect_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/nativeAspect_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/native_aspect.py
//             |  --input "$outputTiffPath"
//             |  --z-factor $zFactor
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//
//  /**
//   * It creates a new layer with the exact same features and geometries as the input one, but assigned to a new CRS.
//   * The geometries are not reprojected, they are just assigned to a different CRS.
//   *
//   * @param sc    Alias object for SparkContext
//   * @param input Input vector layer
//   * @param crs   Select the new CRS to assign to the vector layer
//   * @return Vector layer with assigned projection
//   */
//  def nativeAssignProjection(implicit sc: SparkContext,
//                             input: RDD[(String, (Geometry, Map[String, Any]))],
//                             crs: String = "EPSG:4326 - WGS84")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//    val time = System.currentTimeMillis()
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/nativeAssignProjection_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/nativeAssignProjection_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/native_assignprojection.py
//             |  --input "$outputShpPath"
//             |  --crs "$crs"
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Creates a new point layer, with points placed on the lines of another layer.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input line vector layer
//   * @param pointsNumber Number of points to create
//   * @param minDistance  The minimum distance between points
//   * @return The output random points layer.
//   */
//  def qgisRandomPointsAlongLine(implicit sc: SparkContext,
//                                input: RDD[(String, (Geometry, Map[String, Any]))],
//                                pointsNumber: Double = 1,
//                                minDistance: Double = 0.0)
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//    val time = System.currentTimeMillis()
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/qgisRandomPointsAlongLine_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/qgisRandomPointsAlongLine_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/qgis_randompointsalongline.py
//             |  --input "$outputShpPath"
//             |  --points-number $pointsNumber
//             |  --min-distance $minDistance
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   *
//   * Calculated slope direction
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input elevation raster layer
//   * @param band         The number of the band to use as elevation
//   * @param trigAngle    Activating the trigonometric angle results in different categories: 0° (East), 90° (North), 180° (West), 270° (South).
//   * @param zeroFlat     Activating this option will insert a 0-value for the value -9999 on flat areas.
//   * @param computeEdges Generates edges from the elevation raster
//   * @param zevenbergen  Activates Zevenbergen&Thorne formula for smooth landscapes
//   * @param options      For adding one or more creation options that control the raster to be created.
//   * @return Output raster with angle values in degrees
//   */
//  def gdalAspect(implicit sc: SparkContext,
//                 input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                 band: Int = 1,
//                 trigAngle: String = "False",
//                 zeroFlat: String = "False",
//                 computeEdges: String = "False",
//                 zevenbergen: String = "False",
//                 options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalAspect_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalAspect_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_aspect.py
//             |  --input "$outputTiffPath"
//             |  --band $band
//             |  --trig-angle $trigAngle
//             |  --zero-flat $zeroFlat
//             |  --compute-edges $computeEdges
//             |  --zevenbergen $zevenbergen
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//  }
//
//
//  /**
//   * Applies a coordinate system to a raster dataset.
//   *
//   * @param sc    Alias object for SparkContext
//   * @param input Input raster layer
//   * @param crs   The projection (CRS) of the output layer
//   * @return The output raster layer (with the new projection information)
//   */
//  def gdalAssignProjection(implicit sc: SparkContext,
//                           input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                           crs: String)
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalAssignProjection_" + time + ".tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_assignprojection.py
//             |  --input "$outputTiffPath"
//             |  --crs "$crs"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, outputTiffPath)
//  }
//
//
//  /**
//   * Create buffers around the features of a vector layer.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    The input vector layer
//   * @param distance Minimum: 0.0
//   * @param explodeCollections
//   * @param field    Field to use for dissolving
//   * @param dissolve If set, the result is dissolved. If no field is set for dissolving, all the buffers are dissolved into one feature.
//   * @param geometry The name of the input layer geometry column to use
//   * @param options  Additional GDAL creation options.
//   * @return The output buffer layer
//   */
//  def gdalBufferVectors(implicit sc: SparkContext,
//                        input: RDD[(String, (Geometry, Map[String, Any]))],
//                        distance: Double = 10.0,
//                        explodeCollections: String = "False",
//                        field: String,
//                        dissolve: String = "False",
//                        geometry: String = "geometry",
//                        options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalBufferVectors_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalBufferVectors_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_buffervectors.py
//             |  --input "$outputShpPath"
//             |  --distance $distance
//             |  --explode-collections $explodeCollections
//             |  --field $field
//             |  --dissolve $dissolve
//             |  --geometry $geometry
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Clips any GDAL-supported raster file to a given extent.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    The input raster
//   * @param projwin
//   * @param extra    Add extra GDAL command line options
//   * @param nodata   Defines a value that should be inserted for the nodata values in the output raster
//   * @param dataType Defines the format of the output raster file.
//   * @param options  For adding one or more creation options that control the raster to be created
//   * @return Output raster layer clipped by the given extent
//   */
//  def gdalClipRasterByExtent(implicit sc: SparkContext,
//                             input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                             projwin: String = "",
//                             extra: String = "",
//                             nodata: Double = 0.0,
//                             dataType: String = "0",
//                             options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipRasterByExtent_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipRasterByExtent_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10",
//      "11" -> "11"
//    ).getOrElse(dataType, "0")
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_cliprasterbyextent.py
//             |  --input "$outputTiffPath"
//             |  --projwin $projwin
//             |  --extra $extra
//             |  --nodata $nodata
//             |  --data-type $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * Clips any GDAL-supported raster by a vector mask layer.
//   *
//   * @param sc             Alias object for SparkContext
//   * @param input          The input raster
//   * @param cropToCutLine  Applies the vector layer extent to the output raster if checked.
//   * @param targetExtent   Extent of the output file to be created
//   * @param setResolution  Shall the output resolution (cell size) be specified
//   * @param extra          Add extra GDAL command line options
//   * @param targetCrs      Set the coordinate reference to use for the mask layer
//   * @param xResolution    The width of the cells in the output raster
//   * @param keepResolution The resolution of the output raster will not be changed
//   * @param alphaBand      Creates an alpha band for the result. The alpha band then includes the transparency values of the pixels.
//   * @param options        For adding one or more creation options that control the raster to be created
//   * @param mask           Vector mask for clipping the raster
//   * @param multithreading Two threads will be used to process chunks of image and perform input/output operation simultaneously. Note that computation is not multithreaded itself.
//   * @param nodata         Defines a value that should be inserted for the nodata values in the output raster
//   * @param yResolution    The height of the cells in the output raster
//   * @param dataType       Defines the format of the output raster file.
//   * @param sourceCrs      Set the coordinate reference to use for the input raster
//   * @return Output raster layer clipped by the vector layer
//   */
//  def gdalClipRasterByMaskLayer(implicit sc: SparkContext,
//                                input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                                cropToCutLine: String = "True",
//                                targetExtent: String,
//                                setResolution: String = "False",
//                                extra: String,
//                                targetCrs: String,
//                                xResolution: Double,
//                                keepResolution: String = "False",
//                                alphaBand: String = "False",
//                                options: String = "",
//                                mask: String,
//                                multithreading: String = "False",
//                                nodata: Double,
//                                yResolution: Double,
//                                dataType: String = "0",
//                                sourceCrs: String)
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipRasterByMaskLayer_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipRasterByMaskLayer_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10",
//      "11" -> "11"
//    ).getOrElse(dataType, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_cliprasterbymasklayer.py
//             |  --input "$outputTiffPath"
//             |  --crop-to-cutline $cropToCutLine
//             |  --target-extent $targetExtent
//             |  --set-resolution $setResolution
//             |  --extra $extra
//             |  --target-crs $targetCrs
//             |  --x-resolution $xResolution
//             |  --keep-resolution $keepResolution
//             |  --alpha-band $alphaBand
//             |  --options $options
//             |  --mask $mask
//             |  --multithreading $multithreading
//             |  --nodata $nodata
//             |  --y-resolution $yResolution
//             |  --data-type $dataTypeInput
//             |  --source-crs $sourceCrs
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * Clips any OGR-supported vector file to a given extent.
//   *
//   * @param sc      Alias object for SparkContext
//   * @param input   The input vector file
//   * @param extent  Defines the bounding box that should be used for the output vector file. It has to be defined in target CRS coordinates.
//   * @param options For adding one or more creation options that control the raster to be created
//   * @return The output (clipped) layer. The default format is “ESRI Shapefile”.
//   */
//  def gdalClipVectorByExtent(implicit sc: SparkContext,
//                             input: RDD[(String, (Geometry, Map[String, Any]))],
//                             extent: String,
//                             options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipVectorByExtent_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipVectorByExtent_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_clipvectorbyextent.py
//             |  --input "$outputShpPath"
//             |  --extent $extent
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Clips any OGR-supported vector layer by a mask polygon layer.
//   *
//   * @param sc      Alias object for SparkContext
//   * @param input   The input vector file
//   * @param mask    Layer to be used as clipping extent for the input vector layer.
//   * @param options Additional GDAL creation options.
//   * @return The output (masked) layer. The default format is “ESRI Shapefile”.
//   */
//  def gdalClipVectorByPolygon(implicit sc: SparkContext,
//                              input: RDD[(String, (Geometry, Map[String, Any]))],
//                              mask: String,
//                              options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipVectorByPolygon_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalClipVectorByPolygon_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_clipvectorbypolygon.py
//             |  --input "$outputShpPath"
//             |  --mask $mask
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Extracts contour lines from any GDAL-supported elevation raster.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input raster
//   * @param interval     Defines the interval between the contour lines in the given units of the elevation raster (minimum value 0)
//   * @param ignoreNodata Ignores any nodata values in the dataset.
//   * @param extra        Add extra GDAL command line options. Refer to the corresponding GDAL utility documentation.
//   * @param create3D     Forces production of 3D vectors instead of 2D. Includes elevation at every vertex.
//   * @param nodata       Defines a value that should be inserted for the nodata values in the output raster
//   * @param offset
//   * @param band         Raster band to create the contours from
//   * @param fieldName    Provides a name for the attribute in which to put the elevation.
//   * @param options      Additional GDAL creation options.
//   * @return Output vector layer with contour lines
//   */
//  def gdalContour(implicit sc: SparkContext,
//                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                  interval: Double = 10.0,
//                  ignoreNodata: String = "false",
//                  extra: String,
//                  create3D: String = "false",
//                  nodata: String,
//                  offset: Double = 0.0,
//                  band: Int = 1,
//                  fieldName: String = "ELEV",
//                  options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalContour_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalContour_" + time + "_out.shp"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_contour.py
//             |  --input "$outputTiffPath"
//             |  --interval $interval
//             |  --ignore-nodata $ignoreNodata
//             |  --extra $extra
//             |  --create3D $create3D
//             |  --nodata $nodata
//             |  --offset $offset
//             |  --band $band
//             |  --field-name $fieldName
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   *
//   * Extracts contour polygons from any GDAL-supported elevation raster.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input raster
//   * @param interval     Defines the interval between the contour lines in the given units of the elevation raster (minimum value 0)
//   * @param ignoreNodata Ignores any nodata values in the dataset.
//   * @param extra        Add extra GDAL command line options. Refer to the corresponding GDAL utility documentation.
//   * @param create3D     Forces production of 3D vectors instead of 2D. Includes elevation at every vertex.
//   * @param nodata       Defines a value that should be inserted for the nodata values in the output raster
//   * @param offset       Defines an offset from the base contour elevation for the first contour.
//   * @param band         Raster band to create the contours from
//   * @param fieldNameMax Provides a name for the attribute in which to put the maximum elevation of contour polygon. If not provided no maximum elevation attribute is attached.
//   * @param fieldNameMin Provides a name for the attribute in which to put the minimum elevation of contour polygon. If not provided no minimum elevation attribute is attached.
//   * @param options      Additional GDAL creation options.
//   * @return Output vector layer with contour polygons
//   */
//  def gdalContourPolygon(implicit sc: SparkContext,
//                         input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                         interval: Double = 10.0,
//                         ignoreNodata: String = "false",
//                         extra: String,
//                         create3D: String = "false",
//                         nodata: String,
//                         offset: Double = 0.0,
//                         band: Int = 1,
//                         fieldNameMax: String = "ELEV_MAX",
//                         fieldNameMin: String = "ELEV_MIN",
//                         options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalContourPolygon_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalContourPolygon_" + time + "_out.shp"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_contour_polygon.py
//             |  --input "$outputTiffPath"
//             |  --interval $interval
//             |  --ignore-nodata $ignoreNodata
//             |  --extra $extra
//             |  --create3D $create3D
//             |  --nodata $nodata
//             |  --offset $offset
//             |  --band $band
//             |  --field-name-max $fieldNameMax
//             |  --field-name-min $fieldNameMin
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Dissolve (combine) geometries that have the same value for a given attribute / field. The output geometries are multipart.
//   *
//   * @param sc                  Alias object for SparkContext
//   * @param input               The input layer to dissolve
//   * @param explodeCollections  Produce one feature for each geometry in any kind of geometry collection in the source file
//   * @param field               The field of the input layer to use for dissolving
//   * @param computeArea         Compute the area and perimeter of dissolved features and include them in the output layer
//   * @param keepAttributes      Keep all attributes from the input layer
//   * @param computeStatistics   Calculate statistics (min, max, sum and mean) for the numeric attribute specified and include them in the output layer
//   * @param countFeatures       Count the dissolved features and include it in the output layer.
//   * @param statisticsAttribute The numeric attribute to calculate statistics on
//   * @param options             Additional GDAL creation options.
//   * @param geometry            The name of the input layer geometry column to use for dissolving.
//   * @return The output multipart geometry layer (with dissolved geometries)
//   *
//   */
//  def gdalDissolve(implicit sc: SparkContext,
//                   input: RDD[(String, (Geometry, Map[String, Any]))],
//                   explodeCollections: String = "false",
//                   field: String,
//                   computeArea: String = "false",
//                   keepAttributes: String = "false",
//                   computeStatistics: String = "false",
//                   countFeatures: String = "false",
//                   statisticsAttribute: String,
//                   options: String = "",
//                   geometry: String = "geometry")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalDissolve_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalDissolve_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_dissolve.py
//             |  --input "$outputShpPath"
//             |  --explode-collections $explodeCollections
//             |  --field $field
//             |  --compute-area $computeArea
//             |  --keep-attributes $keepAttributes
//             |  --compute-statistics $computeStatistics
//             |  --count-features $countFeatures
//             |  --statistics-attribute $statisticsAttribute
//             |  --options $options
//             |  --geometry $geometry
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Fill raster regions with no data values by interpolation from edges.
//   * The values for the no-data regions are calculated by the surrounding pixel values using inverse distance weighting.
//   * After the interpolation a smoothing of the results takes place. Input can be any GDAL-supported raster layer.
//   * This algorithm is generally suitable for interpolating missing regions of fairly continuously varying rasters
//   * (such as elevation models for instance). It is also suitable for filling small holes and cracks in more irregularly varying images (like airphotos).
//   * It is generally not so great for interpolating a raster from sparse point data.
//   *
//   * @param sc         Alias object for SparkContext
//   * @param input      Input raster layer
//   * @param distance   The number of pixels to search in all directions to find values to interpolate from
//   * @param iterations The number of 3x3 filter passes to run (0 or more) to smoothen the results of the interpolation.
//   * @param extra      Add extra GDAL command line options
//   * @param maskLayer  A raster layer that defines the areas to fill.
//   * @param noMask     Activates the user-defined validity mask
//   * @param band       The band to operate on. Nodata values must be represented by the value 0.
//   * @param options    For adding one or more creation options that control the raster to be created
//   * @return Output raster
//   */
//  def gdalFillNodata(implicit sc: SparkContext,
//                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                     distance: Double = 10,
//                     iterations: Double = 0,
//                     extra: String,
//                     maskLayer: String,
//                     noMask: String = "False",
//                     band: Int = 1,
//                     options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalFillNodata_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalFillNodata_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_fillnodata.py
//             |  --input "$outputTiffPath"
//             |  --distance $distance
//             |  --iterations $iterations
//             |  --extra $extra
//             |  --mask-layer $maskLayer
//             |  --no-mask $noMask
//             |  --band $band
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//  }
//
//
//  /**
//   * The Moving Average is a simple data averaging algorithm.
//   *
//   * @param sc        Alias object for SparkContext
//   * @param input     Input point vector layer
//   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
//   * @param extra     Add extra GDAL command line options
//   * @param nodata    No data marker to fill empty points
//   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
//   * @param zField    Field for the interpolation
//   * @param dataType  Defines the data type of the output raster file.
//   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
//   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
//   * @param options   For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalGridAverage(implicit sc: SparkContext,
//                      input: RDD[(String, (Geometry, Map[String, Any]))],
//                      minPoints: Double = 0.0,
//                      extra: String,
//                      nodata: Double = 0.0,
//                      angle: Double = 0.0,
//                      zField: String,
//                      dataType: String = "5",
//                      radius2: Double = 0.0,
//                      radius1: Double = 0.0,
//                      options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridAverage_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridAverage_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_gridaverage_.py
//             |  --input "$outputShpPath"
//             |  --min-points $minPoints
//             |  --extra $extra
//             |  --nodata $nodata
//             |  --angle $angle
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --radius2 $radius2
//             |  --radius1 $radius1
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * The algorithm id is displayed when you hover over the algorithm in the Processing Toolbox.
//   * The parameter dictionary provides the parameter NAMEs and values.
//   * See Using processing algorithms from the console for details on how to run processing algorithms from the Python console.
//   *
//   * @param sc        Alias object for SparkContext
//   * @param input     Input point vector layer
//   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
//   * @param extra     Add extra GDAL command line options
//   * @param metric
//   * @param nodata    No data marker to fill empty points
//   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
//   * @param zField    Field for the interpolation
//   * @param dataType  Defines the data type of the output raster file.
//   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
//   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
//   * @param options   For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalGridDataMetrics(implicit sc: SparkContext,
//                          input: RDD[(String, (Geometry, Map[String, Any]))],
//                          minPoints: Double = 0.0,
//                          extra: String,
//                          metric: String = "0",
//                          nodata: Double = 0.0,
//                          angle: Double = 0.0,
//                          zField: String,
//                          dataType: String = "5",
//                          radius2: Double = 0.0,
//                          radius1: Double = 0.0,
//                          options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridDataMetrics_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridDataMetrics_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    val metricInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5"
//    ).getOrElse(metric, "0")
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_griddatametrics.py
//             |  --input "$outputShpPath"
//             |  --min-points $minPoints
//             |  --extra $extra
//             |  --metric $metricInput
//             |  --nodata $nodata
//             |  --angle $angle
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --radius2 $radius2
//             |  --radius1 $radius1
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * The Inverse Distance to a Power gridding method is a weighted average interpolator.
//   *
//   * @param sc        Alias object for SparkContext
//   * @param input     Input point vector layer
//   * @param extra     Add extra GDAL command line options
//   * @param power     Weighting power
//   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
//   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
//   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
//   * @param smoothing Smoothing parameter
//   * @param maxPoints Do not search for more points than this number.
//   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
//   * @param nodata    No data marker to fill empty points
//   * @param zField    Field for the interpolation
//   * @param dataType  Defines the data type of the output raster file.
//   * @param options   For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalGridInverseDistance(implicit sc: SparkContext,
//                              input: RDD[(String, (Geometry, Map[String, Any]))],
//                              extra: String,
//                              power: Double = 2.0,
//                              angle: Double = 0.0,
//                              radius2: Double = 0,
//                              radius1: Double = 0,
//                              smoothing: Double = 0.0,
//                              maxPoints: Double = 0.0,
//                              minPoints: Double = 0.0,
//                              nodata: Double = 0.0,
//                              zField: String,
//                              dataType: String = "5",
//                              options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridInverseDistance_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridInverseDistance_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_gridinversedistance.py
//             |  --input "$outputShpPath"
//             |  --extra $extra
//             |  --power $power
//             |  --angle $angle
//             |  --radius2 $radius2
//             |  --radius1 $radius1
//             |  --smoothing $smoothing
//             |  --max-points $maxPoints
//             |  --min-points $minPoints
//             |  --nodata $nodata
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * Computes the Inverse Distance to a Power gridding combined to the nearest neighbor method.
//   * Ideal when a maximum number of data points to use is required.
//   *
//   * @param sc        Alias object for SparkContext
//   * @param input     Input point vector layer
//   * @param extra     Add extra GDAL command line options
//   * @param power     Weighting power
//   * @param radius    The radius of the search circle
//   * @param smoothing Smoothing parameter
//   * @param maxPoints Do not search for more points than this number.
//   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
//   * @param nodata    No data marker to fill empty points
//   * @param zField    Field for the interpolation
//   * @param dataType  Defines the data type of the output raster file.
//   * @param options   For adding one or more creation options that control the raster to be created
//   * @return
//   */
//  def gdalGridInverseDistanceNearestNeighbor(implicit sc: SparkContext,
//                                             input: RDD[(String, (Geometry, Map[String, Any]))],
//                                             extra: String,
//                                             power: Double = 2.0,
//                                             radius: Double = 1.0,
//                                             smoothing: Double = 0.0,
//                                             maxPoints: Double = 12,
//                                             minPoints: Double = 0,
//                                             nodata: Double = 0.0,
//                                             zField: String,
//                                             dataType: String = "5",
//                                             options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridInverseDistanceNearestNeighbor_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridInverseDistanceNearestNeighbor_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_gridinversedistancenearestneighbor.py
//             |  --input "$outputShpPath"
//             |  --extra $extra
//             |  --power $power
//             |  --radius $radius
//             |  --smoothing $smoothing
//             |  --max-points $maxPoints
//             |  --min-points $minPoints
//             |  --nodata $nodata
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * The Linear method perform linear interpolation by computing a Delaunay triangulation of the point cloud,
//   * finding in which triangle of the triangulation the point is,
//   * and by doing linear interpolation from its barycentric coordinates within the triangle.
//   * If the point is not in any triangle, depending on the radius,
//   * the algorithm will use the value of the nearest point or the NODATA value.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    Input point vector layer
//   * @param radius   In case the point to be interpolated does not fit into a triangle of the Delaunay triangulation, use that maximum distance to search a nearest neighbour, or use nodata otherwise. If set to -1, the search distance is infinite. If set to 0, no data value will be used.
//   * @param extra    Add extra GDAL command line options
//   * @param nodata   No data marker to fill empty points
//   * @param zField   Field for the interpolation
//   * @param dataType Defines the data type of the output raster file.
//   * @param options  For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalGridLinear(implicit sc: SparkContext,
//                     input: RDD[(String, (Geometry, Map[String, Any]))],
//                     radius: Double = 1.0,
//                     extra: String,
//                     nodata: Double = 0.0,
//                     zField: String,
//                     dataType: String = "5",
//                     options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridLinear_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridLinear_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_gridlinear.py
//             |  --input "$outputShpPath"
//             |  --extra $extra
//             |  --radius $radius
//             |  --nodata $nodata
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * The Nearest Neighbor method doesn’t perform any interpolation or smoothing,
//   * it just takes the value of nearest point found in grid node search ellipse and returns it as a result.
//   * If there are no points found, the specified NODATA value will be returned.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    Input point vector layer
//   * @param extra    Add extra GDAL command line options
//   * @param nodata   No data marker to fill empty points
//   * @param angle    Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
//   * @param radius1  The first radius (X axis if rotation angle is 0) of the search ellipse
//   * @param radius2  The second radius (Y axis if rotation angle is 0) of the search ellipse
//   * @param zField   Field for the interpolation
//   * @param dataType Defines the data type of the output raster file.
//   * @param options  For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalGridNearestNeighbor(implicit sc: SparkContext,
//                              input: RDD[(String, (Geometry, Map[String, Any]))],
//                              extra: String,
//                              nodata: Double = 1,
//                              angle: Double = 0.0,
//                              radius1: Double = 0.0,
//                              radius2: Double = 0.0,
//                              zField: String,
//                              dataType: String = "5",
//                              options: String = ""
//                             )
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridNearestNeighbor_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalGridNearestNeighbor_" + time + "_out.tif"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_gridnearestneighbor.py
//             |  --input "$outputShpPath"
//             |  --extra $extra
//             |  --nodata $nodata
//             |  --angle $angle
//             |  --radius1 $radius1
//             |  --radius2 $radius2
//             |  --z-field $zField
//             |  --data-type $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeChangedRasterRDDFromTif(sc, writePath)
//  }
//
//
//  /**
//   * Outputs a raster with a nice shaded relief effect. It’s very useful for visualizing the terrain.
//   * You can optionally specify the azimuth and altitude of the light source,
//   * a vertical exaggeration factor and a scaling factor to account for differences between vertical and horizontal units.
//   *
//   * @param sc                Alias object for SparkContext
//   * @param input             Input Elevation raster layer
//   * @param combined
//   * @param computeEdges      Generates edges from the elevation raster
//   * @param extra             Add extra GDAL command line options
//   * @param band              Band containing the elevation information
//   * @param altitude          Defines the altitude of the light, in degrees. 90 if the light comes from above the elevation raster, 0 if it is raking light.
//   * @param zevenbergenThorne Activates Zevenbergen&Thorne formula for smooth landscapes
//   * @param zFactor           The factor exaggerates the height of the output elevation raster
//   * @param multidirectional
//   * @param scale             The ratio of vertical units to horizontal units
//   * @param azimuth           Defines the azimuth of the light shining on the elevation raster in degrees. If it comes from the top of the raster the value is 0, if it comes from the east it is 90 a.s.o.
//   * @param options           For adding one or more creation options that control the raster to be created
//   * @return Output raster with interpolated values
//   */
//  def gdalHillShade(implicit sc: SparkContext,
//                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                    combined: String = "False",
//                    computeEdges: String = "False",
//                    extra: String = "",
//                    band: Int = 1,
//                    altitude: Double = 45.0,
//                    zevenbergenThorne: String = "False",
//                    zFactor: Double = 1.0,
//                    multidirectional: String = "False",
//                    scale: Double = 1.0,
//                    azimuth: Double = 315.0,
//                    options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalHillShade_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalHillShade_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_hillshade.py
//             |  --input "$outputTiffPath"
//             |  --combined $combined
//             |  --compute-edges $computeEdges
//             |  --extra $extra
//             |  --band $band
//             |  --altitude $altitude
//             |  --zevenbergen-thorne $zevenbergenThorne
//             |  --z-factor $zFactor
//             |  --multidirectional $multidirectional
//             |  --scale $scale
//             |  --azimuth $azimuth
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//  }
//
//
//  /**
//   * Converts nearly black/white borders to black.
//   *
//   * @param sc      Alias object for SparkContext
//   * @param input   Input Elevation raster layer
//   * @param white   Search for nearly white (255) pixels instead of nearly black pixels
//   * @param extra   Add extra GDAL command line options
//   * @param near    Select how far from black, white or custom colors the pixel values can be and still considered near black, white or custom color.
//   * @param options For adding one or more creation options that control the raster to be created
//   * @return Output raster
//   */
//  def gdalNearBlack(implicit sc: SparkContext,
//                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                    white: String = "False",
//                    extra: String,
//                    near: Int = 15,
//                    options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalNearBlack_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalNearBlack_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_nearblack.py
//             |  --input "$outputTiffPath"
//             |  --white $white
//             |  --extra $extra
//             |  --near $near
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//  }
//
//
//  /**
//   * Offsets lines by a specified distance. Positive distances will offset lines to the left, and negative distances will offset them to the right.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    Input vector layer
//   * @param distance The offset distance
//   * @param geometry The name of the input layer geometry column to use
//   * @param options  For adding one or more creation options that control the vector layer to be created
//   * @return The output offset curve layer
//   */
//  def gdalOffsetCurve(implicit sc: SparkContext,
//                      input: RDD[(String, (Geometry, Map[String, Any]))],
//                      distance: Double = 10.0,
//                      geometry: String = "geometry",
//                      options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//    val time = System.currentTimeMillis()
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalOffsetCurve_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalOffsetCurve_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_offsetcurve.py
//             |  --input "$outputShpPath"
//             |  --distance $distance
//             |  --geometry $geometry
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Creates a buffer on one side (right or left) of the lines in a line vector layer.
//   *
//   * @param sc         Alias object for SparkContext
//   * @param input      The input line layer
//   * @param distance   The buffer distance
//   * @param explodeCollections
//   * @param field      Field to use for dissolving
//   * @param bufferSide 0: Right, 1: Left
//   * @param dissolve   If set, the result is dissolved. If no field is set for dissolving, all the buffers are dissolved into one feature.
//   * @param geometry   The name of the input layer geometry column to use
//   * @param options    For adding one or more creation options that control the vector layer to be created
//   * @return
//   */
//  def gdalOneSideBuffer(implicit sc: SparkContext,
//                        input: RDD[(String, (Geometry, Map[String, Any]))],
//                        distance: Double = 10.0,
//                        explodeCollections: String = "False",
//                        field: String,
//                        bufferSide: String = "0",
//                        dissolve: String = "False",
//                        geometry: String = "geometry",
//                        options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//    val time = System.currentTimeMillis()
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalOneSideBuffer_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalOneSideBuffer_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    val bufferSideInput: String = Map(
//      "0" -> "0",
//      "1" -> "1"
//    ).getOrElse(bufferSide, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_onesidebuffer.py
//             |  --input "$outputShpPath"
//             |  --distance $distance
//             |  --explodecollections $explodeCollections
//             |  --field $field
//             |  --bufferSide $bufferSideInput
//             |  --dissolve $dissolve
//             |  --geometry $geometry
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Generates a point on each line of a line vector layer at a distance from start. The distance is provided as a fraction of the line length.
//   *
//   * @param sc       Alias object for SparkContext
//   * @param input    The input line layer
//   * @param distance The distance from the start of the line
//   * @param geometry The name of the input layer geometry column to use
//   * @param options  For adding one or more creation options that control the vector layer to be created
//   * @return
//   */
//  def gdalPointsAlongLines(implicit sc: SparkContext,
//                           input: RDD[(String, (Geometry, Map[String, Any]))],
//                           distance: Double = 0.5,
//                           geometry: String = "geometry",
//                           options: String = "")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//    val time = System.currentTimeMillis()
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalPointsAlongLines_" + time + ".shp"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalPointsAlongLines_" + time + "_out.shp"
//    saveFeatureRDDToShp(input, outputShpPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_pointsalonglines.py
//             |  --input "$outputShpPath"
//             |  --distance $distance
//             |  --geometry $geometry
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Creates vector polygons for all connected regions of pixels in the raster sharing a common pixel value.
//   * Each polygon is created with an attribute indicating the pixel value of that polygon.
//   *
//   * @param sc                 Alias object for SparkContext
//   * @param input              Input raster layer
//   * @param extra              Add extra GDAL command line options
//   * @param field              Specify the field name for the attributes of the connected regions.
//   * @param band               If the raster is multiband, choose the band you want to use
//   * @param eightConnectedness If not set, raster cells must have a common border to be considered connected (4-connected). If set, touching raster cells are also considered connected (8-connected).
//   * @return Output vector layer
//   */
//  def gdalPolygonize(implicit sc: SparkContext,
//                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                     extra: String,
//                     field: String = "DN",
//                     band: Int = 1,
//                     eightConnectedness: String = "False")
//  : RDD[(String, (Geometry, Map[String, Any]))] = {
//
//    val time = System.currentTimeMillis()
//
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalPolygonize_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalPolygonize_" + time + "_out.shp"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_polygonize.py
//             |  --input "$outputTiffPath"
//             |  --extra $extra
//             |  --field $field
//             |  --band $band
//             |  --eightconnectedness $eightConnectedness
//             |  --output "$writePath"""".stripMargin
//
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeFeatureRDDFromShp(sc, writePath)
//  }
//
//
//  /**
//   * Generates a raster proximity map indicating the distance from the center of each pixel to the center of the nearest pixel identified as a target pixel.
//   * Target pixels are those in the source raster for which the raster pixel value is in the set of target pixel values.
//   *
//   * @param sc          Alias object for SparkContext
//   * @param input       Input Elevation raster layer
//   * @param extra       Add extra GDAL command line options
//   * @param nodata      Specify the nodata value to use for the output raster
//   * @param values      A list of target pixel values in the source image to be considered target pixels. If not specified, all non-zero pixels will be considered target pixels.
//   * @param band        Band containing the elevation information
//   * @param maxDistance The maximum distance to be generated. The nodata value will be used for pixels beyond this distance. If a nodata value is not provided, the output band will be queried for its nodata value. If the output band does not have a nodata value, then the value 65535 will be used. Distance is interpreted according to the value of Distance units.
//   * @param replace     Specify a value to be applied to all pixels that are closer than the maximum distance from target pixels (including the target pixels) instead of a distance value.
//   * @param units       Indicate whether distances generated should be in pixel or georeferenced coordinates
//   * @param dataType    Defines the data type of the output raster file.
//   * @param options     For adding one or more creation options that control the vector layer to be created
//   * @return Output raster
//   */
//  def gdalProximity(implicit sc: SparkContext,
//                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                    extra: String,
//                    nodata: Double = 0.00,
//                    values: String = "",
//                    band: Int = 1,
//                    maxDistance: Double = 0.0,
//                    replace: Double = 0.0,
//                    units: String = "1",
//                    dataType: String = "5",
//                    options: String = "")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalProximity_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalProximity_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    val unitsInput: String = Map(
//      "0" -> "0",
//      "1" -> "1"
//    ).getOrElse(units, "1")
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10"
//    ).getOrElse(dataType, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_proximity.py
//             |  --input "$outputTiffPath"
//             |  --extra $extra
//             |  --nodata $nodata
//             |  --values $values
//             |  --band $band
//             |  --maxdistance $maxDistance
//             |  --replace $replace
//             |  --units $unitsInput
//             |  --datatype $dataTypeInput
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//  }
//
//
//  /**
//   * Overwrites a raster layer with values from a vector layer. New values are assigned based on the attribute value of the overlapping vector feature.
//   *
//   * @param sc          Alias object for SparkContext
//   * @param input       Input vector layer
//   * @param inputRaster Input raster layer
//   * @param extra       Add extra GDAL command line options
//   * @param field       Defines the attribute field to use to set the pixels values
//   * @param add         If False, pixels are assigned the selected field’s value. If True, the selected field’s value is added to the value of the input raster layer.
//   * @return The overwritten input raster layer
//   */
//  def gdalRasterizeOver(implicit sc: SparkContext,
//                        input: RDD[(String, (Geometry, Map[String, Any]))],
//                        inputRaster: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                        extra: String = "",
//                        field: String,
//                        add: String = "False")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOver_" + time + ".shp"
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOver_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOver_" + time + "_out.tif"
//
//    saveFeatureRDDToShp(input, outputShpPath)
//    saveRasterRDDToTif(inputRaster, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_rasterize_over.py
//             |  --input "$outputShpPath"
//             |  --inputraster "$outputTiffPath"
//             |  --extra $extra
//             |  --field $field
//             |  --add $add
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, inputRaster, writePath)
//  }
//
//
//  /**
//   * Overwrites parts of a raster layer with a fixed value. The pixels to overwrite are chosen based on the supplied (overlapping) vector layer.
//   *
//   * @param sc          Alias object for SparkContext
//   * @param input       Input vector layer
//   * @param inputRaster Input raster layer
//   * @param burn        The value to burn
//   * @param extra       Add extra GDAL command line options
//   * @param add         If False, pixels are assigned the selected field’s value. If True, the selected field’s value is added to the value of the input raster layer.
//   * @return
//   */
//  def gdalRasterizeOverFixedValue(implicit sc: SparkContext,
//                                  input: RDD[(String, (Geometry, Map[String, Any]))],
//                                  inputRaster: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                                  burn: Double = 0.0,
//                                  extra: String = "",
//                                  add: String = "False")
//  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputShpPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOverFixedValue_" + time + ".shp"
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOverFixedValue_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRasterizeOverFixedValue_" + time + "_out.tif"
//
//    saveFeatureRDDToShp(input, outputShpPath)
//    saveRasterRDDToTif(inputRaster, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_rasterize_over_fixed_value.py
//             |  --input "$outputShpPath"
//             |  --inputraster "$outputTiffPath"
//             |  --extra $extra
//             |  --add $add
//             |  --burn $burn
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, inputRaster, writePath)
//  }
//
//
//  /**
//   * Converts a 24 bit RGB image into a 8 bit paletted.
//   * Computes an optimal pseudo-color table for the given RGB-image using a median cut algorithm on a downsampled RGB histogram.
//   * Then it converts the image into a pseudo-colored image using the color table.
//   * This conversion utilizes Floyd-Steinberg dithering (error diffusion) to maximize output image visual quality.
//   *
//   * @param sc      Alias object for SparkContext
//   * @param input   Input (RGB) raster layer
//   * @param ncolors The number of colors the resulting image will contain. A value from 2-256 is possible.
//   * @return Output raster layer.
//   */
//  def gdalRgbToPct(implicit sc: SparkContext,
//                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                   ncolors: Double = 2):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRgbToPct_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRgbToPct_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_rgbtopct.py
//             |  --input "$outputTiffPath"
//             |  --ncolors $ncolors
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//
//  /**
//   * Outputs a single-band raster with values computed from the elevation.
//   * Roughness is the degree of irregularity of the surface.
//   * It’s calculated by the largest inter-cell difference of a central pixel and its surrounding cell.
//   * The determination of the roughness plays a role in the analysis of terrain elevation data,
//   * it’s useful for calculations of the river morphology, in climatology and physical geography in general.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input elevation raster layer
//   * @param band         The number of the band to use as elevation
//   * @param computeEdges Generates edges from the elevation raster
//   * @param options      Additional GDAL command line options
//   * @return Single-band output roughness raster. The value -9999 is used as nodata value.
//   */
//  def gdalRoughness(implicit sc: SparkContext,
//                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                    band: Int = 1,
//                    computeEdges: String = "False",
//                    options: String = ""):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRoughness_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalRoughness_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_roughness.py
//             |  --input "$outputTiffPath"
//             |  --band $band
//             |  --computeEdges $computeEdges
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//
//  /**
//   * Generates a slope map from any GDAL-supported elevation raster.
//   * Slope is the angle of inclination to the horizontal.
//   * You have the option of specifying the type of slope value you want: degrees or percent slope.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input Elevation raster layer
//   * @param band         Band containing the elevation information
//   * @param computeEdges Generates edges from the elevation raster
//   * @param asPercent    Express slope as percent instead of degrees
//   * @param extra        Additional GDAL command line options
//   * @param scale        The ratio of vertical units to horizontal units
//   * @param zevenbergen  Activates Zevenbergen&Thorne formula for smooth landscapes
//   * @param options      Additional GDAL command line options
//   * @return Output raster
//   */
//  def gdalSlope(implicit sc: SparkContext,
//                input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                band: Int = 1,
//                computeEdges: String = "False",
//                asPercent: String = "False",
//                extra: String,
//                scale: Double = 1.0,
//                zevenbergen: String = "False",
//                options: String = ""):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalSlope_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalSlope_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_slope.py
//             |  --input "$outputTiffPath"
//             |  --band $band
//             |  --computeEdges $computeEdges
//             |  --asPercent $asPercent
//             |  --extra $extra
//             |  --scale $scale
//             |  --zevenbergen $zevenbergen
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//
//  /**
//   * Outputs a single-band raster with values computed from the elevation.
//   * TPI stands for Topographic Position Index,
//   * which is defined as the difference between a central pixel and the mean of its surrounding cells.
//   *
//   * @param sc           Alias object for SparkContext
//   * @param input        Input elevation raster layer
//   * @param band         The number of the band to use for elevation values
//   * @param computeEdges Generates edges from the elevation raster
//   * @param options      Additional GDAL command line options
//   * @return Output raster.
//   */
//  def gdalTpiTopographicPositionIndex(implicit sc: SparkContext,
//                                      input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                                      band: Int = 1,
//                                      computeEdges: String = "False",
//                                      options: String = ""):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalTpiTopographicPositionIndex_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalTpiTopographicPositionIndex_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_tpitopographicpositionindex.py
//             |  --input "$outputTiffPath"
//             |  --band $band
//             |  --computeEdges $computeEdges
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//
//  /**
//   * Converts raster data between different formats.
//   *
//   * @param sc              Alias object for SparkContext
//   * @param input           Input raster layer
//   * @param extra           Additional GDAL command line options
//   * @param targetCrs       Specify a projection for the output file
//   * @param nodata          Defines the value to use for nodata in the output raster
//   * @param dataType        Defines the data type of the output raster file.
//   * @param copySubdatasets Create individual files for subdatasets
//   * @param options         For adding one or more creation options that control the raster to be created
//   * @return Output (translated) raster layer.
//   */
//  def gdalTranslate(implicit sc: SparkContext,
//                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                    extra: String,
//                    targetCrs: String,
//                    nodata: Double,
//                    dataType: String = "0",
//                    copySubdatasets: String = "False",
//                    options: String = ""):
//  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time = System.currentTimeMillis()
//
//    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalTranslate_" + time + ".tif"
//    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/gdalTranslate_" + time + "_out.tif"
//    saveRasterRDDToTif(input, outputTiffPath)
//
//
//    val dataTypeInput: String = Map(
//      "0" -> "0",
//      "1" -> "1",
//      "2" -> "2",
//      "3" -> "3",
//      "4" -> "4",
//      "5" -> "5",
//      "6" -> "6",
//      "7" -> "7",
//      "8" -> "8",
//      "9" -> "9",
//      "10" -> "10",
//      "11" -> "11"
//    ).getOrElse(dataType, "0")
//
//    try {
//      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
//      val st =
//        raw"""conda activate qgis
//             |cd /home/geocube/oge/oge-server/dag-boot/qgis
//             |python algorithmCode/gdal_translate.py
//             |  --input "$outputTiffPath"
//             |  --extra $extra
//             |  --targetCrs $targetCrs
//             |  --nodata $nodata
//             |  --dataType $dataTypeInput
//             |  --copySubdatasets $copySubdatasets
//             |  --options $options
//             |  --output "$writePath"""".stripMargin
//
//      println(s"st = $st")
//      runCmd(st, "UTF-8")
//
//    } catch {
//      case e: Exception =>
//        e.printStackTrace()
//    }
//
//    makeRasterRDDFromTif(sc, input, writePath)
//
//  }
//
//}
//
//
//
//
//
//
//
//
//
//
//
