package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal.ZFactor
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom._
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.{dagId, runMain, workTaskJson}
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._

import scala.collection.mutable
import scala.collection.mutable.Map

object QGIS {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
  }


  /**
   *
   * Calculated slope direction
   *
   * @param sc      Alias object for SparkContext
   * @param input   Digital Terrain Model raster layer
   * @param zFactor Vertical exaggeration. This parameter is useful when the Z units differ from the X and Y units, for example feet and meters. You can use this parameter to adjust for this. The default is 1 (no exaggeration).
   * @return The output aspect raster layer
   */
  def nativeAspect(implicit sc: SparkContext,
                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                   zFactor: Double = 1.0):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeAspect_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/nativeAspect_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_aspect.py --input "$outputTiffPath" --z-factor $zFactor --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Generates a slope map from any GDAL-supported elevation raster.
   * Slope is the angle of inclination to the horizontal.
   * You have the option of specifying the type of slope value you want: degrees or percent slope.
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input Elevation raster layer
   * @param zFactor Vertical exaggeration.
   * @return Output raster
   */
  def nativeSlope(implicit sc: SparkContext,
                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  zFactor: Double = 1.0):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeSlope_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/nativeSlope_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_slope.py --input "$outputTiffPath" --z-factor "$zFactor" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }


    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input raster layer
   * @param minimum Minimum pixel value to use in the rescaled layer
   * @param maximum Maximum pixel value to use in the rescaled layer
   * @param band If the raster is multiband, choose a band.
   * @return Output raster layer with rescaled band values
   */
  def nativeRescaleRaster(implicit sc: SparkContext,
                          input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                          minimum: Double = 0,
                          maximum: Double = 255.0,
                          band: Int = 1):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeRescaleRaster_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/nativeRescaleRaster_" + time + "_out.tif"


    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_rescaleraster.py --input "$outputTiffPath" --minimum $minimum --maximum $maximum --band $band --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc,outputTiffPath)

  }

  /**
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input raster layer
   * @param zFactor Vertical exaggeration.
   * @return The output ruggedness raster layer
   */
  def nativeRuggednessIndex(implicit sc: SparkContext,
                            input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            zFactor: Double = 1.0):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeRuggednessIndex_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/nativeRuggednessIndex_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_ruggednessindex.py --input "$outputTiffPath" --z-factor $zFactor --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input vector layer
   * @param distance Distance to offset geometries, in layer units
   * @param bearing  Clockwise angle starting from North, in degree (°) unit
   * @return The output (projected) point vector layer
   */
  def nativeProjectPoints(implicit sc: SparkContext,
                          input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                          distance: Double = 1.0,
                          bearing: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeProjectPoints_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeProjectPoints_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_projectpointcartesian.py --input "$outputTiffPath" --distance $distance --bearing $bearing --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }
  /**
   *
   * @param sc             Alias object for SparkContext
   * @param input          Input vector layer
   * @param fieldType      Type of the new field. You can choose between
   * @param fieldPrecision Precision of the field. Useful with Float field type.
   * @param fieldName      Name of the new field
   * @param fieldLength    Length of the field
   * @return Vector layer with new field added
   */
  def nativeAddField(implicit sc: SparkContext,
                     input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                     fieldType: String = "0",
                     fieldPrecision: Double = 0,
                     fieldName: String = "",
                     fieldLength: Double = 10):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeAddField_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAddField_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_addfieldtoattributestable.py --input "$outputTiffPath" --field-type "$fieldType" --field-precision $fieldPrecision --field-name "$fieldName" --field-length $fieldLength --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc     Alias object for SparkContext
   * @param input  Input vector layer
   * @param crs    Coordinate reference system to use for the generated x and y fields.
   * @param prefix Prefix to add to the new field names to avoid name collisions with fields in the input layer.
   * @return Vector layer with new field added
   */
  def nativeAddXYField(implicit sc: SparkContext,
                       input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                       crs: String = "EPSG:4326",
                       prefix: String = ""):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeAddXYField_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAddXYField_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_addxyfields.py --input "$outputTiffPath" --crs "$crs" --prefix "$prefix" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input vector layer
   * @param scaleX    Scaling value (expansion or contraction) to apply on the X axis.
   * @param scaleZ    Scaling value (expansion or contraction) to apply on the Z axis.
   * @param rotationZ Angle of the rotation in degrees.
   * @param scaleY    Scaling value (expansion or contraction) to apply on the Y axis.
   * @param scaleM    Scaling value (expansion or contraction) to apply on m values.
   * @param deltaM    Displacement to apply on the M axis.
   * @param deltaX    Displacement to apply on the X axis.
   * @param deltaY    Displacement to apply on the Y axis.
   * @param deltaZ    Displacement to apply on the Z axis.
   * @return Output (transformed) vector layer.
   */
  def nativeAffineTransform(implicit sc: SparkContext,
                            input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                            scaleX: Double = 1,
                            scaleZ: Double = 1,
                            rotationZ: Double = 0,
                            scaleY: Double = 1,
                            scaleM: Double = 1,
                            deltaM: Double = 0,
                            deltaX: Double = 0,
                            deltaY: Double = 0,
                            deltaZ: Double = 0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeAffineTransform_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAffineTransform_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_affinetransform.py --input "$outputTiffPath" --scale-x $scaleX --scale-y $scaleY --scale-z $scaleY --rotation-z $rotationZ --scale-m $scaleM --delta-m $deltaM --delta-x $deltaX --delta-y $deltaY --delta-z $deltaZ --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc    Alias object for SparkContext
   * @param input Input vector layer
   * @return The output line vector layer split at the antimeridian.
   */
  def nativeAntimeridianSplit(implicit sc: SparkContext,
                              input: RDD[(String, (Geometry, mutable.Map[String, Any]))]):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeAntimeridianSplit_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAntimeridianSplit_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_antimeridiansplit.py --input "$outputTiffPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input vector layer
   * @param segments   Number of line segments to use to approximate a quarter circle when creating rounded offsets
   * @param joinStyle  Specify whether round, miter or beveled joins should be used when offsetting corners in a line
   * @param offset     Specify the output line layer with offset features
   * @param count      Number of offset copies to generate for each feature
   * @param miterLimit Only applicable for mitered join styles
   * @return Output line layer with offset features.
   */
  def nativeArrayOffsetLines(implicit sc: SparkContext,
                             input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                             segments: Double = 8,
                             joinStyle: String = "0",
                             offset: Double = 1.0,
                             count: Double = 10,
                             miterLimit: Double = 2.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))]= {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeArrayOffsetLines_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeArrayOffsetLines_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_affinetransform.py --input "$outputTiffPath" --segments $segments --join-style "$joinStyle" --offset $offset --count $count --miter-limit $miterLimit --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc     Alias object for SparkContext
   * @param input  Input vector layer
   * @param count  Number of copies to generate for each feature
   * @param deltaM Displacement to apply on M
   * @param deltaX Displacement to apply on the X axis
   * @param deltaY Displacement to apply on the Y axis
   * @param deltaZ Displacement to apply on the Z axis
   * @return Output vector layer with translated (moved) copies of the features
   */
  def nativeTranslatedFeatures(implicit sc: SparkContext,
                               input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                               count: Double = 10,
                               deltaM: Double = 0,
                               deltaX: Double = 0,
                               deltaY: Double = 0,
                               deltaZ: Double = 0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeTranslatedFeatures_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeTranslatedFeatures_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_arraytranslatedfeatures.py --input "$outputTiffPath" --count $count --delta-m $deltaM --delta-x $deltaX --delta-y $deltaY --delta-z $deltaZ --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   * It creates a new layer with the exact same features and geometries a
   * s the input one, but assigned to a new CRS.
   * The geometries are not reprojected, they are just assigned to a different CRS.
   *
   * @param sc    Alias object for SparkContext
   * @param input Input vector layer
   * @param crs   Select the new CRS to assign to the vector layer
   * @return Vector layer with assigned projection
   */
  def nativeAssignProjection(implicit sc: SparkContext,
                             input: RDD[(String, (Geometry, Map[String, Any]))],
                             crs: String = "EPSG:4326 - WGS84")
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeAssignProjection_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAssignProjection_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_assignprojection.py --input "$outputShpPath" --crs "$crs" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input vector layer
   * @param segments   Controls the number of line segments to use to approximate a quarter circle
   * @param distance   Offset distance
   * @param joinStyle  Specifies whether round, miter or beveled joins should be used when offsetting corners in a line
   * @param miterLimit Controls the maximum distance from the offset curve
   * @return
   */
  def nativeOffsetLine(implicit sc: SparkContext,
                       input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                       segments: Int = 8,
                       distance: Double = 10,
                       joinStyle: String = "0",
                       miterLimit: Double = 2.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeOffsetLine_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeOffsetLine_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_offsetline.py --input "$outputTiffPath" --segments $segments --join-style "$joinStyle" --distance $distance --miter-limit $miterLimit --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc          Alias object for SparkContext
   * @param input       Input line or polygon vector layer
   * @param startOffset Distance from the beginning of the input line
   * @param distance    Distance between two consecutive points along the line
   * @param endOffset   Distance from the end of the input line
   * @return Point vector layer with features placed along lines or polygon
   */
  def nativePointsAlongLines(implicit sc: SparkContext,
                             input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                             startOffset: Double = 0.0,
                             distance: Double = 1.0,
                             endOffset: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativePointsAlongLines_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePointsAlongLines_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_pointsalonglines.py --input "$outputTiffPath" --start-offset $startOffset --distance $distance --end-offset $endOffset --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input line vector layer
   * @param keepFields Check to keep the field
   * @return The output polygon vector layer from lines
   */
  def nativePolygonize(implicit sc: SparkContext,
                       input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                       keepFields: String = "False"):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativePolygonize_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePolygonize_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_polygonize.py --input "$outputTiffPath" --keep-fields "$keepFields" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc    Alias object for SparkContext
   * @param input Input polygon vector layer
   * @return The output line vector layer from polygons
   */
  def nativePolygonsToLines(implicit sc: SparkContext,
                            input: RDD[(String, (Geometry, mutable.Map[String, Any]))]):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativePolygonsToLines_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePolygonsToLines_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_polygonstolines.py --input "$outputTiffPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc                       Alias object for SparkContext
   * @param input                    Input polygon vector layer
   * @param minDistance              The minimum distance between points within one polygon feature
   * @param includePolygonAttributes a point will get the attributes from the line
   * @param maxTriesPerPoint         The maximum number of tries per point
   * @param pointsNumber             Number of points to create
   * @param minDistanceGlobal        The global minimum distance between points
   * @return The output random points layer.
   */
  def nativeRandomPointsInPolygons(implicit sc: SparkContext,
                                   input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                   minDistance: Double = 0.0,
                                   includePolygonAttributes: String = "True",
                                   maxTriesPerPoint: Int = 10,
                                   pointsNumber: Int = 1,
                                   minDistanceGlobal: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeRandomPointsInPolygons_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeRandomPointsInPolygons_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_randompointsinpolygons.py --input "$outputTiffPath" --min-distance $minDistance --include-polygon-attributes "$includePolygonAttributes" --max-tries-per-point $maxTriesPerPoint --points-number $pointsNumber --min-distance-global $minDistanceGlobal --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc                    Alias object for SparkContext
   * @param input                 Input polygon vector layer
   * @param minDistance           The minimum distance between points within one polygon feature
   * @param includeLineAttributes a point will get the attributes from the line
   * @param maxTriesPerPoint      The maximum number of tries per point
   * @param pointsNumber          Number of points to create
   * @param minDistanceGlobal     The global minimum distance between points
   * @return
   */
  def nativeRandomPointsOnLines(implicit sc: SparkContext,
                                input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                minDistance: Double = 0.0,
                                includeLineAttributes: String = "True",
                                maxTriesPerPoint: Int = 10,
                                pointsNumber: Int = 1,
                                minDistanceGlobal: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeRandomPointsOnLines_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeRandomPointsOnLines_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_randompointsonlines.py --input "$outputTiffPath" --min-distance $minDistance --include-line-attributes "$includeLineAttributes" --max-tries-per-point $maxTriesPerPoint --points-number $pointsNumber --min-distance-global $minDistanceGlobal --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc     Alias object for SparkContext
   * @param input  Input vector layer
   * @param anchor X,Y coordinates of the point to rotate the features around
   * @param angle  Angle of the rotation in degrees
   * @return The output vector layer with rotated geometries
   */
  def nativeRotateFeatures(implicit sc: SparkContext,
                           input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                           anchor: String = "",
                           angle: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeRotateFeatures_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeRotateFeatures_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_rotatefeatures.py --input "$outputTiffPath" --anchor "$anchor" --angle $angle --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input line or polygon vector layer
   * @param method    Simplification method
   * @param tolerance Threshold tolerance
   * @return The output (simplified) vector layer
   */
  def nativeSimplify(implicit sc: SparkContext,
                     input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                     method: String = "0",
                     tolerance: Double = 1.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeSimplify_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeSimplify_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_simplifygeometries.py --input "$outputTiffPath" --method "$method" --tolerance $tolerance --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input line or polygon vector layer
   * @param maxAngle   Every node below this value will be smoothed
   * @param iterations Increasing the number of iterations will give smoother geometries
   * @param offset     Increasing values will move the smoothed lines
   * @return Increasing values will move the smoothed lines
   */
  def nativeSmooth(implicit sc: SparkContext,
                   input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                   maxAngle: Double = 180.0,
                   iterations: Int = 1,
                   offset: Double = 0.25):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeSmooth_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeSmooth_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_smoothgeometry.py --input "$outputTiffPath" --max-angle $maxAngle --iterations $iterations --offset $offset --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc    Alias object for SparkContext
   * @param input The input vector layer
   * @return Output (swapped) vector layer
   */
  def nativeSwapXY(implicit sc: SparkContext,
                   input: RDD[(String, (Geometry, mutable.Map[String, Any]))]):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeSwapXY_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeSwapXY_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_swapxy.py --input "$outputTiffPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   *
   * @param sc     Alias object for SparkContext
   * @param input  Input line vector layer
   * @param side   Choose the side of the transect. Available options are
   * @param length Length in map unit of the transect
   * @param angle  Change the angle of the transect
   * @return Output line layer
   */
  def nativeTransect(implicit sc: SparkContext,
                     input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                     side: String = "",
                     length: Double = 5.0,
                     angle: Double = 90.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/nativeTransect_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeTransect_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_transect.py --input "$outputTiffPath" --side "$side" --length $length --angle $angle --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)

  }

  /**
   * Moves the geometries within a layer, by offsetting with a predefined X and Y displacement.
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input vector layer
   * @param delta_x Displacement to apply on the X axis
   * @param delta_y Displacement to apply on the Y axis
   * @param delta_z Displacement to apply on the Z axis
   * @param delta_m Displacement to apply on the M axis
   * @return Output vector layer
   */
  def nativeTranslateGeometry(implicit sc:SparkContext,
                              input:RDD[(String, (Geometry, Map[String, Any]))],
                              delta_x:Double=0.0,
                              delta_y:Double=0.0,
                              delta_z:Double=0.0,
                              delta_m:Double=0.0):
  RDD[(String, (Geometry, Map[String, Any]))]={
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeTranslateGeometry_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeTranslateGeometry_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_translategeometry.py --input "$outputShpPath" --delta-x $delta_x --delta-y $delta_y --delta-z $delta_z --delta-m $delta_m --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Generates a new layer based on an existing one, with a different type of geometry.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input vector layer
   * @param geometryType Geometry type to apply to the output features. One of:0 — Centroids;1 — Nodes;2 — Linestrings;3 — Multilinestrings;4 — Polygons
   * @return Output vector layer - the type depends on the parameters
   */
  def nativeConvertGeometryType(implicit sc:SparkContext,
                                input:RDD[(String,(Geometry,Map[String,Any]))],
                                geometryType :String="0"):
  RDD[(String, (Geometry, Map[String, Any]))]={
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeConvertGeometryType_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeConvertGeometryType_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_convertgeometrytype.py --input "$outputShpPath" --type $geometryType --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Generates a polygon layer using as polygon rings the lines from an input line layer.
   *
   * @param sc    Alias object for SparkContext
   * @param input Input line vector layer
   * @return The output polygon vector layer.
   */
  def nativeLinesToPolygons(implicit sc:SparkContext,
                            input:RDD[(String,(Geometry,Map[String,Any]))])
  :RDD[(String,(Geometry,Map[String,Any]))]={
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeLinesToPolygons_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeLinesToPolygons_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_linestopolygons.py --input "$outputShpPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Given a distance of proximity, identifies nearby point features and radially distributes them over a circle whose center represents their barycenter.
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input point vector layer
   * @param proximity  Distance below which point features are considered close. Close features are distributed altogether.
   * @param distance   Radius of the circle on which close features are placed
   * @param horizontal When only two points are identified as close, aligns them horizontally on the circle instead of vertically.
   * @return Output point vector layer
   */
  def nativePointsDisplacement(implicit sc: SparkContext,
                               input: RDD[(String, (Geometry, Map[String, Any]))],
                               proximity:Double=1.0,
                               distance:Double=1.0,
                               horizontal:String="False")
  :RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativePointsDisplacement_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePointsDisplacement_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_pointsdisplacement.py --input "$outputShpPath" --proximity $proximity --distance $distance --horizontal $horizontal --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates a new point layer, with points placed on the lines of another layer.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input line vector layer
   * @param pointsNumber Number of points to create
   * @param minDistance  The minimum distance between points
   * @return The output random points layer.
   */
  def nativaRandomPointsAlongLine(implicit sc: SparkContext,
                                  input: RDD[(String, (Geometry, Map[String, Any]))],
                                  pointsNumber: Int=1,
                                  minDistance: Double=0.0)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativaRandomPointsAlongLine_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativaRandomPointsAlongLine_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_randompointsalongline.py --input "$outputShpPath" --points-number $pointsNumber --min-distance $minDistance --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates a new point layer with a given number of random points, all of them within the extent of a given layer.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input polygon layer defining the area
   * @param pointsNumber Number of points to create
   * @param minDistance  The minimum distance between points
   * @return The output random points layer.
   */
  def nativeRandomPointsInLayerBounds(implicit sc: SparkContext,
                                      input: RDD[(String, (Geometry, Map[String, Any]))],
                                      pointsNumber: Int=1,
                                      minDistance: Double=0.0)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeRandomPointsInLayerBounds_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeRandomPointsInLayerBounds_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_randompointsinlayerbounds.py --input "$outputShpPath" --points-number $pointsNumber --min-distance $minDistance --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  // TODO: 关于reference layer的输入问题
  /**
   * Calculates the rotation required to align point features with their nearest feature from another reference layer.
   *
   * @param sc             Alias object for SparkContext
   * @param input          Point features to calculate the rotation for
   * @param referenceLayer Layer to find the closest feature from for rotation calculation
   * @param maxDistance    If no reference feature is found within this distance, no rotation is assigned to the point feature.
   * @param fieldName      Field in which to store the rotation value.
   * @param applySymbology Rotates the symbol marker of the features using the angle field value
   * @return The point layer appended with a rotation field. If loaded to QGIS, it is applied by default the input layer symbology, with a data-defined rotation of its marker symbol.
   */
  def nativeAngleToNearest(implicit sc: SparkContext,
                           input: RDD[(String, (Geometry, Map[String, Any]))],
                           referenceLayer:String,
                           maxDistance: Double,
                           fieldName:String="rotation",
                           applySymbology: String="True")
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeAngleToNearest_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeAngleToNearest_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_angletonearest.py --input "$outputShpPath" --max-distance $maxDistance --field-name $fieldName --apply-symbology $applySymbology --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Returns the closure of the combinatorial boundary of the input geometries
   *
   * @param sc    Alias object for SparkContext
   * @param input Input line or polygon vector layer
   * @return Boundaries from the input layer (point for line, and line for polygon)
   */
  def nativeBoundary(implicit sc: SparkContext,
                     input: RDD[(String, (Geometry, Map[String, Any]))])
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeBoundary_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeBoundary_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_boundary.py --input "$outputShpPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Calculates the minimum enclosing circles of the features in the input layer.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input vector layer
   * @param segments The number of segment used to approximate a circle. Minimum 8, maximum 100000.
   * @return The output polygon vector layer.
   */
  def nativeMiniEnclosingCircle(implicit sc: SparkContext,
                                input: RDD[(String, (Geometry, Map[String, Any]))],
                                segments: Int=72)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeMiniEnclosingCircle_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeMiniEnclosingCircle_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_minimumenclosingcircle.py --input "$outputShpPath" --segments $segments --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Computes multi-ring (donut) buffer for the features of the input layer, using a fixed or dynamic distance and number of rings.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input vector layer
   * @param rings    The number of rings. It can be a unique value (same number of rings for all the features) or it can be taken from features data (the number of rings depends on feature values).
   * @param distance Distance between the rings. It can be a unique value (same distance for all the features) or it can be taken from features data (the distance depends on feature values).
   * @return The output polygon vector layer.
   */
  def nativeMultiRingConstantBuffer(implicit sc: SparkContext,
                                    input: RDD[(String, (Geometry, Map[String, Any]))],
                                    rings: Int=1,
                                    distance: Double=1.0)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeMultiRingConstantBuffer_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeMultiRingConstantBuffer_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_multiringconstantbuffer.py --input "$outputShpPath" --rings $rings --distance $distance --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Calculates the minimum area rotated rectangle for each feature in the input layer.
   *
   * @param sc    Alias object for SparkContext
   * @param input Input vector layer
   * @return The output polygon vector layer.
   */
  def nativeOrientedMinimumBoundingBox(implicit sc: SparkContext,
                                       input: RDD[(String, (Geometry, Map[String, Any]))])
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativeOrientedMinimumBoundingBox_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeOrientedMinimumBoundingBox_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_orientedminimumboundingbox.py --input "$outputShpPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  // TODO: python文件参数列表和官网有出入
  /**
   * For each feature of the input layer, returns a point that is guaranteed to lie on the surface of the feature geometry.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input vector layer
   * @param allParts If checked, a point will be created for each part of the geometry.
   * @return The output point vector layer.
   */
  def nativePointOnSurface(implicit sc: SparkContext,
                           input: RDD[(String, (Geometry, Map[String, Any]))],
                           allParts: String)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativePointOnSurface_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePointOnSurface_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_pointonsurface.py --input "$outputShpPath" --all-parts $allParts --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Calculates the pole of inaccessibility for a polygon layer, which is the most distant internal point from the boundary of the surface.
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input vector layer
   * @param tolerance Set the tolerance for the calculation
   * @return The output point vector layer
   */
  def nativePoleOfInaccessibility(implicit sc: SparkContext,
                                  input: RDD[(String, (Geometry, Map[String, Any]))],
                                  tolerance: Double=1.0)
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/nativePoleOfInaccessibility_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativePoleOfInaccessibility_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_poleofinaccessibility.py --input "$outputShpPath" --tolerance $tolerance --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates a buffer area with a rectangle, oval or diamond shape for each feature of the input point layer.
   * The shape parameters can be fixed for all features or dynamic using a field or an expression.
   *
   * @param sc        Alias object for SparkContext.
   * @param input     Input point vector layer.
   * @param rotation  Rotation of the buffer shape.
   * @param shape     The shape to use. one of: 0 --- Rectangles 1 --- Ovals 2 --- Diamonds.
   * @param segments  Number of segments for a full circle (Ovals shape).
   * @param width     Width of the buffer shape.
   * @param height    Height of the buffer shape.
   * @return          The output vector layer (with the buffer shapes).
   */
  def nativeRectanglesOvalsDiamonds(implicit sc: SparkContext,
                                    input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                    rotation: Double = 0.0,
                                    shape: String = "",
                                    segments: Int = 36,
                                    width: Double = 1.0,
                                    height: Double = 1.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/nativeRectangelsOvalsDiamonds_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeRectangelsOvalsDiamonds_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    val shapeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2"
    ).getOrElse(shape, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_rectanglesovalsdiamonds.py --input "$outputShpPath" --rotation $rotation --shape "$shapeInput" --segments $segments --width $width --height $height --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Computes a buffer on lines by a specified distance on one side of the line only.
   *
   * @param sc          Alias object for SparkContext.
   * @param input       Input line vector layer.
   * @param side        Which side to create the buffer on. One of: 0 -- Left 1 -- Right.
   * @param distance    Buffer distance.
   * @param segments    Controls the number of line segments to use to approximate a quarter circle when creating rounded offsets.
   * @param joinStyle   Options are: 0 --- Round 1 --- Miter 2 --- Bevel.
   * @param miterLimit  Sets the maximum distance from the offset geometry to use when creating a mitered join as a factor of the offset distance.
   * @return            Output (buffer) polygon layer
   */
  def nativeSingleSidedBuffer(implicit sc: SparkContext,
                              input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                              side: String = "0",
                              distance: Double = 10.0,
                              segments: Int = 8,
                              joinStyle: String = "0",
                              miterLimit: Double = 2.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/nativeSingleSidedBuffer_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeSingleSidedBuffer_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    val sideInput: String = Map(
      "0" -> "0",
      "1" -> "1"
    ).getOrElse(side, "0")

    val joinStyleInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2"
    ).getOrElse(joinStyle, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_singlesidedbuffer.py --input "$outputShpPath" --side "$sideInput" --distance $distance --segments $segments --joinStyle "$joinStyleInput" --miterLimit $miterLimit --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates tapered buffer along line geometries, using a specified start and end buffer diameter.
   *
   * @param sc          Alias object for SparkContext.
   * @param input       Input line vector layer.
   * @param segments    Controls the number of line segments to use to approximate a quarter circle when creating rounded offsets.
   * @param startWidth  Represents the radius of the buffer applied at the start point of the line feature.
   * @param endWidth    Represents the radius of the buffer applied at the end point of the line feature.
   * @return            Output (buffer) polygon layer.
   */
  def nativeTaperedBuffer(implicit sc: SparkContext,
                          input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                          segments: Int = 16,
                          startWidth: Double = 0.0,
                          endWidth: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/nativeTaperedBuffer_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeTaperedBuffer_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_taperedbuffer.py --input "$outputShpPath" --segments $segments --startWidth $startWidth --endWidth $endWidth --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates wedge shaped buffers from input points.
   *
   * @param sc          Alias object for SparkContext.
   * @param input       Input point vector layer.
   * @param innerRadius Inner radius value. If 0 the wedge will begin from the source point.
   * @param outerRadius The outer size (length) of the wedge: the size is meant from the source point to the edge of the wedge shape.
   * @param width       Width (in degrees) of the buffer.
   * @param azimuth     Angle (in degrees) as the middle value of the wedge.
   * @return            The output (wedge buffer) vector layer.
   */
  def nativeWedgeBuffers(implicit sc: SparkContext,
                         input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                         innerRadius: Double = 0.0,
                         outerRadius: Double = 1.0,
                         width: Double = 45.0,
                         azimuth: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/nativeWedgeBuffers_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/nativeWedgeBuffers_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/native_wedgebuffers.py --input "$outputShpPath" --innerRadius $innerRadius --outerRadius $outerRadius --width $width --azimuth $azimuth --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Computes the concave hull of the features in an input point layer.
   *
   * @param sc              Alias object for SparkContext.
   * @param input           Input point vector layer.
   * @param noMultigeometry Check if you want to have singlepart geometries instead of multipart ones.
   * @param holes           Choose whether to allow holes in the final concave hull.
   * @param alpha           Number from 0 (maximum concave hull) to 1 (convex hull).
   * @return                The output vector layer.
   */
  def nativeConcaveHull(implicit sc: SparkContext,
                        input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                        noMultigeometry: String = "True",
                        holes: String = "True",
                        alpha: Double = 0.3):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/qgisConcaveHull_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/qgisConcaveHull_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_concavehull.py --input "$outputShpPath" --noMultigeometry "$noMultigeometry" --holes "$holes" --alpha $alpha --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates a polygon layer with the Delaunay triangulation corresponding to the input point layer.
   *
   * @param sc    Alias object for SparkContext.
   * @param input Input point vector layer.
   * @return      The output (Delaunay triangulation) vector layer.
   */
  def nativeDelaunayTriangulation(implicit sc: SparkContext,
                                  input: RDD[(String, (Geometry, mutable.Map[String, Any]))]):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/qgisDelaunayTriangulation_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/qgisDelaunayTriangulation_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_delaunaytriangulation.py --input "$outputShpPath" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Takes a point layer and generates a polygon layer containing the Voronoi polygons corresponding to those input points.
   *
   * @param sc     Alias object for SparkContext.
   * @param input  Input point vector layer.
   * @param buffer The extent of the output layer will be this much bigger than the extent of the input layer.
   * @return       Voronoi polygons of the input point vector layer.
   */
  def nativeVoronoiPolygons(implicit sc: SparkContext,
                            input: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                            buffer: Double = 0.0):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/qgisVoronoiPolygons_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/qgisVoronoiPolygons_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/qgis_voronoipolygons.py --input "$outputShpPath" --buffer $buffer --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   *
   * Calculated slope direction
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input elevation raster layer
   * @param band         The number of the band to use as elevation
   * @param trigAngle    Activating the trigonometric angle results in different categories: 0° (East), 90° (North), 180° (West), 270° (South).
   * @param zeroFlat     Activating this option will insert a 0-value for the value -9999 on flat areas.
   * @param computeEdges Generates edges from the elevation raster
   * @param zevenbergen  Activates Zevenbergen&Thorne formula for smooth landscapes
   * @param options      For adding one or more creation options that control the raster to be created.
   * @return Output raster with angle values in degrees
   */
  def gdalAspect(implicit sc: SparkContext,
                 input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                 band: Int = 1,
                 trigAngle: String = "False",
                 zeroFlat: String = "False",
                 computeEdges: String = "False",
                 zevenbergen: String = "False",
                 options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalAspect_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalAspectlzy_" + time + "_out.tif"

    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_aspect.py --input "$outputTiffPath" --band $band --trig-angle "$trigAngle" --zero-flat "$zeroFlat" --compute-edges "$computeEdges" --zevenbergen "$zevenbergen" --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)
  }

  /**
   * Extracts contour lines from any GDAL-supported elevation raster.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input raster
   * @param interval     Defines the interval between the contour lines in the given units of the elevation raster (minimum value 0)
   * @param ignoreNodata Ignores any nodata values in the dataset.
   * @param extra        Add extra GDAL command line options. Refer to the corresponding GDAL utility documentation.
   * @param create3D     Forces production of 3D vectors instead of 2D. Includes elevation at every vertex.
   * @param nodata       Defines a value that should be inserted for the nodata values in the output raster
   * @param offset
   * @param band         Raster band to create the contours from
   * @param fieldName    Provides a name for the attribute in which to put the elevation.
   * @param options      Additional GDAL creation options.
   * @return Output vector layer with contour lines
   */
  def gdalContour(implicit sc: SparkContext,
                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  interval: Double = 10.0,
                  ignoreNodata: String = "false",
                  extra: String = "",
                  create3D: String = "false",
                  nodata: String = "",
                  offset: Double = 0.0,
                  band: Int = 1,
                  fieldName: String = "ELEV",
                  options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputTiffPath = "/mnt/storage/algorithmData/gdalContour_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalContour_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_contour.py --input "$outputTiffPath" --interval $interval --ignore-nodata "$ignoreNodata" --extra "$extra" --create-3d "$create3D" --nodata "$nodata" --offset $offset --band $band --field-name "$fieldName" --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   *
   * Extracts contour polygons from any GDAL-supported elevation raster.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input raster
   * @param interval     Defines the interval between the contour lines in the given units of the elevation raster (minimum value 0)
   * @param ignoreNodata Ignores any nodata values in the dataset.
   * @param extra        Add extra GDAL command line options. Refer to the corresponding GDAL utility documentation.
   * @param create3D     Forces production of 3D vectors instead of 2D. Includes elevation at every vertex.
   * @param nodata       Defines a value that should be inserted for the nodata values in the output raster
   * @param offset       Defines an offset from the base contour elevation for the first contour.
   * @param band         Raster band to create the contours from
   * @param fieldNameMax Provides a name for the attribute in which to put the maximum elevation of contour polygon. If not provided no maximum elevation attribute is attached.
   * @param fieldNameMin Provides a name for the attribute in which to put the minimum elevation of contour polygon. If not provided no minimum elevation attribute is attached.
   * @param options      Additional GDAL creation options.
   * @return Output vector layer with contour polygons
   */
  def gdalContourPolygon(implicit sc: SparkContext,
                         input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                         interval: Double = 10.0,
                         ignoreNodata: String = "false",
                         extra: String = "",
                         create3D: String = "false",
                         nodata: String = "",
                         offset: Double = 0.0,
                         band: Int = 1,
                         fieldNameMax: String = "ELEV_MAX",
                         fieldNameMin: String = "ELEV_MIN",
                         options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputTiffPath = "/mnt/storage/algorithmData/gdalContourPolygon_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalContourPolygon_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_contour_polygon.py --input "$outputTiffPath" --interval $interval --ignore-nodata $ignoreNodata --extra $extra --create-3d $create3D --nodata $nodata --offset $offset --band $band --field-name-max $fieldNameMax --field-name-min $fieldNameMin --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }


  /**
   * Fill raster regions with no data values by interpolation from edges.
   * The values for the no-data regions are calculated by the surrounding pixel values using inverse distance weighting.
   * After the interpolation a smoothing of the results takes place. Input can be any GDAL-supported raster layer.
   * This algorithm is generally suitable for interpolating missing regions of fairly continuously varying rasters
   * (such as elevation models for instance). It is also suitable for filling small holes and cracks in more irregularly varying images (like airphotos).
   * It is generally not so great for interpolating a raster from sparse point data.
   *
   * @param sc         Alias object for SparkContext
   * @param input      Input raster layer
   * @param distance   The number of pixels to search in all directions to find values to interpolate from
   * @param iterations The number of 3x3 filter passes to run (0 or more) to smoothen the results of the interpolation.
   * @param extra      Add extra GDAL command line options
   * @param maskLayer  A raster layer that defines the areas to fill.
   * @param noMask     Activates the user-defined validity mask
   * @param band       The band to operate on. Nodata values must be represented by the value 0.
   * @param options    For adding one or more creation options that control the raster to be created
   * @return Output raster
   */
  def gdalFillNodata(implicit sc: SparkContext,
                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     distance: Double = 10,
                     iterations: Double = 0,
                     extra: String = "",
                     maskLayer: String = "",
                     noMask: String = "False",
                     band: Int = 1,
                     options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalFillNodata_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalFillNodata_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_fillnodata.py --input "$outputTiffPath" --distance $distance --iterations $iterations --extra $extra --mask-layer $maskLayer --no-mask $noMask --band $band --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The Moving Average is a simple data averaging algorithm.
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input point vector layer
   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
   * @param extra     Add extra GDAL command line options
   * @param nodata    No data marker to fill empty points
   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
   * @param zField    Field for the interpolation
   * @param dataType  Defines the data type of the output raster file.
   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
   * @param options   For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalGridAverage(implicit sc: SparkContext,
                      input: RDD[(String, (Geometry, Map[String, Any]))],
                      minPoints: Double = 0.0,
                      extra: String = "",
                      nodata: Double = 0.0,
                      angle: Double = 0.0,
                      zField: String = "",
                      dataType: String = "5",
                      radius2: Double = 0.0,
                      radius1: Double = 0.0,
                      options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridAverage_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridAverage_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_gridaverage.py --input "$outputShpPath" --min-points $minPoints --extra $extra --nodata $nodata --angle $angle --z-field $zField --data-type $dataTypeInput --radius-2 $radius2 --radius-1 $radius1 --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The algorithm id is displayed when you hover over the algorithm in the Processing Toolbox.
   * The parameter dictionary provides the parameter NAMEs and values.
   * See Using processing algorithms from the console for details on how to run processing algorithms from the Python console.
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input point vector layer
   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
   * @param extra     Add extra GDAL command line options
   * @param metric
   * @param nodata    No data marker to fill empty points
   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
   * @param zField    Field for the interpolation
   * @param dataType  Defines the data type of the output raster file.
   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
   * @param options   For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalGridDataMetrics(implicit sc: SparkContext,
                          input: RDD[(String, (Geometry, Map[String, Any]))],
                          minPoints: Double = 0.0,
                          extra: String = "",
                          metric: String = "0",
                          nodata: Double = 0.0,
                          angle: Double = 0.0,
                          zField: String = "",
                          dataType: String = "5",
                          radius2: Double = 0.0,
                          radius1: Double = 0.0,
                          options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridDataMetrics_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridDataMetrics_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)


    val metricInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5"
    ).getOrElse(metric, "0")


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_griddatametrics.py --input "$outputShpPath" --min-points $minPoints --extra $extra --metric $metricInput --nodata $nodata --angle $angle --z-field $zField --data-type $dataTypeInput --radius-2 $radius2 --radius-1 $radius1 --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The Inverse Distance to a Power gridding method is a weighted average interpolator.
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input point vector layer
   * @param extra     Add extra GDAL command line options
   * @param power     Weighting power
   * @param angle     Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
   * @param radius2   The second radius (Y axis if rotation angle is 0) of the search ellipse
   * @param radius1   The first radius (X axis if rotation angle is 0) of the search ellipse
   * @param smoothing Smoothing parameter
   * @param maxPoints Do not search for more points than this number.
   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
   * @param nodata    No data marker to fill empty points
   * @param zField    Field for the interpolation
   * @param dataType  Defines the data type of the output raster file.
   * @param options   For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalGridInverseDistance(implicit sc: SparkContext,
                              input: RDD[(String, (Geometry, Map[String, Any]))],
                              extra: String = "",
                              power: Double = 2.0,
                              angle: Double = 0.0,
                              radius2: Double = 0,
                              radius1: Double = 0,
                              smoothing: Double = 0.0,
                              maxPoints: Double = 0.0,
                              minPoints: Double = 0.0,
                              nodata: Double = 0.0,
                              zField: String = "",
                              dataType: String = "5",
                              options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridInverseDistance_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridInverseDistance_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_gridinversedistance.py --input "$outputShpPath" --extra $extra --power $power --angle $angle --radius-2 $radius2 --radius-1 $radius1 --smoothing $smoothing --max-points $maxPoints --min-points $minPoints --nodata $nodata --z-field $zField --data-type $dataTypeInput --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * Computes the Inverse Distance to a Power gridding combined to the nearest neighbor method.
   * Ideal when a maximum number of data points to use is required.
   *
   * @param sc        Alias object for SparkContext
   * @param input     Input point vector layer
   * @param extra     Add extra GDAL command line options
   * @param power     Weighting power
   * @param radius    The radius of the search circle
   * @param smoothing Smoothing parameter
   * @param maxPoints Do not search for more points than this number.
   * @param minPoints Minimum number of data points to average. If less amount of points found the grid node considered empty and will be filled with NODATA marker.
   * @param nodata    No data marker to fill empty points
   * @param zField    Field for the interpolation
   * @param dataType  Defines the data type of the output raster file.
   * @param options   For adding one or more creation options that control the raster to be created
   * @return
   */
  def gdalGridInverseDistanceNNR(implicit sc: SparkContext,
                                 input: RDD[(String, (Geometry, Map[String, Any]))],
                                 extra: String = "",
                                 power: Double = 2.0,
                                 radius: Double = 1.0,
                                 smoothing: Double = 0.0,
                                 maxPoints: Double = 12,
                                 minPoints: Double = 0,
                                 nodata: Double = 0.0,
                                 zField: String = "",
                                 dataType: String = "5",
                                 options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridInverseDistanceNearestNeighbor_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridInverseDistanceNearestNeighbor_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_gridinversedistancenearestneighbor.py --input "$outputShpPath" --extra $extra --power $power --radius $radius --smoothing $smoothing --max-points $maxPoints --min-points $minPoints --nodata $nodata --z-field $zField --data-type $dataTypeInput --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The Linear method perform linear interpolation by computing a Delaunay triangulation of the point cloud,
   * finding in which triangle of the triangulation the point is,
   * and by doing linear interpolation from its barycentric coordinates within the triangle.
   * If the point is not in any triangle, depending on the radius,
   * the algorithm will use the value of the nearest point or the NODATA value.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input point vector layer
   * @param radius   In case the point to be interpolated does not fit into a triangle of the Delaunay triangulation, use that maximum distance to search a nearest neighbour, or use nodata otherwise. If set to -1, the search distance is infinite. If set to 0, no data value will be used.
   * @param extra    Add extra GDAL command line options
   * @param nodata   No data marker to fill empty points
   * @param zField   Field for the interpolation
   * @param dataType Defines the data type of the output raster file.
   * @param options  For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalGridLinear(implicit sc: SparkContext,
                     input: RDD[(String, (Geometry, Map[String, Any]))],
                     radius: Double = 1.0,
                     extra: String = "",
                     nodata: Double = 0.0,
                     zField: String = "",
                     dataType: String = "5",
                     options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridLinear_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridLinear_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)

    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_gridlinear.py --input "$outputShpPath" --extra $extra --radius $radius --nodata $nodata --z-field $zField --data-type $dataTypeInput --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * The Nearest Neighbor method doesn’t perform any interpolation or smoothing,
   * it just takes the value of nearest point found in grid node search ellipse and returns it as a result.
   * If there are no points found, the specified NODATA value will be returned.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input point vector layer
   * @param extra    Add extra GDAL command line options
   * @param nodata   No data marker to fill empty points
   * @param angle    Angle of ellipse rotation in degrees. Ellipse rotated counter clockwise.
   * @param radius1  The first radius (X axis if rotation angle is 0) of the search ellipse
   * @param radius2  The second radius (Y axis if rotation angle is 0) of the search ellipse
   * @param zField   Field for the interpolation
   * @param dataType Defines the data type of the output raster file.
   * @param options  For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalGridNearestNeighbor(implicit sc: SparkContext,
                              input: RDD[(String, (Geometry, Map[String, Any]))],
                              extra: String = "",
                              nodata: Double = 1,
                              angle: Double = 0.0,
                              radius1: Double = 0.0,
                              radius2: Double = 0.0,
                              zField: String = "",
                              dataType: String = "5",
                              options: String = ""
                             )
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalGridNearestNeighbor_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalGridNearestNeighbor_" + time + "_out.tif"
    saveFeatureRDDToShp(input, outputShpPath)

    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_gridnearestneighbor.py --input "$outputShpPath" --extra $extra --nodata $nodata --angle $angle --radius-1 $radius1 --radius-2 $radius2 --z-field $zField --data-type $dataTypeInput --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * Outputs a raster with a nice shaded relief effect. It’s very useful for visualizing the terrain.
   * You can optionally specify the azimuth and altitude of the light source,
   * a vertical exaggeration factor and a scaling factor to account for differences between vertical and horizontal units.
   *
   * @param sc                Alias object for SparkContext
   * @param input             Input Elevation raster layer
   * @param combined
   * @param computeEdges      Generates edges from the elevation raster
   * @param extra             Add extra GDAL command line options
   * @param band              Band containing the elevation information
   * @param altitude          Defines the altitude of the light, in degrees. 90 if the light comes from above the elevation raster, 0 if it is raking light.
   * @param zevenbergenThorne Activates Zevenbergen&Thorne formula for smooth landscapes
   * @param zFactor           The factor exaggerates the height of the output elevation raster
   * @param multidirectional
   * @param scale             The ratio of vertical units to horizontal units
   * @param azimuth           Defines the azimuth of the light shining on the elevation raster in degrees. If it comes from the top of the raster the value is 0, if it comes from the east it is 90 a.s.o.
   * @param options           For adding one or more creation options that control the raster to be created
   * @return Output raster with interpolated values
   */
  def gdalHillShade(implicit sc: SparkContext,
                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    combined: String = "False",
                    computeEdges: String = "False",
                    extra: String = "",
                    band: Int = 1,
                    altitude: Double = 45.0,
                    zevenbergenThorne: String = "False",
                    zFactor: Double = 1.0,
                    multidirectional: String = "False",
                    scale: Double = 1.0,
                    azimuth: Double = 315.0,
                    options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalHillShade_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalHillShade_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_hillshade.py --input "$outputTiffPath" --combined $combined --compute-edges $computeEdges --extra "$extra" --band $band --altitude $altitude --zevenbergen $zevenbergenThorne --z-factor $zFactor --multidirectional $multidirectional --scale $scale --azimuth $azimuth --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)
  }

  /**
   * Converts nearly black/white borders to black.
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input Elevation raster layer
   * @param white   Search for nearly white (255) pixels instead of nearly black pixels
   * @param extra   Add extra GDAL command line options
   * @param near    Select how far from black, white or custom colors the pixel values can be and still considered near black, white or custom color.
   * @param options For adding one or more creation options that control the raster to be created
   * @return Output raster
   */
  def gdalNearBlack(implicit sc: SparkContext,
                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    white: String = "False",
                    extra: String = "",
                    near: Int = 15,
                    options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalNearBlack_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalNearBlack_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_nearblack.py --input "$outputTiffPath" --white $white --extra "$extra" --near $near --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)
  }

  /**
   * Generates a raster proximity map indicating the distance from the center of each pixel to the center of the nearest pixel identified as a target pixel.
   * Target pixels are those in the source raster for which the raster pixel value is in the set of target pixel values.
   *
   * @param sc          Alias object for SparkContext
   * @param input       Input Elevation raster layer
   * @param extra       Add extra GDAL command line options
   * @param nodata      Specify the nodata value to use for the output raster
   * @param values      A list of target pixel values in the source image to be considered target pixels. If not specified, all non-zero pixels will be considered target pixels.
   * @param band        Band containing the elevation information
   * @param maxDistance The maximum distance to be generated. The nodata value will be used for pixels beyond this distance. If a nodata value is not provided, the output band will be queried for its nodata value. If the output band does not have a nodata value, then the value 65535 will be used. Distance is interpreted according to the value of Distance units.
   * @param replace     Specify a value to be applied to all pixels that are closer than the maximum distance from target pixels (including the target pixels) instead of a distance value.
   * @param units       Indicate whether distances generated should be in pixel or georeferenced coordinates
   * @param dataType    Defines the data type of the output raster file.
   * @param options     For adding one or more creation options that control the vector layer to be created
   * @return Output raster
   */
  def gdalProximity(implicit sc: SparkContext,
                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    extra: String = "",
                    nodata: Double = 0.0,
                    values: String = "",
                    band: Int = 1,
                    maxDistance: Double = 0.0,
                    replace: Double = 0.0,
                    units: String = "1",
                    dataType: String = "5",
                    options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalProximity_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalProximity_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    val unitsInput: String = Map(
      "0" -> "0",
      "1" -> "1"
    ).getOrElse(units, "1")

    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10"
    ).getOrElse(dataType, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_proximity.py --input "$outputTiffPath" --extra "$extra" --nodata $nodata --values "$values" --band $band --max-distance $maxDistance --replace $replace --units $unitsInput --data-type $dataTypeInput --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)
  }

  /**
   * Outputs a single-band raster with values computed from the elevation.
   * Roughness is the degree of irregularity of the surface.
   * It’s calculated by the largest inter-cell difference of a central pixel and its surrounding cell.
   * The determination of the roughness plays a role in the analysis of terrain elevation data,
   * it’s useful for calculations of the river morphology, in climatology and physical geography in general.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input elevation raster layer
   * @param band         The number of the band to use as elevation
   * @param computeEdges Generates edges from the elevation raster
   * @param options      Additional GDAL command line options
   * @return Single-band output roughness raster. The value -9999 is used as nodata value.
   */
  def gdalRoughness(implicit sc: SparkContext,
                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    band: Int = 1,
                    computeEdges: String = "False",
                    options: String = ""):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalRoughness_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalRoughness_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_roughness.py --input "$outputTiffPath" --band $band --compute-edges $computeEdges --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Generates a slope map from any GDAL-supported elevation raster.
   * Slope is the angle of inclination to the horizontal.
   * You have the option of specifying the type of slope value you want: degrees or percent slope.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input Elevation raster layer
   * @param band         Band containing the elevation information
   * @param computeEdges Generates edges from the elevation raster
   * @param asPercent    Express slope as percent instead of degrees
   * @param extra        Additional GDAL command line options
   * @param scale        The ratio of vertical units to horizontal units
   * @param zevenbergen  Activates Zevenbergen&Thorne formula for smooth landscapes
   * @param options      Additional GDAL command line options
   * @return Output raster
   */
  def gdalSlope(implicit sc: SparkContext,
                input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                band: Int = 1,
                computeEdges: String = "False",
                asPercent: String = "False",
                extra: String = "",
                scale: Double = 1.0,
                zevenbergen: String = "False",
                options: String = ""):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalSlope_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalSlope_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_slope.py --input "$outputTiffPath" --band $band --compute-edges $computeEdges --as-percent $asPercent --extra "$extra" --scale $scale --zevenbergen $zevenbergen --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Outputs a single-band raster with values computed from the elevation.
   * TPI stands for Topographic Position Index,
   * which is defined as the difference between a central pixel and the mean of its surrounding cells.
   *
   * @param sc           Alias object for SparkContext
   * @param input        Input elevation raster layer
   * @param band         The number of the band to use for elevation values
   * @param computeEdges Generates edges from the elevation raster
   * @param options      Additional GDAL command line options
   * @return Output raster.
   */
  def gdalTpiTopographicPositionIndex(implicit sc: SparkContext,
                                      input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                      band: Int = 1,
                                      computeEdges: String = "False",
                                      options: String = ""):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalTpiTopographicPositionIndex_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalTpiTopographicPositionIndex_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_tpitopographicpositionindex.py --input "$outputTiffPath" --band $band --compute-edges $computeEdges --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Outputs a single-band raster with values computed from the elevation.
   * TRI stands for Terrain Ruggedness Index, which is defined as the mean difference between a central pixel and its surrounding cells.
   *
   * @param sc           Alias object for SparkContext.
   * @param input        Input elevation raster layer.
   * @param band         The number of the band to use as elevation.
   * @param computeEdges Generates edges from the elevation raster.
   * @param options      For adding one or more creation options that control the raster to be created (colors, block size, file compression...).
   * @return             Output ruggedness raster. The value -9999 is used as nodata value.
   */
  def gdalTriTerrainRuggednessIndex(implicit sc: SparkContext,
                                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                    band: Int = 1,
                                    computeEdges: String = "False",
                                    options: String = ""):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalTriTerrainRuggednessIndex_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalTriTerrainRuggednessIndex_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_triterrainruggednessindex.py --input "$outputTiffPath" --band $band --compute-edges $computeEdges --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

  /**
   * Clips any GDAL-supported raster file to a given extent.
   *
   * @param sc       Alias object for SparkContext
   * @param input    The input raster
   * @param projwin
   * @param extra    Add extra GDAL command line options
   * @param nodata   Defines a value that should be inserted for the nodata values in the output raster
   * @param dataType Defines the format of the output raster file.
   * @param options  For adding one or more creation options that control the raster to be created
   * @return Output raster layer clipped by the given extent
   */
  def gdalClipRasterByExtent(implicit sc: SparkContext,
                             input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                             projwin: String = "",
                             extra: String = "",
                             nodata: Double = 0.0,
                             dataType: String = "0",
                             options: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalClipRasterByExtent_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalClipRasterByExtent_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10",
      "11" -> "11"
    ).getOrElse(dataType, "0")


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_cliprasterbyextent.py --input "$outputTiffPath" --projwin "$projwin" --extra "$extra" --nodata $nodata --data-type "$dataTypeInput" --options "$options" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * Clips any GDAL-supported raster by a vector mask layer.
   *
   * @param sc             Alias object for SparkContext
   * @param input          The input raster
   * @param cropToCutLine  Applies the vector layer extent to the output raster if checked.
   * @param targetExtent   Extent of the output file to be created
   * @param setResolution  Shall the output resolution (cell size) be specified
   * @param extra          Add extra GDAL command line options
   * @param targetCrs      Set the coordinate reference to use for the mask layer
   * @param xResolution    The width of the cells in the output raster
   * @param keepResolution The resolution of the output raster will not be changed
   * @param alphaBand      Creates an alpha band for the result. The alpha band then includes the transparency values of the pixels.
   * @param options        For adding one or more creation options that control the raster to be created
   * @param mask           Vector mask for clipping the raster
   * @param multithreading Two threads will be used to process chunks of image and perform input/output operation simultaneously. Note that computation is not multithreaded itself.
   * @param nodata         Defines a value that should be inserted for the nodata values in the output raster
   * @param yResolution    The height of the cells in the output raster
   * @param dataType       Defines the format of the output raster file.
   * @param sourceCrs      Set the coordinate reference to use for the input raster
   * @return Output raster layer clipped by the vector layer
   */
  def gdalClipRasterByMaskLayer(implicit sc: SparkContext,
                                input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                cropToCutLine: String = "True",
                                targetExtent: String = "",
                                setResolution: String = "False",
                                extra: String = "",
                                targetCrs: String = "",
                                xResolution: Double ,
                                keepResolution: String = "False",
                                alphaBand: String = "False",
                                options: String = "",
                                mask: String = "",
                                multithreading: String = "False",
                                nodata: Double,
                                yResolution: Double,
                                dataType: String = "0",
                                sourceCrs: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalClipRasterByMaskLayer_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalClipRasterByMaskLayer_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10",
      "11" -> "11"
    ).getOrElse(dataType, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_cliprasterbymasklayer.py --input "$outputTiffPath" --crop-to-cutline $cropToCutLine --target-extent "$targetExtent" --set-resolution $setResolution --extra "$extra" --target-crs "$targetCrs" --x-resolution $xResolution --keep-resolution $keepResolution --alpha-band $alphaBand --options "$options" --mask "$mask" --multithreading $multithreading --nodata $nodata --y-resolution $yResolution --data-type "$dataTypeInput" --source-crs "$sourceCrs" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)
  }

  def gdalPolygonize(implicit sc: SparkContext,
                     input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     extra: String = "",
                     field: String = "DN",
                     band: Int = 1,
                     eightConnectedness: String = "False")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputTiffPath = "/mnt/storage/algorithmData/gdalPolygonize_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalPolygonize_" + time + "_out.shp"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_polygonize.py --input "$outputTiffPath" --extra "$extra" --field $field --band $band --eight-connectedness $eightConnectedness --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Overwrites a raster layer with values from a vector layer. New values are assigned based on the attribute value of the overlapping vector feature.
   *
   * @param sc          Alias object for SparkContext
   * @param input       Input vector layer
   * @param inputRaster Input raster layer
   * @param extra       Add extra GDAL command line options
   * @param field       Defines the attribute field to use to set the pixels values
   * @param add         If False, pixels are assigned the selected field’s value. If True, the selected field’s value is added to the value of the input raster layer.
   * @return The overwritten input raster layer
   */
  def gdalRasterizeOver(implicit sc: SparkContext,
                        input: RDD[(String, (Geometry, Map[String, Any]))],
                        inputRaster: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                        extra: String = "",
                        field: String = "",
                        add: String = "False")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalRasterizeOver_" + time + ".shp"
    val outputTiffPath = "/mnt/storage/algorithmData/gdalRasterizeOver_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalRasterizeOver_" + time + "_out.tif"

    saveFeatureRDDToShp(input, outputShpPath)
    saveRasterRDDToTif(inputRaster, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_rasterize_over.py --input "$outputShpPath" --inputraster "$outputTiffPath" --extra "$extra" --field "$field" --add $add --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, inputRaster, writePath)
  }

  /**
   * Overwrites parts of a raster layer with a fixed value. The pixels to overwrite are chosen based on the supplied (overlapping) vector layer.
   *
   * @param sc          Alias object for SparkContext
   * @param input       Input vector layer
   * @param inputRaster Input raster layer
   * @param burn        The value to burn
   * @param extra       Add extra GDAL command line options
   * @param add         If False, pixels are assigned the selected field’s value. If True, the selected field’s value is added to the value of the input raster layer.
   * @return
   */
  def gdalRasterizeOverFixedValue(implicit sc: SparkContext,
                                  input: RDD[(String, (Geometry, Map[String, Any]))],
                                  inputRaster: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                  burn: Double = 0.0,
                                  extra: String = "",
                                  add: String = "False")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalRasterizeOverFixedValue_" + time + ".shp"
    val outputTiffPath = "/mnt/storage/algorithmData/gdalRasterizeOverFixedValue_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalRasterizeOverFixedValue_" + time + "_out.tif"

    saveFeatureRDDToShp(input, outputShpPath)
    saveRasterRDDToTif(inputRaster, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_rasterize_over_fixed_value.py --input "$outputShpPath" --inputraster "$outputTiffPath" --extra "$extra" --add $add --burn $burn --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, inputRaster, writePath)
  }

  /**
   * Converts a 24 bit RGB image into a 8 bit paletted.
   * Computes an optimal pseudo-color table for the given RGB-image using a median cut algorithm on a downsampled RGB histogram.
   * Then it converts the image into a pseudo-colored image using the color table.
   * This conversion utilizes Floyd-Steinberg dithering (error diffusion) to maximize output image visual quality.
   *
   * @param sc      Alias object for SparkContext
   * @param input   Input (RGB) raster layer
   * @param ncolors The number of colors the resulting image will contain. A value from 2-256 is possible.
   * @return Output raster layer.
   */
  def gdalRgbToPct(implicit sc: SparkContext,
                   input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                   ncolors: Double = 2):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalRgbToPct_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalRgbToPct_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_rgbtopct.py --input "$outputTiffPath" --ncolors $ncolors --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Converts raster data between different formats.
   *
   * @param sc              Alias object for SparkContext
   * @param input           Input raster layer
   * @param extra           Additional GDAL command line options
   * @param targetCrs       Specify a projection for the output file
   * @param nodata          Defines the value to use for nodata in the output raster
   * @param dataType        Defines the data type of the output raster file.
   * @param copySubdatasets Create individual files for subdatasets
   * @param options         For adding one or more creation options that control the raster to be created
   * @return Output (translated) raster layer.
   */
  def gdalTranslate(implicit sc: SparkContext,
                    input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    extra: String = "",
                    targetCrs: String = "",
                    nodata: Double = 0,
                    dataType: String = "0",
                    copySubdatasets: String = "False",
                    options: String = ""):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalTranslate_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalTranslate_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)


    val dataTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3",
      "4" -> "4",
      "5" -> "5",
      "6" -> "6",
      "7" -> "7",
      "8" -> "8",
      "9" -> "9",
      "10" -> "10",
      "11" -> "11"
    ).getOrElse(dataType, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_translate.py --input "$outputTiffPath" --extra "$extra" --targetCrs "$targetCrs" --nodata $nodata --dataType $dataTypeInput --copySubdatasets $copySubdatasets --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeRasterRDDFromTif(sc, input, writePath)

  }

  /**
   * Reprojects a raster layer into another Coordinate Reference System (CRS). The output file resolution and the resampling method can be chosen.
   *
   * @param sc               Alias object for SparkContext
   * @param input            Input raster layer to reproject
   * @param sourceCrs        Defines the CRS of the input raster layer
   * @param targetCrs        The CRS of the output layer
   * @param resampling       Pixel value resampling method to use. Options:0 — Nearest neighbour 1 — Bilinear 2 — Cubic 3 — Cubic spline 4 — Lanczos windowed sinc 5 — Average 6 — Mode 7 — Maximum 8 — Minimum 9 — Median 10 — First quartile 11 — Third quartile
   * @param noData           Sets nodata value for output bands. If not provided, then nodata values will be copied from the source dataset.
   * @param targetResolution Defines the output file resolution of reprojection result
   * @param options          For adding one or more creation options that control the raster to be created
   * @param dataType         Defines the format of the output raster file.
   * @param targetExtent     Sets the georeferenced extent of the output file to be created
   * @param targetExtentCrs  Specifies the CRS in which to interpret the coordinates given for the extent of the output file. This must not be confused with the target CRS of the output dataset. It is instead a convenience e.g. when knowing the output coordinates in a geodetic long/lat CRS, but wanting a result in a projected coordinate system.
   * @param multiThreading   Two threads will be used to process chunks of the image and perform input/output operations simultaneously. Note that the computation itself is not multithreaded.
   * @param extra            Add extra GDAL command line options.
   * @return                 Reprojected output raster layer
   */
  def gdalWarp(implicit sc: SparkContext,
               input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               sourceCrs:String,
               targetCrs:String="EPSG:4326",
               resampling:String="0",
               noData:Double,
               targetResolution:Double,
               options:String="",
               dataType:String="0",
               targetExtent:String = "",
               targetExtentCrs:String = "",
               multiThreading:String="False",
               extra:String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalWarp_" + time + ".tif"
    val writePath = "/mnt/storage/algorithmData/gdalWarp_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_warpreproject.py --input "$outputTiffPath" --source-crs $sourceCrs --target-crs $targetCrs --resampling $resampling --nodata $noData --target-resolution $targetResolution --options $options --data-type $dataType --target-extent $targetExtent --target-extent-crs $targetExtentCrs --multithreading $multiThreading --extra $extra --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writePath)
  }

  /**
   * Dissolve (combine) geometries that have the same value for a given attribute / field. The output geometries are multipart.
   *
   * @param sc                  Alias object for SparkContext
   * @param input               The input layer to dissolve
   * @param explodeCollections  Produce one feature for each geometry in any kind of geometry collection in the source file
   * @param field               The field of the input layer to use for dissolving
   * @param computeArea         Compute the area and perimeter of dissolved features and include them in the output layer
   * @param keepAttributes      Keep all attributes from the input layer
   * @param computeStatistics   Calculate statistics (min, max, sum and mean) for the numeric attribute specified and include them in the output layer
   * @param countFeatures       Count the dissolved features and include it in the output layer.
   * @param statisticsAttribute The numeric attribute to calculate statistics on
   * @param options             Additional GDAL creation options.
   * @param geometry            The name of the input layer geometry column to use for dissolving.
   * @return The output multipart geometry layer (with dissolved geometries)
   *
   */
  def gdalDissolve(implicit sc: SparkContext,
                   input: RDD[(String, (Geometry, Map[String, Any]))],
                   explodeCollections: String = "false",
                   field: String = "",
                   computeArea: String = "false",
                   keepAttributes: String = "false",
                   computeStatistics: String = "false",
                   countFeatures: String = "false",
                   statisticsAttribute: String = "",
                   options: String = "",
                   geometry: String = "geometry")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputShpPath = "/mnt/storage/algorithmData/gdalDissolve_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalDissolve_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_dissolve.py --input "$outputShpPath" --explode-collections $explodeCollections --field $field --compute-area $computeArea --keep-attributes $keepAttributes --compute-statistics $computeStatistics --count-features $countFeatures --statistics-attribute $statisticsAttribute --options $options --geometry $geometry --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Clips any OGR-supported vector file to a given extent.
   *
   * @param sc      Alias object for SparkContext
   * @param input   The input vector file
   * @param extent  Defines the bounding box that should be used for the output vector file. It has to be defined in target CRS coordinates.
   * @param options For adding one or more creation options that control the raster to be created
   * @return The output (clipped) layer. The default format is “ESRI Shapefile”.
   */
  def gdalClipVectorByExtent(implicit sc: SparkContext,
                             input: RDD[(String, (Geometry, Map[String, Any]))],
                             extent: String = "",
                             options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()

    val outputShpPath = "/mnt/storage/algorithmData/gdalClipVectorByExtent_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalClipVectorByExtent_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_clipvectorbyextent.py --input "$outputShpPath" --extent $extent --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Clips any OGR-supported vector layer by a mask polygon layer.
   *
   * @param sc      Alias object for SparkContext
   * @param input   The input vector file
   * @param mask    Layer to be used as clipping extent for the input vector layer.
   * @param options Additional GDAL creation options.
   * @return The output (masked) layer. The default format is “ESRI Shapefile”.
   */
  def gdalClipVectorByPolygon(implicit sc: SparkContext,
                              input: RDD[(String, (Geometry, Map[String, Any]))],
                              mask: String = "",
                              options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputShpPath = "/mnt/storage/algorithmData/gdalClipVectorByPolygon_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalClipVectorByPolygon_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_clipvectorbypolygon.py --input "$outputShpPath" --mask $mask --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Offsets lines by a specified distance. Positive distances will offset lines to the left, and negative distances will offset them to the right.
   *
   * @param sc       Alias object for SparkContext
   * @param input    Input vector layer
   * @param distance The offset distance
   * @param geometry The name of the input layer geometry column to use
   * @param options  For adding one or more creation options that control the vector layer to be created
   * @return The output offset curve layer
   */
  def gdalOffsetCurve(implicit sc: SparkContext,
                      input: RDD[(String, (Geometry, Map[String, Any]))],
                      distance: Double = 10.0,
                      geometry: String = "geometry",
                      options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/gdalOffsetCurve_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalOffsetCurve_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_offsetcurve.py --input "$outputShpPath" --distance $distance --geometry $geometry --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Generates a point on each line of a line vector layer at a distance from start. The distance is provided as a fraction of the line length.
   *
   * @param sc       Alias object for SparkContext
   * @param input    The input line layer
   * @param distance The distance from the start of the line
   * @param geometry The name of the input layer geometry column to use
   * @param options  For adding one or more creation options that control the vector layer to be created
   * @return
   */
  def gdalPointsAlongLines(implicit sc: SparkContext,
                           input: RDD[(String, (Geometry, Map[String, Any]))],
                           distance: Double = 0.5,
                           geometry: String = "geometry",
                           options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/gdalPointsAlongLines_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalPointsAlongLines_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_pointsalonglines.py --input "$outputShpPath" --distance $distance --geometry $geometry --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Create buffers around the features of a vector layer.
   *
   * @param sc       Alias object for SparkContext
   * @param input    The input vector layer
   * @param distance Minimum: 0.0
   * @param explodeCollections
   * @param field    Field to use for dissolving
   * @param dissolve If set, the result is dissolved. If no field is set for dissolving, all the buffers are dissolved into one feature.
   * @param geometry The name of the input layer geometry column to use
   * @param options  Additional GDAL creation options.
   * @return The output buffer layer
   */
  def gdalBufferVectors(implicit sc: SparkContext,
                        input: RDD[(String, (Geometry, Map[String, Any]))],
                        distance: Double = 10.0,
                        explodeCollections: String = "False",
                        field: String = "",
                        dissolve: String = "False",
                        geometry: String = "geometry",
                        options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {

    val time = System.currentTimeMillis()


    val outputShpPath = "/mnt/storage/algorithmData/gdalBufferVectors_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalBufferVectors_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_buffervectors.py --input "$outputShpPath" --distance $distance --explode-collections $explodeCollections --field $field --dissolve $dissolve --geometry $geometry --options $options --output "$writePath"""".stripMargin


      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Creates a buffer on one side (right or left) of the lines in a line vector layer.
   *
   * @param sc         Alias object for SparkContext
   * @param input      The input line layer
   * @param distance   The buffer distance
   * @param explodeCollections
   * @param field      Field to use for dissolving
   * @param bufferSide 0: Right, 1: Left
   * @param dissolve   If set, the result is dissolved. If no field is set for dissolving, all the buffers are dissolved into one feature.
   * @param geometry   The name of the input layer geometry column to use
   * @param options    For adding one or more creation options that control the vector layer to be created
   * @return
   */
  def gdalOneSideBuffer(implicit sc: SparkContext,
                        input: RDD[(String, (Geometry, Map[String, Any]))],
                        distance: Double = 10.0,
                        explodeCollections: String = "False",
                        field: String = "",
                        bufferSide: String = "0",
                        dissolve: String = "False",
                        geometry: String = "geometry",
                        options: String = "")
  : RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputShpPath = "/mnt/storage/algorithmData/gdalOneSideBuffer_" + time + ".shp"
    val writePath = "/mnt/storage/algorithmData/gdalOneSideBuffer_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputShpPath)


    val bufferSideInput: String = Map(
      "0" -> "0",
      "1" -> "1"
    ).getOrElse(bufferSide, "0")

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;d /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_onesidebuffer.py --input "$outputShpPath" --distance $distance --explodecollections $explodeCollections --field $field --bufferSide $bufferSideInput --dissolve $dissolve --geometry $geometry --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }

  /**
   * Applies a coordinate system to a raster dataset.
   *
   * @param sc    Alias object for SparkContext
   * @param input Input raster layer
   * @param crs   The projection (CRS) of the output layer
   * @return The output raster layer (with the new projection information)
   */
  def gdalAssignProjection(implicit sc: SparkContext,
                           input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                           crs: String = "")
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = "/mnt/storage/algorithmData/gdalAssignProjection_" + time + ".tif"
    saveRasterRDDToTif(input, outputTiffPath)


    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st =
        raw"""conda activate qgis;cd /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_assignprojection.py --input "$outputTiffPath" --crs "$crs"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, outputTiffPath)
  }

}











