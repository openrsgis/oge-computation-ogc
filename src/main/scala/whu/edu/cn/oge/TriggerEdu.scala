package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.oge.Coverage.reproject
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, makeChangedRasterRDDFromTifNew}

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import whu.edu.cn.algorithms.MLlib._

import scala.util.Random
import org.jpmml.sparkml.PipelineModelUtil

import java.io.File
import sys.process._
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.debug.FeatureDebug.makeFeatureRDDFromShp
import whu.edu.cn.oge.CoverageCollection.mosaic

import scala.collection.mutable
import whu.edu.cn.oge.QGIS.{gdalClipRasterByExtent, gdalClipRasterByMaskLayer}
import whu.edu.cn.util.CoverageUtil.focalMethods

import scala.util.parsing.json._

object TriggerEdu {
  def focalMedian(implicit sc: SparkContext,
                  inputPath: String,
                  kernelType: String = "square",
                  radius: Int = 5,
                  outputPath: String) = {
    val coverage = makeChangedRasterRDDFromTif(sc, inputPath)
    val resRDD = focalMethods(coverage, kernelType, focal.Median.apply, radius)
    makeTIFF(resRDD, outputPath)
  }

  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), outputPath: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    GeoTiff(stitchedTile, coverage._2.crs).write(outputPath)

  }

  def reprojectEdu(implicit sc: SparkContext, inputPath: String, outputPath: String, Crs: String, scale: Double) = {
    val epsg: Int = Crs.split(":")(1).toInt
    val crs: CRS = CRS.fromEpsgCode(epsg)
    val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val coverage2 = reproject(coverage1, crs, scale)
    makeTIFF(coverage2, outputPath)
    println("SUCCESS")
  }

  def mosaicEdu(implicit sc: SparkContext, inputListPath: String, outputPath: String): Unit ={
//    val inputList: List[String] = inputListPath.substring(1, inputListPath.length - 1)
//      .split(",").filter(_.nonEmpty).map(t=>{
//      val noMargin = t.replace(" ", "")
//      noMargin.substring(1,noMargin.length-1)
//    }).toList
    println(inputListPath)
    val inputList: List[String] = JSON.parseFull(inputListPath) match {
      case Some(list: List[String]) => list
      case _ => throw new IllegalArgumentException("Invalid inputListPath format")
    }
    val coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] =
      inputList.map(input=>{
        (input, makeChangedRasterRDDFromTif(sc, input))
      }).toMap
    val newCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = mosaic(coverageCollection)
    makeTIFF(newCoverage, outputPath)
  }
  def clipRasterByMaskLayerEdu(implicit sc: SparkContext,
                               inputPath: String,
                               mask: String,
                               outputPath: String,
                               cropToCutLine: String = "True",
                               targetExtent: String = "",
                               setResolution: String = "False",
                               extra: String = "",
                               targetCrs: String = "",
                               keepResolution: String = "False",
                               alphaBand: String = "False",
                               options: String = "",
                               multithreading: String = "False",
                               dataType: String = "0",
                               sourceCrs: String = ""): Unit ={
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val feature: RDD[(String, (Geometry, mutable.Map[String, Any]))] = makeFeatureRDDFromShp(sc, mask)
    val newCoverage = gdalClipRasterByMaskLayer(sc, coverage, feature, cropToCutLine, targetExtent, setResolution, extra, targetCrs, keepResolution, alphaBand, options, multithreading, dataType, sourceCrs)
    makeTIFF(newCoverage, outputPath)
  }
  def clipRasterByExtentEdu(implicit sc: SparkContext,
                            inputPath: String,
                            outputPath: String,
                            projwin: String = "",
                            extra: String = "",
                            nodata: Double = 0.0,
                            dataType: String = "0",
                            options: String = "") = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val newCoverage = gdalClipRasterByExtent(sc, coverage, projwin, extra, nodata, dataType, options)
    makeTIFF(newCoverage, outputPath)
  }
  def randomForestTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, labelCol: Int = 0, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0) = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.randomForest(checkpointInterval, featureSubsetStrategy, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate)
      .train(spark, featursCoverage, labelCoverage, labelCol)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete() //TODO 检查是否删成功
    println("SUCCESS")
  }

  def classify(implicit sc: SparkContext, featuresPath: String, modelPath: String, classifiedOutputPath: String): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    //若用户未添加后缀，为其添加
    val modelPathWithZip = if (modelPath.endsWith(".zip")) modelPath else modelPath + ".zip"
    PipelineModelUtil.uncompress(new File(modelPathWithZip), new File(modelPathWithZip.stripSuffix(".zip")))
    //    val model: PipelineModel = PipelineModelUtil.load(spark, new File(modelPathWithZip.stripSuffix(".zip")))
    val model: PipelineModel = PipelineModel.load("file://" + modelPathWithZip.stripSuffix(".zip"))
    val predictedCoverage = Classifier.classify(spark, featursCoverage, model)("prediction")
    new File(modelPathWithZip.stripSuffix(".zip")).delete()
    makeTIFF(predictedCoverage, classifiedOutputPath)
    println("SUCCESS")
  }

  def getZoom(implicit sc: SparkContext, inputPath: String): JSONObject = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val coordinateCrs: CRS = CRS.fromEpsgCode(4326)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val layoutScheme1: ZoomedLayoutScheme = ZoomedLayoutScheme(coordinateCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverage._2.bounds.get.minKey.spatialKey, coverage._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage._2.cellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

    val (zoom, _): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
      coverageTMS.reproject(tmsCrs, layoutScheme)
    }
    val (_, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
      coverageTMS.reproject(coordinateCrs, layoutScheme1)
    }

    val extentStr = reprojected.metadata.extent.toString()
    val pattern = """\(([^)]+)\)""".r
    // 匹配并提取子字符串
    val extracted = pattern.findFirstMatchIn(extentStr).map(_.group(1)).getOrElse("No match found")

    val jsonObject: JSONObject = new JSONObject
    jsonObject.put("zoom", zoom)
    jsonObject.put("extent", extracted)
    jsonObject
  }

  def addStyles1Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageOneBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    val band1: Array[Double] = coverageOneBand._1.collect().flatMap(t => t._2.band(0).toArrayDouble()).filter(!_.isNaN)
    val sortedBand1 = band1.sorted
    val b1 = 0.02 * (sortedBand1.length - 1)
    val t1 = 0.98 * (sortedBand1.length - 1)
    val f1 = b1.floor.toInt
    val c1 = t1.ceil.toInt
    val min1 = sortedBand1(f1)
    val max1 = sortedBand1(c1)

      //没有调色盘的情况,保证值不为0
    val interval: Double = (max1 - min1)
    coverageOneBand = (coverageOneBand._1.map(t => {
      val bandR: Tile = t._2.bands(0).mapDouble(d => {
        if (d.isNaN)
          d
        else {
          val value = 1 + 254.0 * (d - min1) / interval
          if (value < 0) 0
          else if (value > 255) 255
          else value
        }
      })
      val bandG: Tile = t._2.bands(0).mapDouble(d => {
        if (d.isNaN)
          d
        else {
          val value = 1 + 254.0 * (d - min1) / interval
          if (value < 0) 0
          else if (value > 255) 255
          else value
        }
      })
      val bandB: Tile = t._2.bands(0).mapDouble(d => {
        if (d.isNaN)
          d
        else {
          val value = 1 + 254.0 * (d - min1) / interval
          if (value < 0) 0
          else if (value > 255) 255
          else value
        }
      })
      (t._1, MultibandTile(bandR, bandG, bandB))
    }), coverageOneBand._2)
    coverageOneBand

  }

  def addStyles2Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageTwoBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    val band1: Array[Double] = coverageTwoBand._1.collect().flatMap(t => t._2.band(0).toArrayDouble()).filter(!_.isNaN)
    val band2: Array[Double] = coverageTwoBand._1.collect().flatMap(t => t._2.band(1).toArrayDouble()).filter(!_.isNaN)
    val sortedBand1 = band1.sorted
    val b1 = 0.02 * (sortedBand1.length - 1)
    val t1 = 0.98 * (sortedBand1.length - 1)
    val sortedBand2 = band2.sorted
    val b2 = 0.02 * (sortedBand2.length - 1)
    val t2 = 0.98 * (sortedBand1.length - 1)
    val f1 = b1.floor.toInt
    val f2 = b2.floor.toInt
    val c1 = t1.ceil.toInt
    val c2 = t2.ceil.toInt
    val min1 = sortedBand1(f1)
    val min2 = sortedBand2(f2)
    val max1 = sortedBand1(c1)
    val max2 = sortedBand2(c2)

    // 拉伸图像
    val interval1: Double = (max1 - min1)
    val interval2: Double = (max2 - min2)

    coverageTwoBand = (coverageTwoBand._1.map(
      t => {
        val bandR: Tile = t._2.bands(0).mapDouble(d => {
          if (d.isNaN)
            d
          else {
            val value = 1 + 254.0 * (d - min1) / interval1
            if (value < 0) 0
            else if (value > 255) 255
            else value
          }
        })
        val bandG: Tile = t._2.bands(1).mapDouble(d => {
          if (d.isNaN)
            d
          else {
            val value = 1 + 254.0 * (d - min2) / interval2
            if (value < 0) 0
            else if (value > 255) 255
            else value
          }
        })
        val bandB: Tile = t._2.bands(0).mapDouble(d => {
          if (d.isNaN)
            d
          else
            63
        })
        (t._1, MultibandTile(bandR, bandG, bandB))
      }),coverageTwoBand._2)
    coverageTwoBand
  }

  def addStyles3Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageThreeBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    val band1: Array[Double] = coverageThreeBand._1.collect().flatMap(t => t._2.band(0).toArrayDouble()).filter(!_.isNaN)
    val band2: Array[Double] = coverageThreeBand._1.collect().flatMap(t => t._2.band(1).toArrayDouble()).filter(!_.isNaN)
    val band3: Array[Double] = coverageThreeBand._1.collect().flatMap(t => t._2.band(2).toArrayDouble()).filter(!_.isNaN)
    val sortedBand1 = band1.sorted
    val b1 = 0.02 * (sortedBand1.length - 1)
    val t1 = 0.98 * (sortedBand1.length - 1)
    val sortedBand2 = band2.sorted
    val b2 = 0.02 * (sortedBand2.length - 1)
    val t2 = 0.98 * (sortedBand1.length - 1)
    val sortedBand3 = band3.sorted
    val b3 = 0.02 * (sortedBand3.length - 1)
    val t3 = 0.98 * (sortedBand1.length - 1)
    val f1 = b1.floor.toInt
    val f2 = b2.floor.toInt
    val f3 = b3.floor.toInt
    val c1 = t1.ceil.toInt
    val c2 = t2.ceil.toInt
    val c3 = t3.ceil.toInt
    val min1 = sortedBand1(f1)
    val min2 = sortedBand2(f2)
    val min3 = sortedBand3(f3)
    val max1 = sortedBand1(c1)
    val max2 = sortedBand2(c2)
    val max3 = sortedBand3(c3)
    println(min1)
    println(min2)
    println(min3)
    println(max1)
    println(max2)
    println(max3)

    // 拉伸图像
    val interval1: Double = (max1 - min1)
    val interval2: Double = (max2 - min2)
    val interval3: Double = (max3 - min3)
    coverageThreeBand = (coverageThreeBand._1.map(
      t => {
        val bandR: Tile = t._2.bands(0).mapDouble(d => {
          if (d.isNaN)
            d
          else {
            val value = 1 + 254.0 * (d - min1) / interval1
            if (value < 0) 0
            else if (value > 255) 255
            else value
          }
        })
        val bandG: Tile = t._2.bands(1).mapDouble(d => {
          if (d.isNaN)
            d
          else {
            val value = 1 + 254.0 * (d - min2) / interval2
            if (value < 0) 0
            else if (value > 255) 255
            else value
          }
        })
        val bandB: Tile = t._2.bands(2).mapDouble(d => {
          if (d.isNaN)
            d
          else {
            val value = 1 + 254.0 * (d - min3) / interval3
            if (value < 0) 0
            else if (value > 255) 255
            else value
          }
        })
        (t._1, MultibandTile(bandR, bandG, bandB))
      }), coverageThreeBand._2)

    coverageThreeBand
  }

  def visualizeOnTheFlyEdu(implicit sc: SparkContext, inputPath: String, outputPath: String, level: Int, jobId: String, coverageReadFromUploadFile: Boolean, bands: String = null, min: String = null, max: String = null, gain: String = null, bias: String = null, gamma: String = null, palette: String = null, opacity: String = null, format: String = null): Unit = {
    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = Coverage.toDouble(makeChangedRasterRDDFromTifNew(sc, inputPath))
    val reprojectedWithoutNodata = coverage.map(rdd => (rdd._1, rdd._2.mapBands((_, tile) => tile.mapDouble(value => if (value.equals(0.0)) Double.NaN else value))))
    val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      if (coverage._1.first()._2.bandCount >= 3) {
        val coverageWith3band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0), rdd._2.band(1), rdd._2.band(2)))), coverage._2)
        addStyles3Band(coverageWith3band)
      }
      else if (coverage._1.first()._2.bandCount == 2) {
        val coverageWith2band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0), rdd._2.band(1)))), coverage._2)
        addStyles2Band(coverageWith2band)
      }
      else if (coverage._1.first()._2.bandCount == 1) {
        val coverageWith1band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0)))), coverage._2)
        addStyles1Band(coverageWith1band)
      }
      else {
        throw new Exception("bandCounts can not be 0")
      }
    }

    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      coverageTMS.reproject(tmsCrs, layoutScheme)

//    val time = System.currentTimeMillis()
//    val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(reprojected.metadata.bounds.get.minKey.col, reprojected.metadata.bounds.get.minKey.row, time), SpaceTimeKey(reprojected.metadata.bounds.get.maxKey.col, reprojected.metadata.bounds.get.maxKey.row, time))
//    val bands = coverage._1.collect().head._1.measurementName
//    val result: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (reprojected.map{case (spatialKey, tile) =>
//      val spaceTimeKey = new SpaceTimeKey(spatialKey.col, spatialKey.row, time)
//      val spaceTimeBandKey = SpaceTimeBandKey(spaceTimeKey, bands)
//      (spaceTimeBandKey, tile)
//    }, TileLayerMetadata(reprojected.metadata.cellType, LayoutDefinition(reprojected.metadata.extent, reprojected.metadata.tileLayout), reprojected.metadata.extent, reprojected.metadata.crs, bounds))
//    result._1.collect().head._2.band(0).foreach(t => println(t))
//    makeTIFF(result, "/D:/Intermediate_results/ISO.tif")

    if (level > zoom) {
      throw new Exception("level can not > " + zoom)
    }
    else {
      // 待修改dagId
      val on_the_fly_path: String = outputPath + jobId
      val file = new File(on_the_fly_path)
      if (file.exists() && file.isDirectory) {
        println("Delete existed on_the_fly_path")
        val command = s"rm -rf $on_the_fly_path"
        println(command)
        //调用系统命令
        command.!!
      }

      // Create the attributes store that will tell us information about our catalog.
      val attributeStore1: FileAttributeStore = FileAttributeStore(outputPath)
      // Create the writer that we will use to store the tiles in the local catalog.
      val writer: FileLayerWriter = FileLayerWriter(attributeStore1)

      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
        if (level - z <= 5 && level - z >= 0) {
          val layerId: LayerId = LayerId(jobId, z)
          println(layerId)
          // If the layer exists already, delete it out before writing
          if (attributeStore1.layerExists(layerId)) {
            //        new FileLayerManager(attributeStore).delete(layerId)
            try {
              writer.overwrite(layerId, rdd)
            } catch {
              case e: Exception =>
                e.printStackTrace()
            }
          }
          else {
            try {
              writer.write(layerId, rdd, ZCurveKeyIndexMethod)
            } catch {
              case e: Exception =>
                println(e)
                println("Continue writing Layers!")
            }
          }
        }
      }
    }
  }

  def visualizeOnTheFlyEduForClassification(implicit sc: SparkContext, inputPath: String, outputPath: String, level: Int, jobId: String, coverageReadFromUploadFile: Boolean, bands: String = null, min: String = null, max: String = null, gain: String = null, bias: String = null, gamma: String = null, palette: String = null, opacity: String = null, format: String = null): Unit = {
    val coverage = Coverage.toDouble(makeChangedRasterRDDFromTifNew(sc, inputPath))
    val hadoopPath = "file://" + inputPath
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(new Path(hadoopPath))(sc)
    // 1. 指定划分的单张瓦片的大小
    val localLayoutScheme = FloatingLayoutScheme(256)
    // 2. 根据布局方案计算并收集影像的元数据信息，包括布局、空间范围、坐标系等
    val (_: Int, metadataOriginal: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    val extentOriginal = metadataOriginal.extent
    val visParam: VisualizationParam = new VisualizationParam()
    val reprojectedWithoutNodata = (coverage._1.map { rdd =>
      val extent: Extent = coverage._2.mapTransform(rdd._1.spaceTimeKey.col, rdd._1.spaceTimeKey.row)
      (rdd._1, rdd._2.mapBands((_, tile) => tile.mapDouble{ (col, row, value) =>
        val cellSize = math.abs(coverage._2.layout.cellSize.width)
        val coordinate = (extent.xmin + cellSize * col, extent.ymax - cellSize * row)
        if (extentOriginal.contains(coordinate._1, coordinate._2)) value else Double.NaN
      }))}, coverage._2)

    val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      if (coverage._1.first()._2.bandCount >= 3) {
        val coverageWith3band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0), rdd._2.band(1), rdd._2.band(2)))), coverage._2)
        addStyles3Band(coverageWith3band)
      }
      else if (coverage._1.first()._2.bandCount == 2) {
        val coverageWith2band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0), rdd._2.band(1)))), coverage._2)
        addStyles2Band(coverageWith2band)
      }
      else if (coverage._1.first()._2.bandCount == 1) {
        val coverageWith1band = (reprojectedWithoutNodata.map(rdd => (rdd._1, MultibandTile(rdd._2.band(0)))), coverage._2)
        addStyles1Band(coverageWith1band)
      }
      else {
        throw new Exception("bandCounts can not be 0")
      }
    }

    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    val coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      coverageTMS.reproject(tmsCrs, layoutScheme)

//    val time = System.currentTimeMillis()
//    val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(reprojected.metadata.bounds.get.minKey.col, reprojected.metadata.bounds.get.minKey.row, time), SpaceTimeKey(reprojected.metadata.bounds.get.maxKey.col, reprojected.metadata.bounds.get.maxKey.row, time))
//    val bands = coverage._1.collect().head._1.measurementName
//    val result: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (reprojected.map{case (spatialKey, tile) =>
//      val spaceTimeKey = new SpaceTimeKey(spatialKey.col, spatialKey.row, time)
//      val spaceTimeBandKey = SpaceTimeBandKey(spaceTimeKey, bands)
//      (spaceTimeBandKey, tile)
//    }, TileLayerMetadata(reprojected.metadata.cellType, LayoutDefinition(reprojected.metadata.extent, reprojected.metadata.tileLayout), reprojected.metadata.extent, reprojected.metadata.crs, bounds))
//    makeTIFF(result, "/D:/Intermediate_results/ANN.tif")

    if (level > zoom) {
      throw new Exception("level can not > " + zoom)
    }
    else {
      // 待修改dagId
      val on_the_fly_path: String = outputPath + jobId
      val file = new File(on_the_fly_path)
      if (file.exists() && file.isDirectory) {
        println("Delete existed on_the_fly_path")
        val command = s"rm -rf $on_the_fly_path"
        println(command)
        //调用系统命令
        command.!!
      }

      // Create the attributes store that will tell us information about our catalog.
      val attributeStore1: FileAttributeStore = FileAttributeStore(outputPath)
      // Create the writer that we will use to store the tiles in the local catalog.
      val writer: FileLayerWriter = FileLayerWriter(attributeStore1)

      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
        if (level - z <= 5 && level - z >= 0) {
          val layerId: LayerId = LayerId(jobId, z)
          println(layerId)
          // If the layer exists already, delete it out before writing
          if (attributeStore1.layerExists(layerId)) {
            //        new FileLayerManager(attributeStore).delete(layerId)
            try {
              writer.overwrite(layerId, rdd)
            } catch {
              case e: Exception =>
                e.printStackTrace()
            }
          }
          else {
            try {
              writer.write(layerId, rdd, ZCurveKeyIndexMethod)
            } catch {
              case e: Exception =>
                println(e)
                println("Continue writing Layers!")
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("New Coverage").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    reprojectEdu(sc, "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1.tif", "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1_reprojected.tif", "EPSG:3857", 100)
    //    randomForestTrain(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0818.zip", 4)
    //    classify(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0817new.zip", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\result.tif")
//    print(getZoom(sc, "/D:/Intermediate_results/Data/black_race/Landsat_wh_iso.tif"))
    visualizeOnTheFlyEduForClassification(sc, "/D:/Intermediate_results/classification/ann.tif", "/D:/Intermediate_results/TMS", 12, "ANN", coverageReadFromUploadFile = false)

//    randomForestTrain(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0818.zip", 4)
//    classify(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0817new.zip", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\result.tif")
//    print(getZoom(sc, "/D:/研究生材料/OGE/应用需求/Vv_part.tif"))
//    clipRasterByMaskLayerEdu(sc, "/C:/Users/HUAWEI/Desktop/毕设/应用_监督分类结果/RGB_Mean.tif","/C:/Users/HUAWEI/Desktop/oge/OGE竞赛/out/polygon4.shp", "/C:/Users/HUAWEI/Desktop/oge/OGE竞赛/out/clip.tiff")

    //    print(getZoom(sc, "/D:/研究生材料/OGE/应用需求/Vv_part.tif"))
  }

}
