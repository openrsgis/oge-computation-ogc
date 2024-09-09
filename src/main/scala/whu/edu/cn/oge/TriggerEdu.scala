package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{BatchParam, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.oge.Coverage.{addStyles, reproject, resolutionTMSArray}
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import java.nio.file.Paths
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import whu.edu.cn.algorithms.MLlib._

import scala.util.Random
import org.jpmml.sparkml.PMMLBuilder
import org.jpmml.sparkml.PipelineModelUtil

import java.io.{File, IOException}
import sys.process._
import com.google.common.io.{MoreFiles, RecursiveDeleteOption}
import org.apache.spark.ml.util.MLWriter
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import whu.edu.cn.config.GlobalConfig

object TriggerEdu {
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
  def visualizeOnTheFlyEdu(implicit sc: SparkContext, inputPath: String, outputPath: String, level: Int, jobId: String, coverageReadFromUploadFile: Boolean, bands: String = null, min: String = null, max: String = null, gain: String = null, bias: String = null, gamma: String = null, palette: String = null, opacity: String = null, format: String = null): Unit = {

    val coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, inputPath)
    val visParam: VisualizationParam = new VisualizationParam
    visParam.setAllParam(bands, min, max, gain, bias, gamma, palette, opacity, format)
    // 待修改Trigger.coverageReadFromUploadFile
    val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = addStyles(if (coverageReadFromUploadFile) {
      reproject(coverage, CRS.fromEpsgCode(3857), resolutionTMSArray(level))
    } else {
      coverage
    }, visParam)

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
        if (level - z <= 2 && level - z >= 0) {
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
//    print(getZoom(sc, "/D:/研究生材料/OGE/应用需求/Vv_part.tif"))
  }

}
