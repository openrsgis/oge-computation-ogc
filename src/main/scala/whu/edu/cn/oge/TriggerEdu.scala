package whu.edu.cn.oge

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.Coverage.reproject
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif
import java.nio.file.Paths

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import whu.edu.cn.algorithms.MLlib._

import scala.util.Random
import org.jpmml.sparkml.PMMLBuilder
import org.jpmml.sparkml.PipelineModelUtil
import java.io.{File, IOException}

import com.google.common.io.{MoreFiles, RecursiveDeleteOption}

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
    val file: File = new File(modelOutputPath)
    file.createNewFile()
    val tmpDir = File.createTempFile("PipelineModel", "")
    if (!tmpDir.delete) throw new IOException
    else {
      PipelineModelUtil.store(model, tmpDir)
      PipelineModelUtil.compress(tmpDir, file)
      tmpDir.delete()
    }
    println("SUCCESS")
  }
  def classify(implicit sc: SparkContext, featuresPath: String, modelPath: String, classifiedOutputPath: String): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val file: File = new File(modelPath)
    val tmpDir: File = PipelineModelUtil.uncompress(file)
    val model: PipelineModel = PipelineModelUtil.load(spark, tmpDir)
    val predictedCoverage = Classifier.classify(spark, featursCoverage, model)("prediction")
    tmpDir.delete()
    makeTIFF(predictedCoverage, classifiedOutputPath)
    println("SUCCESS")
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("New Coverage").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    reprojectEdu(sc, "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1.tif", "/D:/TMS/07-29-2024-09-25-29_files_list/LC08_L1TP_002017_20190105_20200829_02_T1_B1_reprojected.tif", "EPSG:3857", 100)
//    randomForestTrain(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0817new.zip", 4)
    classify(sc, "C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\model0817new.zip", "C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\result.tif")
  }

}
