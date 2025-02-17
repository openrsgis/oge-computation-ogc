package whu.edu.cn.algorithms.MLlib

import java.io.File
import com.alibaba.fastjson.JSONObject
import com.baidubce.services.bos.BosClient
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import io.minio.{MinioClient, UploadObjectArgs}
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Model, PipelineModel, regression}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jpmml.sparkml.PipelineModelUtil
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.{CLIENT_NAME, USER_BUCKET_NAME}
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_BUCKET_NAME
import whu.edu.cn.entity.{BatchParam, SpaceTimeBandKey}
import whu.edu.cn.oge.TriggerEdu.makeTIFF
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.tempFileList
import whu.edu.cn.util.{ClientUtil, PostSender, RDDTransformerUtil}
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import java.nio.file.Paths
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import scala.util.Random
import scala.collection.mutable

object algorithms {
    //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  //分类
  def randomForestClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.randomForest(checkpointInterval, featureSubsetStrategy, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def randomForestClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.randomForest(checkpointInterval, featureSubsetStrategy, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, feature, inputProperties, classProperties.head) //TODO 暂时仅允许一个类标签
    model
  }
  def logisticRegressionClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, family: String = "auto", fitIntercept: Boolean = true, standardization: Boolean = true, threshold: Double = 0.5, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.logisticRegression(maxIter, regParam, elasticNetParam, family, fitIntercept, standardization, threshold, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def logisticRegressionClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, family: String = "auto", fitIntercept: Boolean = true, standardization: Boolean = true, threshold: Double = 0.5, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.logisticRegression(maxIter, regParam, elasticNetParam, family, fitIntercept, standardization, threshold, tol).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def decisionTreeClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, impurity: String = "gini", maxBins: Int = 32, maxDepth: Int = 5, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.decisionTree(checkpointInterval, impurity, maxBins, maxDepth, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def decisionTreeClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), checkpointInterval: Int = 10, impurity: String = "gini", maxBins: Int = 32, maxDepth: Int = 5, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.decisionTree(checkpointInterval, impurity, maxBins, maxDepth, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def gbtClassifierClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 10, featureSubsetStrategy: String = "auto", checkpointInterval: Int = 10, impurity: String = "variance", lossType: String = "logistic", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subSamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.gbtClassifier(maxIter, featureSubsetStrategy, checkpointInterval, impurity, lossType, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subSamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def gbtClassifierClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), maxIter: Int = 10, featureSubsetStrategy: String = "auto", checkpointInterval: Int = 10, impurity: String = "variance", lossType: String = "logistic", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subSamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.gbtClassifier(maxIter, featureSubsetStrategy, checkpointInterval, impurity, lossType, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subSamplingRate).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def multilayerPerceptronClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, layers: Array[Int] = Array[Int](4, 5, 4, 7), blockSize: Int = 128, seed: Long = Random.nextLong(), maxIter: Int = 100, stepSize: Double = 0.03, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.multilayerPerceptronClassifier(layers, blockSize, seed, maxIter, stepSize, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def multilayerPerceptronClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), layers: Array[Int] = Array[Int](4, 5, 4, 7), blockSize: Int = 128, seed: Long = Random.nextLong(), maxIter: Int = 100, stepSize: Double = 0.03, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.multilayerPerceptronClassifier(layers, blockSize, seed, maxIter, stepSize, tol).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def linearSVCClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 10, regParam: Double = 0.1): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.linearSVC(maxIter, regParam).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def linearSVCClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), maxIter: Int = 10, regParam: Double = 0.1): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.linearSVC(maxIter, regParam).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def naiveBayesClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, modelType: String = "multinomial", smoothing: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.naiveBayes(modelType, smoothing).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def naiveBayesClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), modelType: String = "multinomial", smoothing: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.naiveBayes(modelType, smoothing).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def fmClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, stepSize: Double = 1.0, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.fmClassifier(stepSize, factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def fmClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), stepSize: Double = 1.0, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.fmClassifier(stepSize, factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, tol).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def oneVsRestClassifierModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, classifier: String = "logisticRegression"): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val classif = classifier match {
      case "logisticRegression" => Classifier.logisticRegression()
      case "decisionTree" => Classifier.decisionTree()
      case "randomForest" => Classifier.randomForest()
      case "gbtClassifier" => Classifier.gbtClassifier()
      case "multilayerPerceptronClassifier" => Classifier.multilayerPerceptronClassifier()
      case "linearSVC" => Classifier.linearSVC()
      case "naiveBayes" => Classifier.naiveBayes()
      case "fmClassifier" => Classifier.fmClassifier()
      case _ => new IllegalArgumentException("不支持输入的分类器！")
    }
    val model: PipelineModel = Classifier.oneVsRest(classif.asInstanceOf[Classifier]).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def oneVsRestClassifierModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), classifier: String = "logisticRegression"): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val classif = classifier match {
      case "logisticRegression" => Classifier.logisticRegression()
      case "decisionTree" => Classifier.decisionTree()
      case "randomForest" => Classifier.randomForest()
      case "gbtClassifier" => Classifier.gbtClassifier()
      case "multilayerPerceptronClassifier" => Classifier.multilayerPerceptronClassifier()
      case "linearSVC" => Classifier.linearSVC()
      case "naiveBayes" => Classifier.naiveBayes()
      case "fmClassifier" => Classifier.fmClassifier()
      case _ => new IllegalArgumentException("不支持输入的分类器！")
    }
    val model: PipelineModel = Classifier.oneVsRest(classif.asInstanceOf[Classifier]).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def modelClassify(implicit sc: SparkContext, coverage: RDDImage, model: PipelineModel): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val classifiedCoverage: RDDImage = Classifier.classify(spark, coverage, model)("prediction")
    classifiedCoverage
  }
  def modelClassify(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel, featuresCol: List[String]): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    //返回包含全部结果属性的矢量
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val classifiedFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))] = Classifier.classify(spark, feature, model, featuresCol)
    classifiedFeature
  }

  //回归
  def randomForestRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.randomForestRegression(checkpointInterval, featureSubsetStrategy, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def randomForestRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.randomForestRegression(checkpointInterval, featureSubsetStrategy, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def linearRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, fitIntercept: Boolean = true, loss: String = "squaredError", solver: String = "auto", standardization: Boolean = true, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.linearRegression(maxIter, regParam, elasticNetParam, fitIntercept, loss, solver, standardization, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def linearRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, fitIntercept: Boolean = true, loss: String = "squaredError", solver: String = "auto", standardization: Boolean = true, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.linearRegression(maxIter, regParam, elasticNetParam, fitIntercept, loss, solver, standardization, tol).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def generalizedLinearRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, regParam: Double = 0.3, family: String = "gaussian", link: String = "identity", maxIter: Int = 10, fitIntercept: Boolean = true, linkPower: Double = 1, solver: String = "irls", tol: Double = 1E-6, variancePower: Double = 0.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.generalizedLinearRegression(regParam, family, link, maxIter, fitIntercept, linkPower, solver, tol, variancePower).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def generalizedLinearRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), regParam: Double = 0.3, family: String = "gaussian", link: String = "identity", maxIter: Int = 10, fitIntercept: Boolean = true, linkPower: Double = 1, solver: String = "irls", tol: Double = 1E-6, variancePower: Double = 0.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.generalizedLinearRegression(regParam, family, link, maxIter, fitIntercept, linkPower, solver, tol, variancePower).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def decisionTreeRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.decisionTreeRegression(checkpointInterval, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def decisionTreeRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), checkpointInterval: Int = 10, impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.decisionTreeRegression(checkpointInterval, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def gbtRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", lossType: String = "squared", maxBins: Int = 32, maxDepth: Int = 5, maxIter: Int = 10, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.gbtRegression(checkpointInterval, featureSubsetStrategy, impurity, lossType, maxBins, maxDepth, maxIter, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def gbtRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", lossType: String = "squared", maxBins: Int = 32, maxDepth: Int = 5, maxIter: Int = 10, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.gbtRegression(checkpointInterval, featureSubsetStrategy, impurity, lossType, maxBins, maxDepth, maxIter, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subsamplingRate).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def isotonicRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, isotonic: Boolean = true): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.isotonicRegression(isotonic).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def isotonicRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), isotonic: Boolean = true): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.isotonicRegression(isotonic).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def fmRegressionModel_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", stepSize: Double = 1.0, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.fmRegressor(factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, stepSize, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def fmRegressionModel_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], classProperties: List[String], inputProperties: List[String] = List(""), factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", stepSize: Double = 1.0, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.fmRegressor(factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, stepSize, tol).train(spark, feature, inputProperties, classProperties.head)
    model
  }
  def modelRegress(implicit sc: SparkContext, coverage: RDDImage, model: PipelineModel): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val regressCoverage: RDDImage = Regressor.regress(spark, coverage, model)("prediction")
    regressCoverage
  }
  def modelRegress(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel, featuresCol: List[String]): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val regressCoverage: RDD[(String, (Geometry, mutable.Map[String, Any]))] = Regressor.regress(spark, feature, model, featuresCol)
    regressCoverage
  }

  //聚类
  def kMeans_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.kMeans(k, maxIter, seed, tol)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }
  def kMeans_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresCol: List[String], k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.kMeans(k, maxIter, seed, tol)
      .train(spark, feature, featuresCol)
    val predictedFeature = Clusterer.cluster(spark, feature, model, featuresCol)
    predictedFeature
  }
  def latentDirichletAllocation_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, checkpointInterval: Int = 10, k: Int = 2, maxIter: Int = 10, optimizer: String = "online", seed: Long = Random.nextLong(), subsamplingRate: Double = 0.05, topicConcentration: Double = -1): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.latentDirichletAllocation(checkpointInterval, k, maxIter, optimizer, seed, subsamplingRate, topicConcentration)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("topicDistribution")
    predictedCoverage
  }
  def latentDirichletAllocation_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresCol: List[String], checkpointInterval: Int = 10, k: Int = 2, maxIter: Int = 10, optimizer: String = "online", seed: Long = Random.nextLong(), subsamplingRate: Double = 0.05, topicConcentration: Double = -1): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.latentDirichletAllocation(checkpointInterval, k, maxIter, optimizer, seed, subsamplingRate, topicConcentration)
      .train(spark, feature, featuresCol)
    val predictedCoverage = Clusterer.cluster(spark, feature, model, featuresCol)
    predictedCoverage
  }
  def bisectingKMeans_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, distanceMeasure: String = "euclidean", k: Int = 4, maxIter: Int = 10, seed: Long = Random.nextLong()): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.bisectingKMeans(distanceMeasure, k, maxIter, seed)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }
  def bisectingKMeans_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresCol: List[String], distanceMeasure: String = "euclidean", k: Int = 4, maxIter: Int = 10, seed: Long = Random.nextLong()): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.bisectingKMeans(distanceMeasure, k, maxIter, seed)
      .train(spark, feature, featuresCol)
    val predictedCoverage = Clusterer.cluster(spark, feature, model, featuresCol)
    predictedCoverage
  }
  def gaussianMixture_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.gaussianMixture(k, maxIter, seed, tol)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }
  def gaussianMixture_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresCol: List[String], k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.gaussianMixture(k, maxIter, seed, tol)
      .train(spark, feature, featuresCol)
    val predictedCoverage = Clusterer.cluster(spark, feature, model, featuresCol)
    predictedCoverage
  }

  //精度评估
  //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  def multiclassClassificationEvaluator_coverage(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1"), metricLabel: Double = 0.0): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multiclassClassificationEvaluator_coverage(spark, labelCoverage, predictionCoverage, metricName, 0, 0, metricLabel)
  }
  def multiclassClassificationEvaluator_feature(implicit sc: SparkContext, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelCol: String, metricName: List[String] = List("f1"), metricLabel: Double = 0.0): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multiclassClassificationEvaluator_feature(spark, labelFeature, predictionFeature, labelCol, metricName, metricLabel)
  }
  def clusteringEvaluator_coverage(implicit sc: SparkContext, featuresCoverage: RDDImage, predictionCoverage: RDDImage, metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean"): Double = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.clusteringEvaluator_coverage(spark, featuresCoverage, predictionCoverage, metricName, distanceMeasure, 0)
  }
  def clusteringEvaluator_feature(implicit sc: SparkContext, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresColNames: List[String], metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean"): Double = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.clusteringEvaluator_feature(spark, feature, featuresColNames, metricName, distanceMeasure)
  }
  def multilabelClassificationEvaluator_coverage(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1Measure")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multilabelClassificationEvaluator_coverage(spark, labelCoverage, predictionCoverage, metricName)
  }
  def multilabelClassificationEvaluator_feature(implicit sc: SparkContext, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelColNames: List[String], predictionColNames: List[String], metricName: List[String] = List("f1Measure")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multilabelClassificationEvaluator_feature(spark, labelFeature, predictionFeature, labelColNames, predictionColNames, metricName)
  }
  def binaryClassificationEvaluator_coverage(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("areaUnderROC")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.binaryClassificationEvaluator_coverage(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def binaryClassificationEvaluator_feature(implicit sc: SparkContext, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelCol: String, metricName: List[String] = List("areaUnderROC")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.binaryClassificationEvaluator_feature(spark, labelFeature, predictionFeature, labelCol, metricName)
  }
  def regressionEvaluator_coverage(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("rmse")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.regressionEvaluator_coverage(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def regressionEvaluator_feature(implicit sc: SparkContext, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelCol: String, metricName: List[String] = List("rmse")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.regressionEvaluator_feature(spark, labelFeature, predictionFeature, labelCol, metricName)
  }
  def rankingEvaluator_coverage(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("meanAveragePrecision")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.rankingEvaluator_coverage(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def rankingEvaluator_feature(implicit sc: SparkContext, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelCol: String, metricName: List[String] = List("meanAveragePrecision")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.rankingEvaluator_feature(spark, labelFeature, predictionFeature, labelCol, metricName)
  }

  //以下是在OGE主版中保存模型
  def saveModelBatch(implicit sc: SparkContext, model: PipelineModel, batchParam: BatchParam, dagId: String): Unit = {
    // 上传文件
    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}" //这里没有后缀，先走通文件夹，因为压缩也需要用原路径放文件夹
    model.write.overwrite().save("file://" + saveFilePath)
    //TODO 目前仅支持.zip
    val saveFilePathWithZip = saveFilePath + ".zip"
    PipelineModelUtil.compress(new File(saveFilePath), new File(saveFilePathWithZip))
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + ".zip"   // TODO 后面前端对话框允许修改“文件类型”后，将这句改为+ "." + batchParam.getFormat
//    val path = "d2cec4be-13a0-4989-aaf6-d0372d4cbd71" + "/result/" + "model0107" + ".zip" //楼下集群调试 使用这句
    val obj: JSONObject = new JSONObject
    obj.put("path",path)
    PostSender.shelvePost("info",obj)
    clientUtil.Upload(path, saveFilePathWithZip)
  }
  //在OGE主版中加载模型
  def loadModelFromUpload(implicit sc: SparkContext, modelID: String, userID: String, dagId: String): PipelineModel = {
    var path: String = new String()
    if (modelID.endsWith(".zip")) {
      path = s"${userID}/$modelID"
    } else {
      path = s"$userID/$modelID.zip"
    }
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}_${Trigger.file_id}.zip" //从数据库下载下来后临时文件存放路径
    clientUtil.Download(path, filePath) //临时文件有了
    val modelPath = filePath
//    val modelPathWithZip = if (modelPath.endsWith(".zip")) modelPath else modelPath + ".zip"
//    PipelineModelUtil.uncompress(new File(modelPathWithZip), new File(modelPathWithZip.stripSuffix(".zip")))
    util.unCompressFile(modelPath)
    val model: PipelineModel = PipelineModel.load("file://" + modelPath.stripSuffix(".zip"))
    model
  }
}