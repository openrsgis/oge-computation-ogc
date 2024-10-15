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
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.entity.{BatchParam, SpaceTimeBandKey}
import whu.edu.cn.oge.TriggerEdu.makeTIFF
import whu.edu.cn.util.{ClientUtil, PostSender}
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import scala.util.Random

object algorithms {
  //以下是在OGE主版模型中保存算子
  def visualizeBatch(implicit sc: SparkContext, model: PipelineModel, batchParam: BatchParam, dagId: String): Unit = {
    // 上传文件
    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}" //这里没有后缀，先走通文件夹，因为压缩也需要用原路径放文件夹
    //    GeoTiff(reprojectTile, batchParam.getCrs).write(saveFilePath)
    model.write.overwrite().save(saveFilePath)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
    val obj: JSONObject = new JSONObject
    obj.put("path",path.toString)
    PostSender.shelvePost("info",obj)
    //    client.putObject(PutObjectArgs.builder().bucket("oge-user").`object`(batchParam.getFileName + "." + batchParam.getFormat).stream(inputStream,inputStream.available(),-1).build)

    clientUtil.Upload(path, saveFilePath)

    //    client.putObject(PutObjectArgs)
    //    minIOUtil.releaseMinioClient(client)
  }

  //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  //分类
  def randomForestClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.randomForest(checkpointInterval, featureSubsetStrategy, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    //    val model: PipelineModel = Classifier.randomForest(checkpointInterval, "auto", 32, 5, 0.0, 1, 0.0, 20, 123L, 1.0).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def logisticRegressionClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, family: String = "auto", fitIntercept: Boolean = true, standardization: Boolean = true, threshold: Double = 0.5, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.logisticRegression(maxIter, regParam, elasticNetParam, family, fitIntercept, standardization, threshold, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def decisionTreeClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, impurity: String = "gini", maxBins: Int = 32, maxDepth: Int = 5, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.decisionTree(checkpointInterval, impurity, maxBins, maxDepth, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def gbtClassifierClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 10, featureSubsetStrategy: String = "auto", checkpointInterval: Int = 10, impurity: String = "variance", lossType: String = "logistic", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subSamplingRate: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.gbtClassifier(maxIter, featureSubsetStrategy, checkpointInterval, impurity, lossType, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subSamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def multilayerPerceptronClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, layers: Array[Int] = Array[Int](4, 5, 4, 7), blockSize: Int = 128, seed: Long = Random.nextLong(), maxIter: Int = 100, stepSize: Double = 0.03, tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.multilayerPerceptronClassifier(layers, blockSize, seed, maxIter, stepSize, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def linearSVCClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 10, regParam: Double = 0.1): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.linearSVC(maxIter, regParam).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def naiveBayesClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, modelType: String = "multinomial", smoothing: Double = 1.0): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.naiveBayes(modelType, smoothing).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def fmClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, stepSize: Double = 1.0, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", tol: Double = 1E-6): PipelineModel = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Classifier.fmClassifier(stepSize, factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def oneVsRestClassifierModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, classifier: String = "logisticRegression"): PipelineModel = {
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
  def modelClassify(implicit sc: SparkContext, coverage: RDDImage, model: PipelineModel): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val classifiedCoverage: RDDImage = Classifier.classify(spark, coverage, model)("prediction")
    classifiedCoverage
  }


  //回归
  def randomForestRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.randomForestRegression(checkpointInterval, featureSubsetStrategy, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def linearRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, fitIntercept: Boolean = true, loss: String = "squaredError", solver: String = "auto", standardization: Boolean = true, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.linearRegression(maxIter, regParam, elasticNetParam, fitIntercept, loss, solver, standardization, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def generalizedLinearRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, regParam: Double = 0.3, family: String = "gaussian", link: String = "identity", maxIter: Int = 10, fitIntercept: Boolean = true, linkPower: Double = 1, solver: String = "irls", tol: Double = 1E-6, variancePower: Double = 0.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.generalizedLinearRegression(regParam, family, link, maxIter, fitIntercept, linkPower, solver, tol, variancePower).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def decisionTreeRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.decisionTreeRegression(checkpointInterval, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def gbtRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", lossType: String = "squared", maxBins: Int = 32, maxDepth: Int = 5, maxIter: Int = 10, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subsamplingRate: Double = 1.0): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.gbtRegression(checkpointInterval, featureSubsetStrategy, impurity, lossType, maxBins, maxDepth, maxIter, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subsamplingRate).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def isotonicRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, isotonic: Boolean = true): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.isotonicRegression(isotonic).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def fmRegressionModel(implicit sc: SparkContext, featuresCoverage: RDDImage, labelCoverage: RDDImage, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", stepSize: Double = 1.0, tol: Double = 1E-6): PipelineModel ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: PipelineModel = Regressor.fmRegressor(factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, stepSize, tol).train(spark, featuresCoverage, labelCoverage)
    model
  }
  def modelRegress(implicit sc: SparkContext, coverage: RDDImage, model: PipelineModel): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val regressCoverage: RDDImage = Regressor.regress(spark, coverage, model)("prediction")
    regressCoverage
  }

  //聚类
  def kMeans(implicit sc: SparkContext, featuresCoverage: RDDImage, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.kMeans(k, maxIter, seed, tol)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }
  def latentDirichletAllocation(implicit sc: SparkContext, featuresCoverage: RDDImage, checkpointInterval: Int = 10, k: Int = 2, maxIter: Int = 10, optimizer: String = "online", seed: Long = Random.nextLong(), subsamplingRate: Double = 0.05, topicConcentration: Double = -1): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.latentDirichletAllocation(checkpointInterval, k, maxIter, optimizer, seed, subsamplingRate, topicConcentration)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("topicDistribution")
    predictedCoverage
  }
  def bisectingKMeans(implicit sc: SparkContext, featuresCoverage: RDDImage, distanceMeasure: String = "euclidean", k: Int = 4, maxIter: Int = 10, seed: Long = Random.nextLong()): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.bisectingKMeans(distanceMeasure, k, maxIter, seed)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }
  def gaussianMixture(implicit sc: SparkContext, featuresCoverage: RDDImage, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): RDDImage = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val model: Model[_] = Clusterer.gaussianMixture(k, maxIter, seed, tol)
      .train(spark, featuresCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featuresCoverage, model)("prediction")
    predictedCoverage
  }

  //精度评估
  //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  def multiclassClassificationEvaluator(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1"), metricLabel: Double = 0.0): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multiclassClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0, metricLabel)
  }
  def clusteringEvaluator(implicit sc: SparkContext, featuresCoverage: RDDImage, predictionCoverage: RDDImage, metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean"): Double = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.clusteringEvaluator(spark, featuresCoverage, predictionCoverage, metricName, distanceMeasure, 0)
  }
  def multilabelClassificationEvaluator(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1Measure")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.multilabelClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName)
  }
  def binaryClassificationEvaluator(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("areaUnderROC")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.binaryClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def regressionEvaluator(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("rmse")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.regressionEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def rankingEvaluator(implicit sc: SparkContext, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("meanAveragePrecision")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    Evaluator.rankingEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }

}