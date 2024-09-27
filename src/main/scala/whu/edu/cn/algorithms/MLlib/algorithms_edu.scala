package whu.edu.cn.algorithms.MLlib

import java.io.File

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Model, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jpmml.sparkml.PipelineModelUtil
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.TriggerEdu.makeTIFF
import whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif

import scala.util.Random

object algorithms_edu {
  //以下是应用于OGE教育版的MLlib分类、回归、聚类算子
  //所有的输入和输出都以路径的形式传递，函数不返回值
  //分类
  def randomForestClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0) = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.randomForest(checkpointInterval, featureSubsetStrategy, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def logisticRegressionClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, family: String = "auto", fitIntercept: Boolean = true, standardization: Boolean = true, threshold: Double = 0.5, tol: Double = 1E-6): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.logisticRegression(maxIter, regParam, elasticNetParam, family, fitIntercept, standardization, threshold, tol)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def decisionTreeClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, checkpointInterval: Int = 10, impurity: String = "gini", maxBins: Int = 32, maxDepth: Int = 5, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()) : Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.decisionTree(checkpointInterval, impurity, maxBins, maxDepth, minInstancesPerNode, minWeightFractionPerNode, seed)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def gbtClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, maxIter: Int = 10, featureSubsetStrategy: String = "auto", checkpointInterval: Int = 10, impurity: String = "variance", lossType: String = "logistic", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subSamplingRate: Double = 1.0): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.gbtClassifier(maxIter, featureSubsetStrategy, checkpointInterval, impurity, lossType, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subSamplingRate)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def multilayerPerceptronClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, layers: Array[Int] = Array[Int](4, 5, 4, 7), blockSize: Int = 128, seed: Long = Random.nextLong(), maxIter: Int = 100, stepSize: Double = 0.03, tol: Double = 1E-6): Unit ={  //TODO 出来少个类，未解决
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.multilayerPerceptronClassifier(layers, blockSize, seed, maxIter, stepSize, tol)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def linearSVCClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, maxIter: Int = 10, regParam: Double = 0.1): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.linearSVC(maxIter, regParam)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def oneVsRestClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, classifier: String = "logisticRegression"): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featuresCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
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

    val model: PipelineModel = Classifier.oneVsRest(classif.asInstanceOf[Classifier])
      .train(spark, featuresCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete() //TODO 检查是否删成功
    println("SUCCESS")
  }
  def naiveBayesClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, modelType: String = "multinomial", smoothing: Double = 1.0): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.naiveBayes(modelType, smoothing)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def fmClassifierTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, stepSize: Double = 1.0, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", tol: Double = 1E-6): Unit ={
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Classifier.fmClassifier(stepSize, factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, tol)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
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
  def classifyProbability(implicit sc: SparkContext, featuresPath: String, modelPath: String, probabilityOutputPath: String): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    //若用户未添加后缀，为其添加
    val modelPathWithZip = if (modelPath.endsWith(".zip")) modelPath else modelPath + ".zip"
    PipelineModelUtil.uncompress(new File(modelPathWithZip), new File(modelPathWithZip.stripSuffix(".zip")))
    //    val model: PipelineModel = PipelineModelUtil.load(spark, new File(modelPathWithZip.stripSuffix(".zip")))
    val model: PipelineModel = PipelineModel.load("file://" + modelPathWithZip.stripSuffix(".zip"))
    val predictedCoverage =
      try {Classifier.classify(spark, featursCoverage, model)("probability")} catch {
        case e: Exception => throw new IllegalArgumentException("当前分类方法不支持计算probability")
      }
    new File(modelPathWithZip.stripSuffix(".zip")).delete()
    makeTIFF(predictedCoverage, probabilityOutputPath)
    println("SUCCESS")
  }

  //回归
  def linearRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, fitIntercept: Boolean = true, loss: String = "squaredError", solver: String = "auto", standardization: Boolean = true, tol: Double = 1E-6): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.linearRegression(maxIter, regParam, elasticNetParam, fitIntercept, loss, solver, standardization, tol)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }

  def generalizedLinearRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, regParam: Double = 0.3, family: String = "gaussian", link: String = "identity", maxIter: Int = 10, fitIntercept: Boolean = true, linkPower: Double = 1, solver: String = "irls", tol: Double = 1E-6, variancePower: Double = 0.0): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.generalizedLinearRegression(regParam, family, link, maxIter, fitIntercept, linkPower, solver, tol, variancePower)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }

  def decisionTreeRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, checkpointInterval: Int = 10, impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.decisionTreeRegression(checkpointInterval, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }

  def randomForestRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.randomForestRegression(checkpointInterval, featureSubsetStrategy, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }

  def gbtRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", lossType: String = "squared", maxBins: Int = 32, maxDepth: Int = 5, maxIter: Int = 10, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subsamplingRate: Double = 1.0): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.gbtRegression(checkpointInterval, featureSubsetStrategy, impurity, lossType, maxBins, maxDepth, maxIter, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subsamplingRate)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def isotonicRegressionTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, isotonic: Boolean = true): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.isotonicRegression(isotonic)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }
  def fmRegressorTrain(implicit sc: SparkContext, featuresPath: String, labelPath: String, modelOutputPath: String, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", stepSize: Double = 1.0, tol: Double = 1E-6): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val model: PipelineModel = Regressor.fmRegressor(factorSize, fitIntercept, fitLinear, initStd, maxIter, minBatchFraction, regParam, seed, solver, stepSize, tol)
      .train(spark, featursCoverage, labelCoverage)
    model.write.overwrite().save("file://" + modelOutputPath)
    PipelineModelUtil.compress(new File(modelOutputPath), new File(modelOutputPath + ".zip"))
    new File(modelOutputPath).delete()
    println("SUCCESS")
  }

  def regress(implicit sc: SparkContext, featuresPath: String, modelPath: String, regressedOutputPath: String): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    //若用户未添加后缀，为其添加
    val modelPathWithZip = if (modelPath.endsWith(".zip")) modelPath else modelPath + ".zip"
    PipelineModelUtil.uncompress(new File(modelPathWithZip), new File(modelPathWithZip.stripSuffix(".zip")))
    //    val model: PipelineModel = PipelineModelUtil.load(spark, new File(modelPathWithZip.stripSuffix(".zip")))
    val model: PipelineModel = PipelineModel.load("file://" + modelPathWithZip.stripSuffix(".zip"))
    val predictedCoverage = Regressor.regress(spark, featursCoverage, model)("prediction")
    new File(modelPathWithZip.stripSuffix(".zip")).delete()
    makeTIFF(predictedCoverage, regressedOutputPath)
    println("SUCCESS")
  }
  def regressVariance(implicit sc: SparkContext, featuresPath: String, modelPath: String, varianceOutputPath: String): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    //若用户未添加后缀，为其添加
    val modelPathWithZip = if (modelPath.endsWith(".zip")) modelPath else modelPath + ".zip"
    PipelineModelUtil.uncompress(new File(modelPathWithZip), new File(modelPathWithZip.stripSuffix(".zip")))
    //    val model: PipelineModel = PipelineModelUtil.load(spark, new File(modelPathWithZip.stripSuffix(".zip")))
    val model: PipelineModel = PipelineModel.load("file://" + modelPathWithZip.stripSuffix(".zip"))
    val predictedCoverage =
      try {Regressor.regress(spark, featursCoverage, model)("variance")} catch {
        case e: Exception => throw new IllegalArgumentException("当前回归方法不支持计算variance")
      }
    new File(modelPathWithZip.stripSuffix(".zip")).delete()
    makeTIFF(predictedCoverage, varianceOutputPath)
    println("SUCCESS")
  }

  //聚类
  def kMeans(implicit sc: SparkContext, featuresPath: String, clusteredOutputPath: String, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val model: Model[_] = Clusterer.kMeans(k, maxIter, seed, tol)
      .train(spark, featursCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featursCoverage, model)("prediction")
    makeTIFF(predictedCoverage, clusteredOutputPath)
  }
  def latentDirichletAllocation(implicit sc: SparkContext, featuresPath: String, clusteredOutputPath: String, checkpointInterval: Int = 10, k: Int = 2, maxIter: Int = 10, optimizer: String = "online", seed: Long = Random.nextLong(), subsamplingRate: Double = 0.05, topicConcentration: Double = -1): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val model: Model[_] = Clusterer.latentDirichletAllocation(checkpointInterval, k, maxIter, optimizer, seed, subsamplingRate, topicConcentration)
      .train(spark, featursCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featursCoverage, model)("prediction")
    makeTIFF(predictedCoverage, clusteredOutputPath)
  }
  def bisectingKMeans(implicit sc: SparkContext, featuresPath: String, clusteredOutputPath: String, distanceMeasure: String = "euclidean", k: Int = 4, maxIter: Int = 10, seed: Long = Random.nextLong()): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val model: Model[_] = Clusterer.bisectingKMeans(distanceMeasure, k, maxIter, seed)
      .train(spark, featursCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featursCoverage, model)("prediction")
    makeTIFF(predictedCoverage, clusteredOutputPath)
  }
  def gaussianMixture(implicit sc: SparkContext, featuresPath: String, clusteredOutputPath: String, k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): Unit = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featursCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val model: Model[_] = Clusterer.gaussianMixture(k, maxIter, seed, tol)
      .train(spark, featursCoverage)
    val predictedCoverage = Clusterer.cluster(spark, featursCoverage, model)("prediction")
    makeTIFF(predictedCoverage, clusteredOutputPath)
  }

  //精度评估
  //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  def multiclassClassificationEvaluator(implicit sc: SparkContext, labelPath: String, predictionPath: String, metricName: List[String] = List("f1")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.multiclassClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def clusteringEvaluator(implicit sc: SparkContext, featuresPath: String, predictionPath: String, metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean"): Double = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val featuresCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, featuresPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.clusteringEvaluator(spark, featuresCoverage, predictionCoverage, metricName, distanceMeasure, 0)
  }
  def multilabelClassificationEvaluator(implicit sc: SparkContext, labelPath: String, predictionPath: String, metricName: List[String] = List("f1Measure")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.multilabelClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName)
  }
  def binaryClassificationEvaluator(implicit sc: SparkContext, labelPath: String, predictionPath: String, metricName: List[String] = List("areaUnderROC")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.binaryClassificationEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def regressionEvaluator(implicit sc: SparkContext, labelPath: String, predictionPath: String, metricName: List[String] = List("rmse")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.regressionEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
  def rankingEvaluator(implicit sc: SparkContext, labelPath: String, predictionPath: String, metricName: List[String] = List("meanAveragePrecision")): List[Double] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val labelCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, labelPath)
    val predictionCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc, predictionPath)
    Evaluator.rankingEvaluator(spark, labelCoverage, predictionCoverage, metricName, 0, 0)
  }
}
