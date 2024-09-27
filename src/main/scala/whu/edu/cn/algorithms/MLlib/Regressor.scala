package whu.edu.cn.algorithms.MLlib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.shared.HasFitIntercept
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel, regression}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.MLlib.util.{joinTwoCoverage, makeRasterDataFrameFromRDD, makeRasterRDDFromDataFrame}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class Regressor(val ml: Estimator[_]) {
  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage): PipelineModel ={
    val featuresCount: Int = featuresCoverage._1.first()._2.bandCount
    val featuresCol: List[Int] = (0 until featuresCount).toList //所有波段作为特征
    val labelCol: List[Int] = List(0) //第一个波段作为标签
    val rowRdd: RDD[Row] = joinTwoCoverage(featuresCoverage, labelCoverage, featuresCol, labelCol)
    val fieldTypes = List.fill(4 + featuresCount + 1)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for(i<- 1 to featuresCount){
      colNames.append(s"feature$i")
    }
    val labelColNames = List("label1","label2","label3","label4") ::: colNames.toList ::: List("label")
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames:_*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("scaledFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    val pipeline =
      if(ml.isInstanceOf[regression.FMRegressor]) new Pipeline().setStages(Array(featureScaler, this.ml))
      else new Pipeline().setStages(Array(featureIndexer, this.ml))
    pipeline.fit(assembledDF)
  }
  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, featuresCol: List[Int]): PipelineModel ={
    //选择波段作为特征
    val labelCol: List[Int] = List(0) //第一个波段作为标签
    val rowRdd: RDD[Row] = joinTwoCoverage(featuresCoverage, labelCoverage, featuresCol, labelCol)
    val fieldTypes = List.fill(4 + featuresCol.length + 1)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for(i<- 1 to featuresCol.length){
      colNames.append(s"feature$i")
    }
    val labelColNames = List("label1","label2","label3","label4") ::: colNames.toList ::: List("label")
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames:_*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("scaledFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    val pipeline =
      if(ml.isInstanceOf[regression.FMRegressor]) new Pipeline().setStages(Array(featureScaler, this.ml))
      else new Pipeline().setStages(Array(featureIndexer, this.ml))
    pipeline.fit(assembledDF)
  }
  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, labelCol: Int): PipelineModel ={
    val featuresCount: Int = featuresCoverage._1.first()._2.bandCount
    val featuresCol: List[Int] = (0 until featuresCount).toList //所有波段作为特征
    //选择波段作为标签
    val rowRdd: RDD[Row] = joinTwoCoverage(featuresCoverage, labelCoverage, featuresCol, List(labelCol))
    val fieldTypes = List.fill(4 + featuresCount + 1)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for(i<- 1 to featuresCount){
      colNames.append(s"feature$i")
    }
    val labelColNames = List("label1","label2","label3","label4") ::: colNames.toList ::: List("label")
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames:_*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("scaledFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    val pipeline =
      if(ml.isInstanceOf[regression.FMRegressor]) new Pipeline().setStages(Array(featureScaler, this.ml))
      else new Pipeline().setStages(Array(featureIndexer, this.ml))
    pipeline.fit(assembledDF)
  }
  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, featuresCol: List[Int], labelCol: Int): PipelineModel ={
    //选择波段作为特征
    //选择波段作为标签
    val rowRdd: RDD[Row] = joinTwoCoverage(featuresCoverage, labelCoverage, featuresCol, List(labelCol))
    val fieldTypes = List.fill(4 + featuresCol.length + 1)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for(i<- 1 to featuresCol.length){
      colNames.append(s"feature$i")
    }
    val labelColNames = List("label1","label2","label3","label4") ::: colNames.toList ::: List("label")
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames:_*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("scaledFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    val pipeline =
      if(ml.isInstanceOf[regression.FMRegressor]) new Pipeline().setStages(Array(featureScaler, this.ml))
      else new Pipeline().setStages(Array(featureIndexer, this.ml))
    pipeline.fit(assembledDF)
  }

}
object Regressor {
  def linearRegression(maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, fitIntercept: Boolean = true, loss: String = "squaredError", solver: String = "auto", standardization: Boolean = true, tol: Double = 1E-6): Regressor = {
    new Regressor(new regression.LinearRegression()
      .setElasticNetParam(elasticNetParam)
      .setFitIntercept(fitIntercept)
      .setLoss(loss)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setSolver(solver)
      .setStandardization(standardization )
      .setTol(tol)
      .setLabelCol("label") //对于回归算法，label一定是数值型的，因此不用序列化
      .setFeaturesCol("indexedFeatures"))
  }
  def generalizedLinearRegression(regParam: Double = 0.3, family: String = "gaussian", link: String = "identity", maxIter: Int = 10, fitIntercept: Boolean = true, linkPower: Double = 1, solver: String = "irls", tol: Double = 1E-6, variancePower: Double = 0.0): Regressor = {
    new Regressor(new regression.GeneralizedLinearRegression() .setMaxIter(10)
      .setRegParam(regParam)
      .setFitIntercept(fitIntercept)
      .setFamily(family)
      .setLink(link)
      .setLinkPower(linkPower)
      .setSolver(solver)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setVariancePower(variancePower)
      .setLabelCol("label") //对于回归算法，label一定是数值型的，因此不用序列化
      .setFeaturesCol("indexedFeatures"))
  }
  def decisionTreeRegression(checkpointInterval: Int = 10, impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()): Regressor = {
    new Regressor(new regression.DecisionTreeRegressor()
      .setCheckpointInterval(checkpointInterval)
      .setImpurity(impurity)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setSeed(seed)
      .setMinInfoGain(minInfoGain)
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setVarianceCol("variance"))
  }

  def randomForestRegression(checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): Regressor = {
    new Regressor(new regression.RandomForestRegressor()
      .setCheckpointInterval(checkpointInterval)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setImpurity(impurity)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setNumTrees(numTrees)
      .setSeed(seed)
      .setSubsamplingRate(subsamplingRate)
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures"))
  }
  def gbtRegression(checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", lossType: String = "squared", maxBins: Int = 32, maxDepth: Int = 5, maxIter: Int = 10, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subsamplingRate: Double = 1.0): Regressor = {
    new Regressor(new regression.GBTRegressor()
      .setCheckpointInterval(checkpointInterval)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setImpurity(impurity)
      .setLossType(lossType)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMaxIter(maxIter)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setSeed(seed)
      .setStepSize(stepSize)
      .setSubsamplingRate(subsamplingRate)
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(maxIter)
    )
  }
  //TODO 这个太例外了，还需要censor列（参考ML官网），后面再做
//  def aftSurvivalRegression(): Regressor = {
//    new Regressor(new regression.AFTSurvivalRegression()
//      .setLabelCol("label")
//      .setFeaturesCol("indexedFeatures")
//      .setQuantileProbabilities(Array(0.3, 0.6))
//      .setQuantilesCol("quantiles"))
//  }
  def isotonicRegression(isotonic: Boolean = true): Regressor = {
    new Regressor(new regression.IsotonicRegression()
      .setFeaturesCol("indexedFeatures")
      .setIsotonic(isotonic)
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures"))
  }
  def fmRegressor(factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", stepSize: Double = 1.0, tol: Double = 1E-6): Regressor = {
    new Regressor(new regression.FMRegressor()
      .setFactorSize(factorSize)
      .setFitIntercept(fitIntercept)
      .setFitLinear(fitLinear)
      .setInitStd(initStd)
      .setMaxIter(maxIter)
      .setMiniBatchFraction(minBatchFraction)
      .setRegParam(regParam)
      .setSeed(seed)
      .setSolver(solver)
      .setStepSize(stepSize)
      .setTol(tol)
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures"))
  }

  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage)
    val predictionDF: DataFrame = model.transform(df)
    var map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    map += ("features" -> coverage)
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true), coverage._2)) //TODO coverage._2应该还需要改下，不能完全照搬
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), coverage._2))
    map
  }
  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int]): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    var map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, featuresCol = true), coverage._2))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true), coverage._2)) //TODO coverage._2应该还需要改下，不能完全照搬
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), coverage._2))
    map
  }
  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int], labelCol: Int): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol, labelCol)
    val predictionDF: DataFrame = model.transform(df)
    var map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, featuresCol = true), coverage._2))
    if(predictionDF.columns.contains("label")) map += ("label" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, labelCol = true), coverage._2))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true), coverage._2)) //TODO coverage._2应该还需要改下，不能完全照搬
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), coverage._2))
    map
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate() //TODO ?

    val features: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif")
    val label: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif")
    val label2: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\features4class2.tif")

//    val model: PipelineModel = Regressor.linearRegression().train(spark, features, label, labelCol = 4)
//      val model: PipelineModel = Regressor.generalizedLinearRegression().train(spark, features, label, labelCol = 4)
    val model: PipelineModel = Regressor.decisionTreeRegression().train(spark, features, label, labelCol = 4)

    val df: DataFrame = makeRasterDataFrameFromRDD(spark, features)
    val predictionDF: DataFrame = model.transform(df)
    println(predictionDF.show(5))
//    val prediction: RDDImage = Regressor.regress(spark, features, model)("variance")  //会把coverage中的所有列当作特征
//    saveRasterRDDToTif(prediction,"C:\\Users\\HUAWEI\\Desktop\\oge\\coverage_resources1\\MLlib_RFRvariance_0726.tiff")
  }
}