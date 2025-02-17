package whu.edu.cn.algorithms.MLlib

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.TileLayout
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.shared.HasFitIntercept
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel, Transformer, classification, regression}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.MLlib.util.{calculateTileLayerMetadata, joinTwoCoverage, makeFeatureDataFrameFromRDD, makePropertyRDDFromDataFrame, makeRasterDataFrameFromRDD, makeRasterRDDFromDataFrame}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.oge.Feature.geometry

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
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
  def train(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresCol: List[String], labelCol: String):PipelineModel = {
    //各个列的名称取出
    val colNames = feature.first()._2._2.keys.toList
    if(!colNames.contains(labelCol))
      throw new IllegalArgumentException("The parameter labelColNames contains columns not present in the properties of feature.")
    val trueFeatureColNames =
      if(featuresCol != List("")) featuresCol
      else {
        val featuresColTmp = colNames.filter(col => col != labelCol).filter(_.matches(".*\\d$")) //取以数字结尾的特征
        if (featuresColTmp.isEmpty) {
          throw new IllegalArgumentException("The input feature dosen't contain a 'band_order' like property.")
        }
        featuresColTmp
      }
    if(trueFeatureColNames.exists(col => !colNames.contains(col)))
      throw new IllegalArgumentException("The parameter featureColNames contains columns not present in the properties of feature.")
    val assembledDF = makeFeatureDataFrameFromRDD(spark, feature, trueFeatureColNames, labelCol)
    //取消以下索引化的步骤，在util中直接令indexFeatures=features，否则会在某一特征不重复取值个数小于maxCategories时报错，之前影像的分类这个bug一直被忽略是因为影像几乎未出现过某一特征不重复取值个数小于maxCategories的情况
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(2) // features with > 2 distinct values are treated as continuous.
      .setHandleInvalid("skip")
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("scaledFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    val pipeline =
      if(ml.isInstanceOf[regression.FMRegressor]) new Pipeline().setStages(Array(featureScaler, this.ml)) //删除featureScaler
    else new Pipeline().setStages(Array(featureIndexer, this.ml)) //删除featureIndexer
    pipeline.fit(assembledDF)
//    this.ml.fit(assembledDF).asInstanceOf[PipelineModel]
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
      .setFeaturesCol("features")) //将"indexedFeatures"修改为features，因为当某一特征不重复取值个数大于自己设置的maxCategories时，实际上indexedFeatures就和features相同
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
      .setFeaturesCol("features"))
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
      .setFeaturesCol("features")
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
      .setFeaturesCol("features"))
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
      .setFeaturesCol("features")
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
      .setIsotonic(isotonic)
      .setLabelCol("label")
      .setFeaturesCol("features"))
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
      .setFeaturesCol("features"))
  }

  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    map += ("features" -> coverage)
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata))
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), newMetadata))
    map
  }
  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int]): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, featuresCol = true), newMetadata))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata))
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), newMetadata))
    map
  }
  def regress(implicit spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int], labelCol: Int): Map[String, RDDImage] ={
    //输入待回归的图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol, labelCol)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, predictionCol=true)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, featuresCol = true), newMetadata))
    if(predictionDF.columns.contains("label")) map += ("label" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, labelCol = true), newMetadata))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata))
    if(predictionDF.columns.contains("variance")) map += ("variance" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, varianceCol=true), newMetadata))
    map
  }
  def regress(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val featureColNames = feature.first()._2._2.keys.toList
    val df: DataFrame = makeFeatureDataFrameFromRDD(spark, feature, featureColNames)
    val predictionDF: DataFrame = model.transform(df)
    feature.map(t=>(t._1, t._2._1)).join(makePropertyRDDFromDataFrame(predictionDF))
  }
  def regress(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel, featuresCol: List[String]): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val df: DataFrame = makeFeatureDataFrameFromRDD(spark, feature, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    val propertyRDD: RDD[(String, Map[String, Any])] = makePropertyRDDFromDataFrame(predictionDF)
    val result: RDD[(String, (Geometry, mutable.Map[String, Any]))] = feature.map(t=>(t._1, t._2._1)).join(propertyRDD)
    result
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate()
//    println("hello")

//    val features: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif")
//    val label: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif")
//    val label2: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\features4class2.tif")
    val temp = Source.fromFile("C:/Users/马楚瑞/Desktop/OGE/解决模型保存与加载问题/featureCollection2.geojson").mkString
    val feature = geometry(sc, temp, "EPSG:4326")

//    val model: PipelineModel = Regressor.linearRegression().train(spark, features, label, labelCol = 4)
//      val model: PipelineModel = Regressor.generalizedLinearRegression().train(spark, features, label, labelCol = 4)
    val model: PipelineModel = Regressor.decisionTreeRegression().train(spark, feature, List(""), "class")

//    val df: DataFrame = makeRasterDataFrameFromRDD(spark, features)
//    val predictionDF: DataFrame = model.transform(df)
//    println(predictionDF.show(5))
//    val prediction: RDDImage = Regressor.regress(spark, features, model)("variance")  //会把coverage中的所有列当作特征
    val prediction: RDD[(String, (Geometry, mutable.Map[String, Any]))] = Regressor.regress(spark, feature, model, List("band1", "band2", "band3", "band4"))
    val metric = Evaluator.regressionEvaluator_feature(spark, feature, prediction, "class")
    println(metric)
//    println(prediction.collect.toList)
    //    saveRasterRDDToTif(prediction,"C:\\Users\\HUAWEI\\Desktop\\oge\\coverage_resources1\\MLlib_RFRvariance_0726.tiff")
  }
}

// 自定义一个无意义的转换器
//class NoOpTransformer extends Transformer {
//  // 这个转换器不做任何操作，直接返回输入的 DataFrame
//  override def transform(dataset: Dataset[_]): DataFrame = {
//    // 这里直接返回输入的数据集，不做任何变更
//    dataset.toDF()
//  }
//  // 设置输入和输出列
//  override def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
//    schema
//  }
//  // 必须实现的方法，返回自身
//  override def copy(extra: org.apache.spark.ml.param.ParamMap): Transformer = {
//    new NoOpTransformer()
//  }
//}