package whu.edu.cn.algorithms.MLlib

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{CellType, MultibandTile, TileLayout}
import geotrellis.vector.Extent
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.algorithms.ImageProcess.core.MathTools.findSpatialKeyMinMax

import scala.collection.mutable.{ListBuffer, Map}
import whu.edu.cn.algorithms.MLlib.util.{calculateTileLayerMetadata, joinTwoCoverage, makeFeatureDataFrameFromRDD, makePropertyRDDFromDataFrame, makeRasterDataFrameFromRDD, makeRasterRDDFromDataFrame}

import scala.util.Random
import scala.util.matching.Regex
import scala.collection.mutable
import whu.edu.cn.oge.{Feature, Sheet}
import whu.edu.cn.oge.Feature.geometry

import java.time.Instant
import scala.io.Source
//import whu.edu.cn.algorithms.MLlib.MLModel

class Classifier(val ml: Estimator[_]) {
  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage):PipelineModel = {
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

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDF)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("indexedFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    // Convert indexed labels back to original labels. 索引label和原始label是不对应的
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0)) //转回对应关系
    val pipeline = if(this.ml.isInstanceOf[classification.FMClassifier]) new Pipeline().setStages(Array(labelIndexer, featureScaler, this.ml, labelConverter))
    else new Pipeline().setStages(Array(labelIndexer, featureIndexer, this.ml, labelConverter))
    // Train model. This also runs the indexers.
    pipeline.fit(assembledDF)
  }

  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, featuresCol: List[Int]):PipelineModel = {
    //指定波段作为特征 featuresCol
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

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDF)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous features.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("indexedFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    // Convert indexed labels back to original labels. 索引label和原始label是不对应的
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0)) //转回对应关系
    val pipeline = if(this.ml.isInstanceOf[classification.FMClassifier]) new Pipeline().setStages(Array(labelIndexer, featureScaler, this.ml, labelConverter))
    else new Pipeline().setStages(Array(labelIndexer, featureIndexer, this.ml, labelConverter))
    // Train model. This also runs the indexers.
    pipeline.fit(assembledDF)
  }

  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, labelCol: Int):PipelineModel = {
    val featuresCount: Int = featuresCoverage._1.first()._2.bandCount
    val featuresCol: List[Int] = (0 until featuresCount).toList //所有波段作为特征
    //指定波段作为标签
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

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDF)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("indexedFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    // Convert indexed labels back to original labels. 索引label和原始label是不对应的
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0)) //转回对应关系
    val pipeline = if(this.ml.isInstanceOf[classification.FMClassifier]) new Pipeline().setStages(Array(labelIndexer, featureScaler, this.ml, labelConverter))
    else new Pipeline().setStages(Array(labelIndexer, featureIndexer, this.ml, labelConverter))
    // Train model. This also runs the indexers.
    pipeline.fit(assembledDF)
  }

  def train(spark: SparkSession, featuresCoverage: RDDImage, labelCoverage: RDDImage, featuresCol: List[Int], labelCol: Int):PipelineModel = {
    //指定波段作为特征 featuresCol
    //指定波段作为标签
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

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDF)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(assembledDF)
    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
      .setInputCol("features")
      .setOutputCol("indexedFeatures") //内容不一样但名字不变
      .fit(assembledDF)
    // Convert indexed labels back to original labels. 索引label和原始label是不对应的
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0)) //转回对应关系
    val pipeline = if(this.ml.isInstanceOf[classification.FMClassifier]) new Pipeline().setStages(Array(labelIndexer, featureScaler, this.ml, labelConverter))
    else new Pipeline().setStages(Array(labelIndexer, featureIndexer, this.ml, labelConverter))
    // Train model. This also runs the indexers.
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
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDF)
    //取消以下索引化的步骤，在util中直接令indexFeatures=features，否则会在某一特征不重复取值个数小于maxCategories时报错，之前影像的分类这个bug一直被忽略是因为影像几乎未出现过某一特征不重复取值个数小于maxCategories的情况
    // Automatically identify categorical features, and index them.
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setHandleInvalid("skip")
//      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
//      .fit(assembledDF)
//    val featureScaler = new MinMaxScaler() //scale features to be between 0 and 1 to prevent the exploding gradient problem
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures") //内容不一样但名字不变
//      .fit(assembledDF)
    // Convert indexed labels back to original labels. 索引label和原始label是不对应的
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0)) //转回对应关系
    val pipeline = if(this.ml.isInstanceOf[classification.FMClassifier]) new Pipeline().setStages(Array(labelIndexer, this.ml, labelConverter))
    else new Pipeline().setStages(Array(labelIndexer, this.ml, labelConverter))
    // Train model. This also runs the indexers.
    pipeline.fit(assembledDF)
  }
}

object Classifier {
  def logisticRegression(maxIter: Int = 100, regParam: Double = 0.0, elasticNetParam: Double = 0.0, family: String = "auto", fitIntercept: Boolean = true, standardization: Boolean = true, threshold: Double = 0.5, tol: Double = 1E-6): Classifier = {
    new Classifier(new classification.LogisticRegression()   //工厂方法 //TODO 实际参数比这多好多，后面慢点往上加
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setFamily(family)
      .setFitIntercept(fitIntercept)
      .setStandardization(standardization)
      .setThreshold(threshold)
      .setTol(tol)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")) //将"indexedFeatures"修改为features，因为当某一特征不重复取值个数大于自己设置的maxCategories时，实际上indexedFeatures就和features相同
  }
  def decisionTree(checkpointInterval: Int = 10, impurity: String = "gini", maxBins: Int = 32, maxDepth: Int = 5, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong()) : Classifier = {
    new Classifier(new classification.DecisionTreeClassifier()
      .setCheckpointInterval(checkpointInterval)
      .setImpurity(impurity)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setSeed(seed)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features"))
  }
  def randomForest(checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0) : Classifier = {
    new Classifier(new classification.RandomForestClassifier()
      .setCheckpointInterval(checkpointInterval)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setNumTrees(numTrees)
      .setSeed(seed)
      .setSubsamplingRate(subsamplingRate)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
    )
  }
  def gbtClassifier(maxIter: Int = 10, featureSubsetStrategy: String = "auto", checkpointInterval: Int = 10, impurity: String = "variance", lossType: String = "logistic", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode:Int = 1, minWeightFractionPerNode: Double = 0.0, seed: Long = Random.nextLong(), stepSize: Double = 0.1, subSamplingRate: Double = 1.0): Classifier ={
    new Classifier(new classification.GBTClassifier()
      .setMaxIter(maxIter)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setCheckpointInterval(checkpointInterval)
      .setImpurity(impurity)
      .setLossType(lossType)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInfoGain(minInfoGain)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinWeightFractionPerNode(minWeightFractionPerNode)
      .setSeed(seed)
      .setStepSize(stepSize)
      .setSubsamplingRate(subSamplingRate)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
    )
  }
  def multilayerPerceptronClassifier(layers: Array[Int] = Array[Int](4, 5, 4, 7), blockSize: Int = 128, seed: Long = Random.nextLong(), maxIter: Int = 100, stepSize: Double = 0.03, tol: Double = 1E-6): Classifier ={  //TODO 出来少个类，未解决
    new Classifier(new classification.MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(blockSize)
      .setSeed(seed)
      .setMaxIter(maxIter)
      .setStepSize(stepSize)
      .setTol(tol)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features"))
  }
  def linearSVC(maxIter: Int = 10, regParam: Double = 0.1): Classifier ={  //binary classification
    new Classifier(new classification.LinearSVC()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features"))
  }
  def oneVsRest(classifier: Classifier = Classifier.logisticRegression()): Classifier ={  //binary classification
    val estimator = classifier.ml
    new Classifier(new classification.OneVsRest()
      .setClassifier(estimator.asInstanceOf[classification.Classifier[_,_,_]])
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features"))
  }
  def naiveBayes(modelType: String = "multinomial", smoothing: Double = 1.0): Classifier ={
    new Classifier(new classification.NaiveBayes()
      .setModelType(modelType)
      .setSmoothing(smoothing)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features"))
  }
  def fmClassifier(stepSize: Double = 1.0, factorSize: Int = 8, fitIntercept: Boolean = true, fitLinear: Boolean = true, initStd: Double = 0.01, maxIter: Int = 100, minBatchFraction: Double = 1.0, regParam: Double = 0.0, seed: Long = Random.nextLong(), solver: String = "adamW", tol: Double = 1E-6): Classifier ={ //Labels must be integers in range [0, 2)
    new Classifier(new classification.FMClassifier()
      .setStepSize(stepSize)
      .setFactorSize(factorSize)
      .setFitIntercept(fitIntercept)
      .setFitLinear(fitLinear)
      .setInitStd(initStd)
      .setMaxIter(maxIter)
      .setMiniBatchFraction(minBatchFraction)
      .setRegParam(regParam)
      .setSeed(seed)
      .setSolver(solver)
      .setTol(tol)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
    )
  }


  def classify(spark: SparkSession, coverage: RDDImage, model: PipelineModel): Map[String, RDDImage] = {
    //输入待分类图像和模型，输出分类完成的结果（字典形式）
    //输入图像的所有波段都将作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    map += ("features" -> coverage)
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata)) //TileLayerMetadata进行了修改，不能完全照搬
    if(predictionDF.columns.contains("rawPrediction"))map += ("rawPrediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, rawPredictionCol=true), newMetadata))
    if(predictionDF.columns.contains("probability"))map += ("probability" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, probabilityCol=true), newMetadata))
    map
  }
  def classify(spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int]): Map[String, RDDImage] = {
    //图像选取若干波段作为特征
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, featuresCol = true, predictedLabelCol=false), newMetadata))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata))
    if(predictionDF.columns.contains("rawPrediction")) map += ("rawPrediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, rawPredictionCol=true), newMetadata))
    if(predictionDF.columns.contains("probability")) map += ("probability" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, probabilityCol=true), newMetadata))
    map
  }
  def classify(spark: SparkSession, coverage: RDDImage, model: PipelineModel, featuresCol: List[Int], labelCol: Int): Map[String, RDDImage] = {
    //图像选取若干波段作为特征，选取某一波段作为标签
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol, labelCol)
    val predictionDF: DataFrame = model.transform(df)
    val map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    //bounds, layout, extent这些可能有所改变，需要重新生成TileLayerMetadata
    val rasterRdd = makeRasterRDDFromDataFrame(predictionDF)
    val newMetadata = calculateTileLayerMetadata(rasterRdd, coverage)
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, featuresCol = true, predictedLabelCol=false), newMetadata))
    if(predictionDF.columns.contains("label")) map += ("label" -> (makeRasterRDDFromDataFrame(predictionDF, labelCol = true, predictedLabelCol=false), newMetadata))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (rasterRdd, newMetadata))
    if(predictionDF.columns.contains("rawPrediction")) map += ("rawPrediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, rawPredictionCol=true), newMetadata))
    if(predictionDF.columns.contains("probability")) map += ("probability" -> (makeRasterRDDFromDataFrame(predictionDF, predictedLabelCol=false, probabilityCol=true), newMetadata))
    map
  }
  def classify(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val featureColNames = feature.first()._2._2.keys.toList
    val df: DataFrame = makeFeatureDataFrameFromRDD(spark, feature, featureColNames)
    val predictionDF: DataFrame = model.transform(df)
    feature.map(t=>(t._1, t._2._1)).join(makePropertyRDDFromDataFrame(predictionDF))
  }
  def classify(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], model: PipelineModel, featuresCol: List[String]): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val df: DataFrame = makeFeatureDataFrameFromRDD(spark, feature, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    val propertyRDD: RDD[(String, Map[String, Any])] = makePropertyRDDFromDataFrame(predictionDF)
    val result: RDD[(String, (Geometry, mutable.Map[String, Any]))] = feature.map(t=>(t._1, t._2._1)).join(propertyRDD)
    result
  }

  def main(args: Array[String]): Unit = {
    import whu.edu.cn.oge.Coverage
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate() //能不能和SparkContext同时创建？放到自己的函数里创建结束时调用.stop把它停掉

//    val features: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif")
//      val features: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\马楚瑞\\Desktop\\OGE\\重大专项-王川\\gedi_test.tif")
//    val label: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif")
//    val label2: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\features4class2.tif")
//    val predict: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Energy.tif")
//    val coverage: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\马楚瑞\\Desktop\\OGE\\解决模型保存与加载问题\\coverage2.tif")
//    val coverage: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\马楚瑞\\Downloads\\GF6_WFV_E115.8_N40.2_20230815_L1A1420343042-2\\GF6_WFV_E115.8_N40.2_20230815_L1A1420343042-2_B1.tif")
    val csv = Sheet.readCsv("C:\\Users\\马楚瑞\\Desktop\\OGE\\解决模型保存与加载问题\\feature1.csv")
    val feature = Sheet.sheetToFeatureProperties(sc, csv)
//    val temp = Source.fromFile("C:/Users/马楚瑞/Desktop/OGE/解决模型保存与加载问题/featureCollection2.geojson").mkString
//    val temp = Source.fromFile("C:/Users/马楚瑞/Desktop/OGE/解决模型保存与加载问题/featureCollection2.geojson").mkString
//    val feature = geometry(sc, temp, "EPSG:4326")
//    val featureResult = Coverage.sampleRegions(coverage, feature, List("class"))
//    println(featureResult.collect.toList)
    val model: PipelineModel = Classifier.logisticRegression().train(spark, feature, List(""), "class")
//    val df: DataFrame = spark.createDataFrame(Seq((1.0, 2.0, 3.0, 4.0), (2.0, 3.0, 4.0, 5.0))).toDF("band1", "band2", "band3", "band4")
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("band1", "band2", "band3", "band4"))
//      .setOutputCol("indexedFeatures")
//      .setHandleInvalid("skip")
//    val assemblerDF =assembler.transform(df)
//    val result: DataFrame = model.transform(assemblerDF)
//    val prediction: RDD[(String, (Geometry, mutable.Map[String, Any]))] = Classifier.classify(spark, feature, model, List("band1", "band2", "band3", "band4"))
//    val prediction: RDDImage = Classifier.classify(spark, features, model)("prediction")
//    println(Feature.toGeoJSONString(prediction))
//    println(prediction.collect.toList)
//    println(result.show)
    //    val model: PipelineModel = Classifier.randomForest().train(spark, trainingMap, labelCol = 4)
    //layers input layer of size, intermediate layers of size, output layer of size
//    val model: PipelineModel = Classifier.multilayerPerceptronClassifier(Array(4, 8, 16, 7), maxIter = 5).train(spark, features, label, labelCol = 4)
    //    val classifier: Classifier = Classifier.logisticRegression(maxIter = 10, tol = 1E-6, fitIntercept =true)
    //    val model: PipelineModel = Classifier.oneVsRest(classifier).train(spark, features, label2, labelCol = 4)

//    val prediction: RDDImage = Classifier.classify(spark, coverage, model)("prediction")  //会把coverage中的所有列当作特征
        val prediction = Classifier.classify(spark, feature, model, List("feature1", "feature2", "feature3"))  //会把coverage中的所有列当作特征
    println(prediction.collect.toList)
//    val metric = Evaluator.multiclassClassificationEvaluator_feature(spark, feature, prediction, "class")
//    println(metric)
    //    saveRasterRDDToTif(coverage,"C:\\Users\\马楚瑞\\Desktop\\OGE\\解决模型保存与加载问题\\coverage2_0116.tiff")
//    saveRasterRDDToTif(prediction,"C:\\Users\\马楚瑞\\Desktop\\OGE\\解决模型保存与加载问题\\prediction_0116.tiff")
//    println(Evaluator.multiclassClassificationEvaluator(spark, label, prediction, metricName = List("f1"), 4))
    //    sc.stop()

  }


}

