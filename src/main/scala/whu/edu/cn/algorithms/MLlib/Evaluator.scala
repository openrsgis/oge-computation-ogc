package whu.edu.cn.algorithms.MLlib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Model, PipelineModel, evaluation}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, ByteType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.ImageProcess.core.MathTools.findSpatialKeyMinMax
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.oge.Coverage.selectBands

import scala.collection.mutable.ListBuffer
import whu.edu.cn.algorithms.MLlib.util.joinTwoCoverage
import whu.edu.cn.oge.Feature.geometry

import scala.collection.mutable
import scala.io.Source


object Evaluator {
  //TODO 后面看是否要更改actual和predicted为List[String] 多标签使用MultilabelClassificationEvaluator
  def multiclassClassificationEvaluator_coverage(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0, metricLabel: Double = 0): List[Double] = {
    val rowRdd: RDD[Row] = joinTwoCoverage(labelCoverage, predictionCoverage, List(labelBandIndex), List(predictionBandIndex))
    val fieldTypes = List.fill(6)(DoubleType)
    val colNames =List("label", "prediction")
    val locColNames = List("loc1","loc2","loc3","loc4") ::: colNames
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val classMetricList: List[String] = List("f1", "accuracy", "weightedPrecision", "weightedRecall", "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure", "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel", "recallByLabel", "fMeasureByLabel", "logLoss", "hammingLoss")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (classMetricList.contains(name)){
        val evaluator = new evaluation.MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricLabel(metricLabel)
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def multiclassClassificationEvaluator_feature(spark: SparkSession, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))],  labelCol: String = "label", metricName: List[String] = List("f1"), metricLabel: Double = 0): List[Double] = {
    val feature = predictionFeature.join(labelFeature).map(t => {
      t._2._2._2.get(labelCol) match {
        case Some(labelValue) =>
          t._2._1._2 += ("label" -> labelValue)  // 将 prediction 放入 map2
        case None =>
          println(s"labelFeature中不包含{$labelCol}列")  // 如果 map1 中没有 "prediction"
      }
      (t._1, t._2._1)
    })
    val colNames =List("UUID", "label", "prediction")
    val fieldTypes = StringType :: List.fill(2)(DoubleType)
    val fields = colNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val featuresList: List[Double] =  List("label", "prediction").map(key => t._2._2.get(key) match{
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException(s"Error: label或prediction列存在空值")
        case None => throw new IllegalArgumentException("Error: feature需要包含label和prediction属性")
      })
      val list: List[Any] = t._1 :: featuresList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(colNames:_*)
    val classMetricList: List[String] = List("f1", "accuracy", "weightedPrecision", "weightedRecall", "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure", "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel", "recallByLabel", "fMeasureByLabel", "logLoss", "hammingLoss")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (classMetricList.contains(name)){
        val evaluator = new evaluation.MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricLabel(metricLabel)
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }

  //聚类只有一个精度评估指标
  def clusteringEvaluator_coverage(spark: SparkSession, featuresCoverage: RDDImage, predictionCoverage: RDDImage, metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean", predictionBandIndex: Int = 0): Double = {
    val featuresBandCount: Int = featuresCoverage._1.first()._2.bandCount
    val featuresBandList = List.range(0, featuresBandCount)
    val rowRdd: RDD[Row] = joinTwoCoverage(featuresCoverage, predictionCoverage, featuresBandList, List(predictionBandIndex))
    val fieldTypes = List.fill(4+featuresBandCount+1)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to featuresBandCount) {
      colNames.append(s"feature$i")
    }
    val locColNames = List("loc1","loc2","loc3","loc4") ::: colNames.toList ::: List("prediction")
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)

    val clusterMetricList: List[String] = List("silhouette")
    val evaluator =
      if (clusterMetricList.contains(metricName)){
        new evaluation.ClusteringEvaluator()
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setMetricName(metricName)
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$metricName！")
    evaluator.evaluate(assembledDF)
  }
  def clusteringEvaluator_feature(spark: SparkSession, feature: RDD[(String, (Geometry, mutable.Map[String, Any]))], featuresColNames: List[String], metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean"): Double = {
    //设置字段类型
    val trueColNames = List("UUID") ::: List("prediction") ::: featuresColNames
    val fieldTypes = StringType :: List.fill(1+featuresColNames.size)(DoubleType)// ::: List(StringType)
    val fields = trueColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val featuresList: List[Double] =  featuresColNames.map(key => t._2._2.get(key) match{
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException(s"Error: feature特征列{$key}的值类型不是数值类型或暂不支持")
        case None => throw new IllegalArgumentException(s"Error: feature特征列{$key}存在空值")
      }) //TODO 暂不支持特征为非数值类型
      val prediction: Double = t._2._2.get("prediction") match {
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException("Error: feature的prediction列的值类型不是数值类型或暂不支持")
        case None => throw new IllegalArgumentException("Error: feature需要包含prediction属性")
      }
      val list: List[Any] = List(t._1)::: List(prediction) ::: featuresList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(trueColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(featuresColNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assemblerDF =assembler.transform(df)

    val clusterMetricList: List[String] = List("silhouette")
    val evaluator =
      if (clusterMetricList.contains(metricName)){
        new evaluation.ClusteringEvaluator()
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setMetricName(metricName)
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$metricName！")
    evaluator.evaluate(assemblerDF)
  }

  //TODO 未检验 该函数与multiclassClassificationEvaluator的区别是：multilabelClassificationEvaluator可以有多个标签列而multiclassClassificationEvaluator只能有一个标签列
  def multilabelClassificationEvaluator_coverage(spark: SparkSession, labelsCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1Measure")): List[Double] = {
    val labelsCoverageBandCount: Int = labelsCoverage._1.first()._2.bandCount
    val predictionCoverageBandCount: Int = predictionCoverage._1.first()._2.bandCount
    val labelsBandList = List.range(0, labelsCoverageBandCount)
    val predictionBandList = List.range(0, predictionCoverageBandCount)
    val rowRdd: RDD[Row] = joinTwoCoverage(labelsCoverage, predictionCoverage, labelsBandList, predictionBandList)
    val fieldTypes = List.fill(4+labelsCoverageBandCount+predictionCoverageBandCount)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to labelsCoverageBandCount) {
      colNames.append(s"labele$i")
    }
    for (i <- 1 to predictionCoverageBandCount) {
      colNames.append(s"prediction$i")
    }
    val locColNames = List("loc1","loc2","loc3","loc4") ::: colNames.toList
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val assembler1 = new VectorAssembler()
      .setInputCols(colNames.slice(0,labelsCoverageBandCount).toArray)
      .setOutputCol("labels")
      .setHandleInvalid("skip")
    val assembler2 = new VectorAssembler()
      .setInputCols(colNames.slice(labelsCoverageBandCount,colNames.length).toArray)
      .setOutputCol("prediction")
      .setHandleInvalid("skip")
    val assembledDF1 = assembler1.transform(df)
    val assembledDF = assembler2.transform(assembledDF1)
    val multilabelMetricList: List[String] = List("f1Measure", "subsetAccuracy", "accuracy", "hammingLoss", "precision", "recall", "precisionByLabel", "recallByLabel", "f1MeasureByLabel", "microPrecision", "microRecall", "microF1Measure")
    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (multilabelMetricList.contains(name)){
        val evaluator = new evaluation.MultilabelClassificationEvaluator()
          .setLabelCol("labels")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(assembledDF))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def multilabelClassificationEvaluator_feature(spark: SparkSession, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], labelColNames: List[String], predictionColNames: List[String], metricName: List[String] = List("f1Measure")): List[Double] = {
    val feature: RDD[(String, (Geometry, mutable.Map[String, Any]))] = predictionFeature.join(labelFeature).map(t => {
      labelColNames.foreach{ key =>
        t._2._2._2.get(key) match {
          case Some(labelValue) =>
            t._2._1._2 += (key -> labelValue)  // 将 prediction 放入 map2
          case None =>
            println(s"labelFeature中不包含{$key}列")  // 如果 map1 中没有 "prediction"
        }
      }
      (t._1, t._2._1)
    })
    //设置字段类型
    val trueColNames = List("UUID") ::: labelColNames ::: predictionColNames
    val fieldTypes = StringType :: List.fill(labelColNames.size+predictionColNames.size)(DoubleType)// ::: List(StringType)
    val fields = trueColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val labelList: List[Double] =  labelColNames.map(key => t._2._2.get(key) match {
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException(s"Error: label列存在空值")
        case None => throw new IllegalArgumentException("Error: labelFeature需要包含label属性")
      })
      val predictionList: List[Double] = predictionColNames.map(key => t._2._2.get(key) match{
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException(s"Error: prediction列存在空值")
        case None => throw new IllegalArgumentException("Error: predictionFeature需要包含prediction属性")
      })
      val list: List[Any] = List(t._1)::: labelList ::: predictionList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(trueColNames: _*)
    val assembler1 = new VectorAssembler()
      .setInputCols(labelColNames.toArray)
      .setOutputCol("labels")
      .setHandleInvalid("skip")
    val assembler2 = new VectorAssembler()
      .setInputCols(predictionColNames.toArray)
      .setOutputCol("prediction")
      .setHandleInvalid("skip")
    val assembledDF1 = assembler1.transform(df)
    val assembledDF = assembler2.transform(assembledDF1)

    val multilabelMetricList: List[String] = List("f1Measure", "subsetAccuracy", "accuracy", "hammingLoss", "precision", "recall", "precisionByLabel", "recallByLabel", "f1MeasureByLabel", "microPrecision", "microRecall", "microF1Measure")
    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (multilabelMetricList.contains(name)){
        val evaluator = new evaluation.MultilabelClassificationEvaluator()
          .setLabelCol("labels")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(assembledDF))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }


  def binaryClassificationEvaluator_coverage(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("areaUnderROC"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
    val rowRdd: RDD[Row] = joinTwoCoverage(labelCoverage, predictionCoverage, List(labelBandIndex), List(predictionBandIndex))
    val fieldTypes = List.fill(4+2)(DoubleType)
    val locColNames = List("loc1","loc2","loc3","loc4") ::: List("label") ::: List("prediction")
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val binaryMetricList: List[String] = List("areaUnderROC", "areaUnderPR")
    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (binaryMetricList.contains(name)){
        val evaluator = new evaluation.BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setRawPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def binaryClassificationEvaluator_feature(spark: SparkSession, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))],  labelCol: String = "label", metricName: List[String] = List("areaUnderROC")): List[Double] = {
    val feature = predictionFeature.join(labelFeature).map(t => {
      t._2._2._2.get(labelCol) match {
        case Some(labelValue) =>
          t._2._1._2 += ("label" -> labelValue)  // 将 prediction 放入 map2
        case None =>
          println(s"labelFeature中不包含{$labelCol}列")  // 如果 map1 中没有 "prediction"
      }
      (t._1, t._2._1)
    })
    val colNames =List("UUID", "label", "prediction")
    val fieldTypes = List.fill(3)(StringType)
    val fields = colNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val featuresList: List[String] =  List("label", "prediction").map(key => t._2._2.get(key) match{
        case Some(value) => value.toString
        case None => throw new IllegalArgumentException("Error: feature需要包含label和prediction属性")
      })
      val list: List[Any] = t._1 :: featuresList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(colNames:_*)

    val binaryMetricList: List[String] = List("areaUnderROC", "areaUnderPR")
    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (binaryMetricList.contains(name)){
        val evaluator = new evaluation.BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setRawPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }

  def regressionEvaluator_coverage(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("rmse"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
    val rowRdd: RDD[Row] = joinTwoCoverage(labelCoverage, predictionCoverage, List(labelBandIndex), List(predictionBandIndex))
    val fieldTypes = List.fill(6)(DoubleType)
    val colNames =List("label", "prediction")
    val locColNames = List("loc1","loc2","loc3","loc4") ::: colNames
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val regressMetricList: List[String] = List("rmse", "mse", "r2", "mae", "var")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (regressMetricList.contains(name)){
        val evaluator = new evaluation.RegressionEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def regressionEvaluator_feature(spark: SparkSession, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))],  labelCol: String = "label", metricName: List[String] = List("rmse")): List[Double] = {
    val feature = predictionFeature.join(labelFeature).map(t => {
      t._2._2._2.get(labelCol) match {
        case Some(labelValue) =>
          t._2._1._2 += ("label" -> labelValue)  // 将 prediction 放入 map2
        case None =>
          println(s"labelFeature中不包含{$labelCol}列")  // 如果 map1 中没有 "prediction"
      }
      (t._1, t._2._1)
    })
    val colNames =List("UUID", "label", "prediction")
    val fieldTypes = StringType :: List.fill(2)(DoubleType)
    val fields = colNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val featuresList: List[Double] =  List("label", "prediction").map(key => t._2._2.get(key) match{
        case Some(value: Int) => value.toDouble
        case Some(value: Double) => value
        case Some(value: Number) => value.doubleValue()
        case Some(_) => throw new IllegalArgumentException(s"Error: label或prediction列存在空值")
        case None => throw new IllegalArgumentException("Error: feature需要包含label和prediction属性")
      })
      val list: List[Any] = t._1 :: featuresList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(colNames:_*)
    val regressMetricList: List[String] = List("rmse", "mse", "r2", "mae", "var")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (regressMetricList.contains(name)){
        val evaluator = new evaluation.RegressionEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }

  //TODO 未检验
  def rankingEvaluator_coverage(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("meanAveragePrecision"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
    val rowRdd: RDD[Row] = joinTwoCoverage(labelCoverage, predictionCoverage, List(labelBandIndex), List(predictionBandIndex))
    val fieldTypes = List.fill(6)(DoubleType)
    val colNames =List("label", "prediction")
    val locColNames = List("loc1","loc2","loc3","loc4") ::: colNames
    val fields = locColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(locColNames:_*)
    val rankMetricList: List[String] = List("meanAveragePrecision", "meanAveragePrecisionAtK", "precisionAtK", "ndcgAtK", "recallAtK")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (rankMetricList.contains(name)){
        val evaluator = new evaluation.MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def rankingEvaluator_feature(spark: SparkSession, labelFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))], predictionFeature: RDD[(String, (Geometry, mutable.Map[String, Any]))],  labelCol: String = "label", metricName: List[String] = List("meanAveragePrecision")): List[Double] = {
    val feature = predictionFeature.join(labelFeature).map(t => {
      t._2._2._2.get(labelCol) match {
        case Some(labelValue) =>
          t._2._1._2 += ("label" -> labelValue)  // 将 prediction 放入 map2
        case None =>
          println(s"labelFeature中不包含{$labelCol}列")  // 如果 map1 中没有 "prediction"
      }
      (t._1, t._2._1)
    })
    val colNames =List("UUID", "label", "prediction")
    val fieldTypes = List.fill(3)(StringType)
    val fields = colNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val rowRdd: RDD[Row] = feature.map(t => {
      val featuresList: List[String] =  List("label", "prediction").map(key => t._2._2.get(key) match{
        case Some(value) => value.toString
        case None => throw new IllegalArgumentException("Error: feature需要包含label和prediction属性")
      })
      val list: List[Any] = t._1 :: featuresList
      Row(list:_*)
    })
    val df = spark.createDataFrame(rowRdd, schema).toDF(colNames:_*)
    val rankMetricList: List[String] = List("meanAveragePrecision", "meanAveragePrecisionAtK", "precisionAtK", "ndcgAtK", "recallAtK")

    val accuracyList: ListBuffer[Double] = ListBuffer.empty[Double]
    for (name <- metricName){
      if (rankMetricList.contains(name)){
        val evaluator = new evaluation.MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName(name)
        accuracyList.append(evaluator.evaluate(df))
      }
      else throw new IllegalArgumentException(s"metricName中包含不支持的精度评估方法$name！")
    }
    accuracyList.toList
  }
  def main(args: Array[String]): Unit = {
    import whu.edu.cn.oge.Coverage
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate() //能不能和SparkContext同时创建？放到自己的函数里创建结束时调用.stop把它停掉
    val coverage: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\马楚瑞\\Downloads\\GF6_WFV_E115.8_N40.2_20230815_L1A1420343042-2\\GF6_WFV_E115.8_N40.2_20230815_L1A1420343042-2_B1.tif")

    //    val temp = Source.fromFile("C:/Users/马楚瑞/Desktop/OGE/解决模型保存与加载问题/featureCollection2.geojson").mkString
    val temp = Source.fromFile("C:/Users/马楚瑞/Desktop/OGE/解决模型保存与加载问题/featureCollection2.geojson").mkString
    val feature = geometry(sc, temp, "EPSG:4326")
    val featureResult = Coverage.sampleRegions(coverage, feature, List("class"))
    val model = Clusterer.kMeans().train(spark, feature, List("band1", "band2", "band3"))
    val prediction = Clusterer.cluster(spark, feature, model, List("band1", "band2", "band3"))
    val metric = Evaluator.clusteringEvaluator_feature(spark, prediction, List("band1", "band2", "band3"))
    println(metric)
    //    sc.stop()

  }


}
