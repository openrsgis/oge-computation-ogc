package whu.edu.cn.algorithms.MLlib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Model, evaluation}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import whu.edu.cn.algorithms.ImageProcess.core.MathTools.findSpatialKeyMinMax
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.oge.Coverage.selectBands

import scala.collection.mutable.ListBuffer
import whu.edu.cn.algorithms.MLlib.util.joinTwoCoverage


object Evaluator {
  //TODO 后面看是否要更改actual和predicted为List[String] 多标签使用MultilabelClassificationEvaluator
  def multiclassClassificationEvaluator(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0, metricLabel: Double = 0): List[Double] = {
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

  //聚类只有一个精度评估指标
  def clusteringEvaluator(spark: SparkSession, featuresCoverage: RDDImage, predictionCoverage: RDDImage, metricName: String = "silhouette", distanceMeasure: String = "squaredEuclidean", predictionBandIndex: Int = 0): Double = {
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

  //TODO 未检验 该函数与multiclassClassificationEvaluator的区别是：multilabelClassificationEvaluator可以有多个标签列而multiclassClassificationEvaluator只能有一个标签列
  def multilabelClassificationEvaluator(spark: SparkSession, labelsCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1Measure")): List[Double] = {
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

  def binaryClassificationEvaluator(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("areaUnderROC"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
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

  def regressionEvaluator(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("rmse"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
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

  //TODO 未检验
  def rankingEvaluator(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("meanAveragePrecision"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0): List[Double] = {
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



}
