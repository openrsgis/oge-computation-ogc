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


object Evaluator {
  //TODO 后面看是否要更改actual和predicted为List[String] 多标签使用MultilabelClassificationEvaluator
  def multiclassClassificationEvaluator(spark: SparkSession, labelCoverage: RDDImage, predictionCoverage: RDDImage, metricName: List[String] = List("f1"), labelBandIndex: Int = 0, predictionBandIndex: Int = 0, metricLabel: Double = 0): List[Double] = {
    val labelRows: Int = findSpatialKeyMinMax(labelCoverage)._1 //瓦片行数
    val labelCols: Int = findSpatialKeyMinMax(labelCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    if(labelRows != predictionRows || labelCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val labelRdd: RDD[((Int, Int, Int, Int), Double)] = labelCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(labelBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), Double)] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(predictionBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (Double, Double))] = labelRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble, t._2._1, t._2._2)
      Row(list:_*)
    })
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
    val featuresRows: Int = findSpatialKeyMinMax(featuresCoverage)._1 //瓦片行数
    val featuresCols: Int = findSpatialKeyMinMax(featuresCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    val featuresBandCount: Int = featuresCoverage._1.first()._2.bandCount
    if(featuresRows != predictionRows || featuresCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val featuresRdd: RDD[((Int, Int, Int, Int), List[Double])] = featuresCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), List[Double])] = ListBuffer.empty[((Int, Int, Int, Int), List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        val featuresList: ListBuffer[Double] = ListBuffer.empty[Double]
        for(index <- 0 until featuresBandCount){
          featuresList.append(t._2.band(index).getDouble(j, i))
        }
        list.append(((row, col, i, j), featuresList.toList))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), Double)] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(predictionBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (List[Double], Double))] = featuresRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble) ::: t._2._1 ::: List(t._2._2)
      Row(list:_*)
    })
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
    val labelsRows: Int = findSpatialKeyMinMax(labelsCoverage)._1 //瓦片行数
    val labelsCols: Int = findSpatialKeyMinMax(labelsCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    if(labelsRows != predictionRows || labelsCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val labelsCoverageBandCount: Int = labelsCoverage._1.first()._2.bandCount
    val predictionCoverageBandCount: Int = predictionCoverage._1.first()._2.bandCount
    val labelsRdd: RDD[((Int, Int, Int, Int), List[Double])] = labelsCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), List[Double])] = ListBuffer.empty[((Int, Int, Int, Int), List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        val lalbelsList: ListBuffer[Double] = ListBuffer.empty[Double]
        for(index <- 0 until labelsCoverageBandCount){
          lalbelsList.append(t._2.band(index).getDouble(j, i))
        }
        list.append(((row, col, i, j), lalbelsList.toList))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), List[Double])] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), List[Double])] = ListBuffer.empty[((Int, Int, Int, Int), List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        val predictionList: ListBuffer[Double] = ListBuffer.empty[Double]
        for(index <- 0 until predictionCoverageBandCount){
          predictionList.append(t._2.band(index).getDouble(j, i))
        }
        list.append(((row, col, i, j), predictionList.toList))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (List[Double], List[Double]))] = labelsRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble) ::: t._2._1 ::: t._2._2
      Row(list:_*)
    })
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
    val labelRows: Int = findSpatialKeyMinMax(labelCoverage)._1 //瓦片行数
    val labelCols: Int = findSpatialKeyMinMax(labelCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    if(labelRows != predictionRows || labelCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val labelRdd: RDD[((Int, Int, Int, Int), Double)] = labelCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(labelBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), Double)] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(predictionBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (Double, Double))] = labelRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble, t._2._1, t._2._2)
      Row(list:_*)
    })
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
    val labelRows: Int = findSpatialKeyMinMax(labelCoverage)._1 //瓦片行数
    val labelCols: Int = findSpatialKeyMinMax(labelCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    if(labelRows != predictionRows || labelCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val labelRdd: RDD[((Int, Int, Int, Int), Double)] = labelCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(labelBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), Double)] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(predictionBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (Double, Double))] = labelRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble, t._2._1, t._2._2)
      Row(list:_*)
    })
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
    val labelRows: Int = findSpatialKeyMinMax(labelCoverage)._1 //瓦片行数
    val labelCols: Int = findSpatialKeyMinMax(labelCoverage)._2 //瓦片列数
    val predictionRows: Int = findSpatialKeyMinMax(predictionCoverage)._1 //瓦片行数
    val predictionCols: Int = findSpatialKeyMinMax(predictionCoverage)._2 //瓦片列数
    if(labelRows != predictionRows || labelCols != predictionCols) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val labelRdd: RDD[((Int, Int, Int, Int), Double)] = labelCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(labelBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val predictedRdd: RDD[((Int, Int, Int, Int), Double)] = predictionCoverage._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), Double)] = ListBuffer.empty[((Int, Int, Int, Int), Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        list.append(((row, col, i, j), t._2.band(predictionBandIndex).getDouble(j, i)))  //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //join两影像RDD
    val joinRdd: RDD[((Int, Int, Int, Int), (Double, Double))] = labelRdd.join(predictedRdd)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble, t._2._1, t._2._2)
      Row(list:_*)
    })
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
