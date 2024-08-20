package whu.edu.cn.algorithms.MLlib
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml._
import org.apache.spark.ml.clustering
import org.apache.spark.sql.{DataFrame, SparkSession}
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.MLlib.util.{makeRasterDataFrameFromRDD, makeRasterRDDFromDataFrame}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import scala.util.Random



class Clusterer(ml: Estimator[_]){
  def train(spark: SparkSession, featuresCoverage: RDDImage): Model[_] = {
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, featuresCoverage)
    this.ml.fit(df).asInstanceOf[Model[_]]
  }
  def train(spark: SparkSession, featuresCoverage: RDDImage, featuresCol: List[Int]): Model[_] = {
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, featuresCoverage, featuresCol)
    this.ml.fit(df).asInstanceOf[Model[_]]
  }
}
object Clusterer {
  def kMeans(k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): Clusterer ={
    new Clusterer(new clustering.KMeans()
    .setK(k)
    .setMaxIter(maxIter)
    .setSeed(seed)
    .setTol(tol))
  }
  def latentDirichletAllocation(checkpointInterval: Int = 10, k: Int = 2, maxIter: Int = 10, optimizer: String = "online", seed: Long = Random.nextLong(), subsamplingRate: Double = 0.05, topicConcentration: Double = -1): Clusterer = {
    //TODO subsamplingRate fit for "online" only
    val newTopicConcentration: Double =
    if(topicConcentration == -1){
      if(optimizer=="online") 1.0
      else if(optimizer=="em") 1.0/k
      else throw new IllegalArgumentException("不支持当前optimizer参数！")
    }
    else topicConcentration
    new Clusterer(new clustering.LDA()
    .setCheckpointInterval(checkpointInterval)
    .setK(k)
    .setMaxIter(maxIter)
    .setOptimizer(optimizer)
    .setSeed(seed)
    .setSubsamplingRate(subsamplingRate)

//    .setTopicConcentration(newTopicConcentration)
    )
  }
  def bisectingKMeans(distanceMeasure: String = "euclidean", k: Int = 4, maxIter: Int = 10, seed: Long = Random.nextLong()): Clusterer = {
    new Clusterer(new clustering.BisectingKMeans()
    .setDistanceMeasure(distanceMeasure)
    .setK(k)
    .setMaxIter(maxIter)
    .setSeed(seed))
  }
  def gaussianMixture(k: Int = 2, maxIter: Int = 10, seed: Long = Random.nextLong(), tol: Double = 1E-6): Clusterer = {
    new Clusterer(new clustering.GaussianMixture()
    .setK(k)
    .setMaxIter(maxIter)
    .setSeed(seed)
    .setTol(tol))
  }
  //TODO PIC model类型和别的model都不一样，调用模型使用的函数是assignClusters也和别的都不一样，后面再处理
//  def powerIterationClustering(k: Int = 2, maxIter: Int = 10, initMode: String = "degree"): Clusterer = {
//    new Clusterer(new clustering.PowerIterationClustering()
//    .setK(k)
//    .setMaxIter(maxIter)
//    .setInitMode(initMode)
//    )
//  }

  def cluster(implicit spark: SparkSession, coverage: RDDImage, model: Model[_]): Map[String, RDDImage] = {
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage)
    val predictionDF: DataFrame = model.transform(df)
    var map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    map += ("features" -> coverage)
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictionCol=true, predictedLabelCol=false), coverage._2)) //TODO coverage._2应该还需要改下，不能完全照搬
    if(predictionDF.columns.contains("probability")) map += ("probability" -> (makeRasterRDDFromDataFrame(predictionDF, probabilityCol=true, predictedLabelCol=false), coverage._2))
    if(predictionDF.columns.contains("topicDistribution")) map += ("topicDistribution" -> (makeRasterRDDFromDataFrame(predictionDF, topicDistributionCol=true, predictedLabelCol=false), coverage._2))
    map
  }
  def cluster(implicit spark: SparkSession, coverage: RDDImage, model: Model[_], featuresCol: List[Int]): Map[String, RDDImage] = {
    val df: DataFrame = makeRasterDataFrameFromRDD(spark, coverage, featuresCol)
    val predictionDF: DataFrame = model.transform(df)
    var map: Map[String,RDDImage] = Map.empty[String,RDDImage]
    if(predictionDF.columns.contains("features")) map += ("features" -> (makeRasterRDDFromDataFrame(predictionDF, featuresCol = true, predictedLabelCol=false), coverage._2))
    if(predictionDF.columns.contains("prediction")) map += ("prediction" -> (makeRasterRDDFromDataFrame(predictionDF, predictionCol=true, predictedLabelCol=false), coverage._2)) //TODO coverage._2应该还需要改下，不能完全照搬
    if(predictionDF.columns.contains("probability")) map += ("probability" -> (makeRasterRDDFromDataFrame(predictionDF, probabilityCol=true, predictedLabelCol=false), coverage._2))
    if(predictionDF.columns.contains("topicDistribution")) map += ("topicDistribution" -> (makeRasterRDDFromDataFrame(predictionDF, topicDistributionCol=true, predictedLabelCol=false), coverage._2))
    map
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate() //TODO ?

    val features: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\毕设\\应用_监督分类结果\\RGB_Mean.tif")

    val model: Model[_] = Clusterer.latentDirichletAllocation(maxIter = 1).train(spark, features)
    val prediction: RDDImage = Clusterer.cluster(spark, features, model)("topicDistribution")  //会把coverage中的所有列当作特征
    saveRasterRDDToTif(prediction,"C:\\Users\\HUAWEI\\Desktop\\oge\\coverage_resources1\\MLlib_LDA_0729.tiff")

//    val df: DataFrame = makeRasterDataFrameFromRDD(spark, features)
//    val predictionDF: DataFrame = model.transform(df)
////    predictionDF.show(5)
//    (makeRasterRDDFromDataFrame(predictionDF, probabilityCol=true, predictedLabelCol=false), features._2)
//    saveRasterRDDToTif(prediction,"C:\\Users\\HUAWEI\\Desktop\\oge\\coverage_resources1\\MLlib_LDA_0729.tiff")


    //    sc.stop()
  }
}
