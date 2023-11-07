package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.AverageNearestNeighbor
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.DescriptiveStatistics
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.SpatialAutoCorrelation
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LinearRegression.linearRegression
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.{SpatialDurbinModel, SpatialErrorModel, SpatialLagModel}
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWRbasic
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._

object test {
  //global variables
  val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
  val sc = new SparkContext(conf)
  val encode="utf-8"

  def main(args: Array[String]): Unit = {
    val shpPath: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\LNHP.shp"
    val shpfile = readShp(sc, shpPath, encode)

    val shpPath2: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\MississippiHR.shp"
    val shpfile2 = readShp(sc, shpPath2, encode)

    AverageNearestNeighbor.result(shpfile)
    DescriptiveStatistics.result(shpfile, "FLOORSZ")
    CorrelationAnalysis.corrMat(shpfile,"PROF,FLOORSZ,UNEMPLOY,PURCHASE")
    SpatialAutoCorrelation.globalMoranI(shpfile2, "HR60", plot = true, test = true)
    SpatialAutoCorrelation.localMoranI(shpfile2, "HR60")
    TemporalAutoCorrelation.ACF(shpfile, "FLOORSZ", 30)
    GWRbasic.fit(sc, shpfile, "PURCHASE", "FLOORSZ,PROF", 80, adaptive = true)
    GWRbasic.auto(sc, shpfile, "PURCHASE", "FLOORSZ,PROF,UNEMPLOY,CENTHEAT,BLD90S,TYPEDETCH", kernel = "bisquare")
    GWRbasic.autoFit(sc, shpfile, "PURCHASE", "FLOORSZ,PROF,UNEMPLOY",approach = "CV", adaptive = true)
    SpatialLagModel.fit(sc, shpfile2, "HR60", "PO60,UE60")
    SpatialErrorModel.fit(sc, shpfile2, "HR60", "PO60,UE60")
    SpatialDurbinModel.fit(sc, shpfile2, "HR60", "PO60,UE60")
    sc.stop()
  }

  def linear_test(): Unit = {
    val csvpath = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\test_aqi.csv"
    val csvdata = readcsv(sc, csvpath)
    val t1 = System.currentTimeMillis()
    val aqi = attributeSelectNum(csvdata, 2).map(t => t.toDouble)
    val per = attributeSelectHead(csvdata, "precipitation").map(t => t.toDouble)
    val tem = attributeSelectHead(csvdata, "temperature").map(t => t.toDouble)
    val x = Array(DenseVector(tem), DenseVector(per))
//    val re = linearRegression(x, DenseVector(aqi))
//    println(re._1)
//    println(re._2)
//    println(re._3)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

}