package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.AverageNearestNeighbor.aveNearestNeighbor
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.DescriptiveStatistics.describe
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis._
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.SpatialAutoCorrelation._
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation._
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

  val shpPath: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\LNHP.shp"
  val shpfile = readShp(sc, shpPath, encode)

  val shpPath2: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\MississippiHR.shp"
  val shpfile2 = readShp(sc, shpPath2, encode)

  val csvpath = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\test_aqi.csv"
  val csvdata = readcsv(sc, csvpath)
  //写成无参数的函数形式来进行测试，方便区分，以后可以改成 catch...if... 形式

  def main(args: Array[String]): Unit = {

        descriptive_test()
        sarmodel_test()
        morani_test()
        acf_test()
        linear_test()
        correlation_test()
    gwrbasic_test()
    sc.stop()
  }

  def gwrbasic_test(): Unit = {
    val t1 = System.currentTimeMillis()
    val mdl = new GWRbasic
    mdl.init(shpfile)
    mdl.setX("FLOORSZ,PROF")
    mdl.setY("PURCHASE")
//    val re=mdl.fit(bw = 10000,kernel="bisquare",adaptive = false)
//    val bw=mdl.bandwidthSelection(adaptive = false)
//    mdl.fit(bw = bw,kernel="gaussian",adaptive = false)
    mdl.auto(kernel="gaussian",approach = "CV", adaptive = false)
//    val re_rdd=sc.makeRDD(re)
//    writeshpfile(re_rdd,"D:\\Java\\testdata\\re_gwr.shp")
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def correlation_test(): Unit = {
    val t0 = System.currentTimeMillis()
    val s = Array[String]("PROF", "FLOORSZ", "UNEMPLOY", "PURCHASE")
    val mat = corrMat(shpfile, s)
    val tused = (System.currentTimeMillis() - t0) / 1000.0
    println(s"time used is $tused s")
  }

  def descriptive_test(): Unit = {
    aveNearestNeighbor(shpfile)
    val list_double: List[Double] = getNumber(shpfile, "PURCHASE")
    val list_rdd: RDD[Double] = sc.makeRDD(list_double)
    describe(list_rdd, list_double, 10)
  }

  def sarmodel_test(): Unit = {
    val t1 = System.currentTimeMillis()
    val mdl = new SpatialDurbinModel//error，lag
    mdl.init(shpfile2)
    mdl.setX("PO60,UE60")
    mdl.setY("HR60")
    mdl.fit()
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def morani_test(): Unit = {
    val t1 = System.currentTimeMillis()
    val globali = globalMoranI(shpfile2, "HR60", plot = true, test = true)
    println(s"global Moran's I is: ${globali._1}")
    val locali = localMoranI(shpfile2, "HR60")
    println("-----------local moran's I--------------")
    locali._1.foreach(println)
    println("-----------p-value--------------")
    locali._5.foreach(println)
    //    val result1 = writeRDD(sc, shpfile, locali._1, "moran_i")
    //    val result2 = writeRDD(sc, result1, locali._2, "expect")
    //    val outputpath = "testdata\\MississippiMoranI.shp"
    //    writeshpfile(result2, outputpath)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def acf_test(): Unit = {
    val t1 = System.currentTimeMillis()
    //test date calculator
    /*
    val timep = attributeSelectHead(csvdata, "time_point")
    val timepattern = "yyyy/MM/dd"
    val date = timep.map(t => {
      val date = new SimpleDateFormat(timepattern).parse(t)
      date
    })
    date.foreach(println)
    println((date(300).getTime - date(0).getTime) / 1000 / 60 / 60 / 24)
     */
    val tem = attributeSelectHead(csvdata, "temperature")
    val db_tem = tem.map(t => t.toDouble)
    //    println(db_tem.sum)
    val tem_acf = timeSeriesACF(db_tem, 30)
    tem_acf.foreach(println)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def linear_test(): Unit = {
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