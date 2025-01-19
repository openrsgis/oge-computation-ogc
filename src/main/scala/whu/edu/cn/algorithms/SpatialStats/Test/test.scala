package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.{DenseMatrix, DenseVector, norm, normalize}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Point}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics, PrincipalComponentAnalysis, RipleysK}
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis.corrMat
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation._
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.{LinearRegression, LogisticRegression, PoissonRegression, SpatialDurbinModel, SpatialErrorModel, SpatialLagModel}
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWRbasic
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWDA
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWAverage
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWCorrelation
import whu.edu.cn.algorithms.SpatialStats.GWModels.GTWR
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.STSampling.Sampling.{randomSampling, regularSampling, stratifiedSampling}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.STSampling.SandwichSampling
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.Kriging.{OrdinaryKriging, selfDefinedKriging}
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils
import whu.edu.cn.util.ShapeFileUtil._
import breeze.linalg.{norm, normalize}
import breeze.numerics._


object test {
  //global variables
  val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
      .set("spark.testing.memory", "512000000")
  val sc = new SparkContext(conf)
  val encode="utf-8"

  val shpPath: String = "src/main/scala/whu/edu/cn/algorithms/SpatialStats/Test/testdata/cn_aging.shp"
  val shpfile = readShp(sc, shpPath, encode)

  val shpPath2: String = "src/main/scala/whu/edu/cn/algorithms/SpatialStats/Test/testdata/points.shp"
  val shpfile2 = readShp(sc, shpPath2, encode)

  val shpPath3: String = "src/main/scala/whu/edu/cn/algorithms/SpatialStats/Test/testdata/LNHP100.shp"
  val shpfile3 = readShp(sc, shpPath3, encode)

  val csvpath = "src/main/scala/whu/edu/cn/algorithms/SpatialStats/Test/testdata/test_aqi.csv"
  val csvdata = readcsv(sc, csvpath)

  def main(args: Array[String]): Unit = {

    val t1 = System.currentTimeMillis()

    //    AverageNearestNeighbor.result(shpfile)
    //    DescriptiveStatistics.describe(shpfile)
    //    RipleysK.ripley(shpfile)
    //    PrincipalComponentAnalysis.PCA(shpfile,"aging,GDP,pop,GI,sci_tech,education,revenue",keep = 2,is_scale =true)

    //    GWRbasic.auto(sc, shpfile, "aging", "PCGDP,GI,FD,TS,CL,PCD,PIP,SIP,TIP,education", kernel = "bisquare")
    //    GWRbasic.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education", 50, adaptive = true)
    //    GWRbasic.autoFit(sc, shpfile, "aging", "PCGDP,GI,FD,education",approach = "AICc", adaptive = true)
    //    GTWR.fit(sc,shpfile3,"PURCHASE","FLOORSZ,UNEMPLOY,PROF","TYPEDETCH", bandwidth=40,adaptive=true, lambda = 0.5)
    //    GTWR.autoFit(sc,shpfile3,"PURCHASE","FLOORSZ,UNEMPLOY,PROF","TYPEDETCH", adaptive=true, lambda = 0.5)
    //    GWDA.calculate(sc,shpfile3,"TYPEDETCH","FLOORSZ,UNEMPLOY,PROF",kernel = "bisquare",method = "wlda")
    //    GWCorrelation.cal(sc, shpfile, "aging", "GDP,pop", bw=20, kernel = "bisquare", adaptive = true)
    //    GWAverage.cal(sc, shpfile, "aging", "GDP,pop", 50)
    //    testGWRpredict()

    //    val ras=OrdinaryKriging(sc,shpfile2,"z",10,10)
    //    interpolationUtils.makeTiff(ras,"src/main/scala/whu/edu/cn/algorithms/SpatialStats/Test/testdata/","kriging")
    //    selfDefinedKriging(sc,shpfile2,"z",10,10,"Sph",0.1,0.1,0.1)

    //    LinearRegression.fit(sc, shpfile3,y="PURCHASE", x="FLOORSZ,PROF,UNEMPLOY")
    //    LogisticRegression.fit(sc, shpfile3,y="TYPEFLAT", x="FLOORSZ,PROF,UNEMPLOY")
    //    PoissonRegression.fit(sc, shpfile3,y="PURCHASE", x="FLOORSZ,PROF,UNEMPLOY",Intercept = true)
    //    SpatialLagModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")
    //    SpatialErrorModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")
    //    SpatialDurbinModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")

    //    SpatialAutoCorrelation.globalMoranI(shpfile, "aging", plot = false, test = true)
    //    SpatialAutoCorrelation.localMoranI(sc, shpfile, "aging")
    //    TemporalAutoCorrelation.ACF(shpfile, "aging", 20)
    //    CorrelationAnalysis.corrMat(shpfile, "aging,GDP,pop,GI,sci_tech,education,revenue", method = "spearman")
    //    acf_test()

    //    println(Geodetector.factorDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,pop,city,employee"))
    //    println(Geodetector.interactionDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,pop,city,employee"))
    //    println(Geodetector.ecologicalDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,pop,city,employee"))
    //    println(Geodetector.riskDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,pop,city,employee"))

    //    val rddSample=SandwichSampling.sampling(sc, shpfile3,"PURCHASE", "FLOORSZ", "TYPEDETCH")
    //    rddSample.foreach(println)
    //    randomSampling(sc, shpfile2)
    //    regularSampling(sc, shpfile2)
    //    stratifiedSampling(sc, shpfile2, "aging")

    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
    sc.stop()
  }

  def testGWRpredict()= {
    val newRDD = shpfile3.map { case (id, (geometry, attributes)) =>
      val centroid = geometry.getCentroid()
      val newCoordinate = new Coordinate(centroid.getX + 500, centroid.getY + 500)
      val newPoint = geometry match {
        case point: Point => centroid.getFactory.createPoint(newCoordinate)
        case _ =>
          geometry
      }
      (id, (newPoint, attributes))
    }
    GWRbasic.predict(sc, shpfile3, newRDD, "PURCHASE", "FLOORSZ,UNEMPLOY,PROF",bandwidth = 50, kernel = "gaussian", adaptive = true)
  }

  //这个是要写一个日期类型数据间隔计算的（甚至可以用到gtwr里面），现在的计算是单纯的数字相加减，希望支持yyyymmdd这些类型的
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
    //    tem_acf.foreach(println)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

}