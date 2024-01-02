package whu.edu.cn.algorithms.SpatialStats.BasicStatistics
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.{pow, sqrt}

//whm
object AverageNearestNeighbor {

  /**
   * 输入RDD，计算要素最小外接矩形面积
   *
   * @param testshp RDD
   * @return Double 返回要素最小外接矩形面积
   */
  //要素的最小外接矩形面积
  def ExRectangularArea(testshp: RDD[(String, (Geometry, Map[String, Any]))]) : Double = {
    val coorxy : Array[(Double, Double)] = getCoorXY(testshp: RDD[(String, (Geometry, Map[String, Any]))])
    val coorx_min : Double = coorxy.map{case t => t._1}.min
    val coory_min : Double = coorxy.map{case t => t._2}.min
    val coorx_max : Double = coorxy.map{case t => t._1}.max
    val coory_max : Double = coorxy.map{case t => t._2}.max
    val Area : Double = (coorx_max - coorx_min) * (coory_max - coory_min)
    Area
  }

  /**
   * 输入RDD计算平均最近邻指数（ANN），返回相关计算结果
   *
   * @param featureRDD   RDD
   * @return String形式存储计算结果
   */
  //平均最近邻指数算子
  def  result(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): String={
    val DisArray = getDist(featureRDD)
    val DisSum = DisArray.map(t => t.sorted.apply(1)).sum //t指每一行，取出RDD矩阵每行最小距离，求和 欧式距离矩阵
    val RDDsize =  DisArray.length //RDD要素个数，同length
    val A  = ExRectangularArea(featureRDD)
    val Do = DisSum/RDDsize  //平均观测距离（Observed Mean Distance）
    val De = 0.5/(sqrt(RDDsize/A))  //预期平均距离 A为研究区域面积，要素的外接矩形
    val ANN = Do/De  //平均最近邻指数  ANN>1离散  ANN<1聚集
    val SE = 0.26136/(sqrt(pow(RDDsize, 2)/ A))
    val Z = (Do - De)/SE
    val gaussian = breeze.stats.distributions.Gaussian(0, 1)
    val Pvalue = 2 * (gaussian.cdf(Z))
    var str="平均最近邻汇总\n"
    str += f"最近邻比率:$ANN%.4f\n平均观测距离:$Do%.4f\n预期平均距离:$De%.4f\nZ-Score:$Z%.4f\nP值：$Pvalue%.6f\n要素最小外接矩阵面积:$A\n"
    println(str)
    str
  }
}
