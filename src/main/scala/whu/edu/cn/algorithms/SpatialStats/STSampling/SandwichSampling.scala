package whu.edu.cn.algorithms.SpatialStats.STSampling

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import breeze.stats
import breeze.linalg
import org.apache.commons.math3
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.FDistribution
import breeze.linalg.DenseVector

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._

object SandwichSampling {

  private var _Y: List[Double] = _
  private var _knowledge: List[String] = _
  private var _reporting: List[String] = _

  /**
   *三明治随机抽样
   *
   * @param featureRDD      RDD
   * @param y_title         因变量名称
   * @param knowledge_title 知识层名称
   * @param reporting_title 报告层名称
   * @param accuracy        抽取精度
   * @return                String
   */
  def sampling(sc: SparkContext, featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
               y_title: String, knowledge_title: String, reporting_title: String, accuracy: Double = 0.05): //String
  RDD[(String, (Geometry, Map[String, Any]))]= {
    // test error
    if (accuracy > 1.0 || accuracy <= 0.0) {
      throw new IllegalArgumentException("The accuracy must be in (0,1].")
    }

    //val oriVals = getNumber(featureRDD, y_title)
    //val oriVals = get(featureRDD, y_title).map(t => t.toString.toDouble)
    _Y = shpToList(featureRDD,y_title)
    val oriOrderNum = (0 until _Y.length).toList // 序号
    //val oriKnowledge = Feature.getString(featureRDD, knowledge_title)
    //val oriReport = Feature.getString(featureRDD, reporting_title)
    _knowledge = shpToList(featureRDD, knowledge_title).map(t=>t.toString)
    _reporting = shpToList(featureRDD, reporting_title).map(t=>t.toString)

    // 根据Cochran的方法计算样本数
    val t_alpha = 2.5758 // 置信度0.99下的标准正态偏差
    val std = stats.stddev(_Y)
    val d = stats.mean(_Y) * accuracy
    val sampleSizeTotal = math.pow((t_alpha * std) / d, 2).toInt
    val df: ListBuffer[(Int, Double, String, String)] = ListBuffer.empty[(Int, Double, String, String)]
    for (i <- 0 until _Y.length) {
      df.append((i, _Y(i), _knowledge(i), _reporting(i)))
    }

    // 根据知识层进行分类
    val df_groupedByKnowledge = Grouping(df.toList, _knowledge) // 根据知识层对df进行分层
    val orderNum_groupedByKnowledge = GroupingInt(oriOrderNum, _knowledge)
    var sampleSizeKnowledge = df_groupedByKnowledge.map(t => math.round(t.length.toDouble * sampleSizeTotal / _Y.length).toInt)
    sampleSizeKnowledge = sampleSizeKnowledge.map(t => {
      if (t == 0) {
        1 // 确保每层至少一个样本
      }
      else {
        t
      }
    })

    /*---------------------------------Sampling--------------------------------------------*/
    //抽样样本的序号，保留两层（知识层和层内的样本），便于后续处理
    val sampleOrderNumKnowledge: ListBuffer[List[Int]] = ListBuffer.empty[List[Int]]
    for (i <- 0 until orderNum_groupedByKnowledge.length) {
      val s = Random.shuffle(orderNum_groupedByKnowledge(i)).take(sampleSizeKnowledge(i)) //层内抽样
      sampleOrderNumKnowledge.append(s)
    }

    // 抽样生成新的RDD
    //val t0 = System.currentTimeMillis()
    //val res: ListBuffer[(Int, Double, String, String)] = ListBuffer.empty[(Int, Double, String, String)]
    type typeRddElement = (String, (Geometry, Map[String, Any]))
    val res1: ArrayBuffer[typeRddElement] = ArrayBuffer.empty[typeRddElement]
    val featureArray = featureRDD.collect()
    for (i <- 0 until sampleOrderNumKnowledge.length) {
      for (j <- 0 until sampleOrderNumKnowledge(i).length) {
        val no = sampleOrderNumKnowledge(i)(j)
        //res.append(df(no))
        res1.append(featureArray(no))
      }
    }
    //println(s"t = ${(System.currentTimeMillis() - t0) / 1000.0} s.")

    /*-------------------------计算抽样结果指标------------------------*/
    val sampleVals = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._2))
    val meansKnowledge = sampleVals.map(t => stats.mean(t)) //yZbar
    val varsKnowledge = sampleVals.map(t => stats.variance(t))
    val sizeKnowledge = sampleVals.map(t => t.length)
    val var_meansKnowledge = stats.variance(meansKnowledge.toList) //V_yZbar
    val sampleVals_flat: ListBuffer[Double] = ListBuffer.empty[Double] // no grouped
    for (i <- 0 until sampleVals.length) {
      for (j <- 0 until sampleVals(i).length) {
        sampleVals_flat.append(sampleVals(i)(j))
      }
    }

    val sampleKnowledge = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._3))
    val namesKnowledge = sampleKnowledge.map(t => t.distinct(0)) //知识层各层名称
    val namesReport = _reporting.distinct //报告层各层名称

    //计算各报告层样本量
    val sampleReport = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._4))
    val sampleReport_flat: ListBuffer[String] = ListBuffer.empty[String] // no grouped
    for (i <- 0 until sampleReport.length) {
      for (j <- 0 until sampleReport(i).length) {
        sampleReport_flat.append(sampleReport(i)(j))
      }
    }
    val sampleSizeReport = sampleReport_flat.groupBy(identity).mapValues(_.length)

    //计算报告层与知识层相交后，各子区内的样本量
    val sampleZoneAndReport = sampleOrderNumKnowledge.map(t => t.map(t => (df(t)._3, df(t)._4)))
    val sampleZR_flat: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
    for (i <- 0 until sampleZoneAndReport.length) {
      for (j <- 0 until sampleZoneAndReport(i).length) {
        sampleZR_flat.append(sampleZoneAndReport(i)(j))
      }
    }
    val sampleSzZR = sampleZR_flat.groupBy(identity).mapValues(_.length)
    val matSzZR = linalg.Matrix.zeros[Int](namesKnowledge.length, namesReport.length)
    for (i <- 0 until namesKnowledge.length) {
      for (j <- 0 until namesReport.length) {
        val key0 = (namesKnowledge(i), namesReport(j))
        //println(key0)
        if (sampleSzZR.contains(key0)) {
          matSzZR(i, j) = sampleSzZR(key0)
        }
      }
    }

    // 正式计算报告层的均值与方差
    val meansReport: ListBuffer[Double] = ListBuffer.empty[Double]
    val varsReport: ListBuffer[Double] = ListBuffer.empty[Double]
    for (i <- 0 until namesReport.length) {
      var yRbar = 0.0
      var V_yRbar = 0.0
      val Nr = sampleSizeReport(namesReport(i))
      //println(s"Nr is $Nr")
      for (j <- 0 until namesKnowledge.length) {
        val Nrz = matSzZR(j, i)
        val yZbar = meansKnowledge(j)
        yRbar += Nrz.toDouble * yZbar / Nr
        V_yRbar += math.pow(Nrz.toDouble / Nr, 2) * var_meansKnowledge
      }
      meansReport.append(yRbar)
      varsReport.append(V_yRbar)
    }

    var str = "************************Results of Sandwich Sampling************************\n"
    str += f"Variable: $y_title\n" +
      f"Knowledge layer: ${knowledge_title}%-10s, number of strata: ${namesKnowledge.length}\n" +
      f"Reporting layer: ${reporting_title}%-10s, number of strata: ${namesReport.length}\n"+
      f"Total size: ${_Y.length}\nAccuracy: ${accuracy}\n"+
      f"Expected sample size: ${sampleSizeTotal}\nActual sample size: ${res1.length}\n"+
      f"************************Parameters of Knowledge Layer***********************\n"+
      f"Mean-variance: ${stats.variance(meansKnowledge)}%-2.3f\n"+
      f"Spatial standard deviation: ${stats.stddev(meansKnowledge)}%-2.3f\n"+
      f"Upper limit: ${meansKnowledge.max}%-2.3f\n"+
      f"Lower limit: ${meansKnowledge.min}%-2.3f\n"
    for (i <- 0 until meansKnowledge.length) {
      str += f"stratum ${i+1}: ${namesKnowledge(i)}, mean: ${meansKnowledge(i)}%-5f, variance: ${varsKnowledge(i)}%-5f, sample size: ${sizeKnowledge(i)}\n"
    }
    str += f"************************Parameters of Reporting Layer***********************\n"
    for (i <- 0 until meansReport.length) {
      str += f"stratum ${i + 1}: ${namesReport(i)}, mean: ${meansReport(i)}%-5f, variance: ${varsReport(i)}%-5f, sample size: ${sampleSizeReport(namesReport(i))}\n"
    }
    str += f"****************************************************************************\n"
    print(str)
    //最终选择的样本
    val resRDD = sc.makeRDD(res1)
    resRDD
  }

  /**
   * 从RDD中得到对应属性p的数据
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p       String的形式
   * @return        List[Double]
   */
  protected def shpToList(testshp: RDD[(String, (Geometry, Map[String, Any]))], p: String): List[Double] = {
    val list: List[Any] = Feature.get(testshp, p)//可能有问题
    val typeOfElement = list(0).getClass.getSimpleName
    var lst: List[Double] = List.empty
    typeOfElement match {
      case "String" => lst = list.collect({ case (i: String) => (i.toDouble) })//shp原始数据
      case "Double" => lst = list.collect({ case (i: Double) => (i.toDouble) })//后期写入shp的属性
    }
    lst
  }

  /**
   * 以X为依据对Y分层
   *
   * @param Y List
   * @param X List
   * @return Y的分层结果
   */
  protected def Grouping(Y: List[Any], X: List[Any]): List[List[Any]] = {
    if (Y.length != X.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${Y.length} while size of x is ${X.length}.")
    }

    val list_xy: ListBuffer[Tuple2[Any, Any]] =
      ListBuffer(("", List(0, 0.0, "", "")))
    for (i <- 0 until Y.length) {
      list_xy.append((X(i), Y(i)))
    }
    val sorted_xy = list_xy.drop(1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    })).map(r => r._2)
    sorted_xy.toList
  }

  /**
   * 以X为依据对Y分层
   *
   * @param Y List[Int]
   * @param X List
   * @return Y的分层结果
   */
  protected def GroupingInt(Y: List[Int], X: List[Any]): List[List[Int]] = {
    if (Y.length != X.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${Y.length} while size of x is ${X.length}.")
    }

    val list_xy: ListBuffer[Tuple2[Any, Int]] =
      ListBuffer(("", 0))
    for (i <- 0 until Y.length) {
      list_xy.append((X(i), Y(i)))
    }
    val sorted_xy = list_xy.drop(1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    })).map(r => r._2)
    sorted_xy.toList
  }
}
