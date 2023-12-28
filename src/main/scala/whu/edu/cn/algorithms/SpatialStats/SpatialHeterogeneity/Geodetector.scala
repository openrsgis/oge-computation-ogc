package whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import breeze.stats
import breeze.linalg
import org.apache.commons.math3.special.Beta
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.FDistribution


object Geodetector {

  private var _nameX: List[String] = _
  private var _nameY: String = ""
  private var _X: List[List[Any]] = _
  private var _Y: List[Double] = _

  def fget(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[Any] = {
    featureRDD.map(t => t._2._2("properties").asInstanceOf[com.alibaba.fastjson.JSONArray].getJSONObject(0).get(property)).collect().toList
  }

  private def setX(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], x_titles: String): Unit = {
    _nameX = x_titles.split(",").toList
    _X = _nameX.map(t => fget(featureRDD, t))
  }

  private def setY(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], y_title: String): Unit = {
    _nameY = y_title
    _Y = shpToList(featureRDD, _nameY)
  }

  /**
   * 因子探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @param is_print   是否打印，默认为true
   * @return           String
   */
  def factorDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                     y_title: String, x_titles: String): String = {
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    val QandP = _X.map(t => FD_single(_Y, t))
    val res = (_nameX, QandP.map(t => t._1), QandP.map(t => t._2))
    val str = FD_print(res)
    str
  }

  /**
   * 针对单个X进行因子探测器的计算
   *
   * @param Y
   * @param X
   * @return (q-statistic,p-value)
   */
  protected def FD_single(Y: List[Double], X: List[Any]): (Double, Double) = {
    val y_grouped = Grouping(Y, X)
    val SST = getVariation(Y) * Y.length
    val SSW = y_grouped.map(t => t.length * getVariation(t)).sum
    val q = 1 - SSW / SST
    val sum_sq_mean = y_grouped.map(t => Math.pow(stats.mean(t), 2)).sum
    val sq_sum = y_grouped.map(t => Math.sqrt(t.length) * stats.mean(t)).sum
    val N_sample: Double = Y.length
    val N_strata: Double = y_grouped.length
    val ncp = (sum_sq_mean - sq_sum * sq_sum / N_sample) / stats.variance(Y)
    val f_val: Double = (N_sample - N_strata) / (N_strata - 1) * (q / (1 - q))
    val p = 1 - noncentralFCDF(f_val, df1 = N_strata - 1, df2 = N_sample - N_strata, ncp)
    (q, p)
  }

  protected def FD_print(res: (List[String], List[Double], List[Double])): String = {
    var str = "********************Results of Factor Detector********************\n"
    val len = res._1.length
    for (i <- 0 until len) {
      str += f"variable ${i + 1}: ${res._1(i)}%-10s, q: ${res._2(i)}%-1.5f, p: ${res._3(i)}%-5g\n"
    }
    str += "******************************************************************\n"
    str
  }

  /**
   * 交互探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @param is_print   是否打印，默认为true
   * @return           String
   */
  def interactionDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                          y_title: String, x_titles: String)
  : String = {
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    val interactions = linalg.Matrix.zeros[Double](_nameX.length, _nameX.length)
    val enhancement = linalg.Matrix.zeros[String](_nameX.length, _nameX.length)
    for (i <- 0 until _nameX.length) {
      for (j <- 0 until i) {
        val q = ID_single(_Y, _X(i), _X(j))
        val q1 = getQ(_Y, _X(i))
        val q2 = getQ(_Y, _X(j))
        val q_min = Math.min(q1, q2)
        val q_max = Math.max(q1, q2)
        interactions(i, j) = q
        interactions(j, i) = q
        if (q < q_min) {
          enhancement(i, j) = "Weakened, nonlinear"
        }
        else if (q > q_min && q < q_max) {
          enhancement(i, j) = "Weakened, uni-"
        }
        else {
          if (q > q1 + q2) {
            enhancement(i, j) = "Enhanced, Nonlinear"
          }
          else if (q == q1 + q2) {
            enhancement(i, j) = "Independent"
          }
          else {
            enhancement(i, j) = "Enhanced, bi-"
          }
        }
      }
    }
    val res = (_nameX, interactions, enhancement)
    //if (is_print) {
    val str = ID_print(res)
    //}
    str
  }

  /**
   * 对单对因子进行检测
   *
   * @param Y
   * @param X1
   * @param X2
   * @return
   */
  protected def ID_single(Y: List[Double], X1: List[Any], X2: List[Any]): Double = {
    val interaction: ListBuffer[Any] = ListBuffer(List("0", "0"))
    for (i <- 0 until X1.length) {
      interaction.append(List(X1(i), X2(i)))
    }
    val q = getQ(Y, interaction.drop(1).toList)
    q
  }

  protected def ID_print(res: (List[String], linalg.Matrix[Double], linalg.Matrix[String])): String = {
    var str = "*****************************Results of Interaction Detector*****************************\n"
    //printMatrixWithTitles_Double((res._1,res._2))
    //printMatrixWithTitles_String((res._1,res._3))
    val N_var = res._1.length
    var no = 1
    for (i <- 0 until N_var) {
      for (j <- 0 until i) {
        str += f"interaction ${no}, variable1: ${res._1(i)}%-10s, variable2: ${res._1(j)}%-10s, q: ${res._2(i, j)}%-1.5f, sig: ${res._3(i, j)}%s\n"
        no += 1
      }
    }
    str += "*****************************************************************************************\n"
    str
  }

  /**
   * 生态探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @param is_print   是否打印，默认为true
   * @return           String
   */
  def ecologicalDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                         y_title: String, x_titles: String) :String = {
    //(List[String], linalg.Matrix[Boolean])
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    //val F_mat = linalg.Matrix.zeros[Double](x_titles.length, x_titles.length) // Matrix of Statistic F
    val SigMat = linalg.Matrix.zeros[Boolean](_nameX.length, _nameX.length) // Matrix of True or False
    for (i <- 0 until _nameX.length) {
      for (j <- 0 until _nameX.length) {
        val F_val = ED_single(_Y, _X(i), _X(j))
        //F_mat(i, j) = F_val
        //F-test
        val Nx1 = _X(i).length
        val Nx2 = _X(j).length
        val F = new FDistribution((Nx1 - 1), (Nx2 - 1))
        SigMat(i, j) = F_val > F.inverseCumulativeProbability(0.9)
      }
    }
    val res = (_nameX, SigMat)
    //if (is_print) {
    val str = ED_print(res)
    //}
    str
  }

  /**
   * 对两个因子进行检测
   *
   * @param Y
   * @param X1
   * @param X2
   * @return
   */
  protected def ED_single(Y: List[Double], X1: List[Any], X2: List[Any]): Double = {
    val f_numerator = getQ(Y, X2)
    val f_denominator = getQ(Y, X1)
    val res = f_numerator / f_denominator
    breeze.linalg.max(res, 1 / res)
  }

  protected def ED_print(res: (List[String], linalg.Matrix[Boolean])): String = {
    var str = "*****************************Results of Ecological Detector*****************************\n"
    var no = 1
    //println("*****************************Results of Ecological Detector*****************************")
    for (i <- 0 until res._2.rows) {
      for (j <- 0 until res._2.cols) {
        str += f"${no} variable1: ${res._1(i)}%-10s, variable2: ${res._1(i)}%-10s, significance: ${res._2(i, j)}\n"
        no += 1
      }
    }
    // printMatrixWithTitles_Boolean(res)
    str += "****************************************************************************************\n"
    str
  }

  /**
   * 因子探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @param is_print   是否打印，默认为true
   * @return           String
   */
  def riskDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                   y_title: String, x_titles: String): String= {
    //(List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[Boolean]])
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    val lst1 = ListBuffer("")
    val lst2 = ListBuffer(List(""))
    val lst3 = ListBuffer(List(0.0))
    val lst4 = ListBuffer(linalg.Matrix.zeros[Boolean](1, 1))
    for (i <- 0 until _X.length) {
      val res = RD_single(_Y, _X(i))
      lst1.append(_nameX(i))
      lst2.append(res._1)
      lst3.append(res._2)
      lst4.append(res._3)
    }
    val res = (lst1.drop(1).toList, lst2.drop(1).toList, lst3.drop(1).toList, lst4.drop(1).toList)
    //if (is_print) {
    val str = RD_print(res)
    //}
    str
  }

  /**
   * 对单个X进行检测
   *
   * @param Y
   * @param X
   * @return
   */
  protected def RD_single(Y: List[Double], X: List[Any]):
  (List[String], List[Double], linalg.Matrix[Boolean]) = {
    val groupXY = GroupingXAndY(Y, X)
    val groupY = groupXY.map(t => t._2)
    val groupX = groupXY.map(t => t._1)
    val N_strata = groupY.length
    val means_variable = groupY.map(t => stats.mean(t))
    val T_Mat = linalg.Matrix.zeros[Boolean](N_strata, N_strata)
    // val T_Mat1 = linalg.Matrix.zeros[Int](N_strata, N_strata)
    for (i <- 0 until N_strata) {
      for (j <- 0 until N_strata) {
        val group1 = groupY(i)
        val group2 = groupY(j)
        val mean1 = stats.mean(group1)
        val mean2 = stats.mean(group2)
        val var1 = stats.variance(group1)
        val var2 = stats.variance(group2)
        val n1 = group1.length
        val n2 = group2.length
        val t_numerator = mean1 - mean2
        val t_denominator = math.sqrt(var1 / n1 + var2 / n2)
        val t_val = (t_numerator / t_denominator).abs
        // Test
        // T_Mat1(i, j) = -1
        if (n1 > 1 && n2 > 1) {
          val df_numerator = math.pow(var1 / n1 + var2 / n2, 2)
          val df_denominator = math.pow(var1 / n1, 2) / (n1 - 1) + math.pow(var2 / n2, 2) / (n2 - 1)
          if (df_denominator != 0) {
            val df_val = df_numerator / df_denominator
            T_Mat(i, j) = new TDistribution(df_val).cumulativeProbability(t_val) > 0.975
            //  if (T_Mat(i, j)) {
            //    T_Mat1(i, j) = 1
            //  }
            //  else T_Mat1(i, j) = 0
            /* 1 means true, 0 means false, -1 means incapable to compute */
          }
        }
      }
    }
    (groupX, means_variable, T_Mat)
  }

  protected def RD_print(res: (List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[Boolean]])):
  String = {
    var str = "******************************Result of Risk Detector******************************\n"
    val str_split = "***********************************************************************************\n"
    //print(str0)
    val N_var = res._1.length
    for (no <- 0 until N_var) {
      str += f"Variable ${no + 1}: ${res._1(no)}\n" + "\n" + "Means of Strata: \n"
      for (i <- 0 until res._2(no).length) {
        str += f"stratum ${i + 1}: ${res._2(no)(i)}, mean: ${res._3(no)(i)}%10f\n"
      }
      str += "\nSignificance: \n"
      var no_MatElem = 1
      for (i <- 0 until res._2(no).length) {
        for (j <- 0 until res._2(no).length) {
          str += f"${no_MatElem} stratum1: ${res._2(no)(i)}, stratum2: ${res._2(no)(j)}, significance: ${res._4(no)(i, j)}\n"
          no_MatElem += 1
        }
      }
      str += str_split
      // printMatrixWithTitles_Boolean((res._2(no), res._4(no)))
      //print(str_split)
    }
    str
  }

  //***********************************************************************************************

  /**
   * 从RDD中得到对应属性p的数据
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p       String的形式
   * @return List[Double]
   */
  protected def shpToList(testshp: RDD[(String, (Geometry, Map[String, Any]))], p: String): List[Double] = {
    val list: List[Any] = fget(testshp, p)
    val typeOfElement = list(0).getClass.getSimpleName
    var lst: List[Double] = List.empty
    typeOfElement match {
      case "String" => lst = list.collect({ case (i: String) => (i.toDouble) }) //shp原始数据
      case "Double" => lst = list.collect({ case (i: Double) => (i.toDouble) }) //后期写入shp的属性
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
  protected def Grouping(Y: List[Double], X: List[Any]): List[List[Double]] = {
    if (Y.length != X.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${Y.length} while size of x is ${X.length}.")
    }
    val list_xy: ListBuffer[Tuple2[Any, Double]] = ListBuffer(("", 0.0))
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
   * @param Y List[Double]
   * @param X List
   * @return Y的分层结果及其对应的X
   */
  protected def GroupingXAndY(Y: List[Double], X: List[Any]): List[Tuple2[String, List[Double]]] = {
    if (Y.length != X.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${Y.length} while size of x is ${X.length}.")
    }
    val list_xy: ListBuffer[Tuple2[String, Double]] = ListBuffer(("", 0.0))
    for (i <- 0 until Y.length) {
      list_xy.append((X(i).toString, Y(i)))
    }
    val sorted_xy = list_xy.drop(1).sortBy(_._1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    }))
    sorted_xy.toList.sortBy(_._1)
  }

  /**
   * 计算总体方差
   *
   * @param list
   * @return
   */
  protected def getVariation(list: List[Double]): Double = {
    val mean: Double = list.sum / list.length
    var res: Double = 0
    for (i <- 0 until list.length) {
      res = res + (mean - list(i)) * (mean - list(i))
    }
    res / list.length
  }

  /**
   * 计算q值
   *
   * @param Y
   * @param X
   * @return
   */
  protected def getQ(Y: List[Double], X: List[Any]): Double = {
    val y_grouped = Grouping(Y, X)
    val SST = getVariation(Y) * Y.length
    val SSW = y_grouped.map(t => t.length * getVariation(t)).sum
    val q = 1 - SSW / SST
    q
  }

  /**
   * 计算非中心F分布的CDF
   *
   * @param x
   * @param df1
   * @param df2
   * @param noncentrality
   * @param maxIterations
   * @param tolerance
   * @return
   */
  protected def noncentralFCDF(x: Double, df1: Double, df2: Double, noncentrality: Double,
                               maxIterations: Int = 2000, tolerance: Double = 1e-9): Double = {
    var result = 0.0
    var k = 0
    var term = 0.999
    while (k < maxIterations) {
      //power = math.pow(noncentrality * 0.5, k)
      //fact = factorial(k)
      //powerDividedByFactorial = power/fact
      //这里不直接求幂和阶乘，防止值过大导致计算错误
      var powerDividedByFactorial = 1.0
      if (k == 0) {
        powerDividedByFactorial = 1
      }
      else {
        for (i <- 0 until k) {
          powerDividedByFactorial *= noncentrality / (2 * (i + 1))
        }
      }
      val expo = math.exp(-0.5 * noncentrality)
      val beta = Beta.regularizedBeta(df1 * x / (df2 + df1 * x), df1 / 2.0 + k, df2 / 2.0)
      term = powerDividedByFactorial * expo * beta
      //println((powerDividedByFactorial, expo, beta))
      //print(f"${k+1} ")
      //println(term)
      if (math.abs(term) > tolerance && math.abs(term) < 1) {
        result += term
      }
      k += 1
    }
    if (result < 0) {
      result = 0
    }
    else if (result > 1) {
      result = 1
    }
    result
  }

  /**
   * 将自变量的名字及其显著性以矩阵的形式打印
   *
   * @param res 包含自变量名（List）及其显著性（Matrix[Boolean]）
   */
  protected def printMatrixWithTitles_Boolean(res: (List[String], linalg.Matrix[Boolean])): Unit = {
    val len = res._1.length
    val mat_print = linalg.Matrix.zeros[String](len + 1, len + 1)
    for (i <- 0 until len + 1) {
      for (j <- 0 until len + 1) {
        if (i == 0 && j == 0) {}
        else if (i == 0) {
          mat_print(i, j) = res._1(j - 1)
        } else if (j == 0) {
          mat_print(i, j) = res._1(i - 1)
        } else {
          mat_print(i, j) = res._2(i - 1, j - 1).toString
        }
      }
    }
    println(mat_print)
  }

}
