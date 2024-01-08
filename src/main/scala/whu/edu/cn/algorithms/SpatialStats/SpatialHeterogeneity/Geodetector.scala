package whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import breeze.stats
import breeze.linalg
import breeze.linalg.{DenseMatrix, mmwrite}
import breeze.util.Terminal
import org.apache.commons.math3.special.Beta
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.FDistribution

import scala.io.StdIn._
import scala.util.control.ControlThrowable


object Geodetector {

  private var _nameX: List[String] = _
  private var _nameY: String = ""
  private var _X: List[List[Any]] = _
  private var _Y: List[Double] = _

  def fget(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): List[Any] = {
    featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal]).collect().toList
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
   * @return String
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
    val groupY= GroupingXAndY(Y, X).map(t => t._2)
    val SST = getVariation(Y) * (Y.length)
    val SSW = groupY.map(t => (t.length) * getVariation(t)).sum
    val q = 1 - SSW / SST
    val sum_sq_mean = groupY.map(t => Math.pow(stats.mean(t), 2)).sum
    val sq_sum = groupY.map(t => Math.sqrt(t.length) * stats.mean(t)).sum
    val N_sample: Double = Y.length
    val N_strata: Double = groupY.length
    val ncp = (sum_sq_mean - sq_sum * sq_sum / N_sample) / stats.variance(Y)
    val f_val: Double = (N_sample - N_strata) / (N_strata - 1) * (q / (1 - q))
    val p = 1 - noncentralFCDF(f_val, df1 = N_strata - 1, df2 = N_sample - N_strata, ncp)
    (q, p)
  }

  protected def FD_print(res: (List[String], List[Double], List[Double])): String = {
    var str = "********************Results of Factor Detector********************\n"
    val len = res._1.length
    var content = linalg.DenseMatrix.zeros[String](len, 3)
    for (i <- 0 until len) {
      content(i, 0) = res._1(i)
      content(i, 1) = BigDecimal(res._2(i)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      content(i, 2) = BigDecimal(res._3(i)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      //str += f"variable ${i + 1}: ${res._1(i)}%-10s, q: ${res._2(i)}%-1.5f, p: ${res._3(i)}%-5g\n"
    }
    val contentStr = mat2str(List("variable", "q-statistics", "p-value"),
      (1 to len).toList.map(t => t.toString), content)
    str += s"$contentStr\n"
    str += "******************************************************************\n"
    str
  }

  /**
   * 交互探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @return String
   */
  def interactionDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                          y_title: String, x_titles: String)
  : String = {
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    val interactions = linalg.Matrix.zeros[String](_nameX.length, _nameX.length)
    val enhancement = linalg.Matrix.zeros[String](_nameX.length, _nameX.length)
    for (i <- 0 until _nameX.length) {
      for (j <- 0 to i) {
        val q = ID_single(_Y, _X(i), _X(j))
        val q1 = getQ(_Y, _X(i))
        val q2 = getQ(_Y, _X(j))
        val q_min = Math.min(q1, q2)
        val q_max = Math.max(q1, q2)
        interactions(i, j) = BigDecimal(q).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
        interactions(j, i) = interactions(i, j)
        if (q < q_min) {
          enhancement(i, j) = "Weakened, nonlinear"
          //enhancement(i, j) = "Wkn.,nl."
          enhancement(j, i) = enhancement(i, j)
        }
        else if (q > q_min && q < q_max) {
          enhancement(i, j) = "Weakened, uni-"
          //enhancement(i, j) = "Wkn.,uni-"
          enhancement(j, i) = enhancement(i, j)
        }
        else {
          if (q > q1 + q2) {
            enhancement(i, j) = "Enhanced, Nonlinear"
            //enhancement(i, j) = "Enh.,nl."
            enhancement(j, i) = enhancement(i, j)
          }
          else if (q == q1 + q2) {
            enhancement(i, j) = "Independent"
            //enhancement(i, j) = "Ind."
            enhancement(j, i) = enhancement(i, j)
          }
          else {
            enhancement(i, j) = "Enhanced, bi-"
            //enhancement(i, j) = "Enh.,bi-"
            enhancement(j, i) = enhancement(i, j)
          }
        }
      }
    }
    //for (i <- 0 until _nameX.length) {
    //  interactions(i, i) = "NA"
    //  enhancement(i, i) = "NA"
    //}
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

  protected def ID_print(res: (List[String], linalg.Matrix[String], linalg.Matrix[String])): String = {
    var str = "*****************************Results of Interaction Detector*****************************\n"
    var qStr = mat2str(res._1, res._1, res._2.toDenseMatrix)
    var sigStr = mat2str(res._1, res._1, res._3.toDenseMatrix)
    val regex = "\\((.*?)\\)".r
    //矩阵省略时修改括号内信息
    qStr = regex.replaceAllIn(qStr, matchResult => {
      s"(${res._1.length} variables total)"
    })
    sigStr = regex.replaceAllIn(sigStr, matchResult => {
      s"(${res._1.length} variables total)"
    })
    str += s"Q-statistics:\n$qStr\n\n"
    str += s"Significance:\n$sigStr\n"
    //str += "\nInd.: independent\nWkn.: weakened\nEnh.: enhanced\nnl.: nonlinear\nuni-: uni-factor\nbi-: bi-factors\n"
    //val N_var = res._1.length
    //var no = 1
    //for (i <- 0 until N_var) {
    //  for (j <- 0 until i) {
    //    str += f"interaction ${no}, variable1: ${res._1(i)}%-10s, variable2: ${res._1(j)}%-10s, q: ${res._2(i, j)}%-1.5f, sig: ${res._3(i, j)}%s\n"
    //    no += 1
    //  }
    //}
    str += "*****************************************************************************************\n"
    str
  }

  /**
   * 生态探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @return String
   */
  def ecologicalDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                         y_title: String, x_titles: String): String = {
    //(List[String], linalg.Matrix[Boolean])
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    //val F_mat = linalg.Matrix.zeros[Double](x_titles.length, x_titles.length) // Matrix of Statistic F
    val SigMat = linalg.Matrix.zeros[String](_nameX.length, _nameX.length) // Matrix of True or False
    for (i <- 0 until _nameX.length) {
      for (j <- 0 until _nameX.length) {
        val F_val = ED_single(_Y, _X(i), _X(j))
        //F_mat(i, j) = F_val
        //F-test
        val Nx1 = _X(i).length
        val Nx2 = _X(j).length
        val F = new FDistribution((Nx1 - 1), (Nx2 - 1))
        SigMat(i, j) = (F_val > F.inverseCumulativeProbability(0.9)).toString
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

  protected def ED_print(res: (List[String], linalg.Matrix[String])): String = {
    var str = "*****************************Results of Ecological Detector******************************\n"
    var no = 1
    var sigStr = mat2str(res._1, res._1, res._2.toDenseMatrix)
    val regex = "\\((.*?)\\)".r
    sigStr = regex.replaceAllIn(sigStr, matchResult => {
      s"(${res._1.length} variables total)"
    })
    //for (i <- 0 until res._2.rows) {
    //  for (j <- 0 until res._2.cols) {
    //    str += f"${no} variable1: ${res._1(i)}%-10s, variable2: ${res._1(j)}%-10s, significance: ${res._2(i, j)}\n"
    //    no += 1
    //  }
    //}
    // printMatrixWithTitles_Boolean(res)
    str += sigStr
    str += "\n*****************************************************************************************\n"
    str
  }

  /**
   * 因子探测器
   *
   * @param featureRDD RDD
   * @param y_title    因变量名称
   * @param x_titles   自变量名称
   * @return String
   */
  def riskDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                   y_title: String, x_titles: String): String = {
    //(List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[Boolean]])
    setX(featureRDD, x_titles)
    setY(featureRDD, y_title)
    val lst1 = ListBuffer.empty[String]
    val lst2 = ListBuffer.empty[List[String]]
    val lst3 = ListBuffer.empty[List[Double]]
    val lst4 = ListBuffer.empty[linalg.Matrix[String]]
    for (i <- 0 until _X.length) {
      val res = RD_single(_Y, _X(i), _nameX(i))
      lst1.append(_nameX(i))
      lst2.append(res._1)
      lst3.append(res._2)
      lst4.append(res._3)
    }
    // (自变量名，层名，各层平均值，显著性矩阵)
    val res = (lst1.toList, lst2.toList, lst3.toList, lst4.toList)
    //if (is_print) {
    val str = RD_print(res)
    //}
    str
  }

  /**
   * 对单个X进行检测
   *
   * @param Y 因变量
   * @param X 自变量
   * @param X_name 自变量名
   * @return (层名，各层平均值，显著性矩阵)
   */
  protected def RD_single(Y: List[Double], X: List[Any], X_name: String):
  (List[String], List[Double], linalg.Matrix[String]) = {
    val groupXY = GroupingXAndY(Y, X)
    val groupY = groupXY.map(t => t._2)
    val groupX = groupXY.map(t => t._1)
    val N_strata = groupY.length
    if (N_strata > 100) {
      //throw new Exception(s"The number of strata in ${X_name} is $N_strata. The data should be dispersed.\n")
      print(s"Warning: The number of strata in variable ${X_name} is $N_strata. It is recommended to simplify the stratification.\n")
    }
    val means_variable = groupY.map(t => stats.mean(t))
    //val T_Mat = linalg.Matrix.zeros[Boolean](N_strata, N_strata)
    val T_Mat1 = linalg.Matrix.zeros[String](N_strata, N_strata)
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
        T_Mat1(i, j) = "NA"
        if (n1 > 1 && n2 > 1) {
          val df_numerator = math.pow(var1 / n1 + var2 / n2, 2)
          val df_denominator = math.pow(var1 / n1, 2) / (n1 - 1) + math.pow(var2 / n2, 2) / (n2 - 1)
          if (df_denominator != 0) {
            val df_val = df_numerator / df_denominator
            //T_Mat(i, j) = new TDistribution(df_val).cumulativeProbability(t_val) > 0.975
            if (new TDistribution(df_val).cumulativeProbability(t_val) > 0.975) {
              T_Mat1(i, j) = "true"
            }
            else T_Mat1(i, j) = "false"
            /* 1 means true, 0 means false, -1 means incapable to compute */
          }
        }
      }
    }
    (groupX, means_variable, T_Mat1)
  }

  /**
   *
   * @param res (自变量名，层名，各层均值，子区域显著性矩阵)
   * @return
   */
  protected def RD_print(res: (List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[String]])):
  String = {
    val builderFinal = new StringBuilder
    val str_start = "******************************Result of Risk Detector******************************\n"
    val str_split = "***********************************************************************************\n"
    builderFinal.append(str_start)
    val N_var = res._1.length
    for (no <- 0 until N_var) {
      val builder = new StringBuilder
      var meanMat = breeze.linalg.DenseMatrix.zeros[String](res._2(no).length, 2)
      for (i <- 0 until res._2(no).length) {
        meanMat(i, 0) = res._2(no)(i)
        meanMat(i, 1) = BigDecimal(res._3(no)(i)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
        //builder.append(f"stratum ${i + 1}: ${res._2(no)(i)}, mean: ${res._3(no)(i)}%.3f\n")
      }
      var meanStr = mat2str(List("stratum", "mean"), (1 to meanMat.rows).toList.map(t => t.toString), meanMat)
      var sigStr = mat2str(res._2(no), res._2(no), res._4(no).toDenseMatrix)
      val regex = "\\((.*?)\\)".r
      meanStr = regex.replaceAllIn(meanStr, matchResult => {
        s"(${res._2(no).length} variables total)"
      })
      sigStr = regex.replaceAllIn(sigStr, matchResult => {
        s"(${res._2(no).length} variables total)"
      })
      builder.append(f"Variable ${no + 1}: ${res._1(no)}\n")
      builder.append(s"\nMeans of Strata: \n$meanStr\n")
      builder.append(s"\nSignificance: \n$sigStr\n")
      //var no_MatElem = 0
      //for (i <- 0 until res._2(no).length) {
      //  for (j <- 0 until res._2(no).length) {
      //    //println(s"(${no+1},${i+1}, ${j+1})")
      //    builder.append(f"${no_MatElem + 1}: stratum ${i + 1}: ${res._2(no)(i)}, stratum ${j + 1}: ${res._2(no)(j)}; sig: ${res._4(no)(i, j)}\n")
      //    no_MatElem += 1
      //  }
      //}
      builder.append(str_split)
      builderFinal.append(builder.toString())
      // printMatrixWithTitles_Boolean((res._2(no), res._4(no)))
      // print(str_split)
    }
    builderFinal.toString()
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
      case "BigDecimal" => lst = list.collect({case (i: java.math.BigDecimal) => (i.asInstanceOf[java.math.BigDecimal].doubleValue)}) //后期写入shp的属性
    }
    lst
  }

  //  /**
  //   * 以X为依据对Y分层
  //   *
  //   * @param Y List
  //   * @param X List
  //   * @return Y的分层结果
  //   */
  //  protected def Grouping(Y: List[Double], X: List[Any]): List[List[Double]] = {
  //    if (Y.length != X.length) {
  //      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${Y.length} while size of x is ${X.length}.")
  //    }
  //    val list_xy = ListBuffer.empty[Tuple2[Any, Double]]
  //    for (i <- 0 until Y.length) {
  //      list_xy.append((X(i), Y(i)))
  //    }
  //    val sorted_xy = list_xy.toList.groupBy(_._1).mapValues(r => r.map(r => {
  //      r._2
  //    })).map(r => r._2)
  //    sorted_xy.toList
  //  }

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
    val list_xy = ListBuffer.empty[Tuple2[String, Double]]
    for (i <- 0 until Y.length) {
      list_xy.append((X(i).toString, Y(i)))
    }
    val groupedXY = list_xy.toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    }))
    groupedXY.toList
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
    val y_grouped = GroupingXAndY(Y, X).map(t => t._2)
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
   * 给矩阵的行列添加标题，并转化为String形式
   *
   * @param titleCols 矩阵列标题
   * @param titleRows 矩阵行标题
   * @param contentMat 原始矩阵
   * @return
   */
  protected def mat2str(titleCols: List[String], titleRows: List[String],
                        contentMat: linalg.DenseMatrix[String]): String = {
    var extendedMat = DenseMatrix.vertcat(DenseMatrix.zeros[String](1, contentMat.cols), contentMat)
    extendedMat = DenseMatrix.horzcat(DenseMatrix.zeros[String](extendedMat.rows, 1), extendedMat)
    val titleCols1 = breeze.linalg.DenseMatrix.create[String](1, titleCols.length, titleCols.toArray)
    val titleRows1 = breeze.linalg.DenseMatrix.create[String](1, titleRows.length, titleRows.toArray)
    extendedMat(0 to 0, 1 until extendedMat.cols) := titleCols1
    extendedMat(1 until extendedMat.rows, 0 to 0) := titleRows1.t
    extendedMat(0, 0) = ""
    val res = extendedMat.toString()
    res
  }
}
