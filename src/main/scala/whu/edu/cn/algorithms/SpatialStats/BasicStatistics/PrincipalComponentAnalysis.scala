package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.linalg.{DenseMatrix, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, Map}

object PrincipalComponentAnalysis {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
//    val sc = new SparkContext(conf)
//    val shpPath: String = "testdata\\LNHP100.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
//    val shpfile = ShapeFileUtil.readShp(sc, shpPath, ShapeFileUtil.DEF_ENCODE) //或者直接utf-8
//    PCA(shpfile)
//  }

  /**
   * 输入RDD，对数据进行主成分分析
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @return DenseMatrix[Double] 结果为降维处理后的数据
   */
  def PCA(testshp: RDD[(String, (Geometry, Map[String, Any]))],properties: String,
          keep: Int=2, split: String = ",",is_scale: Boolean = false): DenseMatrix[Double] = {
    //原始的数据
    val pArr= properties.split(split)
    if(keep>properties.length){
      throw new IllegalArgumentException("the principal components should be less than properties input value")
    }
    //    val pArr: Array[String] = Array[String]("PROF", "FLOORSZ", "UNEMPLOY", "PURCHASE") //属性字符串数组
    var lst = shpToList(testshp, pArr(0))
    val col = pArr.length
    val row = lst.length
    val matrix = new DenseMatrix[Double](row, col)
    for (i <- 0 to (col - 1)) {
      lst = shpToList(testshp, pArr(i))
      for (j <- 0 to (row - 1)) {
        matrix(j, i) = lst(j)
      }
    }
    //选择topK的特征
    val s = generatePCA(matrix, keep, pArr,is_scale)
    s
  }

  /**
   * 从RDD中得到对应属性p的数据
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p String的形式
   * @return List[Double]
   */
  def shpToList(testshp: RDD[(String, (Geometry, Map[String, Any]))], p: String): List[Double] = {
    val list: List[Any] = Feature.get(testshp, p)
    val lst: List[Double] = list.collect({ case (i: String) => (i.toDouble) })
    lst
  }

  /**
   *
   *
   * @param matrix DenseMatrix[Double]的形式
   * @param indexes Arary[Int]的形式
   * @return DenseMatrix[Double]
   */
  def selectCols(matrix: DenseMatrix[Double], indexes: Array[Int]): DenseMatrix[Double] = {
    val ab = new ArrayBuffer[Double]
    for (index <- indexes) {
      val colVector = matrix(::, index).toArray
      ab ++= colVector
    }
    new DenseMatrix[Double](matrix.rows, indexes.length, ab.toArray)
  }

  /**
   * 选择topK的特征值，对矩阵A作主成分分析
   *
   * @param A DenseMatrix[Double]的形式
   * @param topK Int的形式
   * @return DenseMatrix[Double]
   */
  def generatePCA(A: DenseMatrix[Double], topK: Int, varName: Array[String],is_scale: Boolean = false): DenseMatrix[Double] = {

    val B = A.copy
    val avg = sum(B, Axis._0).inner.map(_ / B.rows) // center

    //    println(f"is_scale:$is_scale")
    val scale =DenseVector.ones[Double](B.cols)
    if(is_scale){
      for(j <- 0 until B.cols){
        scale.update(j,breeze.stats.stddev(B(::,j)))
      }
    }
    //    println(f"scale:\n$scale")

    //中心化处理
    for (i <- 0 to B.rows - 1; j <- 0 to B.cols - 1) {
      val o = BigDecimal(B.apply(i, j))
      val jj = BigDecimal(avg.apply(j))
      val updated = (o-jj).toDouble/scale(j)
      B.update(i, j, updated)
    }

    //奇异值分解
    val svd = breeze.linalg.svd(B)
    val U = svd.leftVectors
    val S = svd.singularValues
    val eigenvalues = S.map(t => t*t)
    val resPC = DenseMatrix.zeros[Double](B.rows,topK)// dimension reduced
    for(i <- 0 until topK){
      val score_i = U(::,i)*S(i)
      resPC(::,i) := score_i
    }

    // print original data
    val A_print : DenseMatrix[String] = DenseMatrix.zeros(A.rows+1,A.cols)
    for(i<- 0 until A_print.rows){
      for(j<- 0 until A_print.cols){
        A_print(i,j) = if(i==0){
          varName(j)
        } else{
          A(i-1,j).formatted("%.4f")
        }
      }
    }

    // stdDev, proportion of variance, cumulative proportion
    val importance : DenseMatrix[String] = DenseMatrix.zeros(4,resPC.cols+1)
    importance(0,0)=""
    importance(1,0)="Standard Deviation"
    importance(2,0)="Proportion of Variance"
    importance(3,0)="Cumulative Proportion"
    val totalVar = sum(eigenvalues)
    val propVar = eigenvalues.map(_/totalVar)
    val cumulProp = DenseVector.zeros[Double](eigenvalues.length)
    //    println(f"totalVar:$totalVar")
    //    println(f"propVar:$propVar")
    //    println(f"cumulProp:$cumulProp")
    cumulProp(0) = propVar(0)
    for(i <- 1 until eigenvalues.length){
      cumulProp(i) = cumulProp(i-1) + propVar(i)
    }
    for (j <- 0 until resPC.cols) {
      importance(0,j+1) = f"PC${j+1}"
      importance(1,j+1) = breeze.stats.stddev(resPC(::,j)).formatted("%.4f").toString // stddev
      importance(2,j+1) = propVar(j).formatted("%.4f").toString
      importance(3,j+1) = cumulProp(j).formatted("%.4f").toString
    }

    // print processed data
    val res_rows = min(resPC.rows, 10)
    val res_print: DenseMatrix[String] = DenseMatrix.zeros(res_rows + 1, resPC.cols)
    for(i<- 0 until res_print.rows){
      for(j<- 0 until res_print.cols){
        res_print(i, j) = if (i == 0) {
          f"PC${j+1}"
        } else {
          resPC(i - 1, j).formatted("%.4f")
        }
      }
    }

    var str_1st10 = if(resPC.rows>10){"(First 10)"}else{""}
    var str = "\n*******************Summary of PCA*******************\n"
    str += s"Number of Principal Components(PCs): ${topK}\nImportance of Components:\n"+ f"${importance}\n"+
//      "\nOriginal Data (First 10):\n" + f"${A_print}\n...\n" +
      "\nProcessed Data " + str_1st10 + ":\n" + f"${res_print}\n"
    str += "****************************************************\n"
    print(str)
    resPC
  }
}

