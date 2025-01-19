package whu.edu.cn.algorithms.SpatialStats.Utils

import breeze.linalg.{DenseVector, sum}
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, TopologyException}

import scala.collection.mutable.{ArrayBuffer, Map}
//import cern.colt.matrix._

object FeatureSpatialWeight {

  //  object Kernal extends Enumeration {
  //    val gaussian, exponential, bisquare, tricube, boxcar = Value
  //  }

  /**
   * 对距离RDD进行权重向量求解，尤其是通过getDistRDD函数得到的距离
   *
   * @param distRDD    Distance RDD
   * @param bw       Bandwidth size
   * @param kernel   Kernel function, default is "gaussian"
   * @param adaptive bandwidth type: adaptive(true) or fixed(false, default)
   * @return Weight value of RDD
   */
  def getSpatialweight(distRDD: RDD[Array[Double]], bw: Double, kernel: String = "gaussian", adaptive: Boolean = false): RDD[DenseVector[Double]] = {
    val RDDdvec = distRDD.map(t => Array2DenseVector(t))
    RDDdvec.map(t => getSpatialweightSingle(t, bw, kernel, adaptive))
  }

  /**
   * 获取一个面状矢量RDD的邻接权重矩阵，输入如果不是面状数据，输出所有权重将是0
   *
   * @param polyRDD 输入的面状数据，项目矢量RDD类型
   * @param style   邻接矩阵的权重计算类型，计算结果只针对邻接的对象算权重，非邻接的对象权重均为0。默认为W类型。
   *                W：1/邻居数； B：1； C：1/平均邻居数； U：1/总邻居数；
   * @return RDD形式的权重向量
   */
  def getNeighborWeight(polyRDD: RDD[(String, (Geometry, Map[String, Any]))], style: String = "W"): RDD[DenseVector[Double]] = {
    val geomtype = getGeometryType(polyRDD)
    if ("Point" == geomtype) {
      println(s" Remind!!! The geometry type of input RDD is: $geomtype")
    }
    val geomRDD = getGeometry(polyRDD)
    val nb_bool = getNeighborBool(geomRDD)
    val nb_weight = boolNeighborWeight(nb_bool)
    val nb_collect = nb_weight.collect()
    val sum_nb: Double = nb_collect.map(t => t.toArray.sum).sum
    val avg_nb: Double = nb_collect.length.toDouble
    style match {
      case "W" => nb_weight.map(t => t * (t / sum(t)))
      case "B" => nb_weight.map(t => t)
      case "C" => nb_weight.map(t => (t * (1.0 * avg_nb / sum_nb)))
      case "U" => nb_weight.map(t => (t * (1.0 / sum_nb)))
    }
  }

  /**
   * 对单个距离向量进行权重向量求解
   *
   * @param dist     \~english Distance vector \~chinese 距离向量
   * @param bw       \~english Bandwidth size \~chinese 带宽大小
   * @param kernel   \~english Kernel function, default is "gaussian" \~chinese 核函数，默认为高斯核函数
   * @param adaptive \~english bandwidth type: adaptive(true) or fixed(false, default) \~chinese 带宽类型，可变带宽为true，固定带宽为false，默认为固定带宽
   * @return \~english Weight value \~chinese 权重向量
   */
  def getSpatialweightSingle(dist: DenseVector[Double], bw: Double, kernel: String = "gaussian", adaptive: Boolean = false): DenseVector[Double] = {
    var weight: DenseVector[Double] = DenseVector.zeros(dist.length)
    if (adaptive == false) {
      kernel match {
        case "gaussian" => weight = gaussianKernelFunction(dist, bw)
        case "exponential" => weight = exponentialKernelFunction(dist, bw)
        case "bisquare" => weight = bisquareKernelFunction(dist, bw)
        case "tricube" => weight = tricubeKernelFunction(dist, bw)
        case "boxcar" => weight = boxcarKernelFunction(dist, bw)
        case _ => throw new IllegalArgumentException("Illegal Argument of kernal")
      }
    } else if (adaptive == true) {
      val fbw = fixedwithadaptive(dist, bw.toInt)
      kernel match {
        case "gaussian" => weight = gaussianKernelFunction(dist, fbw)
        case "exponential" => weight = exponentialKernelFunction(dist, fbw)
        case "bisquare" => weight = bisquareKernelFunction(dist, fbw)
        case "tricube" => weight = tricubeKernelFunction(dist, fbw)
        case "boxcar" => weight = boxcarKernelFunction(dist, fbw)
        case _ => throw new IllegalArgumentException("Illegal Argument of kernal")
      }
    } else {
      throw new IllegalArgumentException("Illegal Argument of adaptive")
    }
    weight
  }

  def Array2DenseVector(inputArr: Array[Double]): DenseVector[Double] = {
    val dvec: DenseVector[Double] = new DenseVector(inputArr)
    dvec
  }

  def DenseVector2Array(inputDvec: DenseVector[Double]): Array[Double] = {
    val arr = inputDvec.toArray
    arr
  }

  /**
   * \~english Gaussian kernel function. \~chinese Gaussian 核函数。
   *
   * @param dist \~english Distance vector \~chinese 距离向量
   * @param bw   \~english Bandwidth size (its unit is equal to that of distance vector) \~chinese 带宽大小（和距离向量的单位相同）
   * @return \~english Weight value \~chinese 权重值
   */
  def gaussianKernelFunction(dist: DenseVector[Double], bw: Double): DenseVector[Double] = {
    //    exp((dist % dist) / ((-2.0) * (bw * bw)))
    exp(-0.5 * ((dist / bw) * (dist / bw)))
  }

  def exponentialKernelFunction(dist: DenseVector[Double], bw: Double): DenseVector[Double] = {
    exp(-dist / bw);
  }

  //bisquare (1-(vdist/bw)^2)^2
  def bisquareKernelFunction(dist: DenseVector[Double], bw: Double): DenseVector[Double] = {
    val d2_d_b2: DenseVector[Double] = 1.0 - (dist / bw) * (dist / bw);
    //    val arr_re: Array[Double] = dist.toArray.filter(_<bw)
    //    val dist_bw = DenseVector(dist.toArray.filter(_<bw))//这样会筛选出重新组成一个数组
    val weight = (d2_d_b2 * d2_d_b2)
    filterwithBw(dist, weight, bw)
  }

  def tricubeKernelFunction(dist: DenseVector[Double], bw: Double): DenseVector[Double] = {
    val d3_d_b3: DenseVector[Double] = 1.0 - (dist * dist * dist) / (bw * bw * bw);
    val weight = (d3_d_b3 * d3_d_b3 * d3_d_b3)
    filterwithBw(dist, weight, bw)
  }

  def boxcarKernelFunction(dist: DenseVector[Double], bw: Double): DenseVector[Double] = {
    val weight: DenseVector[Double] = DenseVector.ones(dist.length)
    filterwithBw(dist, weight, bw)
  }

  def filterwithBw(dist: DenseVector[Double], weight: DenseVector[Double], bw: Double): DenseVector[Double] = {
    for (i <- 0 until dist.length) {
      if (dist(i) > bw) {
        weight(i) = 0.0
      }
    }
    weight
  }

  def fixedwithadaptive(dist: DenseVector[Double], abw: Int): Double = {
    val distcopy = dist.copy.toArray.sorted
    var fbw = distcopy.max
    if (abw < distcopy.length) {
      fbw = distcopy(abw - 1)
    }
    else {
      fbw = distcopy.max * abw / dist.length
    }
    fbw
  }

  def getGeometry(geomRDD: RDD[(String, (Geometry, Map[String, Any]))]): RDD[Geometry]={
    geomRDD.map(t=>t._2._1)
  }

  def getGeometryType(geomRDD: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    geomRDD.map(t => t._2._1).first().getGeometryType
  }

  def getNeighborBool(polyrdd: RDD[(Geometry)]): RDD[Array[Boolean]] = {
    val polyidx=polyrdd.zipWithIndex()
    val arr_geom = polyrdd.collect()
    val rdd_isnb = polyidx.map(t => testNeighborBool(t._1, arr_geom,t._2.toInt))
    rdd_isnb
  }

  def testNeighborBool(poly1: Geometry, poly2: Array[Geometry],id:Int): Array[Boolean] = {
    val arr_isnb = new Array[Boolean](poly2.length)
    val poly2idx=poly2.zipWithIndex
    poly2idx.foreach(t=> {
      try {
        if (t._2 != id) {
          arr_isnb(t._2) = poly1.touches(t._1)
        }
      } catch {
        case e: TopologyException => {
          arr_isnb(t._2) = true //这里可能有问题的，不确定。相当于将touch出错的全部认为是由于矢量精度问题导致的
        }
      }
      arr_isnb(id) = false
    })
//    for (i <- poly2.indices) {
//      try {
//        if(i!=id) {
//          arr_isnb(i) = poly1.touches(poly2(i))
//        }
//      } catch {
//        case e: TopologyException => {
//          arr_isnb(i) = true//这里可能有问题的，不确定。相当于将touch出错的全部认为是由于矢量精度问题导致的
//        }
//      }
//      arr_isnb(id)=false
//    }
    arr_isnb
  }

  def boolNeighborWeight(rdd_isnb: RDD[Array[Boolean]]): RDD[DenseVector[Double]] = {
//    var nb_weight: DenseVector[Double] = DenseVector.zeros(rdd_isnb.take(0).length)
    val nb_w = rdd_isnb.map(t => {
      val arr_t=new Array[Double](t.length)
      for (i <- t.indices) {
        if (t(i)) {
          arr_t(i)=1
        }
      }
      val dvec_t=DenseVector(arr_t)
      dvec_t
    })
//    nb_w.foreach(println)
    nb_w
//    nb_weight
  }

  def boolNeighborIndex(rdd_isnb: RDD[Array[Boolean]]): RDD[Array[String]] = {
    val nb_idx = rdd_isnb.map(t => {
      var arridx = arrIndextrue(t)
      arridx
    })
    nb_idx
  }

  def arrIndextrue(arr: Array[Boolean]): Array[String] = {
    var arrbufidx: ArrayBuffer[String] = ArrayBuffer()
    for (i <- arr.indices) {
      if (arr(i)) {
        arrbufidx += i.toString
      }
    }
    arrbufidx.toArray
  }
}
