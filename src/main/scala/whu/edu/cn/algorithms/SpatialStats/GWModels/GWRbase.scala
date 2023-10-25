package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, det, eig, inv, qr, sum, trace, max}
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

class GWRbase {

  protected var shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _
  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _
  protected var _nameX: Array[String] = _

  protected var _geom: RDD[Geometry] = _
  protected var _dist: Array[DenseVector[Double]]=_
  protected var spweight_dvec: Array[DenseVector[Double]] = _

  protected var max_dist: Double = _
  var _kernel:String=_
  var _adaptive:Boolean=_

  protected def calDiagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residual: DenseVector[Double], shat: DenseMatrix[Double]): Unit = {
    val shat0 = trace(shat)
//    val shat1 = trace(shat * shat.t)
    val shat1=sum(shat.map(t=>t*t))
    val rss = residual.toArray.map(t => t * t).sum
    val n = X.rows
    val AIC = n * log(rss / n) + n * log(2 * math.Pi) + n + shat0
    val AICc = n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
    val edf = n - 2.0 * shat0 + shat1
    val enp = 2.0 * shat0 - shat1
    val yss = sum((Y - mean(Y)) * (Y - mean(Y)))
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (edf - 1)
    println("*****************************Diagnostic information******************************")
    println(f"Number of data points: $n \nEffective number of parameters (2trace(S) - trace(S'S)): $enp%.4f")
    println(f"Effective degrees of freedom (n-2trace(S) + trace(S'S)): $edf%.4f\nAICc (GWR book, Fotheringham, et al. 2002, p. 61, eq 2.33): $AICc%.3f")
    println(f"AIC (GWR book, Fotheringham, et al. 2002,GWR p. 96, eq. 4.22): $AIC%.3f\nResidual sum of squares: $rss%.2f\nR-square value: $r2%.7f\nAdjusted R-square value: $r2_adj%.7f")
    println("*********************************************************************************")
  }

  def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Unit = {
    _geom = getGeometry(inputRDD)
    shpRDD = inputRDD
    if (_dist == null) {
      _dist = getDist(shpRDD).map(t => Array2DenseVector(t))
      max_dist = _dist.map(t => max(t)).max
    }
  }

  protected def setX(properties: String, split:String=","): Unit = {
    _nameX=properties.split(split)
    val x=_nameX.map(s=>{
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[String].toDouble).collect())
    })
    _X = x
  }

  protected def setY(property: String): Unit = {
    val y = shpRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect()
    _Y = DenseVector(y)
  }

  def setcoords(lat: Array[Double], lon: Array[Double]): Unit = {
    val geomcopy = _geom.zipWithIndex()
    geomcopy.map(t => {
      t._1.getCoordinate.x = lat(t._2.toInt)
      t._1.getCoordinate.y = lon(t._2.toInt)
    })
    _geom = geomcopy.map(t => t._1)
  }

  def setweight(bw:Double, kernel:String, adaptive:Boolean): Unit = {
    if (_dist == null) {
      _dist = getDist(shpRDD).map(t => Array2DenseVector(t))
      max_dist = _dist.map(t => max(t)).max
    }
    if(_kernel==null) {
      _kernel=kernel
      _adaptive=adaptive
    }
    spweight_dvec = _dist.map(t => getSpatialweightSingle(t, bw = bw, kernel = kernel, adaptive = adaptive))
  }

  def printweight(): Unit = {
    if (spweight_dvec != null) {
      spweight_dvec.foreach(println)
    }
  }
}
