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

  private var shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _
  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _

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
    println(s"\ndiagnostics:\nSSE is $rss\nAIC is $AIC \nAICc is $AICc\nedf is $edf \nenp is $enp\nR2 is $r2\nadjust R2 is $r2_adj")
  }

  def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Unit = {
    _geom = getGeometry(inputRDD)
    shpRDD = inputRDD
    if (_dist == null) {
      _dist = getDist(shpRDD).map(t => Array2DenseVector(t))
      max_dist = _dist.map(t => max(t)).max
    }
  }

  protected def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
  }

  protected def setY(y: Array[Double]): Unit = {
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
