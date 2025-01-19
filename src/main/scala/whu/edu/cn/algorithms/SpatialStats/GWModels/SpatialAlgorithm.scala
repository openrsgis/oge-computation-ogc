package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{DenseMatrix, DenseVector, max}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance.getDistRDD
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.getGeometry

import scala.collection.mutable

class SpatialAlgorithm(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends Algorithm {

  protected var _shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = inputRDD

  protected var _rawX: Array[DenseVector[Double]] = _
  protected var _dvecX: Array[DenseVector[Double]] = _
  protected var _dmatX: DenseMatrix[Double] = _
  protected var _dvecY: DenseVector[Double] = _

  protected var _nameX: Array[String] = _
  protected var _nameY: String = _

  protected var _geom: RDD[Geometry] = _
  protected var _dist: RDD[Array[Double]] = _

  var _rows = 0
  var _cols = 0
  var _disMax = 0.0

  //构造函数会自动调用
  init(inputRDD)

  protected def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Unit = {
    _geom = getGeometry(inputRDD)
    _shpRDD = inputRDD
    if (_dist == null) {
      _dist = getDistRDD(_shpRDD)
      _disMax = _dist.map(t => max(t)).max
    }
  }

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(_shpRDD.map(t => t._2._2(s).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
    })
    _rawX = x
    _rows = x(0).length
    _cols = x.length + 1
    val onesVector = DenseVector.ones[Double](_rows)
    _dvecX = onesVector +: x
    _dmatX = DenseMatrix.create(rows = _rows, cols = _cols, data = _dvecX.flatMap(_.toArray))
  }

  override def setY(property: String): Unit = {
    _nameY = property
    _dvecY = DenseVector(_shpRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue).collect())
  }


}
