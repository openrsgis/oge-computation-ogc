package whu.edu.cn.geocube.application.cloud

import cern.colt.matrix.impl.{DenseDoubleMatrix1D, DenseDoubleMatrix2D}
import cern.colt.matrix.{DoubleMatrix1D, DoubleMatrix2D}
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn


/**
 * An one-dimensional median
 * */
object Median{
  /**
   * An one-dimensional median
   *
   * @param a Input one-dimensional matrix
   * @return Median result
   */
  def median(a:DoubleMatrix1D):Double = {
    val size = a.size()
    var minDistance: Double = Double.MaxValue
    var minIndex: Int = -1
    for(i <- 0 until size){
      var distance = 0.0
      for(j <- 0 until size){
        distance += (a.getQuick(i) - a.getQuick(j)) * (a.getQuick(i) - a.getQuick(j))
      }
      if(distance < minDistance){
        minDistance = distance
        minIndex = i
      }
    }
    a.getQuick(minIndex)
  }

  /**
   * An one-dimensional median which handles NaN value
   *
   * @param a Input one-dimensional matrix
   * @return Median result
   */
  def nanmedian(a:DoubleMatrix1D):Double = {
    val size = a.size()
    val indexArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    (0 until size).foreach(indexArray += _)
    val indexList = indexArray.toList
    var nanindexs: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for(i <- 0 until size){
      if(a.getQuick(i).isNaN)
        nanindexs += i
    }
    val validIndexs = indexList.diff(nanindexs)
    var minDistance: Double = Double.MaxValue
    var minIndex: Int = -1
    for(i <- validIndexs){
      var distance = 0.0
      for(j <- validIndexs){
        distance += (a.getQuick(i) - a.getQuick(j)) * (a.getQuick(i) - a.getQuick(j))
      }
      if(distance < minDistance){
        minDistance = distance
        minIndex = i
      }
    }
    a.getQuick(minIndex)
  }

  /**
   * Applying one-dimensional median to each dimension in high-dimensional matrix
   *
   * @param a Input high-dimensional matrix
   * @return A one-dimensional matrix
   */
  def median(a:DoubleMatrix2D, axis:Int = 1):DoubleMatrix1D = {
    if(axis == 0){
      val medianArray = new ArrayBuffer[Double]()
      for(i <- 0 until a.columns()) medianArray += median(a.viewColumn(i))
      new DenseDoubleMatrix1D (medianArray.toArray)
    }
    else if(axis == 1){
      val medianArray = new ArrayBuffer[Double]()
      for(i <- 0 until a.rows()) medianArray += median(a.viewRow(i))
      new DenseDoubleMatrix1D (medianArray.toArray)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * Applying one-dimensional median to each dimension in high-dimensional matrix which handles NaN value
   *
   * @param a Input high-dimensional matrix
   * @return A one-dimensional matrix
   */
  def nanmedian(a:DoubleMatrix2D, axis:Int = 1):DoubleMatrix1D = {
    if(axis == 0){
      val medianArray = new ArrayBuffer[Double]()
      for(i <- 0 until a.columns()) medianArray += nanmedian(a.viewColumn(i))
      new DenseDoubleMatrix1D (medianArray.toArray)
    }
    else if(axis == 1){
      val medianArray = new ArrayBuffer[Double]()
      for(i <- 0 until a.rows()) medianArray += nanmedian(a.viewRow(i))
      new DenseDoubleMatrix1D (medianArray.toArray)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  def main(args:Array[String]):Unit = {
    try{
      val matrix = new DenseDoubleMatrix2D(3,4)
      matrix.assign(Array[Array[Double]](Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12)))
      val nanmatrix = new DenseDoubleMatrix2D(3,4)
      nanmatrix.assign(Array[Array[Double]](Array(1, 2, 3, 4), Array(Double.NaN, 6, 7, 8), Array(9, 10, 11, 12)))
      println(median(matrix, 1))
      println(nanmedian(nanmatrix, 1))

      println("Hit enter to exit")
      StdIn.readLine()
    }finally {

    }
  }
}

