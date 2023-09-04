package whu.edu.cn.geocube.application.cloud

import cern.colt.matrix.impl.{DenseDoubleMatrix2D, SparseDoubleMatrix2D}
import cern.colt.matrix.{DoubleMatrix1D, DoubleMatrix2D}
import cern.jet.math.Functions._
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

/**
 * A high-dimensional median, which is introduced in the paper -
 * "C. G. Small, "A survey of multidimensional medians," Int. Stat. Rev., vol. 58, no. 3, pp. 263–277, 1990"
 *
 * */
object Medoid{

  /**
   * A high-dimensional median
   *
   * @param a Input high-dimensional matrix
   * @param axis 0 or 1 will generate 1 * m or n * 1 median matrix
   *
   * @return A one-dimensional matrix
   */
  def medoid(a:DoubleMatrix2D, axis:Int = 1):DoubleMatrix1D = {
    if(axis == 1){
      var minColumn: Int = -1
      var minDistance: Double = Double.MaxValue
      for(i <- 0 until a.columns()){
        val srcCol = a.viewColumn(i)
        var distance = 0.0
        for(j <- 0 until i) distance += srcCol.aggregate(a.viewColumn(j), plus, chain(square, minus))
        for(j <- i until a.columns()) distance += srcCol.aggregate(a.viewColumn(j), plus, chain(square, minus))
        if(distance < minDistance){
          minDistance = distance
          minColumn = i
        }
      }
      a.viewColumn(minColumn)
    }
    else if(axis == 0){
      var minRow: Int = -1
      var minDistance: Double = Double.MaxValue
      for(i <- 0 until a.rows()){
        val srcRow = a.viewRow(i)
        var distance = 0.0
        for(j <- 0 until i) distance += srcRow.aggregate(a.viewRow(j), plus, chain(square, minus))
        for(j <- i until a.rows()) distance += srcRow.aggregate(a.viewRow(j), plus, chain(square, minus))
        if(distance < minDistance){
          minDistance = distance
          minRow = i
        }
      }
      a.viewRow(minRow)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * A high-dimensional median which handle NaN value
   *
   * @param a Input high-dimensional matrix
   * @param axis 0 or 1 will generate 1 * m or n * 1 median matrix
   *
   * @return A one-dimensional matrix
   */
  def nanMedoid(a:DoubleMatrix2D, axis:Int = 1):DoubleMatrix1D = {
    val columnArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    (0 until a.columns()).foreach(columnArray += _)
    val rowArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    (0 until a.rows()).foreach(rowArray += _)

    if(axis == 1){
      var minColumn: Int = -1
      var nanColumns: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      var minDistance: Double = Double.MaxValue
      val columnList = columnArray.toList

      for(i <- 0 until a.columns()){
        if(a.viewColumn(i).toArray().exists(_.isNaN))
          nanColumns += i
      }
      val validColumns = columnList.diff(nanColumns)
      for(i <- validColumns){
        val srcCol = a.viewColumn(i)
        var distance = 0.0
        val dstColumns = validColumns.diff(Array(i))
        for(j <- dstColumns) distance += srcCol.aggregate(a.viewColumn(j), plus, chain(square, minus))
        if(distance < minDistance){
          minDistance = distance
          minColumn = i
        }

      }
      a.viewColumn(minColumn)
    }
    else if(axis == 0){
      var minRow: Int = -1
      var nanRows: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      var minDistance: Double = Double.MaxValue
      val rowList = rowArray.toList

      for(i <- 0 until a.rows()){
        if(a.viewRow(i).toArray().exists(_.isNaN))
          nanRows += i
      }
      val validRows = rowList.diff(nanRows)
      for(i <- validRows){
        val srcRow = a.viewRow(i)
        var distance = 0.0
        val dstRows = validRows.diff(Array(i))
        for(j <- dstRows) distance += srcRow.aggregate(a.viewRow(j), plus, chain(square, minus))
        if(distance < minDistance){
          minDistance = distance
          minRow = i
        }

      }
      a.viewRow(minRow)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  def main(args:Array[String]):Unit = {
    try{
      val matrix = new SparseDoubleMatrix2D(3,4)
      matrix.assign(Array[Array[Double]](Array(1, 2, 3, 4), Array(5, Double.NaN, 7, 8), Array(9, 10, 11, 12)))
      val medoidMatrix:DoubleMatrix1D = nanMedoid(matrix, 0)
      println(medoidMatrix)

      println("Hit enter to exit")
      StdIn.readLine()
    }finally {

    }

  }
}


