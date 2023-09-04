package whu.edu.cn.geocube.application.cloud

import cern.colt.matrix.impl.{DenseDoubleMatrix1D, DenseDoubleMatrix2D, SparseDoubleMatrix1D, SparseDoubleMatrix2D}
import cern.colt.matrix.{DoubleMatrix1D, DoubleMatrix2D}
import cern.jet.math.Functions._
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import util.control.Breaks._

/**
 * A high-dimensional median, which is introduced in the paper -
 * "Roberts D , Mueller N , Mcintyre A . High-Dimensional Pixel
 * Composites From Earth Observation Time Series[J]. IEEE Transactions
 * on Geoscience & Remote Sensing, 2017, 55(11):6254-6264."
 *
 * */

object Geomedian{
  /**
   * A high-dimensional median
   *
   * @param a Input high-dimensional matrix
   * @param axis 0 or 1 will generate 1 * m or n * 1 median matrix
   * @param eps
   * @param maxiters Maximum iterations
   * @return A one-dimensional matrix
   */
  def geomedian(a: DoubleMatrix2D, axis:Int = 1, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    if(axis == 1)
      geomedian_axis_one(a, eps, maxiters)
    else if(axis == 0)
      geomedian_axis_zero(a, eps, maxiters)
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * A high-dimensional median which handles NaN value
   *
   * @param a Input high-dimensional matrix
   * @param axis 0 or 1 will generate 1 * m or n * 1 median matrix
   * @param eps
   * @param maxiters Maximum iterations
   * @return A one-dimensional matrix
   */
  def nangeomedian(a: DoubleMatrix2D, axis:Int = 1, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    if(axis == 1)
      nangeomedian_axis_one(a, eps, maxiters)
    else if(axis == 0)
      nangeomedian_axis_zero(a, eps, maxiters)
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * Dot calculation between two matrix.
   *
   * @param a 1-dimensional double-type matrix
   * @param b 1-dimensional double-type matrix
   * @return a value of double type
   */
  def dot(a: DoubleMatrix1D, b: DoubleMatrix1D): Double = a.aggregate(b, plus, mult)

  /**
   * Sum calculation of a 1-dimensional double-type matrix.
   *
   * @param a 1-dimensional double-type matrix
   * @return a value of double type
   */
  def sum(a: DoubleMatrix1D): Double = a.zSum()

  /**
   * Distance euclidean calculation between two matrix.
   *
   * @param a 1-dimensional double-type matrix
   * @param b 1-dimensional double-type matrix
   * @return a value of double type
   */
  def distEuclidean(a: DoubleMatrix1D, b: DoubleMatrix1D):Double =
    Math.sqrt(a.aggregate(b, plus, chain(square, minus)))

  /**
   * Norm euclidean calculation.
   *
   * @param a 1-dimensional double-type matrix
   * @return a value of double type
   */
  def normEuclidean(a: DoubleMatrix1D):Double =
    Math.sqrt(a.aggregate(plus, square))

  /**
   * Sum calculation of a 1-dimensional double-type matrix with nan value.
   *
   * @param a 1-dimensional double-type matrix
   * @return a value of double type
   */
  def nansum(a: DoubleMatrix1D): Double = {
    val size = a.size()
    var sum = 0.0
    for(i <- 0 until size){
      if(!a.getQuick(i).isNaN) sum += a.getQuick(i)
    }
    sum
  }

  /**
   * Distance euclidean calculation between two matrix with nan value.
   *
   * @param a 1-dimensional double-type matrix
   * @param b 1-dimensional double-type matrix
   * @return a value of double type
   */
  def dist_naneuclidean(a: DoubleMatrix1D, b: DoubleMatrix1D):Double = {
    val size = a.size()
    var sum = 0.0
    var tmp = 0.0
    for(i <- 0 until size){
      if(!a.getQuick(i).isNaN && !b.getQuick(i).isNaN){
        tmp = a.getQuick(i) - b.getQuick(i)
        sum += tmp * tmp
      }
    }
    Math.sqrt(sum)
  }

  /**
   * Mean calculation along one axis of a 2-dimensional double-type matrix.
   *
   * @param a 2-dimensional double-type matrix
   * @param axis
   * @return a 1-dimensional double-type matrix
   */
  def mean(a: DoubleMatrix2D, axis: Int = 1):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    val meanArray = new ArrayBuffer[Double]()
    if(axis == 1){
      for(i <- 0 until rows) meanArray += a.viewRow(i).zSum()/cols
      new DenseDoubleMatrix1D (meanArray.toArray)
    }
    else if(axis == 0){
      for(i <- 0 until cols) meanArray += a.viewColumn(i).zSum()/rows
      new DenseDoubleMatrix1D (meanArray.toArray)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * Mean calculation along one axis of a 2-dimensional double-type matrix with nan value.
   *
   * @param a 2-dimensional double-type matrix
   * @param axis
   * @return a 1-dimensional double-type matrix
   */
  def nanmean(a: DoubleMatrix2D, axis: Int = 1):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    val meanArray = new ArrayBuffer[Double]()
    if(axis == 1){
      for(i <- 0 until rows) {
        var sum = 0.0
        var count = 0.0
        for(j <- 0 until cols){
          if(!a.getQuick(i, j).isNaN){
            sum += a.getQuick(i, j)
            count += 1
          }
        }
        meanArray += sum / count
      }
      new DenseDoubleMatrix1D (meanArray.toArray)
    }
    else if(axis == 0){
      for(i <- 0 until cols) {
        var sum = 0.0
        var count = 0.0
        for(j <- 0 until rows){
          if(!a.getQuick(j, i).isNaN){
            sum += a.getQuick(j, i)
            count += 1
          }
        }
        meanArray += sum / count
      }
      new DenseDoubleMatrix1D (meanArray.toArray)
    }
    else
      throw new RuntimeException("axis is 0 or 1!")
  }

  /**
   * Geomedian calculation along zero-axis of a 2-dimensional double-type matrix.
   *
   * @param a 2-dimensional double-type matrix
   * @param eps
   * @param maxiters max iteration number
   * @return 1-dimensional double-type matrix
   */
  def geomedian_axis_zero(a: DoubleMatrix2D, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    var b = mean(a, 0)

    if(rows == 1) b

    var D:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var Dinv:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var W:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var T:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var y1:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var R:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))

    var dist, Dinvs, total, r, rinv, tmp, Di = 0.0
    var nzeros = rows
    var iteration = 0

    breakable{
      while(iteration < maxiters){
        for(i <- 0 until rows){
          Di = distEuclidean(a.viewRow(i), b)
          if (Math.abs(Di) > eps) Dinv.setQuick(i, 1.0 / Di)
          else Dinv.setQuick(i, 0.0)
          D.setQuick(i, Di)
        }

        Dinvs = sum(Dinv)

        for(i <- 0 until rows) W.setQuick(i, Dinv.getQuick(i) / Dinvs)

        for(j <- 0 until cols){
          total = 0.0
          for(i <- 0 until rows){
            if (Math.abs(D.getQuick(i)) > eps)
              total += W.getQuick(i) * a.getQuick(i, j)
          }
          T.setQuick(j, total)
        }

        nzeros = rows
        for(i <- 0 until rows){
          if(Math.abs(D.getQuick(i)) > eps) nzeros -= 1
        }

        if (nzeros == 0) y1 = T
        else if (nzeros == rows) break()
        else {
          for(j <- 0 until cols) R.setQuick(j, (T.getQuick(j) - b.getQuick(j)) * Dinvs)
          r = normEuclidean(R)
          if(r > eps) rinv = nzeros / r
          else rinv = 0.0
          for(j <- 0 until cols)
            y1.setQuick(j, Math.max(0.0, 1-rinv)*T.getQuick(j) + Math.min(1.0, rinv)*b.getQuick(j))
        }

        dist = distEuclidean(b, y1)
        if(dist < eps) break()

        b = y1
        iteration += 1
      }
    }
    b
  }

  /**
   * Geomedian calculation along one-axis of a 2-dimensional double-type matrix.
   *
   * @param a 2-dimensional double-type matrix
   * @param eps
   * @param maxiters max iteration number
   * @return 1-dimensional double-type matrix
   */
  def geomedian_axis_one(a: DoubleMatrix2D, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    var b = mean(a, 1)

    if(cols == 1) b

    var D:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var Dinv:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var W:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var T:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var y1:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var R:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))

    var dist, Dinvs, total, r, rinv, tmp, Di = 0.0
    var nzeros = rows
    var iteration = 0

    breakable{
      while(iteration < maxiters){
        for(i <- 0 until cols){
          Di = distEuclidean(a.viewColumn(i), b)
          D.setQuick(i, Di)
          if (Math.abs(Di) > eps) Dinv.setQuick(i, 1.0 / Di)
          else Dinv.setQuick(i, 0.0)
        }

        Dinvs = sum(Dinv)

        for(i <- 0 until cols) W.setQuick(i, Dinv.getQuick(i) / Dinvs)

        for(j <- 0 until rows){
          total = 0.0
          for(i <- 0 until cols){
            if (Math.abs(D.getQuick(i)) > eps)
              total += W.getQuick(i) * a.getQuick(j, i)
          }
          T.setQuick(j, total)
        }

        nzeros = cols
        for(i <- 0 until cols){
          if(Math.abs(D.getQuick(i)) > eps) nzeros -= 1
        }

        if (nzeros == 0) y1 = T
        else if (nzeros == cols) break()
        else {
          for(j <- 0 until rows) R.setQuick(j, (T.getQuick(j) - b.getQuick(j)) * Dinvs)
          r = normEuclidean(R)
          if(r > eps) rinv = nzeros / r
          else rinv = 0.0
          for(j <- 0 until rows)
            y1.setQuick(j, Math.max(0.0, 1-rinv)*T.getQuick(j) + Math.min(1.0, rinv)*b.getQuick(j))
        }

        dist = distEuclidean(b, y1)
        if(dist < eps) break()

        b = y1
        iteration += 1
      }
    }
    b
  }

  /**
   * Geomedian calculation along zero-axis of a 2-dimensional double-type matrix with nan value.
   * There are some bugs to fix.
   *
   * @param a 2-dimensional double-type matrix
   * @param eps
   * @param maxiters max iteration number
   * @return 1-dimensional double-type matrix
   */
  def nangeomedian_axis_zero(a: DoubleMatrix2D, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    val nan = Double.NaN
    var b = nanmean(a, 0)
    var D:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var Dinv:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var W:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var T:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var y1:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var R:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))

    var dist, Dinvs, total, r, rinv, tmp, Di = 0.0
    var nzeros = rows
    var iteration = 0

    breakable{
      while(iteration < maxiters){
        for(i <- 0 until rows){
          Di = dist_naneuclidean(a.viewRow(i), b)
          if (Math.abs(Di) > 0) Dinv.setQuick(i, 1.0 / Di)
          else Dinv.setQuick(i, nan)
          D.setQuick(i, Di)
        }

        Dinvs =nansum(Dinv)

        for(i <- 0 until rows) W.setQuick(i, Dinv.getQuick(i) / Dinvs)

        for(j <- 0 until cols){
          total = 0.0
          for(i <- 0 until rows){
            tmp = W.getQuick(i) * a.getQuick(i, j)
            if(!tmp.isNaN)
              total += tmp
          }
          T.setQuick(j, total)
        }

        nzeros = rows
        for(i <- 0 until rows){
          if(D.getQuick(i).isNaN) nzeros -= 1
          else if(Math.abs(D.getQuick(i)) > 0.0) nzeros -= 1
        }

        if (nzeros == 0) y1 = T
        else if (nzeros == rows) break()
        else {
          for(j <- 0 until cols) R.setQuick(j, (T.getQuick(j) - b.getQuick(j)) * Dinvs)
          r = normEuclidean(R)
          if(r > 0) rinv = nzeros / r
          else rinv = 0.0
          for(j <- 0 until cols)
            y1.setQuick(j, Math.max(0.0, 1-rinv)*T.getQuick(j) + Math.min(1.0, rinv)*b.getQuick(j))
        }

        dist = dist_naneuclidean(b, y1)
        if(dist < eps) break()

        b = y1
        iteration += 1
      }
    }
    b
  }

  /**
   * Geomedian calculation along one-axis of a 2-dimensional double-type matrix with nan value.
   * There are some bugs to fix.
   *
   * @param a 2-dimensional double-type matrix
   * @param eps
   * @param maxiters max iteration number
   * @return 1-dimensional double-type matrix
   */
  def nangeomedian_axis_one(a: DoubleMatrix2D, eps: Double = 1e-7, maxiters: Int = 500):DoubleMatrix1D = {
    val rows = a.rows()
    val cols = a.columns()
    var b = nanmean(a, 1)
    val nan = Double.NaN

    var D:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var Dinv:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var W:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(cols)(0.0))
    var T:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var y1:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))
    var R:DenseDoubleMatrix1D = new DenseDoubleMatrix1D(Array.fill(rows)(0.0))

    var dist, Dinvs, total, r, rinv, tmp, Di = 0.0
    var nzeros = rows
    var iteration = 0

    breakable{
      while(iteration < maxiters){
        for(i <- 0 until cols){
          Di = dist_naneuclidean(a.viewColumn(i), b)
          if (Math.abs(Di) > 0.0) Dinv.setQuick(i, 1.0 / Di)
          else Dinv.setQuick(i, nan)
          D.setQuick(i, Di)
        }

        Dinvs = nansum(Dinv)

        for(i <- 0 until cols) W.setQuick(i, Dinv.getQuick(i) / Dinvs)

        for(j <- 0 until rows){
          total = 0.0
          for(i <- 0 until cols){
            tmp = W.getQuick(i) * a.getQuick(j, i)
            if (!tmp.isNaN)
              total += tmp
          }
          T.setQuick(j, total)
        }

        nzeros = cols
        for(i <- 0 until cols){
          if (D.getQuick(i).isNaN) nzeros -= 1
          if(Math.abs(D.getQuick(i)) > 0.0) nzeros -= 1
        }

        if (nzeros == 0) y1 = T
        else if (nzeros == cols) break()
        else {
          for(j <- 0 until rows) R.setQuick(j, (T.getQuick(j) - b.getQuick(j)) * Dinvs)
          r = normEuclidean(R)
          if(r > 0.0) rinv = nzeros / r
          else rinv = 0.0
          for(j <- 0 until rows)
            y1.setQuick(j, Math.max(0.0, 1-rinv)*T.getQuick(j) + Math.min(1.0, rinv)*b.getQuick(j))
        }

        dist = dist_naneuclidean(b, y1)
        if(dist < eps) break()

        b = y1
        iteration += 1
      }
    }
    b
  }

  /**
   * API test
   */
  def main(args:Array[String]):Unit = {
    try{
      val a = new DenseDoubleMatrix1D( Array[Double](0, 1, 2, 3) )
      val b = new DenseDoubleMatrix1D( Array[Double](4, 5, 6, 7) )
      val c = new DenseDoubleMatrix1D( Array[Double](Double.NaN, 9, 10, 11) )

      val matrix = new SparseDoubleMatrix2D(3,4)
      matrix.assign(Array[Array[Double]](Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12)))
      println(geomedian_axis_zero(matrix))
      println(geomedian(matrix, 0))
      println(geomedian_axis_one(matrix))
      println(geomedian(matrix, 1))

      val nanmatrix = new SparseDoubleMatrix2D(3,4)
      nanmatrix.assign(Array[Array[Double]](Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(Double.NaN, Double.NaN, Double.NaN, Double.NaN)))
      println(nangeomedian_axis_zero(nanmatrix))
      println(nangeomedian(nanmatrix, 0))
      println(nangeomedian_axis_one(nanmatrix))
      println(nangeomedian(nanmatrix, 1))

      println("Hit enter to exit")
      StdIn.readLine()
    }finally {

    }
  }

}
