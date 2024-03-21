package whu.edu.cn.algorithms.ImageProcess.core

import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, MultibandTile, NODATA, Tile, isNoData}
import org.apache.spark.rdd.RDD
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey
//import whu.edu.cn.geocube.core.entity.SpaceTimeBandKey

import scala.collection.mutable.ListBuffer
import scala.math.sqrt

object MathTools {

  def findSpatialKeyMinMax(coverage: RDDImage): (Int, Int) = {
    val rowCol: RDD[(Int, Int)] = coverage._1.map(t => {
      val row: Int = t._1.spaceTimeKey.spatialKey.row
      val col: Int = t._1.spaceTimeKey.spatialKey.col
      (row, col)
    })
    val rowColMax: (Int, Int) = rowCol.reduce((a, b) => {
      (math.max(a._1, b._1), math.max(a._2, b._2))
    })
    //map里面运算的row和col好像带不出来
    (rowColMax._1 + 1, rowColMax._2 + 1)
  }
  def findMinMaxValueDouble(coverage: RDDImage): Map[Int, (Double, Double)] = {
    //计算每个瓦片的最大最小值
    val minmaxValue: RDD[(Int, (Double, Double))] = coverage._1.flatMap(t => {
      val result = new ListBuffer[(Int, (Double, Double))]()
      val bandCount: Int = t._2.bandCount
      for (bandIndex <- 0 until bandCount) {
        val tile: Tile = t._2.band(bandIndex)
        result.append((bandIndex, tile.findMinMaxDouble))
      }
      result.toList
    })
    //计算每个波段最大最小值
    val bandMinMax: Map[Int, (Double, Double)] = minmaxValue.reduceByKey((a, b) => {
      var min = Double.NaN
      var max = Double.NaN
      if (isNoData(a._1)) min = b._1
      else if (isNoData(b._1)) min = a._1
      else min = math.min(a._1, b._1)
      if (isNoData(a._2)) max = b._2
      else if (isNoData(b._2)) max = a._2
      else max = math.max(a._2, b._2)
      (min, max)
    }).collect().toMap
    bandMinMax
  }
  def findMinMaxValue(coverage: RDDImage): Map[Int, (Int, Int)] = {
    //计算每个瓦片的最大最小值
    val minmaxValue: RDD[(Int, (Int, Int))] = coverage._1.flatMap(t => {
      val result = new ListBuffer[(Int, (Int, Int))]()
      val bandCount: Int = t._2.bandCount
      for (bandIndex <- 0 until bandCount) {
        val tile: Tile = t._2.band(bandIndex)
        val bandTile: IntArrayTile = IntArrayTile(tile.toArray, tile.cols, tile.rows)
        result.append((bandIndex, bandTile.findMinMax))
      }
      result.toList
    })

    //计算每个波段最大最小值
    val bandMinMax: Map[Int, (Int, Int)] = minmaxValue.reduceByKey((a, b) => {
      (math.min(a._1, b._1), math.max(a._2, b._2))
    }).collect().toMap

    bandMinMax
  }


  def globalNormalizeDouble(coverage: RDDImage, nmin: Double, nmax: Double): RDDImage = {
    val minAndMax = findMinMaxValueDouble(coverage)

    val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(
      image => {
        val rows = image._2.rows
        val cols = image._2.cols
        val bandCount = image._2.bandCount
        val band_Array = Array.ofDim[Tile](bandCount)
        for (bandIndex <- 0 until bandCount) {
          val tile = image._2.band(bandIndex)
          val min = minAndMax(bandIndex)._1
          val max = minAndMax(bandIndex)._2
//          println(min,max)
          val result = Array.ofDim[Int](rows, cols)
          for (i <- 0 until rows; j <- 0 until cols) {
            result(i)(j) = ((tile.getDouble(j, i) - min) * (nmax - nmin) / (max - min) + nmin).toInt
            //            println(result(i)(j),tile.getDouble(j, i))
          }
          band_Array(bandIndex) = IntArrayTile(result.flatten, cols, rows)
        }
        (image._1, MultibandTile(band_Array))
      })
    (newRDD, coverage._2)
  }
  def OTSU(coverage: RDDImage): Int = {
    val pixel: Array[(Int, Int)] = coverage._1.flatMap(
      image => {
        val result = new ListBuffer[(Int, Int)]()
        val tile: Tile = image._2.band(0)
        for (i <- 0 until tile.cols; j <- 0 until tile.rows) {
          val pixelValue = tile.get(j, i)
          if ((pixelValue != NODATA) & (pixelValue != 0)) result.append((pixelValue, 1))
          //          if ((pixelValue != NODATA)) result.append((pixelValue, 1))

        }
        result.toList
      }
    ).reduceByKey(
      (x, y) => {
        x + y
      }
    ).collect().sortBy(_._1)

    val sum = pixel.foldLeft(0)((accumulatedSum, tuple) => accumulatedSum + tuple._2)
    //println(s"图像像素点总数为${sum}")

    //val pro = pixel.map { case (first, second) => (first, second.toDouble / sum) }
    val blankPro = new ListBuffer[(Int, Double)]()
    for (element <- pixel) {
      val probability = element._2.toDouble / sum
      blankPro.append((element._1, probability))
    }
    val pro = blankPro.toList
    pro.foreach(println)

    var t: Int = 0
    var threshold: Int = 0
    var dev: Double = 0
    var v: Double = 0

    while (t <= 255) {
      var i: Int = 0
      var w0: Double = 0
      var w1: Double = 0
      while (i < pro.length) {
        if (pro(i)._1 <= t) w0 += pro(i)._2
        i += 1
      }
      w1 = 1 - w0
      //println(s"w0 = ${w0}, w1 = ${w1}")
      var j: Int = 0
      var u0: Double = 0
      var u1: Double = 0
      while (j < pro.length) {
        if (pro(j)._1 <= t) u0 += (pro(j)._2 * pro(j)._1)
        else u1 += (pro(j)._2 * pro(j)._1)
        j += 1
      }
      //println(s"u0 = ${u0}, u1 = ${u1}")
      //println("---" * 10)
      v = w0 * w1 * math.pow(u1 - u0, 2)
      if (v > dev) {
        dev = v
        threshold = t
      }
      t += 1
    }

    threshold

  }
  //reduce计算平均值
  private def processMeanValue(newRDD: RDD[(Int, (Double, Int))]): Map[Int, Double] = {
    val mean_Map: Map[Int, Double] = newRDD.reduceByKey(
      (x, y) => (x._1 + y._1, x._2 + y._2)
    ).map(
      t => (t._1, (t._2._1 / t._2._2).toDouble)
    ).collect().toMap
    mean_Map
  }

  def meanValueCalculate(coverage: RDDImage): Map[Int, Double] = {
    //计算每个波段单个瓦片的像素平均值
    val mean_RDD: RDD[(Int, (Double, Int))] = coverage._1.flatMap(
      image => {
        val result = new ListBuffer[(Int, (Double, Int))]()
        val bandCount = image._2.bandCount
        val pixelList = new ListBuffer[Int]()
        for (bandIndex <- 0 until bandCount) {
          val tile: Tile = image._2.band(bandIndex)
          for (i <- 0 until tile.rows; j <- 0 until tile.cols)
            pixelList += tile.get(j, i)
          val mean: Double = (pixelList.sum).toDouble / (pixelList.length).toDouble
          result.append((bandIndex, (mean, 1)))
        }
        result.toList
      })
    //计算每个波段的全局平均值
    val mean_Band = processMeanValue(mean_RDD)
    mean_Band
  }


  def standardDeviationCalculate(coverage: RDDImage): Map[Int, Double] = {
    //计算每个波段的全局平均值
    val mean_Band = meanValueCalculate(coverage)

    //计算每个Tile的方差
    val variance_RDD: RDD[(Int, (Double, Int))] = coverage._1.flatMap(
      image => {
        val result = new ListBuffer[(Int, (Double, Int))]()
        val bandCount = image._2.bandCount
        val pixelList = new ListBuffer[Int]()
        for (bandIndex <- 0 until bandCount) {
          val tile: Tile = image._2.band(bandIndex)
          val mean_overall = mean_Band(bandIndex)
          for (i <- 0 until tile.rows; j <- 0 until tile.cols)
            pixelList += tile.get(j, i)
          val squaredDiffs = pixelList.map(x => {
            math.pow(x - mean_overall, 2)
          })
          val variance = (squaredDiffs.sum).toDouble / (pixelList.length).toDouble
          result.append((bandIndex, (variance, 1)))
        }
        result.toList
      })

    //计算每个波段的标准差
    val standardDeviation_Band = (processMeanValue(variance_RDD)).map(t => (t._1, (sqrt(t._2)).toDouble))
    standardDeviation_Band
  }


  //图像最大值最小值归一化
  def globalNormalize(coverage: RDDImage, nmin: Double, nmax: Double): RDDImage = {
    val minAndMax = findMinMaxValue(coverage)

    val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(
      image => {
        val rows = image._2.rows
        val cols = image._2.cols
        val bandCount = image._2.bandCount
        val band_Array = Array.ofDim[Tile](bandCount)
        for (bandIndex <- 0 until bandCount) {
          val tile = image._2.band(bandIndex)
          val min = minAndMax(bandIndex)._1
          val max = minAndMax(bandIndex)._2
          val result = Array.ofDim[Double](rows, cols)
          for (i <- 0 until rows; j <- 0 until cols) {
            result(i)(j) = tile.get(j, i) * (nmax - nmin) / (max - min) + nmin
          }
          band_Array(bandIndex) = DoubleArrayTile(result.flatten, cols, rows)
        }
        (image._1, MultibandTile(band_Array))
      })
    (newRDD, coverage._2)
  }
  //输出像素值比较
  def outputPixelValue(tiff1: MultibandGeoTiff , tiff2: MultibandGeoTiff )
  : Unit = {
    // 获取栅格数据总波段数
    println(tiff1.bandCount, tiff2.bandCount)
    //对第1波段进行比较
    val raster1: Tile = tiff1.tile.band(0)
    val raster2: Tile = tiff2.tile.band(0)
    if (raster1.cols == raster2.cols && raster1.rows == raster2.rows) {
      // 计算每个像素的差异，并创建一个新的栅格数据表示差异值
      val diffRaster: Tile = raster2.localSubtract(raster1)
      var count = 0
      for (row <- 0 until diffRaster.rows) {
        for (col <- 0 until diffRaster.cols) {
          val diffValue: Double = diffRaster.getDouble(col, row)
          if (math.abs(diffValue)>0) {
            count += 1
            //println(raster1.get(col, row), raster2.get(col, row))
            println(diffValue)
          }
        }
      }
      println("验证完成！", count)
    }
    else{
      println("大小不同！")
    }
  }


  def findTotalPixel(coverage: RDDImage): Int = { //TODO 暂用整型
    val eachCount: RDD[Int] = coverage._1.map(t => {
      t._2.rows * t._2.cols
    })
    val totalPixelCount: Int = eachCount.reduce(_ + _)
    totalPixelCount
  }
}
