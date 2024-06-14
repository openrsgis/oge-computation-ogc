package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{IntArrayTile, MultibandTile, Tile}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable.ListBuffer

object HistogramEqualization {
  //再一次
  //直方图均衡化 局部计算
  def HistogramEqualization(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    //求整张影像像素总数
    val totalPixelCount: Int = findTotalPixel(coverage)
    //定义一个查询字典，调用MathTools函数查询整张影像的每个波段的像素最大最小值（根据像素值类型）
    val bandMinMax: Map[Int, (Int, Int)] = findminmaxValue(coverage)

    //计算单波段的像素值累计频率
    def PixelFrequency_single(image: IntArrayTile, bandIndex: Int): Array[Int] = {
      val Rows = image.rows
      val Cols = image.cols
      // 像素值的取值范围存储在bandMinMax字典中 Map[Int, (Int, Int)]
      // 灰度级数
      val (minValue, maxValue) = bandMinMax(bandIndex)
      val levels: Int = maxValue - minValue + 1 //TODO 后面考虑是否有必要用Long，或根据情况判断
      // 计算每个像素灰度值的频率
      val histogram = Array.ofDim[Int](levels)
      for (i <- 0 until Rows; j <- 0 until Cols) {
        val pixel = image.get(j, i)
        histogram(pixel - minValue) += 1
      }
      //print("histogram(0)", histogram(0))
      //计算累计像素值频率
      val cumuHistogram = Array.ofDim[Int](levels)
      cumuHistogram(0) = histogram(0)
      for(i <- 1 until levels){
        cumuHistogram(i) = histogram(i) + cumuHistogram(i-1)
      }
      cumuHistogram
    }

    //返回RDD[(波段索引，像素值), 累计像素频率)]
    val bandPixelFrequency: RDD[((Int, Int), Int)] = coverage._1.flatMap(imageRdd => {
      val bandCount: Int = imageRdd._2.bandCount
      val PixelFrequency: ListBuffer[((Int, Int), Int)] = ListBuffer.empty[((Int, Int), Int)]
      for (bandIndex <- 0 until bandCount) {
        val tile: Tile = imageRdd._2.band(bandIndex) //TODO 暂时使用整型，因为直方图均衡化好像必须得是整型
        val bandTile: IntArrayTile = tile match {
          case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
          case _ => IntArrayTile(tile.toArray, tile.cols, tile.rows) // 否则，将 Tile 转换为 IntArrayTile
        }
        val bandPixelFrequecy: Array[Int] = PixelFrequency_single(bandTile, bandIndex)
        for (i <- 0 until bandPixelFrequecy.length) {
          PixelFrequency.append(((bandIndex, i + bandMinMax(bandIndex)._1), bandPixelFrequecy(i)))
        }
      }
      PixelFrequency.toList
    })

    //整张影像的[(波段索引，像素值), 像素频率)]
    val bandPixelFrequencyToCount: RDD[((Int, Int), Int)] = bandPixelFrequency.reduceByKey(_ + _)
    //字典，方便查询
    val collectDictionary: Map[(Int, Int), Int] = bandPixelFrequencyToCount.collect().toMap

    //.map 更新每个像素值
    val newCoverage: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(imageRdd=>{
      val Rows = imageRdd._2.rows
      val Cols = imageRdd._2.cols
      val bandCount = imageRdd._2.bandCount
      val band_intArrayTile = Array.ofDim[IntArrayTile](bandCount)
      // 计算均衡化后的像素值
      for(bandIndex <- 0 until bandCount){
        val intArrayTile: IntArrayTile = IntArrayTile.empty(Cols, Rows) //放在bandIndex循环前，会导致每波段得到的值相同！！！
        val tile: Tile = imageRdd._2.band(bandIndex)
        val bandTile: IntArrayTile = tile match {
          case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
          case _ => IntArrayTile(tile.toArray, tile.cols, tile.rows) // 否则，将 Tile 转换为 IntArrayTile
        }
        val (minValue, maxValue) = bandMinMax(bandIndex)
        val levels: Int = maxValue - minValue + 1
        //println(minValue, maxValue)
        for(i <- 0 until Rows; j <- 0 until Cols){
          val pixel: Int = bandTile.get(j, i)
          val frequency = collectDictionary((bandIndex, pixel))
          intArrayTile.set(j, i, (frequency.toDouble/totalPixelCount * levels + minValue).toInt)
        }
        band_intArrayTile(bandIndex) = intArrayTile
      }
      //组合各波段的运算结果
      (imageRdd._1, MultibandTile(band_intArrayTile.toList))
    })
    (newCoverage, coverage._2)
  }

  def findTotalPixel(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Int = {
    val eachCount: RDD[Int] = coverage._1.map(t => {
      t._2.rows * t._2.cols
    })
    val totalPixelCount: Int = eachCount.reduce(_ + _)
    totalPixelCount
  }
  def findminmaxValue(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Map[Int, (Int, Int)] = {
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
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val courage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = makeChangedRasterRDDFromTif(sc: SparkContext,"C:/Users/HUAWEI/Desktop/oge/coverage_resources/collection2coverage.tiff")
    val newRDD: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = HistogramEqualization(courage)
    saveRasterRDDToTif(newRDD,"C:/Users/HUAWEI/Desktop/oge/coverage_resources1/Histogram0411.tiff")
    sc.stop()
  }
}
