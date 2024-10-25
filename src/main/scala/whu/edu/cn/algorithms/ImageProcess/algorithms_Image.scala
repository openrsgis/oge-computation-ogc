package whu.edu.cn.algorithms.ImageProcess
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import breeze.linalg.Tensor.transposeTensor
import breeze.linalg.{DenseMatrix, InjectNumericOps}
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{CellType, DoubleArrayTile, DoubleConstantNoDataCellType, Histogram, IntArrayTile, IntCellType, IntConstantNoDataCellType, MultibandTile, Tile, UShortConstantNoDataCellType, isNoData}
import geotrellis.spark.ContextRDD.tupleToContextRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.{PipelineModel, regression}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import whu.edu.cn.algorithms.ImageProcess.core.MathTools._
import whu.edu.cn.algorithms.ImageProcess.core.RDDTransformerUtil.paddingRDD
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.CoverageUtil.checkProjResoExtent

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
//import whu.edu.cn.geocube.core.entity.SpaceTimeBandKey

import scala.collection.mutable.ListBuffer
import scala.math.sqrt
import scala.util.control.Breaks.{break, breakable}
import whu.edu.cn.algorithms.MLlib._

object algorithms_Image {
  //双边滤波

  //高斯滤波
  def gaussianBlur(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), d: Int, sigmaX: Double, sigmaY: Double, borderType: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    //ksize：高斯核的大小，正奇数；sigmaX sigmaY：X和Y方向上的方差
    // 构建高斯核矩阵
    var kernel = Array.ofDim[Double](d, d)
    for (i <- 0 until d; j <- 0 until d) {
      val x = math.abs(i - d / 2) //模糊距离x
      val y = math.abs(j - d / 2) //模糊距离y
      kernel(i)(j) = math.exp(-x * x / (2 * sigmaX * sigmaX) - y * y / (2 * sigmaY * sigmaY)) //距离中心像素的模糊距离
    }
    // 归一化高斯核函数
    val sum = kernel.flatten.sum
    kernel = kernel.map(_.map(_ / sum))
    val radius: Int = d / 2
    val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(coverage, radius, borderType)
    val cellType: CellType = coverage._1.first()._2.cellType

    //遍历每个像素，对其进行高斯滤波
    def gaussian_single(image: Tile, kernel: Array[Array[Double]]): Tile = {
      val numRows = image.rows
      val numCols = image.cols
      val doubleArrayTile: DoubleArrayTile = DoubleArrayTile.empty(numCols, numRows)
      for (i <- radius until numRows - radius; j <- radius until numCols - radius) { //处理区域内的像素
        var sum: Double = 0.0
        var weightSum: Double = 1.0 //TODO 11.9 用于解决边缘像素进行高斯滤波，周边有nodata的情况，padding内的处理策略先没管
        breakable {
          for (k <- 0 until d; l <- 0 until d) {
            val x = i - d / 2 + k
            val y = j - d / 2 + l
            if (isNoData(image.getDouble(j, i))) {
              doubleArrayTile.setDouble(j, i, Double.NaN) // 因为newImage是Double的数组，所以即使赋给它NoData，它也会化为一个最小数
              break
            }
            if (isNoData(image.getDouble(y, x))) {
              sum += 0
              weightSum -= kernel(k)(l)
            }
            else sum += kernel(k)(l) * image.getDouble(y, x)
          }
          doubleArrayTile.setDouble(j, i, sum / weightSum) //TODO 权重相加，不需要再做除法，逻辑还有待确认
        }
      }
      doubleArrayTile
    }

    //遍历延宽像素后的瓦片
    val GaussianBlurRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(image => {
      val numRows = image._2.rows
      val numCols = image._2.cols
      //遍历每个波段，调用gaussian_single函数进行高斯滤波
      val bandCount: Int = image._2.bandCount
      val band_ArrayTile = Array.ofDim[Tile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val tile: Tile = image._2.band(bandIndex) //TODO 保留Tile类型，后面参考下opencv
        band_ArrayTile(bandIndex) = gaussian_single(tile, kernel).convert(cellType)
      }
      //组合各波段的运算结果
      (image._1, MultibandTile(band_ArrayTile.toList).crop(radius, radius, numCols - radius - 1, numRows - radius - 1)) //需确保传递给它的 Tile 对象数量和顺序与所希望组合的波段顺序一致
    })
    println(GaussianBlurRDD.first()._2.cellType)
    (GaussianBlurRDD, coverage._2)
  }

  def reduction(coverage: RDDImage, option: Int): RDDImage = {
    var outcome: RDDImage = coverage
    val time=coverage.first()._1.spaceTimeKey.time
    option match {
      case 1 => outcome = reductionAverage(coverage)
      case 2 => outcome = reductionAll(coverage)
      case 3 => outcome = reductionMax(coverage)
      case 4 => outcome = reductionMin(coverage)
      case _ => outcome = coverage
    }

    def reductionAverage(coverage: RDDImage): RDDImage = {
      val statute = coverage._1.map(t => {
        val bandCount: Int = t._2.bandCount
        val tile: Tile = t._2.band(0)
        val cols = tile.cols
        val rows = tile.rows
        val result = Array.ofDim[IntArrayTile](bandCount).toBuffer
        val statuteImage: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)
        for (i <- 0 until cols; j <- 0 until rows) {
          var pixelList: Double = 0.0
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = t._2.band(bandIndex)
            val value = tile.getDouble(j, i)
            pixelList = pixelList + value
          }
          statuteImage.setDouble(j, i, pixelList / bandCount)
        }
        val aspect: ListBuffer[String] = new ListBuffer[String]()
        aspect.append("Aspect")
        (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col,t._1.spaceTimeKey.row,time),aspect),MultibandTile(statuteImage))
        (t._1, MultibandTile(statuteImage))
      })
      (statute, coverage._2)
    }


    def reductionAll(coverage: RDDImage): RDDImage = {
      val statute = coverage._1.map(t => {
        val bandCount: Int = t._2.bandCount
        val tile: Tile = t._2.band(0)
        val cols = tile.cols
        val rows = tile.rows
        val result = Array.ofDim[IntArrayTile](bandCount).toBuffer
        val statuteImage: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)
        for (i <- 0 until cols; j <- 0 until rows) {
          var pixelList: Double = 0.0
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = t._2.band(bandIndex)
            val value = tile.getDouble(j, i)
            pixelList = pixelList + value
          }
          statuteImage.setDouble(j, i, pixelList)
        }
        val aspect: ListBuffer[String] = new ListBuffer[String]()
        aspect.append("Aspect")
        (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, time), aspect), MultibandTile(statuteImage))
      })
      (statute, coverage._2)
    }

    def reductionMax(coverage: RDDImage): RDDImage = {
      val statute = coverage._1.map(t => {
        val bandCount: Int = t._2.bandCount
        val tile: Tile = t._2.band(0)
        val cols = tile.cols
        val rows = tile.rows
        val result = Array.ofDim[IntArrayTile](bandCount).toBuffer
        val statuteImage: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)
        for (i <- 0 until cols; j <- 0 until rows) {
          var pixelList: Double = 0.0
          var arrayBuffer = new ArrayBuffer[Double](bandCount)
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = t._2.band(bandIndex)
            val value = tile.getDouble(j, i)
            arrayBuffer += value
          }
          val maxValue = arrayBuffer.max
          statuteImage.setDouble(j, i, maxValue)
        }
        val aspect: ListBuffer[String] = new ListBuffer[String]()
        aspect.append("Aspect")
        (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, time), aspect), MultibandTile(statuteImage))
      })
      (statute, coverage._2)
    }


    def reductionMin(coverage: RDDImage): RDDImage = {
      val statute = coverage._1.map(t => {
        val bandCount: Int = t._2.bandCount
        val tile: Tile = t._2.band(0)
        val cols = tile.cols
        val rows = tile.rows
        val result = Array.ofDim[IntArrayTile](bandCount).toBuffer
        val statuteImage: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)
        for (i <- 0 until cols; j <- 0 until rows) {
          var pixelList: Double = 0.0
          var arrayBuffer = new ArrayBuffer[Double](bandCount)
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = t._2.band(bandIndex)
            val value = tile.getDouble(j, i)
            arrayBuffer += value
          }
          val minValue = arrayBuffer.min
          statuteImage.setDouble(j, i, minValue)
        }
        val aspect: ListBuffer[String] = new ListBuffer[String]()
        aspect.append("Aspect")
        (SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, time), aspect), MultibandTile(statuteImage))
      })
      (statute, coverage._2)
    }

    outcome

  }


  def broveyFusion(multispectral: RDDImage, panchromatic: RDDImage): RDDImage = {
    if (multispectral._1.first()._2.bandCount < 3 || panchromatic._1.first()._2.bandCount < 1) {
      throw new IllegalArgumentException("Error: 波段数量不足")
    }
    val (newmultispectral, newpanchromatic) = checkProjResoExtent(multispectral, panchromatic)
    val time1: Long = newmultispectral.first()._1.spaceTimeKey.instant //以这个时间为准
    val time2: Long = newpanchromatic.first()._1.spaceTimeKey.instant
    val band1: mutable.ListBuffer[String] = newmultispectral.first()._1.measurementName //以这个波段列表为准
    val band2: mutable.ListBuffer[String] = newpanchromatic.first()._1.measurementName
    val multispectraltileRDD: RDD[(SpatialKey, MultibandTile)] = newmultispectral._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val panchromatictileRDD: RDD[(SpatialKey, MultibandTile)] = newpanchromatic._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = multispectraltileRDD.join(panchromatictileRDD)
    val newfusedImage: RDD[(SpaceTimeBandKey, MultibandTile)] = rdd.map { case (spatialKey, (multibandTile1, multibandTile2)) =>
      val sumRGB: Tile = multibandTile1.band(0).convert(DoubleConstantNoDataCellType)
        .localAdd(multibandTile1.band(1).convert(DoubleConstantNoDataCellType))
        .localAdd(multibandTile1.band(2).convert(DoubleConstantNoDataCellType))
      var fusedBands: Seq[Tile] = Seq.empty[Tile]
      for (i <- 0 until 3) {
        val fusedBand = multibandTile1.band(i).convert(DoubleConstantNoDataCellType)
          .localMultiply(multibandTile2.band(0).convert(DoubleConstantNoDataCellType))
          .localDivide(sumRGB)
        fusedBands = fusedBands :+ (fusedBand.convert(multibandTile1.cellType))
      }
      (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(fusedBands))
    }
    //将影像每个波段的值都重新映射到0-255
    val minAndMax = findMinMaxValueDouble((newfusedImage, multispectral._2)) //findminmaxValue这个函数不会使用到multispectral._2数据
    val normalizedCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = newfusedImage.map(
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
            result(i)(j) = (tile.get(j, i).toDouble - min) / (max - min) * 255
          }
          band_Array(bandIndex) = DoubleArrayTile(result.flatten, cols, rows)
        }
        (image._1, MultibandTile(band_Array))
      })
    (normalizedCoverageRdd, newmultispectral._2)
  }

//只为了截图
  //直方图均衡化
  def histogramEqualization(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    //影像波段数
    val bandCount: Int = coverage._1.first()._2.bandCount
    //求整张影像像素总数
    val totalPixelCount: Int = findTotalPixel(coverage)
    var totalPixelCountExceptNoData: Array[Int] = Array.fill(bandCount)(totalPixelCount) //TODO 11.9 为了记录各波段除NoData外的像素总数，考虑了各波段NoData可能位置不同
    //记录影像原始类型
    val cellType: CellType = coverage._1.first()._2.cellType
    //定义一个查询字典，调用MathTools函数查询整张影像的每个波段的像素最大最小值（根据像素值类型）
    val bandMinMax: Map[Int, (Int, Int)] = findMinMaxValue(coverage)
    println(bandMinMax)

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
        if (!isNoData(pixel)) histogram(pixel - minValue) += 1
        else {
          totalPixelCountExceptNoData(bandIndex) -= 1
        }
      }
      //计算累计像素值频率
      val cumuHistogram = Array.ofDim[Int](levels)
      cumuHistogram(0) = histogram(0)
      for (i <- 1 until levels) {
        cumuHistogram(i) = histogram(i) + cumuHistogram(i - 1)
      }
      cumuHistogram
    }

    //返回RDD[(波段索引，像素值), 累计像素频率)]
    val bandPixelFrequency: RDD[((Int, Int), Int)] = coverage._1.flatMap(imageRdd => {
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
    val newCoverage: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(imageRdd => {
      val Rows = imageRdd._2.rows
      val Cols = imageRdd._2.cols
      val band_intArrayTile = Array.ofDim[IntArrayTile](bandCount)
      // 计算均衡化后的像素值
      for (bandIndex <- 0 until bandCount) {
        val intArrayTile: IntArrayTile = IntArrayTile.empty(Cols, Rows) //放在bandIndex循环前，会导致每波段得到的值相同！！！
        val tile: Tile = imageRdd._2.band(bandIndex)
        val bandTile: IntArrayTile = tile match {
          case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
          case _ => IntArrayTile(tile.toArray, tile.cols, tile.rows) // 否则，将 Tile 转换为 IntArrayTile
        }
        val (minValue, maxValue) = bandMinMax(bandIndex)
        val levels: Int = maxValue - minValue + 1
        //println(minValue, maxValue)
        for (i <- 0 until Rows; j <- 0 until Cols) {
          val pixel: Int = bandTile.get(j, i)
          if (!isNoData(pixel)) {
            val frequency = collectDictionary((bandIndex, pixel))
            intArrayTile.set(j, i, (frequency.toDouble / totalPixelCountExceptNoData(bandIndex) * levels + minValue).toInt)
          }
          else intArrayTile.set(j, i, pixel)
        }
        band_intArrayTile(bandIndex) = intArrayTile
      }
      //组合各波段的运算结果
      (imageRdd._1, MultibandTile(band_intArrayTile.toList).convert(cellType)) //TODO 增加了一个类型转换
    })
    (newCoverage, coverage._2)
  }

  def dilate(coverage: RDDImage, length: Int): RDDImage = {
    val radius = (length - 1) / 2
    val paddingCoverage = paddingRDD(coverage, radius)
    val newRDDImage: RDD[(SpaceTimeBandKey, MultibandTile)] = paddingCoverage._1.map(image => {
      val rawTile = image._2.band(0)

      val cols = rawTile.cols
      val rows = rawTile.rows
      val newTile: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)

      for (col <- 0 until (cols)) {
        for (row <- 0 until (rows)) {
          var flag: Boolean = false
          val pixel = rawTile.getDouble(col, row)
          if (pixel > 0) {
            flag = true
          }
          if (flag == false) {
            for (icol <- -radius to (radius)) {
              for (irow <- -radius to (radius)) {
                if (icol + col >= 0 && icol + col < cols && irow + row >= 0 && irow + row < rows) {
                  if (rawTile.getDouble(icol + col, irow + row) > 0)
                    flag = true
                }
              }
            }
          }


          if (flag)
            newTile.setDouble(col, row, 255)
          else
            newTile.setDouble(col, row, 0)
        }

      }
      val crop_tile = MultibandTile(newTile).crop(radius, radius, cols - radius - 1, rows - radius - 1)
      (image._1, crop_tile)
    })

    val newCoverage = (newRDDImage, coverage._2)
    newCoverage
  }

  def erosion(coverage: RDDImage, k: Int): RDDImage = {
    val radius: Int = k / 2
    val group: RDDImage = paddingRDD(coverage, radius)
    val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(
      image => {
        val tile = image._2.band(0)
        val rows = tile.rows
        val cols = tile.cols
        val newTile: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)

        for (y <- 0 until rows; x <- 0 until cols) {
          var flag: Boolean = true
          //val pixel = tile.getDouble(x, y)
          if (flag) {
            for (i <- -radius to radius; j <- -radius to radius) {
              if (x + j >= 0 && x + j < cols && y + i >= 0 && y + i < rows) {
                if (tile.getDouble(x + j, y + i) == 0) {
                  flag = false
                }
              }
            }
          }
          if (flag) {
            newTile.setDouble(x, y, 255)
          }
          else {
            newTile.setDouble(x, y, 0)
          }
        }
        val croppedTile = MultibandTile(newTile).crop(radius, radius, cols - radius - 1, rows - radius - 1)

        (image._1, croppedTile)
      })
    val newCoverage = (newRDD, coverage._2)
    newCoverage

  }
  //canny边缘提取
  def cannyEdgeDetection(coverage: RDDImage, lowCoefficient: Double = -1.0, highCoefficient: Double = -1.0)
  : RDDImage = {
    def gradXTileCalculate(tile: Tile, radius: Int): Tile = {
      val rows = tile.rows
      val cols = tile.cols
      val result = Array.ofDim[Double](rows, cols)
      var count = 0
      for (i <- radius until rows - radius; j <- radius until cols - radius) {
        val gradX: Double = tile.getDouble(j + 1, i + 1) - tile.getDouble(j - 1, i + 1) + 2 * tile.getDouble(j + 1, i) - 2 * tile.getDouble(j - 1, i) + tile.getDouble(j + 1, i - 1) - tile.getDouble(j - 1, i - 1)
        result(i)(j) = gradX

      }

      DoubleArrayTile(result.flatten, cols, rows)

    }

    //计算每个Tile的Gy
    def gradYTileCalculate(tile: Tile, radius: Int): Tile = {
      val rows = tile.rows
      val cols = tile.cols
      val result = Array.ofDim[Double](rows, cols)

      for (i <- radius until rows - radius; j <- radius until cols - radius) {
        val gradY: Double = tile.getDouble(j - 1, i - 1) - tile.getDouble(j - 1, i + 1) + 2 * tile.getDouble(j, i - 1) - 2 * tile.getDouble(j, i + 1) + tile.getDouble(j + 1, i - 1) - tile.getDouble(j + 1, i + 1)
        result(i)(j) = gradY
      }
      DoubleArrayTile(result.flatten, cols, rows)
    }

    //计算每个Tile的梯度幅值
    def gradTileCalculate(tileX: Tile, tileY: Tile, radius: Int): Tile = {
      val rows = tileX.rows
      val cols = tileX.cols
      val result = Array.ofDim[Double](rows, cols)
      var gCount = 0
      for (i <- radius until rows - radius; j <- radius until cols - radius) {
        val grad: Double = math.sqrt(math.pow(tileX.getDouble(j, i), 2) + math.pow(tileY.getDouble(j, i), 2))
        result(i)(j) = grad
        if (grad != 0) {
          gCount = gCount + 1
        }
      }
      DoubleArrayTile(result.flatten, cols, rows)
    }

    //计算图像的梯度幅值，并输出梯度图


    //计算图像在X方向上的梯度幅值，并输出梯度图
    def nonMaxSuppression(tile1: Tile, tile2: Tile, tile3: Tile) = {
      var count = 0
      val amplitude: DoubleArrayTile = tile1 match {
        case doubleTile: DoubleArrayTile => doubleTile // 如果已经是 IntArrayTile，则直接使用
        case _ => DoubleArrayTile(tile1.toArrayDouble(), tile1.cols, tile1.rows) // 否则，将 Tile 转换为 IntArrayTile
      }
      val gradX: DoubleArrayTile = tile2 match {
        case intTile: DoubleArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
        case _ => DoubleArrayTile(tile2.toArrayDouble(), tile2.cols, tile2.rows) // 否则，将 Tile 转换为 IntArrayTile
      }
      val gradY: DoubleArrayTile = tile3 match {
        case intTile: DoubleArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
        case _ => DoubleArrayTile(tile3.toArrayDouble(), tile3.cols, tile3.rows) // 否则，将 Tile 转换为 IntArrayTile
      }
      val cols = amplitude.cols
      val rows = amplitude.rows
      val result: DoubleArrayTile = DoubleArrayTile.empty(cols, rows)
      for (i <- 0 until (cols)) {
        for (j <- 0 until (rows)) {
          //获得每个像素的幅值跟梯度
          val currentAmplitude = amplitude.getDouble(j, i)
          val currentGradX = gradX.getDouble(j, i)
          val currentGradY = gradY.getDouble(j, i)
          var weight = 0.0
          //如果梯度为0，直接结果赋值为0
          if (currentAmplitude == 0) {
            result.setDouble(j, i, 0)
          }
          else {
            var grad1 = 0.0
            var grad2 = 0.0
            var grad3 = 0.0
            var grad4 = 0.0
            // y大于x方向梯度
            //            if(currentGradX!=0 && currentGradY!=0&&(math.abs(currentGradY) / math.abs(currentGradX)!=1))

            if (math.abs(currentGradY) > math.abs(currentGradX)) {

              // 计算权重edq
              weight = ((math.abs(currentGradX)).toDouble / (math.abs(currentGradY)).toDouble)
              // 中心点上下两点
              if (i != 0) {
                grad2 = amplitude.getDouble(j, i - 1);
              }
              if (i != cols - 1) {
                grad4 = amplitude.getDouble(j, i + 1);
              }
              // gradx和grady同号
              if (currentGradX * currentGradY > 0) {
                // 插值用到的另外两点
                if (i != 0 && j != 0) {
                  grad1 = amplitude.getDouble(j - 1, i - 1);
                }
                if (i != cols - 1 && j != rows - 1) {
                  grad3 = amplitude.getDouble(j + 1, i + 1);
                }
              }
              // gradx和grady异号
              else {
                if (i != 0 && j != rows - 1) {
                  grad1 = amplitude.getDouble(j + 1, i - 1);
                }
                if (j != 0 && i != cols - 1) {
                  grad3 = amplitude.getDouble(j - 1, i + 1);
                }
              }
            }
            // y方向梯度小于x方向梯度
            else {
              weight = ((math.abs(currentGradY)).toDouble / (math.abs(currentGradX)).toDouble)
              if (j != 0) {
                grad2 = amplitude.getDouble(j - 1, i);
              }
              if (j != rows - 1) {
                grad4 = amplitude.getDouble(j + 1, i);
              }
              // gradx和grady同号
              if (currentGradX * currentGradY > 0) {
                if (j != 0 && i != cols - 1) {
                  grad1 = amplitude.getDouble(j - 1, i + 1);
                }
                if (i != 0 && j != rows - 1) {
                  grad3 = amplitude.getDouble(j + 1, i - 1);
                }
              }
              //gradx和grady异号
              else {
                if (i != 0 && j != 0) {
                  grad1 = amplitude.getDouble(j - 1, i - 1);
                }
                if (i != cols - 1 && j != rows - 1) {
                  grad3 = amplitude.getDouble(j + 1, i + 1);
                }
              }
            }
            val gradTemp1 = weight * grad1 + (1 - weight) * grad2;
            val gradTemp2 = weight * grad3 + (1 - weight) * grad4;
            // 比较中心像素点和两个亚像素点的梯度值
            if ((currentAmplitude >= gradTemp1) && (currentAmplitude >= gradTemp2))
            // 中心点在其邻域内为极大值 ，在结果中保留其梯度值
            {
              result.setDouble(j, i, currentAmplitude);
              count = count + 1
            }
            else
            // 否则的话 ，在结果中置0
              result.setDouble(j, i, 0)


          }
        }
      }

      result
    }

    def doubleThresholdDetection(NMSResult: DoubleArrayTile, highThreshold: Double, lowThreshold: Double, radius: Int) = {
      //高阈值 TH×Max  TH=0.3
      //      val highThreshold =maxGradient
      //      //低阈值 TL×Max  TH=0.1
      //      val lowThreshold = 0.5*maxGradient

      val cols = NMSResult.cols
      val rows = NMSResult.rows
      //存储经过阈值检验和边缘连接后的像素的点
      val result: IntArrayTile = IntArrayTile.empty(cols, rows)
      for (i <- 1 until cols - 1) {

        for (j <- 1 until rows - 1) {
          val currentAmplitude = NMSResult.getDouble(j, i)
          //如果大于高阈值，说明是边缘值，像素值赋为255
          if (currentAmplitude > highThreshold) {
            result.set(j, i, 255)

          }
          else if (currentAmplitude < lowThreshold) {
            result.set(j, i, 0)
          }
          else if (currentAmplitude >= lowThreshold && currentAmplitude <= highThreshold) {


            var grad1 = 0.0
            var grad2 = 0.0
            var grad3 = 0.0
            var grad4 = 0.0
            var grad5 = 0.0
            var grad6 = 0.0
            var grad7 = 0.0
            var grad8 = 0.0
            if ((i != 0)) {
              grad1 = NMSResult.getDouble(j, i - 1); //上
            }
            if (i != cols - 1) {
              grad2 = NMSResult.getDouble(j, i + 1); //下
            }
            if (i != 0 && j != 0) {
              grad3 = NMSResult.getDouble(j - 1, i - 1); //左上
            }
            if (i != cols - 1 && j != rows - 1) {
              grad4 = NMSResult.getDouble(j + 1, i + 1); //右下
            }
            if (i != 0 && j != rows - 1) {
              grad5 = NMSResult.getDouble(j + 1, i - 1); //右上
            }
            if (j != 0 && i != cols - 1) {
              grad6 = NMSResult.getDouble(j - 1, i + 1); //左下
            }
            if (j != 0) {
              grad7 = NMSResult.getDouble(j - 1, i); //左
            }
            if (j != rows - 1) {
              grad8 = NMSResult.getDouble(j + 1, i); //右
            }

            if ((grad1 > highThreshold) || (grad2 > highThreshold) || (grad3 > highThreshold) || (grad4 > highThreshold) || (grad5 > highThreshold) || (grad6 > highThreshold) || (grad7 > highThreshold) || (grad8 > highThreshold)) {
              result.set(j, i, 255)

            }
            else {
              result.set(j, i, 0)
            }
          }
        }
      }

      result

    }

    val radius: Int = 1
    val normalizRDD = globalNormalizeDouble(coverage, 0, 255)
    val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(normalizRDD, radius)
    val InternRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(
      image => {
        val tile: Tile = image._2.band(0)

        val gradX = gradXTileCalculate(tile, 1)
        val cols = gradX.cols
        val rows = gradX.rows
        val gradY = gradYTileCalculate(tile, 1)
        val amplitude = gradTileCalculate(gradX, gradY, 1)
        val Crop_gradx = MultibandTile(gradX).crop(radius, radius, cols - radius - 1, rows - radius - 1)
        val Crop_grady = MultibandTile(gradY).crop(radius, radius, cols - radius - 1, rows - radius - 1)
        val Crop_amplitude = MultibandTile(amplitude).crop(radius, radius, cols - radius - 1, rows - radius - 1)
        var InternTile = Array.ofDim[Tile](3)
        InternTile(0) = Crop_gradx.band(0)
        InternTile(1) = Crop_grady.band(0)
        InternTile(2) = Crop_amplitude.band(0)
        (image._1, MultibandTile(InternTile))
      }

    )
    val InternRDDImage: RDDImage = (InternRDD, group._2)
    val PaddingVriable = paddingRDD(InternRDDImage, radius)
    val processedRDD: RDD[Array[Double]] = PaddingVriable._1.map(
      image => {

        val gradX: Tile = image._2.band(0)

        val gradY: Tile = image._2.band(1)


        val amplitude = image._2.band(2)


        val NMS: DoubleArrayTile = nonMaxSuppression(amplitude, gradX, gradY)
        NMS.toArrayDouble()

        //        (image._1, MultibandTile(band_Array).crop(radius, radius, cols - radius - 1, rows - radius - 1))
      })
    val singleBandGrad = PaddingVriable._1.map(
      image => {

        val grad = image._2.band(2)

        (image._1, MultibandTile(grad))
      }
    )
    val singleBandGradRDDImage = (singleBandGrad, PaddingVriable._2)
    val normalizedGrad: RDDImage = globalNormalizeDouble(singleBandGradRDDImage, 0, 255)
    val threshold255 = OTSU(normalizedGrad)
    val minMaxMap = findMinMaxValueDouble(singleBandGradRDDImage)
    println("threshold:" + threshold255.toString)
    val minGrad = minMaxMap(0)._1
    val maxGrad = minMaxMap(0)._2
    val normalThreshold = (threshold255.toDouble / 255.0) * (maxGrad - minGrad) + minGrad
    println(normalThreshold)
    val maxGradient = processedRDD.map(_.max).reduce(math.max)
    //    println("经过非极大抑制后的最大幅值为：" + maxGradient)
    val time=PaddingVriable._1.first()._1.spaceTimeKey.instant
    val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = PaddingVriable._1.map(
      image => {
        val cols = image._2.cols
        val rows = image._2.rows
        //        println("row:"+(image._1.spaceTimeKey.row).toString+" col:"+(image._1.spaceTimeKey.col).toString)
        val gradX: Tile = image._2.band(0)
        val gradY: Tile = image._2.band(1)
        val amplitude = image._2.band(2)
        val NMS: DoubleArrayTile = nonMaxSuppression(amplitude, gradX, gradY)

        var highThreshold: Double = 6 * normalThreshold
        var lowThreshold: Double = 3 * normalThreshold
        if (highCoefficient != -1) {
          highThreshold = normalThreshold * highCoefficient
        }
        if (lowCoefficient != -1) {
          lowThreshold = normalThreshold * lowCoefficient
        }
        if (image._1.spaceTimeKey.col == 0 && image._1.spaceTimeKey.row == 0) println("Low threshold: " + lowThreshold.toString + ",high threshold: " + highThreshold.toString)

        val cannyEdgeExtraction = doubleThresholdDetection(NMS, highThreshold, lowThreshold, radius)

        val aspect:ListBuffer[String]=new ListBuffer[String]()
        aspect.append("Aspect")
        (SpaceTimeBandKey(SpaceTimeKey(image._1.spaceTimeKey.col,image._1.spaceTimeKey.row,time),aspect), MultibandTile(cannyEdgeExtraction).crop(radius, radius, cols - radius - 1, rows - radius - 1))
      })
    (newRDD, coverage._2)
  }
  //计算标准差
  def standardDeviationCalculation(coverage: RDDImage): Map[Int,Double] = {
    def processMeanValue(newRDD: RDD[(Int, (Double, Int))]): Map[Int, Double] = {
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
            for (i <- 0 until tile.cols; j <- 0 until tile.rows)
              pixelList += tile.get(i, j)
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
            for (i <- 0 until tile.cols; j <- 0 until tile.rows)
              pixelList += tile.get(i, j)
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
    standardDeviationCalculate(coverage)
  }
//线性灰度拉伸
  def linearTransformation(coverage: RDDImage, k: Double = 1.0, b: Int = 0): RDDImage = {

    def linearTransRespectively(image: DoubleArrayTile, k: Double, b: Int, Min: Double, Max: Double, cellType: String): DoubleArrayTile = {

      val col = image.cols
      val row = image.rows

      val newImages: DoubleArrayTile = DoubleArrayTile.empty(col, row)
      for (i <- 0 until row; j <- 0 until col) {
        val pixel = image.getDouble(j, i)

        var transformedPixel = (k * pixel + b)
        //TODO:minmax函数得改
        if (transformedPixel < Min) {
          transformedPixel = Min
        }
        if (transformedPixel > Max) {
          transformedPixel = Max
        }

        if (cellType.contains("int")) {
          transformedPixel = math.round(transformedPixel)
        }
        newImages.setDouble(j, i, transformedPixel)
      }

      newImages
    }


    var MinMaxValue = findMinMaxValueDouble(coverage)

    val newImage: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(image => {

      val bandCount: Int = image._2.bandCount
      val cellType = image._2.band(0).cellType

      val bandIntArrayTile = Array.ofDim[IntArrayTile](bandCount)
      val bandDoubleArrayTile = Array.ofDim[DoubleArrayTile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val Min = MinMaxValue(bandIndex)._1
        val Max = MinMaxValue(bandIndex)._2
        //        println(Max,Min)
        val tile: Tile = image._2.band(bandIndex)


        val bandTile: DoubleArrayTile = DoubleArrayTile(tile.toArrayDouble(), tile.cols, tile.rows)


        val doubleBandTile = DoubleArrayTile(bandTile.toArrayDouble(), bandTile.cols, bandTile.rows)
        bandDoubleArrayTile(bandIndex) = linearTransRespectively(doubleBandTile, k, b, Min, Max, cellType.toString())

      }


      val resultTile = MultibandTile(bandDoubleArrayTile)
      resultTile.convert(cellType)
      (image._1, resultTile)

    })

    (newImage, coverage._2)
  }
    //假彩色合成
  def falseColorComposite (coverage:RDDImage,BandRed:Int,BandBlue:Int,BandGreen:Int):RDDImage=
  {
    def Strench2ProperScale(RawTile: IntArrayTile, MinPixel: Int, MaxPixel: Int): IntArrayTile = {
      val cols = RawTile.cols
      val rows = RawTile.rows
      val StrenchedTile: IntArrayTile = IntArrayTile.empty(cols, rows)

      val b = 0 - MinPixel
      val k = 255.0 / (MaxPixel - MinPixel).toDouble

      for (i <- 0 until rows; j <- 0 until cols) {
        var Pixel = RawTile.get(j, i)
        Pixel = (k * (Pixel).toDouble + b).toInt
        if (Pixel > 255) {
          Pixel = 255
        }
        else if (Pixel < 0) {
          Pixel = 0
        }
        StrenchedTile.set(j, i, Pixel)
      }

      StrenchedTile
    }
    val MinMaxMap=findMinMaxValue(coverage)
    val time=coverage._1.first()._1.spaceTimeKey.instant
    val Changed_Image:RDD[(SpaceTimeBandKey, MultibandTile)]=coverage._1.map(x=>{
      val ColorTile=Array.ofDim[IntArrayTile](3)
      val RedTile=x._2.band(BandRed)
      val BlueTIle=x._2.band(BandBlue)
      val GreenTIle=x._2.band(BandGreen)

      val RedRawTile= RedTile match {
        case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
        case _ => IntArrayTile(RedTile.toArray, RedTile.cols, RedTile.rows) // 否则，将 Tile 转换为 IntArrayTile
      }

      val BlueRawTile = BlueTIle match {
        case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
        case _ => IntArrayTile(BlueTIle.toArray, BlueTIle.cols, BlueTIle.rows) // 否则，将 Tile 转换为 IntArrayTile
      }

      val GreenRawTile = GreenTIle match {
        case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
        case _ => IntArrayTile(GreenTIle.toArray, GreenTIle.cols, GreenTIle.rows) // 否则，将 Tile 转换为 IntArrayTile
      }

      val RedOutputTile=Strench2ProperScale(RedRawTile,MinMaxMap(BandRed)._1,MinMaxMap(BandRed)._2)
      val BlueOutputTile=Strench2ProperScale(BlueRawTile,MinMaxMap(BandBlue)._1,MinMaxMap(BandBlue)._2)
      val GreenOutputTile=Strench2ProperScale(GreenRawTile,MinMaxMap(BandGreen)._1,MinMaxMap(BandGreen)._2)
      ColorTile(0)=RedOutputTile
      ColorTile(1)=BlueOutputTile
      ColorTile(2)=GreenOutputTile
      val aspect: ListBuffer[String] = new ListBuffer[String]()
      aspect.append("Aspect")
      (SpaceTimeBandKey(SpaceTimeKey(x._1.spaceTimeKey.col,x._1.spaceTimeKey.row,time),aspect),MultibandTile(ColorTile))
    })
    (Changed_Image,coverage._2)
  }
   //标准差拉伸
   def bilateralFilter(coverage: RDDImage, d: Int, sigmaSpace: Double, sigmaColor: Double, borderType: String): RDDImage = {
     val radius: Int = d / 2
     val group: RDDImage = paddingRDD(coverage, radius, borderType)
     val cellType: CellType = coverage._1.first()._2.cellType

     // 定义双边滤波kernel
     def bilateral_kernel(x: Double, y: Double, sigmaSpace: Double, sigmaColor: Double): Double = {
       math.exp(-(x * x) / (2 * sigmaSpace * sigmaSpace)) * math.exp(-(y * y) / (2 * sigmaColor * sigmaColor))
     }

     //遍历单波段每个像素，对其进行双边滤波
     def bilateral_single(image: Tile): Tile = {
       val numRows = image.rows
       val numCols = image.cols
       val newImage = Array.ofDim[Double](numRows, numCols)
       for (i <- radius until numRows - radius; j <- radius until numCols - radius) {
         breakable {
           val centerValue = image.getDouble(j, i)
           if (isNoData(centerValue)) {
             newImage(i)(j) = Double.NaN
             break
           }
           var sum = 0.0
           var weightSum = 0.0
           for (k <- 0 until d; l <- 0 until d) {
             breakable {
               val x = math.abs(i - radius + k)
               val y = math.abs(j - radius + l)
               val neighborValue = image.getDouble(y, x)
               if (isNoData(neighborValue)) {
                 break
               }
               val colorDiff = image.getDouble(j, i) - image.getDouble(y, x)
               val spaceDiff = math.sqrt((x - i) * (x - i) + (y - j) * (y - j))
               val weight = bilateral_kernel(spaceDiff, colorDiff, sigmaSpace, sigmaColor)

               sum += weight * neighborValue
               weightSum += weight
             }
           }
           newImage(i)(j) = sum / weightSum
         }
       }
       DoubleArrayTile(newImage.flatten, numCols, numRows)

     }

     // 遍历延宽像素后的瓦片
     val bilateralFilterRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(image => {
       val numRows = image._2.rows
       val numCols = image._2.cols

       //对每个波段进行双边滤波处理
       val bandCount: Int = image._2.bandCount
       val band_ArrayTile = Array.ofDim[Tile](bandCount)
       for (bandIndex <- 0 until bandCount) {
         val tile: Tile = image._2.band(bandIndex)
         band_ArrayTile(bandIndex) = bilateral_single(tile).convert(cellType)
       }
       //组合各波段的运算结果
       (image._1, MultibandTile(band_ArrayTile.toList).crop(radius, radius, numCols - radius - 1, numRows - radius - 1))
     })
     (bilateralFilterRDD, coverage._2)
   }

  def standardDeviationStretching(coverage: RDDImage)
  :RDDImage = {


    def CaculateMeanValue(coverage:RDDImage):Map[Int,Double]=
    {

      //获得每个瓦片的平均值，用double存储
      val MeanValue:RDD[(Int,(Double,Int))]=coverage._1.flatMap(t=>
      {
        val result = new ListBuffer[(Int,(Double,Int))]()
        val bandCount:Int=t._2.bandCount
        for (bandIndex <- 0 until bandCount)
        {
          var MeanValue:Double=0.0
          var TotalValue=0
          var PixelCount=0
          val tile: Tile = t._2.band(bandIndex)
          val bandTile: IntArrayTile = IntArrayTile(tile.toArray, tile.cols, tile.rows)
          for(i<-0 until tile.cols;j<-0 until tile.rows)
          {
            TotalValue=TotalValue + tile.get(i,j)
            PixelCount=PixelCount+1
          }
          MeanValue=((TotalValue).toDouble/PixelCount.toDouble)
          result.append((bandIndex,( MeanValue,1)))
        }
        result.toList
      })

      // 计算每个波段所有像素值之和
      val Totalpixel:RDD[(Int,(Double,Int))]=MeanValue.reduceByKey((x,y)=>
      {
        (x._1+y._1 ,x._2+y._2)
      })

      //计算每个波段平均值
      val MeanPixel:Map[Int,Double]=Totalpixel.map(a=>(a._1,(a._2._1/a._2._2).toDouble)).collect().toMap

      MeanPixel
    }
    val averageMap: Map[Int, Double] =CaculateMeanValue(coverage)


    def standardDeviationCalculate(coverage: RDDImage): Map[Int, Double] = {
      //计算每个波段单个瓦片的像素平均值
      val mean_RDD: RDD[(Int, (Double, Int))] = coverage._1.flatMap(
        image => {
          val result = new ListBuffer[(Int, (Double, Int))]()
          val bandCount = image._2.bandCount
          val pixelList = new ListBuffer[Int]()
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = image._2.band(bandIndex)
            for (i <- 0 until tile.cols; j <- 0 until tile.rows)
              pixelList += tile.get(i, j)
            val mean: Double = (pixelList.sum).toDouble / (pixelList.length).toDouble
            result.append((bandIndex, (mean, 1)))
          }
          result.toList
        })

      //计算每个波段的像素平均值
      val mean_Band: Map[Int, Double] = mean_RDD.reduceByKey(
        (x, y) => (x._1 + y._1, x._2 + y._2)
      ).map(
        t => (t._1, (t._2._1 / t._2._2).toDouble)
      ).collect().toMap

      //计算每个Tile的[(x-m)^2之和]/n
      val variance_RDD: RDD[(Int, (Double, Int))] = coverage._1.flatMap(
        image => {
          val result = new ListBuffer[(Int, (Double, Int))]()
          val bandCount = image._2.bandCount
          val pixelList = new ListBuffer[Int]()
          for (bandIndex <- 0 until bandCount) {
            val tile: Tile = image._2.band(bandIndex)
            var mean_overall: Double = 0
            val mean_get = mean_Band.get(bandIndex) match {
              case Some(value) => mean_overall = value
              case None => println("Key not found")
            }
            for (i <- 0 until tile.cols; j <- 0 until tile.rows)
              pixelList += tile.get(i, j)
            val squaredDiffs = pixelList.map(x => {
              math.pow(x - mean_overall, 2)
            })

            val totalVariance = (squaredDiffs.sum).toDouble / (pixelList.length).toDouble
            result.append((bandIndex, (totalVariance, 1)))
          }
          result.toList
        })

      //计算每个波段的标准差
      val standardDeviation_Band: Map[Int, Double] = variance_RDD.reduceByKey(
        (x, y) => (x._1 + y._1, x._2 + y._2)
      ).map(
        t => (t._1, (sqrt(t._2._1 / t._2._2)).toDouble)
      ).collect().toMap

      standardDeviation_Band

    }
    val devMap: Map[Int, Double] =standardDeviationCalculate(coverage)

    //新像素值 = (原始像素值 - 平均值) / 标准差 * 标准差倍数
    def calculate(image: IntArrayTile,average:Double,stdDev:Double): IntArrayTile = {
      val cols = image.cols
      val rows = image.rows
      val stretchedImage: IntArrayTile = IntArrayTile.empty(cols, rows)
      for (i <- 0 until rows; j <- 0 until cols) {
        val pixel = image.get(j, i)
        var stretchedPixel = ((pixel - average) / stdDev *127.5).toInt
        //对于拉伸的图像的像素边界进行裁剪，超过255的赋值255，比0小的赋值为0
        if(stretchedPixel>255){
          stretchedPixel=255
        }
        if(stretchedPixel<0){
          stretchedPixel=0
        }
        stretchedImage.set(j, i,stretchedPixel)
        //        println(stretchedPixel)
      }
      stretchedImage
    }

    val newCoverageRdd: RDD[(SpaceTimeBandKey,MultibandTile)] = coverage._1.map(image => {
      //遍历每个波段，调用calculate函数计算每个波段的标准差
      val bandCount: Int = image._2.bandCount
      val band_doubleStdDev = Array.ofDim[IntArrayTile](bandCount).toBuffer
      //对每个波段进行遍历
      println("波段数："+bandCount)
      for (bandIndex <- 0 until bandCount) {
        println("这是第"+bandIndex+"波段")
        val tile: Tile = image._2.band(bandIndex)           //将多波段Tile中取出其中的那一个Tile
        val bandTile: IntArrayTile = tile match {
          case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
          case _ => IntArrayTile(tile.toArray, tile.cols, tile.rows) // 否则，将 Tile 转换为 IntArrayTile
        }
        //得到每个波段的平均值
        var averageValue:Double=0
        val average=averageMap.get(bandIndex) match {
          case Some(value) =>
            averageValue=value
          //            println("平均值是："+value)
          case None => println("Key not found")
        }
        //得到每个波段的平均标准差
        var devValue: Double = 0
        val dev = devMap.get(bandIndex) match {
          case Some(value) =>
            devValue = value
          //            println("标准差是："+value)
          case None => println("Key not found")
        }
        //        println(s"第${bandIndex}波段")
        band_doubleStdDev(bandIndex) = calculate(bandTile,averageValue,devValue)
      }
      //组合各波段的运算结果
      (image._1, MultibandTile(band_doubleStdDev.toList))
    })
    (newCoverageRdd, coverage._2)
  }

  //灰度共生纹理矩阵
  def GLCM(coverage: RDDImage, d: Int, dist: Int, orient: Int, greyLevels: Int, feature: String = "Mean", borderType: String): RDDImage = {
    //coverage可以有多波段，分别为每个波段生成GLCM纹理
    //d表示核的宽度；dist和orient表示探索灰度相关性的距离和方向；feature表示所选择的特征量（先默认为能量"ASM"）；borderType指接边策略
    if (d % 2 != 1) throw new IllegalArgumentException("Error: d应为正奇数")
    if (dist > d - 1) throw new IllegalArgumentException("Error: dist应小于等于d-1")
    if (greyLevels > 65536) throw new IllegalArgumentException("Error: 设置的灰度阶数过大，UShort数据类型不支持")
    val (di, dj): (Int, Int) = orient match {
      case 0 => (0, dist)
      case 45 => (-dist, dist)
      case 90 => (-dist, 0)
      case 135 => (-dist, -dist)
      case _ => throw new IllegalArgumentException("Error: orient输入不符合要求")
    }
    // TODO Correlation和Dissimilarity根据结果图像来看有点问题
    val featureFunc: (Array[Array[Double]] => Double) = feature match {
      case "Mean" => (GLCM: Array[Array[Double]]) => { //均值
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += GLCM(i)(j) * i
        }
        sum
      }
      case "Variance" => (GLCM: Array[Array[Double]]) => { //方差
        var mean: Double = 0;
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          mean += GLCM(i)(j) * i
        }
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += GLCM(i)(j) * (i - mean) * (i - mean)
        }
        sum / greyLevels
      }
      case "Std" => (GLCM: Array[Array[Double]]) => { //标准差
        var mean: Double = 0;
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          mean += GLCM(i)(j) * i
        }
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += math.pow(GLCM(i)(j) * (i - mean) * (i - mean), 0.5)
        }
        sum
      }
      case "ASM" => (GLCM: Array[Array[Double]]) => { //角二阶矩 Angular Second Moment 体现了图像纹理的整体强度或能量分布情况
        var sum: Double = 0
        GLCM.flatten.foreach(t => sum += t * t)
        sum
      }
      case "Energy" => (GLCM: Array[Array[Double]]) => { //能量 =根号ASM
        var sum: Double = 0
        GLCM.flatten.foreach(t => sum += t * t)
        math.pow(sum, 0.5)
        //        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        //        for (i <- 0 until numRows; j <- 0 until numCols) {sum += GLCM(i)(j) * GLCM(i)(j)}
        //        math.pow(sum, 0.5)
      }
      case "Contrast" => (GLCM: Array[Array[Double]]) => { //对比度 反映了图像中灰度级对比程度的强度
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += GLCM(i)(j) * (i - j) * (i - j)
        }
        sum
      }
      case "Dissimilarity" => (GLCM: Array[Array[Double]]) => { //非相似性
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += GLCM(i)(j) * math.abs(i - j)
        }
        sum
      }
      case "Homogeneity" => (GLCM: Array[Array[Double]]) => { //反差分矩阵 Inverse Different Moment 同质度 衡量了图像中灰度级相似的像素对之间的密集程度
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          sum += GLCM(i)(j) / (1 + (i - j) * (i - j))
        }
        sum
      }
      case "Entropy" => (GLCM: Array[Array[Double]]) => { //熵 描述了图像纹理的复杂性和不规则性
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          if (GLCM(i)(j) > 0) sum += GLCM(i)(j) * math.log(GLCM(i)(j)) / math.log(2)
        }
        sum
      }
      case "Correlation" => (GLCM: Array[Array[Double]]) => { //相关性（局部灰度相关性），值越大相关性越大 反映了图像中像素对之间的线性相关性
        var mean_i: Double = 0;
        var mean_j: Double = 0;
        var variance_i: Double = 0;
        var variance_j: Double = 0;
        var sum: Double = 0
        val (numRows, numCols) = (GLCM.length, GLCM(0).length)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          mean_i += GLCM(i)(j) * i
        }
        for (i <- 0 until numRows; j <- 0 until numCols) {
          mean_j += GLCM(i)(j) * j
        }
        //        for (i <- 0 until numRows; j <- 0 until numCols) {variance_i += GLCM(i)(j) * (i-mean_i)*(i-mean_i)}
        //        for (i <- 0 until numRows; j <- 0 until numCols) {variance_j += GLCM(i)(j) * (j-mean_j)*(j-mean_j)}
        for (i <- 0 until numRows; j <- 0 until numCols) {
          variance_i += (i - mean_i) * (i - mean_i)
        }
        variance_i = variance_i / (numRows * numCols)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          variance_j += (j - mean_j) * (j - mean_j)
        }
        variance_j = variance_j / (numRows * numCols)
        //        if(variance_i==0||variance_j==0) sum会得到NaN的值
        for (i <- 0 until numRows; j <- 0 until numCols) {
          if (variance_i * variance_j > 0) sum += GLCM(i)(j) * (i - mean_i) * (j - mean_j) / (math.pow(variance_i * variance_j, 0.5))
        }
        //        println(mean_i, mean_j, variance_i, variance_j, sum)
        sum
      }
      case _ => throw new IllegalArgumentException("Error: 其它特征量的求解暂未实现")
    }
    //grayLevels表示量化的灰度阶数，通常为8
    val bandCount: Int = coverage._1.first()._2.bandCount
    val radius: Int = d / 2
    val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(coverage, radius, borderType)
    //先得到各波段的最大最小像素值
    val minMaxValue: Map[Int, (Double, Double)] = findMinMaxValueDouble(coverage)
    //计算灰度量化后的coverage，量化完后像素值变为了整型（Tile是IntArrayTile）
    val reduceGreyLevelRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(image => {
      val numRows = image._2.rows
      val numCols = image._2.cols
      val band_ArrayTile = Array.ofDim[Tile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val bandTile: Tile = image._2.band(bandIndex)
        val intArrayTile: IntArrayTile = IntArrayTile.empty(numCols, numRows)
        val min: Double = minMaxValue(bandIndex)._1
        val rate: Double = (minMaxValue(bandIndex)._2 - minMaxValue(bandIndex)._1) / greyLevels
        //        println(rate, min, minMaxValue(bandIndex)._2)
        for (i <- 0 until numRows; j <- 0 until numCols) {
          if (isNoData(bandTile.getDouble(j, i))) intArrayTile.set(j, i, UShortConstantNoDataCellType.noDataValue) //NODATA先这样设置
          else {
            val value: Int = math.max(0, math.min(((bandTile.getDouble(j, i) - min) / rate).toInt, greyLevels - 1))
            //            println(value)
            intArrayTile.set(j, i, value)
          }
        }
        band_ArrayTile(bandIndex) = intArrayTile.convert(UShortConstantNoDataCellType) //暂时这样设CellType，这就要求用户输入的greyLevels<=65536，不过一般设这么大也没有意义
      }
      (image._1, MultibandTile(band_ArrayTile.toList))
    })
    //计算特征量
    val featureRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = reduceGreyLevelRDD.map(image => {
      val numRows = image._2.rows
      val numCols = image._2.cols
      val band_ArrayTile = Array.ofDim[Tile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val doubleArrayTile: DoubleArrayTile = DoubleArrayTile.empty(numCols, numRows)
        val bandTile: Tile = image._2.band(bandIndex)
        for (i <- radius until numRows - radius; j <- radius until numCols - radius) {
          //灰度共生矩阵的长宽均为灰度阶数
          val GLCM = Array.fill[Int](greyLevels, greyLevels)(0)
          //计算灰度共生矩阵
          for (m <- 0 until d; n <- 0 until d) {
            if (i - radius + m + di >= i - radius && j - radius + n + dj >= j - radius && j - radius + n + dj <= j + radius) {
              val value1: Int = bandTile.get(j - radius + n, i - radius + m)
              val value2: Int = bandTile.get(j - radius + n + dj, i - radius + m + di)
              if ((!isNoData(value1)) && (!isNoData(value2))) GLCM(value1)(value2) += 1 //当前是进去NoData，出来也是NoData的策略
              else if (isNoData(value1) && isNoData(value2)) GLCM(0)(0) += 1 //取消以下3行注释，就是opencv的处理策略
              else if (isNoData(value1) && !isNoData(value2)) GLCM(0)(value2) += 1
              else GLCM(value1)(0) += 1
            }
          }
          //灰度共生矩阵归一化
          val sum = GLCM.flatten.sum.toDouble
          if (isNoData(bandTile.get(j, i)) || sum == 0) doubleArrayTile.setDouble(j, i, 0) //当前是进去NoData，出来也是NoData的策略；  Double.NaN-》0，就是opencv的处理策略
          else {
            val GLCM_1: Array[Array[Double]] = GLCM.map(_.map(_ / sum))
            //计算特征量
            val featureValue: Double = featureFunc(GLCM_1)
            doubleArrayTile.setDouble(j, i, featureValue)
          }
        }
        band_ArrayTile(bandIndex) = doubleArrayTile.convert(DoubleConstantNoDataCellType) //对cellType进行了改变，因为结果取值范围可能与初始像素取值范围完全不同，不能再使用原先像素值类型
      }
      //组合各波段的运算结果
      (image._1, MultibandTile(band_ArrayTile.toList).crop(radius, radius, numCols - radius - 1, numRows - radius - 1)) //需确保传递给它的 Tile 对象数量和顺序与所希望组合的波段顺序一致
    })
    (featureRDD, coverage._2)
  }

  def PCA(coverage: RDDImage, num: Int): RDDImage = {
    // 创建SparkSession对象
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate()
    val numRows: Int = findSpatialKeyMinMax(coverage)._1 //瓦片行数
    val numCols: Int = findSpatialKeyMinMax(coverage)._2 //瓦片列数
    //    println(coverage._1.first()._1._measurementName)
    val bandCount: Int = coverage._1.first()._2.bandCount
    val cellType: CellType = coverage._1.first()._2.cellType
    val time: Long = coverage._1.first()._1.spaceTimeKey.instant
    // 将RDD转换为DataFrame
    val IntVectorRdd: RDD[(Int, Int, Int, Int, List[Double])] = coverage._1.flatMap(t => {
      val list: ListBuffer[(Int, Int, Int, Int, List[Double])] = ListBuffer.empty[(Int, Int, Int, Int, List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for (i <- 0 until 256; j <- 0 until 256) { // (i, j)是像素在该瓦片中的定位
        val pixelBands: ListBuffer[Double] = ListBuffer.empty[Double] //TODO 类型先写作Double，后面改成与cellType同
        for (bandIndex <- 0 until bandCount) {
          pixelBands.append(t._2.band(bandIndex).getDouble(j, i))
        }
        list.append((row, col, i, j, pixelBands.toList)) //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val rowRdd: RDD[Row] = IntVectorRdd.map(t => {
      val list = List(t._1.toDouble, t._2.toDouble, t._3.toDouble, t._4.toDouble) ::: t._5
      Row(list: _*)
    })
    val fieldTypes = List.fill(bandCount + 4)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to bandCount) {
      colNames.append(s"col$i")
    }
    val labelColNames = List("label1", "label2", "label3", "label4") ::: colNames.toList
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(num)
      .fit(assembledDF)
    val result = pca.transform(assembledDF) //.drop("pcaFeatures")

    val colSelect: RDD[Row] = result.select("label1", "label2", "label3", "label4", "pcaFeatures").rdd
    val groupRdd: RDD[((Double, Double), Iterable[((Double, Double), DenseVector)])] =
      colSelect.map(t => ((t.getDouble(0), t.getDouble(1)), ((t.getDouble(2), t.getDouble(3)), t.getAs[DenseVector](4)))) //.getAs[List[Double]](4))
        .groupByKey()
    val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = groupRdd.map(t => {
      val multibandTileArray = Array.ofDim[Tile](num)
      //定位瓦片
      val row = t._1._1.toInt
      val col = t._1._2.toInt
      val newBand: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String] //TODO 需要改成新的波段列表
      val arr: Iterable[((Double, Double), DenseVector)] = t._2 // 取消 新增.toArray ; java.util.List ; DenseVector[Double] ; Vector
      //分波段计算新像素值
      for (bandIndex <- 0 until num) {
        newBand.append(bandIndex.toString())
        val doubleArrayTile: DoubleArrayTile = DoubleArrayTile.empty(256, 256)
        doubleArrayTile.fill(Double.NaN)
        for (list <- arr) {
          doubleArrayTile.setDouble(list._1._2.toInt, list._1._1.toInt, -list._2(bandIndex)) //加个负号，否则结果会是负数，原因还不明
        }
        multibandTileArray(bandIndex) = doubleArrayTile.convert(cellType)
      }
      (SpaceTimeBandKey(SpaceTimeKey(col, row, time), newBand), MultibandTile(multibandTileArray))
    })

    (newCoverageRdd, coverage._2)
  }

//Kmeans聚类
  def kMeans(coverage: RDDImage, k: Int, seed: Long, maxIter: Int, distanceMeasure: String): RDDImage = {
    val spark = SparkSession.builder().appName("SparkKMeansJob").getOrCreate()
    val bandCount: Int = coverage._1.first()._2.bandCount
    val cellType: CellType = coverage._1.first()._2.cellType
    val time: Long = coverage._1.first()._1.spaceTimeKey.instant

    val IntVectorRdd: RDD[(Int, Int, Int, Int, List[Double])] = coverage._1.flatMap(t => {
      val list: ListBuffer[(Int, Int, Int, Int, List[Double])] = ListBuffer.empty[(Int, Int, Int, Int, List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for (i <- 0 until 256; j <- 0 until 256) { // (i, j)是像素在该瓦片中的定位
        val pixelBands: ListBuffer[Double] = ListBuffer.empty[Double] //TODO 类型先写作Double，后面改成与cellType同
        for (bandIndex <- 0 until bandCount) {
          pixelBands.append(t._2.band(bandIndex).getDouble(j, i))
        }
        list.append((row, col, i, j, pixelBands.toList)) //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    val rowRdd: RDD[Row] = IntVectorRdd.map(t => {
      val list = List(t._1.toDouble, t._2.toDouble, t._3.toDouble, t._4.toDouble) ::: t._5
      Row(list: _*)
    })
    val fieldTypes = List.fill(bandCount + 4)(DoubleType)
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to bandCount) {
      colNames.append(s"col$i")
    }
    val labelColNames = List("label1", "label2", "label3", "label4") ::: colNames.toList
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    val df = spark.createDataFrame(rowRdd, schema).toDF(labelColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assembledDF = assembler.transform(df)

    val kmeans = new KMeans()
      .setK(k)
      .setSeed(seed)
      .setMaxIter(maxIter)
      .setDistanceMeasure(distanceMeasure)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(assembledDF)
    val predictions = model.transform(assembledDF)
    // 选择需要的列并进行分组
    val colSelect: RDD[Row] = predictions.select("label1", "label2", "label3", "label4", "cluster").rdd
    val groupRdd: RDD[((Double, Double), Iterable[((Double, Double), Int)])] =
      colSelect.map(t => ((t.getDouble(0), t.getDouble(1)), ((t.getDouble(2), t.getDouble(3)), t.getInt(4))))
        .groupByKey()

    // 构建新的覆盖范围RDD
    val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = groupRdd.map(t => {
      //定位瓦片
      val row = t._1._1.toInt
      val col = t._1._2.toInt
      val arr: Iterable[((Double, Double), Int)] = t._2 // 取消 新增.toArray ; java.util.List ; DenseVector[Double] ; Vector
      val doubleArrayTile: DoubleArrayTile = DoubleArrayTile.empty(256, 256)
      doubleArrayTile.fill(Double.NaN)
      for (list <- arr) {
        doubleArrayTile.setDouble(list._1._2.toInt, list._1._1.toInt, list._2) //加个负号，否则结果会是负数，原因还不明
      }
      val tile: Tile = doubleArrayTile.convert(cellType)
      (SpaceTimeBandKey(SpaceTimeKey(col, row, time), ListBuffer("cluster")), MultibandTile(tile))
    })
    (newCoverageRdd, coverage._2)
  }
//  IHSFusion
def IHSFusion(coverage1: RDDImage, coverage2: RDDImage): RDDImage = {
  //coverage1为低分彩色图像（RGB），coverage2为高分全色图像，允许传入多波段，但分别只取用前三波段和前一波段
  //注意，整数除法默认会返回整数结果
  if (coverage1._1.first()._2.bandCount < 3 || coverage2._1.first()._2.bandCount < 1) {
    throw new IllegalArgumentException("Error: 波段数量不足")
  }
  val cellType: CellType = coverage1._1.first()._2.cellType
  val (newCoverage1, newCoverage2) = checkProjResoExtent(coverage1, coverage2)
  val time1: Long = newCoverage1._1.first()._1.spaceTimeKey.instant //以这个时间为准
  val time2: Long = newCoverage2._1.first()._1.spaceTimeKey.instant
  val band1: mutable.ListBuffer[String] = newCoverage1._1.first()._1.measurementName //以这个波段列表为准
  val band2: mutable.ListBuffer[String] = newCoverage2._1.first()._1.measurementName
  val coverage1tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage1._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val coverage2tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage2._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val RGB_IHS: BreezeDenseMatrix[Double] = BreezeDenseMatrix((1.0 / 3, 1.0 / 3, 1.0 / 3), (-Math.pow(2, 0.5) / 6, -Math.pow(2, 0.5) / 6, Math.pow(2, 1.5) / 6), (1 / Math.pow(2, 0.5), -1 / Math.pow(2, 0.5), 0.0))
  val IHS_RGB: BreezeDenseMatrix[Double] = BreezeDenseMatrix((1.0, -1 / Math.pow(2, 0.5), 1 / Math.pow(2, 0.5)), (1.0, -1 / Math.pow(2, 0.5), -1 / Math.pow(2, 0.5)), (1.0, Math.pow(2, 0.5), 0.0))
  val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1tileRDD.join(coverage2tileRDD)
  val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = rdd.map { case (spatialKey, (multibandTile1, multibandTile2)) =>
    //将coverage1的MultibandTile（取前3波段）转化为二维矩阵（波段数×pixels）
    val pixels = multibandTile1.rows * multibandTile1.cols
    val RGB_matrix = BreezeDenseMatrix.zeros[Double](3, pixels) //先创建一个空的二维矩阵
    for (i <- 0 until 3) {
      val pixelArray: Array[Double] = multibandTile1.band(i).toArrayDouble()
      val pixelVector: BreezeDenseVector[Double] = BreezeDenseVector(pixelArray)
      RGB_matrix(i, ::) := pixelVector.t //DenseVector 默认是列向量，所以要转成行向量
    }
    //RGB->IHS
    val IHS_matrix: BreezeDenseMatrix[Double] = RGB_IHS * RGB_matrix
    //将coverage2的MultibandTile（取前1波段）转化为一维向量（pixels）
    val pixelArray: Array[Double] = multibandTile2.band(0).toArrayDouble()
    val pixelVector: BreezeDenseVector[Double] = BreezeDenseVector(pixelArray)
    //IHS的I分量替换
    IHS_matrix(0, ::) := pixelVector.t
    //IHS->RGB
    val RGB_matrix_new = IHS_RGB * IHS_matrix
    //将二维矩阵（波段数×pixels）转化为MultibandTile，合成新coverage输出
    val band_ArrayTile = Array.ofDim[Tile](3)
    for (i <- 0 until 3) {
      band_ArrayTile(i) = DoubleArrayTile(RGB_matrix_new(i, ::).t.toArray, multibandTile1.cols, multibandTile1.rows)
    }
    (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(band_ArrayTile))
  }
  //将影像每个波段的值都重新映射到0-255
  val minAndMax = findMinMaxValueDouble((newCoverageRdd, coverage1._2)) //findminmaxValue这个函数不会使用到coverage1._2数据
  val normalizedCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = newCoverageRdd.map(
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
          result(i)(j) = (tile.get(j, i).toDouble - min) / (max - min) * 255
        }
        band_Array(bandIndex) = DoubleArrayTile(result.flatten, cols, rows)
      }
      (image._1, MultibandTile(band_Array).convert(cellType)) //TODO 11.21增加.convert
    })
  //TODO TileLayerMetadata部分，暂时先这么返回,
  (normalizedCoverageRdd, newCoverage1._2)
}
def panSharp(coverage1: RDDImage, coverage2: RDDImage, method: String = "IHS", bandList: List[Short] = List(1,2,3), weightList: List[Double] = List(1,1,1)): RDDImage = {
  //coverage1为低分彩色图像（RGB），coverage2为高分全色图像，允许传入多波段，但分别只取用前三波段和前一波段
  //注意，整数除法默认会返回整数结果
  //此算子暂时只考虑RGB三通道的实现，不考虑近红外或更多波段
  println("cellType",coverage1._1.first()._2.cellType)
  if(coverage1._1.first()._2.bandCount<3 || coverage2._1.first()._2.bandCount<1){
    throw new IllegalArgumentException("Error: 影像波段数量不足")
  }
  //TODO bandList必须是从1开始，规定必须是三波段RGB
  if(bandList.length!=3 || weightList.length>3){
    throw new IllegalArgumentException("Error: 波段列表应包含三个元素，权重列表不超过三个元素")
  }
  var weight: ListBuffer[Double] = ListBuffer.empty
  for(i<-0 until 3){
    if(i<weightList.length) weight.append(weightList(i))
    else weight.append(0)
  }
  weight = weight.map(_/weight.sum)
  val cellType: CellType = coverage1._1.first()._2.cellType
  val (newCoverage1, newCoverage2) = checkProjResoExtent(coverage1, coverage2)
  val time1: Long = newCoverage1._1.first()._1.spaceTimeKey.instant  //以这个时间为准
  val time2: Long = newCoverage2._1.first()._1.spaceTimeKey.instant
  val band1: mutable.ListBuffer[String] = newCoverage1._1.first()._1.measurementName  //以这个波段列表为准
  val band2: mutable.ListBuffer[String] = newCoverage2._1.first()._1.measurementName
  val coverage1tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage1._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val coverage2tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage2._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1tileRDD.join(coverage2tileRDD)
  var newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = null

  if (method == "IHS"){
    val RGB_IHS: BreezeDenseMatrix[Double] = BreezeDenseMatrix((1.0/3,1.0/3,1.0/3),(-Math.pow(2,0.5)/6,-Math.pow(2,0.5)/6,Math.pow(2,1.5)/6),(1/Math.pow(2,0.5),-1/Math.pow(2,0.5),0.0))
    val IHS_RGB: BreezeDenseMatrix[Double] = BreezeDenseMatrix((1.0,-1/Math.pow(2,0.5),1/Math.pow(2,0.5)),(1.0,-1/Math.pow(2,0.5),-1/Math.pow(2,0.5)),(1.0,Math.pow(2,0.5),0.0))
    newCoverageRdd  = rdd.map{ case(spatialKey, (multibandTile1, multibandTile2)) =>
      //将coverage1的MultibandTile（取前3波段）转化为二维矩阵（波段数×pixels）
      val pixels = multibandTile1.rows * multibandTile1.cols
      val RGB_matrix = BreezeDenseMatrix.zeros[Double](3, pixels) //先创建一个空的二维矩阵
      for (i <- 0 until 3){
        val pixelArray: Array[Double] = multibandTile1.band(bandList(i)-1).toArrayDouble()
        val pixelVector: BreezeDenseVector[Double] = BreezeDenseVector(pixelArray)
        RGB_matrix(i, ::) := pixelVector.t //DenseVector 默认是列向量，所以要转成行向量
      }
      //RGB->IHS
      val IHS_matrix: BreezeDenseMatrix[Double] = RGB_IHS * RGB_matrix
      //将coverage2的MultibandTile（取前1波段）转化为一维向量（pixels）
      val pixelArray: Array[Double] = multibandTile2.band(0).toArrayDouble()
      val pixelVector: BreezeDenseVector[Double] = BreezeDenseVector(pixelArray)
      //IHS的I分量替换
      IHS_matrix(0, ::) := pixelVector.t
      //IHS->RGB
      val RGB_matrix_new = IHS_RGB * IHS_matrix
      //将二维矩阵（波段数×pixels）转化为MultibandTile，合成新coverage输出
      val band_ArrayTile = Array.ofDim[Tile](3)
      for (i <- 0 until 3){
        band_ArrayTile(i) = DoubleArrayTile(RGB_matrix_new(i, ::).t.toArray, multibandTile1.cols, multibandTile1.rows)
      }
      (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(band_ArrayTile).convert(cellType))
    }
  }
  else if(method == "Brovey"){
    newCoverageRdd = rdd.map{ case(spatialKey, (multibandTile1, multibandTile2))=>
      val sumRGB: Tile = multibandTile1.band(bandList(0)-1).convert(DoubleConstantNoDataCellType).localMultiply(weight(0)) //11.21增加括号
        .localAdd((multibandTile1.band(bandList(1)-1).convert(DoubleConstantNoDataCellType)).localMultiply(weight(1)))
        .localAdd((multibandTile1.band(bandList(2)-1).convert(DoubleConstantNoDataCellType)).localMultiply(weight(2)))
      var fusedBands: Seq[Tile] = Seq.empty[Tile]
      for(i<- 0 until 3){
        val fusedBand = multibandTile1.band(bandList(i)-1).convert(DoubleConstantNoDataCellType)
          .localMultiply(multibandTile2.band(0).convert(DoubleConstantNoDataCellType))
          .localDivide(sumRGB).localDivide(3)
        fusedBands = fusedBands :+ (fusedBand)
      }
      (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(fusedBands).convert(cellType))
    }
  }
  else if(method == "Esri"){
    newCoverageRdd = rdd.map{ case(spatialKey, (multibandTile1, multibandTile2))=>
      val mean: Tile = multibandTile1.band(bandList(0)-1).convert(DoubleConstantNoDataCellType).localMultiply(weight(0))
        .localAdd((multibandTile1.band(bandList(1)-1).convert(DoubleConstantNoDataCellType)).localMultiply(weight(1)))
        .localAdd((multibandTile1.band(bandList(2)-1).convert(DoubleConstantNoDataCellType)).localMultiply(weight(2)))
      //.localDivide(3)
      val ADJ: Tile = multibandTile2.band(0).convert(DoubleConstantNoDataCellType).localSubtract(mean)
      var fusedBands: Seq[Tile] = Seq.empty[Tile]
      for(i<- 0 until 3){
        val fusedBand = multibandTile1.band(bandList(i)-1).convert(DoubleConstantNoDataCellType)
          .localAdd(ADJ)
        fusedBands = fusedBands :+ (fusedBand)
      }
      (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(fusedBands).convert(cellType))
    }
  }
  else if(method == "Simple mean"){
    //不存在权重选择 R G B
    newCoverageRdd = rdd.map{ case(spatialKey, (multibandTile1, multibandTile2))=>
      var fusedBands: Seq[Tile] = Seq.empty[Tile]
      for(i<- 0 until 3){
        val fusedBand = multibandTile1.band(bandList(i)-1).convert(DoubleConstantNoDataCellType)
          .localAdd(multibandTile2.band(0).convert(DoubleConstantNoDataCellType))
          .localMultiply(0.5)  //注意localAdd和localMultiply的运算顺序问题
        fusedBands = fusedBands :+ (fusedBand.convert(multibandTile1.cellType))
      }
      (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time1), band1), MultibandTile(fusedBands))
    }
  }
  else if(method == "Gram-Schmidt"){
    throw new IllegalArgumentException("Error: 该方法暂未实现")
  }
  else{
    throw new IllegalArgumentException("Error: 不存在当前method")
  }
  //将影像每个波段的值都重新映射到0-255
  val minAndMax = findMinMaxValueDouble((newCoverageRdd, coverage1._2)) //findminmaxValue这个函数不会使用到coverage1._2数据
  val normalizedCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = newCoverageRdd.map(
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
          result(i)(j) = (tile.get(j, i).toDouble - min) / (max - min) * 255
        }
        band_Array(bandIndex) = DoubleArrayTile(result.flatten, cols, rows)
      }
      (image._1, MultibandTile(band_Array).convert(cellType))
    })
  (newCoverageRdd, newCoverage1._2) // TODO 暂时不进行映射
}

def catTwoCoverage(coverage1: RDDImage, coverage2: RDDImage): RDDImage = {
  //对齐像素、分辨率等等
  val (newCoverage1, newCoverage2) = checkProjResoExtent(coverage1, coverage2)
  //    val (newCoverage1, newCoverage2) = (coverage1, coverage2)
  val cellType1: CellType = newCoverage1._1.first()._2.cellType
  val cellType2: CellType = newCoverage2._1.first()._2.cellType
  val cellType = cellType1.union(cellType2) //获得更通用的那种类型
  //时间戳、波段列表暂时以coverage1为准
  val time: Long = newCoverage1._1.first()._1.spaceTimeKey.instant
  //计算新影像波段数，便于后面新建一个空的类型为Tile的Array
  val bandCount1: Int = newCoverage1._1.first()._2.bandCount
  val bandCount2: Int = newCoverage2._1.first()._2.bandCount
  val bandCount: Int = bandCount1 + bandCount2
  val coverage1tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage1._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val coverage2tileRDD: RDD[(SpatialKey, MultibandTile)] = newCoverage2._1.map(t => {
    (t._1.spaceTimeKey.spatialKey, t._2)
  })
  val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1tileRDD.join(coverage2tileRDD)
  val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = rdd.map { case (spatialKey, (multibandTile1, multibandTile2)) =>
    val bandArray = Array.ofDim[Tile](bandCount)
    val band: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String] //先创建一个空波段列表，后面再填充
    for (index1 <- 0 until bandCount1) {
      bandArray(index1) = multibandTile1.band(index1).convert(cellType)
      band.append("B" + (index1+1).toString())
    }
    for (index2 <- 0 until bandCount2) {
      bandArray(bandCount1 + index2) = multibandTile2.band(index2).convert(cellType)
      band.append("B" + (bandCount1 + index2 + 1).toString())
    }
    (SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, time), band), MultibandTile(bandArray))
  }
  val metaData = TileLayerMetadata(cellType, newCoverage1._2.layout, newCoverage1._2.extent, newCoverage1._2.crs, newCoverage1._2.bounds)
  (newCoverageRdd, metaData)
}

//8.19对接王川算子
def histogramBin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), min: Int, max: Int, binSize: Int, bandIndex: Int = 0): Map[Int, Long] = {
  //当前会把最后一个区间的数值作为一个分箱
  //coverage的黑边好像值是0，还不清楚原因，但王川算法后面会将0值筛除所以不影响
  //返回RDD[像素值, 累计像素频率)]
  val bandPixelFrequency: RDD[(Int, Long)] = coverage._1.flatMap(imageRdd => {
    val PixelFrequency: ListBuffer[(Int, Long)] = ListBuffer.empty[(Int, Long)]
    val tile: Tile = imageRdd._2.band(bandIndex)
    val Rows = tile.rows
    val Cols = tile.cols
    // 计算每个像素灰度值的频率
    for (i <- 0 until Rows; j <- 0 until Cols) {
      val pixel = tile.get(j, i)
      if (!isNoData(pixel)) PixelFrequency.append((pixel, 1))
    }
    PixelFrequency.toList
  })
  //整张影像的[(像素值, 像素频率)]
  val bandPixelFrequencyToCount: RDD[(Int, Long)] = bandPixelFrequency.reduceByKey(_ + _)
  //字典，方便查询
  val collectDictionary: Map[Int, Long] = bandPixelFrequencyToCount.collect().toMap
  // 初始化一个Map来存储每个分箱的像素计数
  var binMap: Map[Int, Long] = Map().withDefaultValue(0)
  // 遍历直方图中的所有值
  collectDictionary.foreach { item =>
    if (item._1 >= min && item._1 <= max) {
      // 计算该值属于哪个分箱
      val binIndex = ((item._1 - min) / binSize).toInt
      // 计算该分箱的起始值
      val binKey = min + (binIndex * binSize)
      // 将该值的计数累加到对应分箱中
      binMap = binMap.updated(binKey, binMap(binKey) + item._2)
    }
  }
  var res :String = new String()
  binMap.foreach(item => {
    res += s"${item._1} -> ${item._2} \n"
  })
//  res
  binMap
}
def reduceRegion(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), reducer: String, bandIndex: Int = 0): Double = {
  // reducer 聚合操作类型 (比如 "histogram", "mean" 等)
  val res: Double =
  reducer match{
    case "mean" =>
      val meanMap = meanValueCalculate(coverage)
      meanMap(bandIndex)
    // 其他聚合操作可以类似添加
    case "min" =>
      val minmaxMap: Map[Int, (Double, Double)] = findMinMaxValueDouble(coverage)
      minmaxMap(bandIndex)._1
    case "max" =>
      val minmaxMap: Map[Int, (Double, Double)] = findMinMaxValueDouble(coverage)
      minmaxMap(bandIndex)._2
    case "num" =>
      findTotalPixel(coverage)
    case "std" =>
      val stdMap = standardDeviationCalculate(coverage)
      stdMap(bandIndex)
    case _ =>
      throw new UnsupportedOperationException(s"Reducer '$reducer' not supported.")
  }
  res
}
def RandomForestTrainAndRegress(featuresCoverage: RDDImage, labelCoverage: RDDImage, predictCoverage: RDDImage, checkpointInterval: Int = 10, featureSubsetStrategy: String = "auto", impurity: String = "variance", maxBins: Int = 32, maxDepth: Int = 5, minInfoGain: Double = 0.0, minInstancesPerNode: Int = 1, minWeightFractionPerNode: Double = 0.0, numTrees: Int = 20, seed: Long = Random.nextLong(), subsamplingRate: Double = 1.0): RDDImage ={
  //暂时不提供筛选波段，前端可以使用Coverage.selectBands选
  val spark = SparkSession.builder().appName("RandomForestJob").getOrCreate()
  val model: PipelineModel = Regressor.randomForestRegression(checkpointInterval, featureSubsetStrategy, impurity, maxBins, maxDepth, minInfoGain, minInstancesPerNode, minWeightFractionPerNode, numTrees, seed, subsamplingRate)
    .train(spark, featuresCoverage, labelCoverage)
  val regressCoverage: RDDImage = Regressor.regress(spark, predictCoverage, model)("prediction")
  regressCoverage
}

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HE").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SparkStatCleanJob").getOrCreate() //能不能和SparkContext同时创建？放到自己的函数里创建结束时调用.stop把它停掉

    val coverage1: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\features4label.tif")
    val coverage2: RDDImage = makeChangedRasterRDDFromTif(sc: SparkContext,"C:\\Users\\HUAWEI\\Desktop\\oge\\OGE竞赛\\out\\features4label_clip1.tif")
    val coverage = catTwoCoverage(coverage1, coverage2)
    val result = whu.edu.cn.oge.Coverage.selectBands(coverage, List("B1", "B2"))
    saveRasterRDDToTif(result,"C:\\Users\\HUAWEI\\Desktop\\oge\\coverage_resources1\\cat1021.tiff")


  }

}
