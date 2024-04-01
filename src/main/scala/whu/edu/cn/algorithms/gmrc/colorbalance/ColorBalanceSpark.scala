package whu.edu.cn.algorithms.gmrc.colorbalance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.{Band, Dataset, Driver, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import scala.math.{abs, ceil, sqrt}

object ColorBalanceSpark {
  def main(args: Array[String]): Unit = {
    colorBalance()
  }

  def colorBalance(): Any = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
    val sc = new SparkContext(sparkConf)

    initGdal()
    val inputImagePath = "./data\\testdata\\colorbalance\\input\\Ortho_8bit.tiff"
    val dataset: Dataset = gdal.Open(inputImagePath, gdalconstConstants.GA_ReadOnly)
    if (null == dataset) {
      println("open input file [" + inputImagePath + "] failed")
    }
    val width = dataset.getRasterXSize
    val height = dataset.getRasterYSize
    val blockSizeX = 1024
    val blockSizeY = 1024
    val overlap = 32

    val imageBlocksArr: (ImagesPixelArr, ImagesGeoArr) = splitImage(dataset, blockSizeX, blockSizeY, overlap)
    val XBlockCounts = getDimensionSize(imageBlocksArr._2, 1)
    val yBlockCounts = getDimensionSize(imageBlocksArr._2, 2)

    val lineImgBlkArr = lineSplitImgBlk(imageBlocksArr._1)
    val imgBlk_rdd: PixelBlkMapRDD = sc.makeRDD(lineImgBlkArr)

    val blockMeanVar_rdd: MeanVarMapRDD = calBlocksMeanVariance(imgBlk_rdd)
    blockMeanVar_rdd.foreach(meanVarianceMap => {
      println("band= " + meanVarianceMap._1 + " x= " + meanVarianceMap._2._1 + " y= " + meanVarianceMap._2._2 +
      " mean= " + meanVarianceMap._3._1 + " variance= " + meanVarianceMap._3._2)
    })

    val bigImageMeanVarArr = calBigImageMeanVariance(imgBlk_rdd, blockMeanVar_rdd, XBlockCounts * yBlockCounts, width * height)
//
//    val weightArr: Array[Array[Array[Array[Array[Array[Array[Float]]]]]]] = calWeight(imageBlocksArr._2)
//
//    val abPar = abParameter(weightArr, imageBlocksArr._2, bigImageMeanVarArr, blockMeanVarArr)
//    val cbImgBlks = calColorBalancedImageBlocks(imageBlocksArr._1, abPar)
//
//    val outputFile = "./data\\testdata\\colorbalance\\output\\Ortho_8bit_output.tif"
//    val result = writeCbdImgBlks(outputFile, cbImgBlks, dataset, overlap, overlap)
//
//    destroyGdal()

    sc.stop()
  }

  // 线性化数组，为了可以生成 rdd
  private def lineSplitImgBlk(splitImgBlk: ImagesPixelArr): Array[ImageBlockMap] = {
    val bandCounts = getDimensionSize(splitImgBlk, 0)
    val xBlockCounts = getDimensionSize(splitImgBlk, 1)
    val yBlockCounts = getDimensionSize(splitImgBlk, 2)

    val lineArr: Array[ImageBlockMap] = new Array[(Int, (Int, Int), Array[Array[Short]])](bandCounts * xBlockCounts * yBlockCounts)

    for (bandIndex <- 1 to  bandCounts) {
      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {
          lineArr((bandIndex - 1) * xBlockCounts * yBlockCounts + x * yBlockCounts + y) = (bandIndex, (x, y), splitImgBlk(bandIndex - 1)(x)(y))
        }
      }
    }

    lineArr
  }

  // 图像的像素数组，一维为波段，二三维分别为 x 和 y 方向的分块，四五维分别为图像块 x 和 y 方向块的位置，模板值为像素值
  type ImagesPixelArr = Array[Array[Array[Array[Array[Short]]]]]
  // 图像的几何数据数组，一维为波段，二三维分别为 x 和 y 方向的分块，模板值为分块的 x 和 y 方向的像素个数
  type ImagesGeoArr = Array[Array[Array[(Int, Int)]]]

  type ImageBlockMap = (Int, (Int, Int), Array[Array[Short]])
  type PixelBlkMapRDD = RDD[(Int, (Int, Int), Array[Array[Short]])]
  type MeanVarMapRDD = RDD[(Int, (Int, Int), (Double, Double))]

  /**
   * 按照 LRM 思想，划分图像
   *
   * @param dataset    gdal 打开的图像数据集
   * @param blockSizeX 要划分块的 X 方向的大小
   * @param blockSizeY 要划分块的 Y 方向的大小
   * @param overlap    划分块的重叠区域
   * @return  (ImagesPixelArr, ImagesGeoArr) 返回分块数组和分块数组的几何结构
   */
  private def splitImage(dataset: Dataset, blockSizeX: Int, blockSizeY: Int, overlap: Int): (ImagesPixelArr, ImagesGeoArr) = {

    val bandCounts = dataset.getRasterCount
    val width = dataset.getRasterXSize
    val height = dataset.getRasterYSize

    // 分块以不重叠分块，读取数据时，以包含重叠部分来读取，在最后一列和最后一行中，则没有重叠，直接按照最后分块的实际大小来读取
    val xBlockCounts = ceil(width.toDouble / (blockSizeX - overlap)).toInt
    val yBlockCounts = ceil(height.toDouble / (blockSizeY - overlap)).toInt
    val pixelValuesArr: ImagesPixelArr = Array.ofDim[Array[Array[Short]]](bandCounts, xBlockCounts, yBlockCounts)
    val imageBlocksGeoArr: ImagesGeoArr = Array.ofDim[(Int, Int)](bandCounts, xBlockCounts, yBlockCounts)

    for (bandIndex <- 1 to bandCounts) {
      for (x <- 0 until width by (blockSizeX - overlap)) {
        for (y <- 0 until height by (blockSizeY - overlap)) {
          val xSize = math.min(blockSizeX, width - x)  // 在最后一列中，则没有重叠，直接按照最后分块的实际大小来读取
          val ySize = math.min(blockSizeY, height - y)  // 在最后一行中，则没有重叠，直接按照最后分块的实际大小来读取

          // 读取每块影像
          val band: Band = dataset.GetRasterBand(bandIndex)
          val blockData: Array[Short] = Array.ofDim[Short](xSize * ySize)
          band.ReadRaster(x, y, xSize, ySize, blockData)

          // 线性数组二维化，便于后续更好处理数据
          val twoDimensionalBlockData: Array[Array[Short]] = Array.ofDim[Short](xSize, ySize)
          for (index <- blockData.indices) {
            val xDimIndex: Int = index % xSize
            val yDimIndex: Int = index / xSize
            twoDimensionalBlockData(xDimIndex)(yDimIndex) = blockData(index)
          }

          // 将块数据写入数组
          val blockX = x / (blockSizeX - overlap)
          val blockY = y / (blockSizeY - overlap)
          pixelValuesArr(bandIndex - 1)(blockX)(blockY) = twoDimensionalBlockData
          imageBlocksGeoArr(bandIndex - 1)(blockX)(blockY) = (xSize, ySize) // 后续图像需要几何数据，使用此参数
        }
      }
    }

    (pixelValuesArr, imageBlocksGeoArr)
  }

  /**
   * 将匀光后在内存中的数据写入到磁盘中
   * @param file  要写入的结果文件
   * @param cbdImgBlksArr  匀光处理后的图像块
   * @param surDataset  待匀光的源图像的 Dataset，这里可以用其他数据替代
   * @param splitOverlap  划分的块的重叠大小
   * @param writeOverlap  写入块的重叠大小
   */
  private def writeCbdImgBlks(file: String, cbdImgBlksArr: ImagesPixelArr,
                              surDataset: Dataset, splitOverlap: Int, writeOverlap: Int): Unit = {
    val driver: Driver = gdal.GetDriverByName("GTiff")
    val dataset = driver.Create(file, surDataset.getRasterXSize,
      surDataset.getRasterYSize, surDataset.getRasterCount, surDataset.GetRasterBand(1).getDataType)

    val bandCounts = getDimensionSize(cbdImgBlksArr, 0)
    val xBlockCounts = getDimensionSize(cbdImgBlksArr, 1)
    val yBlockCounts = getDimensionSize(cbdImgBlksArr, 2)

    val xMaxSize = getDimensionSize(cbdImgBlksArr(0)(0)(0), 0)
    val yMaxSize = getDimensionSize(cbdImgBlksArr(0)(0)(0), 1)

    for (bandIndex <- 1 to bandCounts) {
      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {
          val xSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x)(y), 0)
          val ySize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x)(y), 1)

          // 1.处理每块写入的起始位置
          var xOff = x * (xSize - splitOverlap)
          var yOff = y * (ySize - splitOverlap)

          // 1.1 处理倒数第二行、倒数第二列的特殊数据
          if (xBlockCounts - 2 == x) {
            val regularXSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x - 2)(y), 0)
            xOff = x * (regularXSize - splitOverlap)
          }
          if (yBlockCounts - 2 == y) {
            val regularYSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x)(y - 2), 1)
            yOff = y * (regularYSize - splitOverlap)
          }

          // 1.2 处理倒数第一行、倒数第一列的特殊数据
          if ((xBlockCounts - 1) == x) {
            val regularXSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x - 3)(y), 0)
            val posX = (x - 1) * (regularXSize - splitOverlap)
            val lastSecondXSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x - 1)(y), 0)
            if (regularXSize == lastSecondXSize) {
              xOff = posX + (lastSecondXSize - splitOverlap)
            } else {
              xOff = posX + (lastSecondXSize - xSize)
            }
          }
          if ((yBlockCounts - 1) == y) {
            val regularYSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x)(y - 3), 1)
            val posY = (y - 1) * (regularYSize - splitOverlap)
            val lastSecondYSize = getDimensionSize(cbdImgBlksArr(bandIndex - 1)(x)(y - 1), 1)
            if (regularYSize == lastSecondYSize) {
              yOff = posY + (lastSecondYSize - splitOverlap)
            } else {
              yOff = posY + (lastSecondYSize - ySize)
            }
          }

          // 2.处理每块写入的长和宽
          var xPixelSize = xSize - writeOverlap
          var yPixelSize = ySize - writeOverlap
          if (xMaxSize > xSize) {
            xPixelSize = xSize
          }
          if (yMaxSize > ySize) {
            yPixelSize = ySize
          }

          // 3.读取图片像素值，二维数据一维化，便于用 gdal 写数据
          val colorBalancedTwoDimArr: Array[Array[Short]] = cbdImgBlksArr(bandIndex - 1)(x)(y)
          val colorBalancedLineArr: Array[Short] = Array.ofDim[Short](xPixelSize * yPixelSize)
          for (index <- colorBalancedLineArr.indices) {
            val xDimIndex: Int = index % xPixelSize
            val yDimIndex: Int = index / xPixelSize
            colorBalancedLineArr(index) = colorBalancedTwoDimArr(xDimIndex)(yDimIndex)
          }

          // 4.写入数据
          dataset.GetRasterBand(bandIndex).WriteRaster(xOff, yOff, xPixelSize, yPixelSize, colorBalancedLineArr)
        }
      }
    }
  }

  /**
   * 计算所有分块图像的均值和方差
   * @param imgBlk_rdd 图像分块 RDD
   * @return MeanVarMapRDD 图像分块的均值和方差 RDD
   */
  private def calBlocksMeanVariance(imgBlk_rdd: RDD[ImageBlockMap]): MeanVarMapRDD = {

    val imgBlk_meanVar_rdd: MeanVarMapRDD = imgBlk_rdd.map(imageBlockMap => {
      (imageBlockMap._1, imageBlockMap._2, getMeanVariance(imageBlockMap._3))
    })

    imgBlk_meanVar_rdd
  }

  /**
   * 依据图像的分块，和分块后的均值和方差（主要是均值），计算整幅图像的均值和方差
   * @param pixelBlk_rdd 图像分块数组的像素值
   * @param meanVar_rdd 图像分块数组的均值和方差
   * @param bandBlkCounts  图像块数
   * @param bigImageScale 整幅图像的范围
   * @return Array[(Double, Double)] 整幅图像的均值和方差数组，_1 为均值，_2 为方差
   */
  private def calBigImageMeanVariance(pixelBlk_rdd: PixelBlkMapRDD, meanVar_rdd: MeanVarMapRDD,
                                      bandBlkCounts: Int, bigImageScale: Long): Any = {

    // 1.计算整幅图像的均值之和
//    val meanSumArr: Array[Double] = new Array[Double](bandCounts)
//
//    val meanSum: Double = meanVar_rdd.map(meanVarianceMap => {
//      meanVarianceMap._3._1
//    }).reduce((mean1, mean2) => mean1 + mean2)

    val band_mean_rdd: RDD[(Int, Iterable[Double])] = meanVar_rdd.map(meanVarMap => {
      (meanVarMap._1, meanVarMap._3._1)
    }).groupByKey()

    val band_mean: Array[(Int, Double)] = band_mean_rdd.map(band_meanArr => {
      var meanSum = 0.0
      band_meanArr._2.foreach(mean => {
        meanSum += mean
      })
      var bigImageMean = meanSum / bandBlkCounts
      (band_meanArr._1, bigImageMean)
    }).collect()

    val a = 1

    // 2.由每个分块的均值之和计算整幅图像的均值，此处均值不是真正的均值
//    val bigImageMeanArr: Array[Double] = new Array[Double](bandCounts)
//    for (bandIndex <- 1 to bandCounts) {
//      bigImageMeanArr(bandIndex - 1) = meanSumArr(bandIndex - 1) / (xBlockCounts * yBlockCounts) // 这里是由每个块的和构成，所以除以块数
//    }
//
//    // 3.计算整幅图像的方差之和
//    val varianceSumArr: Array[Double] = new Array[Double](bandCounts)
//    for (bandIndex <- 1 to bandCounts) {
//      for (x <- 0 until xBlockCounts) {
//        for (y <- 0 until yBlockCounts) {
//
//          val imagePixelBlock: Array[Array[Short]] = pixelBlk_rdd(bandIndex - 1)(x)(y)
//          val xSize = getDimensionSize(imagePixelBlock, 0)
//          val ySize = getDimensionSize(imagePixelBlock, 1)
//
//          for (i <- 0 until xSize) {
//            for (j <- 0 until ySize) {
//              val deviationPixel: Double = imagePixelBlock(i)(j) - bigImageMeanArr(bandIndex - 1)
//              varianceSumArr(bandIndex - 1) += deviationPixel * deviationPixel
//            }
//          }
//        }
//      }
//    }
//
//    // 4.由整幅图像的方差之和计算整幅图像的方差
//    val bigImageVarianceArr: Array[Double] = new Array[Double](bandCounts)
//    for (bandIndex <- 1 to bandCounts) {
//      bigImageVarianceArr(bandIndex - 1) = varianceSumArr(bandIndex - 1) / (bigImageScale - 1)
//    }
//
//    // 5.将值以元组数组的形式，返回
//    for (bandIndex <- 1 to bandCounts) {
//      bigImageMeanVarianceArr(bandIndex - 1) = (bigImageMeanArr(bandIndex - 1), bigImageVarianceArr(bandIndex - 1))
//    }

//    bigImageMeanVarianceArr
  }


  /**
   * 计算单个像素块的均值和方差
   * @param imagePixelBlock 图像块的像素数组，二维
   * @return  (Double, Double) _1 为均值、_2 为方差
   */
  private def getMeanVariance(imagePixelBlock: Array[Array[Short]]): (Double, Double) = {
    var mean: Double = -1.0
    var variance: Double = -1.0

    if (!imagePixelBlock.isEmpty) {
      val xSize = getDimensionSize(imagePixelBlock, 0)
      val ySize = getDimensionSize(imagePixelBlock, 1)

      var pixelSum: Long = 0
      for (x <- 0 until xSize) {
        for (y <- 0 until ySize) {
          pixelSum += imagePixelBlock(x)(y)
        }
      }

      mean = pixelSum.toDouble / (xSize * ySize)

      var varianceSum: Double = 0.0
      for (x <- 0 until xSize) {
        for (y <- 0 until ySize) {
          val deviationPixel: Double = imagePixelBlock(x)(y).toDouble - mean
          varianceSum += deviationPixel * deviationPixel
        }
      }

      variance = varianceSum / (xSize * ySize - 1)
    }

    (mean, variance)
  }

  /**
   * 平方，加快速度
   * @param absNum 要平方数的数
   * @return  数的平方
   */
  private def square(absNum: Int): Int = {
    abs(absNum) << 1
  }

  /**
   * 获取像素块的有效邻域
   * @param x  像素块 x 坐标
   * @param y  像素块 y 坐标
   * @param xSize  像素块总的 x 的大小
   * @param ySize  像素块总的 y 的大小
   * @return  像素块的有效邻域，为 1，无效领域为 0
   */
  private def neighborhoodPos(x: Int, y: Int, xSize: Int, ySize: Int): Array[Array[Byte]] = {
    val weightPosArr: Array[Array[Byte]] = Array.ofDim[Byte](3, 3)

    // 默认是有 3 * 3 维的邻域
    for (i <- 0 until 3) {
      for (j <- 0 until 3) {
        weightPosArr(i)(j) = 1
      }
    }

    // 提前结束，防止 5% 的特殊邻域浪费时间
    if (((0 < x) && (x < (xSize - 1))) && ((0 < y) && (y < (ySize - 1)))) {
      return weightPosArr
    }

    // 至少要分两块
    if (0 > (x - 1)) { // 左边无第一列
      weightPosArr(0)(0) = 0
      weightPosArr(0)(1) = 0
      weightPosArr(0)(2) = 0
    } else if ((x + 1) > (xSize - 1)) { // 右边无第三列
      weightPosArr(2)(0) = 0
      weightPosArr(2)(1) = 0
      weightPosArr(2)(2) = 0
    }

    if (0 > (y - 1)) { // 上边无第一行
      weightPosArr(0)(0) = 0
      weightPosArr(1)(0) = 0
      weightPosArr(2)(0) = 0
    } else if ((y + 1) > (ySize - 1)) { // 下边无第三行
      weightPosArr(0)(2) = 0
      weightPosArr(1)(2) = 0
      weightPosArr(2)(2) = 0
    }

    weightPosArr
  }

  /**
   * 由像素块邻域位置映射到相应的坐标方向
   * @param i  像素块邻域的 x 方向索引
   * @param j  像素块邻域的 y 方向索引
   * @return  坐标方向
   */
  private def posMapCorDir(i: Int, j: Int): (Int, Int) = {
    var x = 0
    var y = 0

    i match {
      case 0 => x = -1
      case 1 => x = 0
      case 2 => x = 1
      case _ =>
    }

    j match {
      case 0 => y = -1
      case 1 => y = 0
      case 2 => y = 1
    }

    (x, y)
  }

  /**
   * 获取某块像素的邻域均值和方差
   * @param meanVarianceArr  像素块的均值和方差数组
   * @param bandIndex  波段索引
   * @param x  像素块 x 方向的索引
   * @param y  像素块 y 方向的索引
   * @param xBlockCounts  像素块 x 方向的个数
   * @param yBlockCounts  像素块 y 方向的个数
   * @return  均值和方差的邻域数组
   */
  private def meanVarianceNbh(meanVarianceArr: Array[Array[Array[(Double, Double)]]],
                              bandIndex: Int, x: Int, y: Int, xBlockCounts: Int, yBlockCounts: Int): Array[Array[(Double, Double)]] = {
    val meanVarianceNbhArr: Array[Array[(Double, Double)]] = Array.ofDim[(Double, Double)](3, 3)  // 具体块的邻域均值和方差

    val weightXSize = 3
    val weightYSize = 3
    for (i <- 0 until weightXSize) {
      for (j <- 0 until weightYSize) {

        // 邻域位置转换为具体坐标，然后返回
        val corDir = posMapCorDir(i, j)  // 获取中心像素得到邻域坐标的方向
        val xNbh = x + corDir._1  // 获取邻域 x 坐标
        val yNbh = y + corDir._2  // 获取邻域 y 坐标

        var mean = -1.0  // 超过范围的均值由后续的邻域位置负责
        var variance = -1.0  //  超过范围的方差由后续的邻域位置负责
        if (((-1 < xNbh) && (xNbh < xBlockCounts)) && ((-1 < yNbh) && (yNbh < yBlockCounts))) {
          mean = meanVarianceArr(bandIndex)(xNbh)(yNbh)._1
          variance = meanVarianceArr(bandIndex)(xNbh)(yNbh)._2
        }

        meanVarianceNbhArr(i)(j) = (mean, variance)
      }
    }

    meanVarianceNbhArr
  }

  /**
   * 计算某块的邻域块的像素范围，用于计算权重
   * @param imageBlocksBandGeoArr  图像分块的几何数组
   * @param x  某分块的 x
   * @param y  某分块的 y
   * @param xBlockCounts  分块的 x 方向个数
   * @param yBlockCounts  分块的 y 方向个数
   * @return  某块的邻域块的范围
   */
  private def scaleSizeNbh(imageBlocksBandGeoArr: Array[Array[(Int, Int)]], x: Int, y: Int,
                           xBlockCounts: Int, yBlockCounts: Int): Array[Array[(Int, Int)]] = {
    val weightXSize = 3
    val weightYSize = 3
    val scaleSizeNbh: Array[Array[(Int, Int)]] = Array.ofDim[(Int, Int)](weightXSize, weightYSize)

    for (i <- 0 until weightXSize) {
      for (j <- 0 until weightYSize) {

        val corDir = posMapCorDir(i, j) // 获取中心像素得到邻域坐标的方向
        val xNbh = x + corDir._1 // 获取邻域 x 坐标
        val yNbh = y + corDir._2 // 获取邻域 y 坐标

        var xSize = 0  // 没在范围则设置为 0，由邻域位置保证
        var ySize = 0  // 没在范围则设置为 0，由邻域位置保证
        if (((-1 < xNbh) && (xNbh < xBlockCounts)) && ((-1 < yNbh) && (yNbh < yBlockCounts))) {
          xSize = imageBlocksBandGeoArr(xNbh)(yNbh)._1
          ySize = imageBlocksBandGeoArr(xNbh)(yNbh)._2
        }

        scaleSizeNbh(i)(j) = (xSize, ySize)
      }
    }

    scaleSizeNbh
  }

  /**
   * 计算每个块的邻域权重块
   * @param imageBlocksGeoArr  图像分块的几何数组
   * @return  一二三维分别为波段、块 x 方向、块 y 方向，四五维为邻域的索引，六七维为具体块中 x 方向、y 方向，模板为 权重值
   */
  private def calWeight(imageBlocksGeoArr: ImagesGeoArr): Array[Array[Array[Array[Array[Array[Array[Float]]]]]]] = {
    val bandCounts = getDimensionSize(imageBlocksGeoArr, 0)
    val xBlockCounts = getDimensionSize(imageBlocksGeoArr, 1)
    val yBlockCounts = getDimensionSize(imageBlocksGeoArr, 2)

    // 所有分块的总个的权重数组，最后的返回值  模板为某块图像周围的大概 9 个块（这里是两个维度），每个块中的每个元素的权重值（这里是两个维度)
    val weightArr: Array[Array[Array[Array[Array[Array[Array[Float]]]]]]] = Array.ofDim[Array[Array[Array[Array[Float]]]]](bandCounts, xBlockCounts, yBlockCounts)

    // 计算每个块中周围的权重
    val weightXSize = 3
    val weightYSize = 3
    for (bandIndex <- 1 to bandCounts) {
      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {

          val xPixelSize = imageBlocksGeoArr(bandIndex - 1)(x)(y)._1
          val yPixelSize = imageBlocksGeoArr(bandIndex - 1)(x)(y)._2

          val weightBlocksArr: Array[Array[Array[Array[Float]]]] = Array.ofDim[Float](weightXSize, weightXSize, xPixelSize, yPixelSize)
          val weightPosArr: Array[Array[Byte]] = neighborhoodPos(x, y, xBlockCounts, yBlockCounts)
          val scaleSizeNbhArr: Array[Array[(Int, Int)]] = scaleSizeNbh(imageBlocksGeoArr(bandIndex - 1), x, y, xBlockCounts, yBlockCounts)

          // 具体计算每块图像的周围 9 块的每个像素的权重
          for (i <- 0 until xPixelSize) {
            for (j <- 0 until yPixelSize) {

              weightBlocksArr(0)(0)(i)(j) = weightPosArr(0)(0) * 1 / sqrt(square(scaleSizeNbhArr(0)(0)._1 + i) +
                square(scaleSizeNbhArr(0)(0)._2 + j)).toFloat
              weightBlocksArr(0)(1)(i)(j) = weightPosArr(0)(1) * 1 / sqrt(square(scaleSizeNbhArr(0)(1)._1 + i) +
                square(scaleSizeNbhArr(0)(1)._2 - j)).toFloat
              weightBlocksArr(0)(2)(i)(j) = weightPosArr(0)(2) * 1 / sqrt(square(scaleSizeNbhArr(0)(2)._1 + i) +
                square(scaleSizeNbhArr(0)(2)._2 + 2 * scaleSizeNbhArr(1)(1)._2 - j)).toFloat

              weightBlocksArr(1)(0)(i)(j) = weightPosArr(1)(0) * 1 / sqrt(square(scaleSizeNbhArr(1)(0)._1 - i) +
                square(scaleSizeNbhArr(1)(0)._2 + j)).toFloat
              weightBlocksArr(1)(1)(i)(j) = weightPosArr(1)(1) * 1 / sqrt(square(scaleSizeNbhArr(1)(1)._1 - i) +
                square(scaleSizeNbhArr(1)(1)._2 - j)).toFloat
              weightBlocksArr(1)(2)(i)(j) = weightPosArr(1)(2) * 1 / sqrt(square(scaleSizeNbhArr(1)(2)._1 - i) +
                square(scaleSizeNbhArr(1)(2)._2 + 2 * scaleSizeNbhArr(1)(1)._1 - j)).toFloat

              weightBlocksArr(2)(0)(i)(j) = weightPosArr(2)(0) * 1 / sqrt(square(scaleSizeNbhArr(2)(0)._1 + 2 * scaleSizeNbhArr(1)(1)._1 - i) +
                square(scaleSizeNbhArr(2)(0)._2 + j)).toFloat
              weightBlocksArr(2)(1)(i)(j) = weightPosArr(2)(1) * 1 / sqrt(square(scaleSizeNbhArr(2)(1)._1 + 2 * scaleSizeNbhArr(1)(1)._1 - i) +
                square(scaleSizeNbhArr(2)(1)._2 - j)).toFloat
              weightBlocksArr(2)(2)(i)(j) = weightPosArr(2)(2) * 1 / sqrt(square(scaleSizeNbhArr(2)(2)._1 + 2 * scaleSizeNbhArr(1)(1)._1 - i) +
                square(scaleSizeNbhArr(2)(2)._2 + 2 * scaleSizeNbhArr(1)(1)._2 - j)).toFloat

              // 求所有权重和
              var weightBlocksSum: Float = 0.0.toFloat
              for (m <- 0 until weightXSize) {
                for (n <- 0 until weightYSize) {
                  weightBlocksSum += weightBlocksArr(m)(n)(i)(j)
                }
              }

              // 权重归一化
              for (m <- 0 until weightXSize) {
                for (n <- 0 until weightYSize) {
                  weightBlocksArr(m)(n)(i)(j) = weightBlocksArr(m)(n)(i)(j) / weightBlocksSum
                }
              }
            }
          }

          weightArr(bandIndex - 1)(x)(y) = weightBlocksArr
        }
      }
    }

    weightArr
  }

  /**
   * 计算线性系数 a 和 b
   * @param weightArr  权重数组
   * @param imageBlockScale 分块图像的几何数组
   * @param bigImageMeanVarianceArr  大图像的均值和方差数组
   * @param imageBlockMeanVarianceArr  分块图像的均值和方差数组
   * @return Array[Array[Array[(Array[Array[Float]], Array[Array[Float]])]]]  一维波段、二维分块 x 方向，三维分块 y 方向，
   *         _1 为系数 a 的模块值，_2 为系数 b 的模块值
   */
    private def abParameter(weightArr: Array[Array[Array[Array[Array[Array[Array[Float]]]]]]], imageBlocksGeoArr: ImagesGeoArr,
                            bigImageMeanVarianceArr: Array[(Double, Double)],
                            imageBlockMeanVarianceArr: Array[Array[Array[(Double, Double)]]]):
    Array[Array[Array[(Array[Array[Float]], Array[Array[Float]])]]] = {

    val bandCounts = getDimensionSize(imageBlocksGeoArr, 0)
    val xBlockCounts = getDimensionSize(imageBlocksGeoArr, 1)
    val yBlockCounts = getDimensionSize(imageBlocksGeoArr, 2)

    val weightXSize = 3
    val weightYSize = 3

    val abParameterArr: Array[Array[Array[(Array[Array[Float]], Array[Array[Float]])]]] =
      Array.ofDim[(Array[Array[Float]], Array[Array[Float]])](bandCounts, xBlockCounts, yBlockCounts)
    for (bandIndex <- 1 to bandCounts) {
      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {

          val xPixelSize = imageBlocksGeoArr(bandIndex - 1)(x)(y)._1
          val yPixelSize = imageBlocksGeoArr(bandIndex - 1)(x)(y)._2

          val aWeightVarianceRateArr: Array[Array[Float]] = Array.ofDim(xPixelSize, yPixelSize) // 计算的加权的方差比值权重，即线性系数 a
          val bWeightVarianceRateMeanArr: Array[Array[Float]] = Array.ofDim(xPixelSize, yPixelSize) // 计算的加权的方差比值权重然后加上均值影响，即线性系数 b

          val meanVariancePosArr: Array[Array[Byte]] = neighborhoodPos(x, y, xBlockCounts, yBlockCounts)  // 获取当下块的 3 * 3 的邻域位置数组

          val bigImageMean: Float = bigImageMeanVarianceArr(bandIndex - 1)._1.toFloat // 整幅图像的均值
          val bigImageVariance: Float = bigImageMeanVarianceArr(bandIndex - 1)._2.toFloat // 整幅图像的方差

          val meanVarianceNbhArr: Array[Array[(Double, Double)]] = meanVarianceNbh(imageBlockMeanVarianceArr,
            bandIndex - 1, x, y, xBlockCounts, yBlockCounts)  // 获取当下块的 3 * 3 的邻域均值和方差

          val weightBlockValueNbhArr: Array[Array[Array[Array[Float]]]] = weightArr(bandIndex - 1)(x)(y) // 包含当下块的 3 * 3 的邻域的权重

          for (i <- 0 until xPixelSize) {
            for (j <- 0 until yPixelSize) {
              for (m <- 0 until weightXSize) {
                for (n <- 0 until weightYSize) {

                  var variance: Double = meanVarianceNbhArr(m)(n)._2
                  if (0.0 == variance) {
                    variance =  bigImageVariance
                  }

                  aWeightVarianceRateArr(i)(j) += (meanVariancePosArr(m)(n) * weightBlockValueNbhArr(m)(n)(i)(j) * bigImageVariance / variance).toFloat // a 系数公式
                  bWeightVarianceRateMeanArr(i)(j) += (meanVariancePosArr(m)(n) * weightBlockValueNbhArr(m)(n)(i)(j) *
                    (bigImageMean - bigImageVariance / variance * meanVarianceNbhArr(m)(n)._1)).toFloat // b 系数公式

                }
              }
            }
          }

          abParameterArr(bandIndex - 1)(x)(y) = (aWeightVarianceRateArr, bWeightVarianceRateMeanArr)
        }
      }
    }

    abParameterArr
  }

  /**
   * 计算匀光后的图像块
   * @param imageBlocksArr  分块后的图像块数组
   * @param abParameterArr  计算的 a 系数和 b 系数
   * @return  匀光后的分块图像
   */
  private def calColorBalancedImageBlocks(imageBlocksArr: Array[Array[Array[Array[Array[Short]]]]],
                                       abParameterArr: Array[Array[Array[(Array[Array[Float]], Array[Array[Float]])]]]): ImagesPixelArr = {

    val bandCounts = getDimensionSize(imageBlocksArr, 0)
    val xBlockCounts = getDimensionSize(imageBlocksArr, 1)
    val yBlockCounts = getDimensionSize(imageBlocksArr, 2)

    val colorBalancedImageBlocksArr: Array[Array[Array[Array[Array[Short]]]]]  = Array.ofDim[Array[Array[Short]]](bandCounts, xBlockCounts, yBlockCounts)
    for (bandIndex <- 1 to bandCounts) {
      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {
          val xPixelSize = getDimensionSize(imageBlocksArr(bandIndex - 1)(x)(y), 0)
          val yPixelSize = getDimensionSize(imageBlocksArr(bandIndex - 1)(x)(y), 1)

          val aWeightVarianceRateArr: Array[Array[Float]] = abParameterArr(bandIndex - 1)(x)(y)._1 // 计算的加权的方差比值权重，即线性系数 a
          val bWeightVarianceRateMeanArr: Array[Array[Float]] = abParameterArr(bandIndex - 1)(x)(y)._2 // 计算的加权的方差比值权重然后加上均值影响，即线性系数 b

          val oldImageBlock: Array[Array[Short]] = imageBlocksArr(bandIndex - 1)(x)(y)
          val colorBalancedImageBlock: Array[Array[Short]] = Array.ofDim[Short](xPixelSize, yPixelSize)
          for (i <- 0 until xPixelSize) {
            for (j <- 0 until yPixelSize) {
              colorBalancedImageBlock(i)(j) = (aWeightVarianceRateArr(i)(j) * oldImageBlock(i)(j) + bWeightVarianceRateMeanArr(i)(j)).toShort
            }
          }

          colorBalancedImageBlocksArr(bandIndex - 1)(x)(y) = colorBalancedImageBlock
        }
      }
    }

    colorBalancedImageBlocksArr
  }

  /**
   * 得到多维数组的某个深度的维度大小
   * @param array 多维数组
   * @param depth 数组深度
   * @return Int     array 的 depth 深度维度大小
   */
  private def getDimensionSize(array: Any, depth: Int): Int = {
    array match {
      case a: Array[_] if depth > 0 =>
        if (a.isEmpty) 0 else getDimensionSize(a.head, depth - 1)
      case a: Array[_] if depth == 0 =>
        a.length
      case _ =>
        0 // 深度超出了数组的维度
    }
  }

  /**
   * 注册 gdal
   */
  def initGdal(): Unit = {
    gdal.AllRegister()
    ogr.RegisterAll()
  }

  /**
   * 销毁 gdal
   */
  def destroyGdal(): Unit = {
    gdal.GDALDestroyDriverManager()
  }
}