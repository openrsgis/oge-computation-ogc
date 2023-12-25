package whu.edu.cn.algorithms.gmrc.colorbalance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.{Band, Dataset, Driver, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.trigger.Trigger.dagId
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.math.{abs, ceil, sqrt}

object ColorBalance {

  class Data {
    val m_weight_nbh = (3, 3)
    var m_image_scale = (0, 0)  // 图片的实际总大小，_1 为图片的宽，_2 为图片的高

    var m_splitBlock_total_count = (0, 0) // _1 为 x 方向的总块数，_2 为 y 方向的总块数

    var m_band_scale = (0, 0) // _1 为起始波段，_2 为终止波段
    var m_band_counts = 0

    var m_xBlock_cal_scale = (0, 0)  // 对算法外，因内存不够，x 方向实际只能需要的块，_1 为块的起始，_2 为块的终止
    var m_xBlock_cal_counts = 0  // x 块的计算块数
    var m_yBlock_cal_scale = (0, 0) // 对算法外，因内存不够，y 方向实际只能需要的块，_1 为块的起始，_2 为块的终止
    var m_yBlock_cal_counts = 0  // y 块的计算块数

    var m_xBlock_weight_scale = (0, 0)  // 对算法内，因算法正确性需要， x 方向实际必须得块，_1 为块的起始，_2 为块的终止
    var m_xBlock_weight_counts = 0  // x 块实际的权重需要块数
    var m_yBlock_weight_scale = (0, 0)  // 对算法内，因算法正确性需要， x 方向实际必须得块，_1 为块的起始，_2 为块的终止
    var m_yBlock_weight_counts = 0  // y 块实际的权重需要块数

    def setCounts(): Unit = {
      m_band_counts = m_band_scale._2 - m_band_scale._1 + 1

      m_xBlock_cal_counts = m_xBlock_cal_scale._2 - m_xBlock_cal_scale._1
      m_yBlock_cal_counts = m_yBlock_cal_scale._2 - m_yBlock_cal_scale._1

      m_xBlock_weight_counts = m_xBlock_weight_scale._2 - m_xBlock_weight_scale._1 + 1
      m_yBlock_weight_counts = m_yBlock_weight_scale._2 - m_yBlock_weight_scale._1 + 1
    }

    // 一般情况下，建议这种分块
    def setRealScale_general(): Any = {
      m_xBlock_cal_scale = (0, m_splitBlock_total_count._1 - 1)
      m_yBlock_cal_scale = (0, m_splitBlock_total_count._2 - 1)

      m_xBlock_weight_scale = m_xBlock_cal_scale
      m_yBlock_weight_scale = m_yBlock_cal_scale
    }

    // 特殊情况下，内存不足时，用这样的分块，但是会浪费不小的空间，因为要围起来
    def setRealScale_special(xNeed: (Int, Int), yNeed: (Int, Int)): Any = {
      m_xBlock_cal_scale = xNeed
      m_yBlock_cal_scale = yNeed

      m_xBlock_weight_scale = m_xBlock_cal_scale
      m_yBlock_weight_scale = m_yBlock_cal_scale

      setRealScale()
    }

    // 设置内存不足情况下的特殊值
    private def setRealScale(): Unit = {
      var x1_isChange = false
      if ((0 < m_xBlock_cal_scale._1) && (m_xBlock_cal_scale._1 < (m_splitBlock_total_count._1 - 1))) {
        m_xBlock_weight_scale = (m_xBlock_cal_scale._1 - 1, m_xBlock_cal_scale._2)
        x1_isChange = true
      }

      if ((0 < m_xBlock_cal_scale._2) && (m_xBlock_cal_scale._2 < (m_splitBlock_total_count._1 - 1))) {
        if (x1_isChange) {
          m_xBlock_weight_scale = (m_xBlock_cal_scale._1 - 1, m_xBlock_cal_scale._2 + 1)
        } else {
          m_xBlock_weight_scale = (m_xBlock_cal_scale._1, m_xBlock_cal_scale._2 + 1)
        }

      }

      var y1_isChange = false
      if ((0 < m_yBlock_cal_scale._1) && (m_yBlock_cal_scale._1 < (m_splitBlock_total_count._2 - 1))) {
        m_yBlock_weight_scale = (m_yBlock_cal_scale._1 - 1, m_yBlock_cal_scale._2)
        y1_isChange = true
      }

      if ((0 < m_yBlock_cal_scale._2) && (m_yBlock_cal_scale._2 < (m_splitBlock_total_count._2 - 1))) {
        if (y1_isChange) {
          m_yBlock_weight_scale = (m_yBlock_cal_scale._1 - 1, m_yBlock_cal_scale._2 + 1)
        } else {
          m_yBlock_weight_scale = (m_yBlock_cal_scale._1, m_yBlock_cal_scale._2 + 1)
        }
      }
    }
  }

  private val m_data: Data = new Data

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
    val sc = new SparkContext(sparkConf)

    val startTime = System.nanoTime()
    colorBalance(sc, null)
    val endTime = System.nanoTime()
    val costTime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
    println("spark cost time is: " + costTime.toString + "s")

    sc.stop()
  }

  def colorBalance(sc: SparkContext, coverage: RDDImage): RDDImage = {
    val isTest = false // 此变量为 true 时，本算子在本地可以正常正确运行

    if (isTest) {
      val inputImgPath: String = new String("./data\\testdata\\colorbalance\\input\\Ortho_8bit.tif")
      //      val inputImgPath: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__1.tiff")
      val outputImgPath: String = new String("./data\\testdata\\colorbalance\\output\\Ortho_8bit_output_spark.tif")

      colorBalance(sc, inputImgPath, outputImgPath)

      val outputfile_absolute_path = new File(outputImgPath).getAbsolutePath
      val hadoop_file_path = "/" + outputfile_absolute_path
      val startTime = System.nanoTime()
      val tuple: RDDImage = makeChangedRasterRDDFromTif(sc, hadoop_file_path) // 此函数耗时为本算法的 30% 以上
      val endTime = System.nanoTime()
      val costTime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
      println("makeChangedRasterRDDFromTif() cost time is: " + costTime.toString + "s")
      tuple
    } else {
      val inputSavePath = s"/mnt/storage/algorithmData/${dagId}_colorBalance.tiff"
      saveRasterRDDToTif(coverage, inputSavePath)

      val resFile = s"/mnt/storage/algorithmData/${dagId}_colorBalance_output_temp.tiff"
      colorBalance(sc, inputSavePath, resFile)

      makeChangedRasterRDDFromTif(sc, resFile)
    }
  }
  /**
   * 进行图像匀光
   *
   * @param sc              SparkContext
   * @param inputImageFile  待匀光的图片路径
   * @param outputImageFile 匀光后的图片路径
   * @return
   */
  private def colorBalance(sc: SparkContext, inputImageFile: String, outputImageFile: String): String = {
    initGdal()

    val dataset: Dataset = gdal.Open(inputImageFile, gdalconstConstants.GA_ReadOnly)
    val geoTrans = dataset.GetGeoTransform()
    val proj = dataset.GetProjection()
    if (null == dataset) {
      println("open input file [" + inputImageFile + "] failed")
    }

    m_data.m_image_scale = (dataset.getRasterXSize, dataset.getRasterYSize)
//    println("image size (x = " + m_data.m_image_scale._1 + ", y = " + m_data.m_image_scale._2 + ")")
    m_data.m_splitBlock_total_count = (5, 5)
    if (0 == m_data.m_image_scale._1 % m_data.m_splitBlock_total_count._1) {
      m_data.m_splitBlock_total_count = (6, 5)
    }
    if (0 == m_data.m_image_scale._2 % m_data.m_splitBlock_total_count._2) {
      m_data.m_splitBlock_total_count = (5, 6)
    }
    if ((0 == m_data.m_image_scale._1 % m_data.m_splitBlock_total_count._1) &&
      (0 == m_data.m_image_scale._2 % m_data.m_splitBlock_total_count._2)) {
      m_data.m_splitBlock_total_count = (6, 6)
    }

    val blockSizeX = m_data.m_image_scale._1 / m_data.m_splitBlock_total_count._1
    val blockSizeY = m_data.m_image_scale._2 / m_data.m_splitBlock_total_count._2
    val overlap = 32

    val imgBlkGeoArr: Array[Array[(Int, Int, Int, Int)]] = splitImageGeo(dataset, blockSizeX, blockSizeY, overlap)
    m_data.m_band_scale = (m_data.m_band_scale._1, m_data.m_band_scale._2)
    m_data.setRealScale_general()
    //    m_data.setRealScale_special((1, 5), (1, 3))
    m_data.setCounts()
//    println("split block (x = " + m_data.m_splitBlock_total_count._1 + ", y = " + m_data.m_splitBlock_total_count._2 + ")")

    val imgBlkPixelArr: ImagesPixelArr = readSplitImage(dataset, imgBlkGeoArr)
    val lineImgBlkArr: Array[Array[((Int, Int), Array[Array[Short]])]] = lineSplitImgBlk(imgBlkPixelArr)

    val driver: Driver = gdal.GetDriverByName("GTiff")
    val datasetDes = driver.Create(outputImageFile, m_data.m_image_scale._1, m_data.m_image_scale._2, m_data.m_band_counts, dataset.GetRasterBand(1).getDataType)

    if (datasetDes != null) {
      datasetDes.SetGeoTransform(geoTrans)
      datasetDes.SetProjection(proj)
    }

    // 分波段处理图片
    for (bandIndex <- m_data.m_band_scale._1 to m_data.m_band_scale._2) {
      val imgBlk_rdd: RDD[((Int, Int), Array[Array[Short]])] = sc.makeRDD(lineImgBlkArr(bandIndex - 1))
      val abPar = abParameterSingleBand(sc, imgBlk_rdd, imgBlkGeoArr)
      val cbImgBlksArr: Array[((Int, Int), Array[Array[Short]])] = calColorBalancedImageBlocks(imgBlkPixelArr(bandIndex - 1), abPar).collect()

      writeCbdImgBlks(datasetDes, bandIndex, cbImgBlksArr, dataset, blockSizeX, blockSizeY, overlap, overlap)
    }

    destroyGdal()

    println("color balance end")

    outputImageFile
  }

  /**
   * 分割图片的几何信息，计算权重等使用
   * @param dataset  输入图像的 dataset
   * @param blockSizeX  图像分块的 x 方向大小
   * @param blockSizeY  图像分块的 y 方向大小
   * @param overlap  图像重叠区域
   * @return
   */
  private def splitImageGeo(dataset: Dataset, blockSizeX: Int, blockSizeY: Int, overlap: Int): Array[Array[(Int, Int, Int, Int)]] = {

    m_data.m_band_scale = (1, dataset.getRasterCount)

    // 分块以不重叠分块，读取数据时，以包含重叠部分来读取；在最后一列和最后一行中，则没有重叠，直接按照最后分块的实际大小来读取
    val xBlockCounts = ceil(m_data.m_image_scale._1.toDouble / (blockSizeX - overlap)).toInt
    val yBlockCounts = ceil(m_data.m_image_scale._2.toDouble / (blockSizeY - overlap)).toInt
    val imageBlocksGeoArr: Array[Array[(Int, Int, Int, Int)]] = Array.ofDim[(Int, Int, Int, Int)](xBlockCounts, yBlockCounts)

    m_data.m_splitBlock_total_count = (xBlockCounts, yBlockCounts)

    for (x <- 0 until m_data.m_image_scale._1 by (blockSizeX - overlap)) {
      for (y <- 0 until m_data.m_image_scale._2 by (blockSizeY - overlap)) {

        // 获取分块的索引
        val blockX = x / (blockSizeX - overlap)
        val blockY = y / (blockSizeY - overlap)

        // 在最后一列中，则没有重叠，直接按照最后分块的实际大小来读取，最后和(blockSizeX - overlap)交织中，会有最后两行两列特殊
        val xSize = math.min(blockSizeX, m_data.m_image_scale._1 - x)
        val ySize = math.min(blockSizeY, m_data.m_image_scale._2 - y)

        imageBlocksGeoArr(blockX)(blockY) = (x, y, xSize, ySize) // 后续图像需要几何数据，使用此参数

      }
    }

    imageBlocksGeoArr
  }

  /**
   * 读取分块后的图像
   * @param dataset  输入图像的 Dataset
   * @param imgBlkGeoArr  输入图像的分块几何信息
   * @return  全部波段的图片数据
   */
  private def readSplitImage(dataset: Dataset, imgBlkGeoArr: Array[Array[(Int, Int, Int, Int)]]): ImagesPixelArr = {

    val bandScale = m_data.m_band_scale
    val xReadScale = m_data.m_xBlock_weight_scale
    val yReadScale = m_data.m_yBlock_weight_scale

    val pixelValuesArr: ImagesPixelArr = Array.ofDim[Array[Array[Short]]](m_data.m_band_counts, m_data.m_xBlock_weight_counts, m_data.m_yBlock_weight_counts)

    for (bandIndex <- bandScale._1 to bandScale._2) {
      for (xBlkIndex <- xReadScale._1 to xReadScale._2) {
        for (yBlkIndex <- yReadScale._1 to yReadScale._2) {

          // 获取每个分块在原图像的像素索引，每个块像素块的大小
          val imgBlkGeo: (Int, Int, Int, Int) = imgBlkGeoArr(xBlkIndex)(yBlkIndex)
          val xBlkOff = imgBlkGeo._1
          val yBlkOff = imgBlkGeo._2
          val xSize = imgBlkGeo._3
          val ySize = imgBlkGeo._4

          // 读取每块影像
          val band: Band = dataset.GetRasterBand(bandIndex)
          val blockData: Array[Short] = Array.ofDim[Short](xSize * ySize)
          band.ReadRaster(xBlkOff, yBlkOff, xSize, ySize, blockData)

          // 线性数组二维化，便于后续更好处理数据
          val twoDimensionalBlockData: Array[Array[Short]] = Array.ofDim[Short](xSize, ySize)
          for (index <- blockData.indices) {
            val xDimIndex: Int = index % xSize
            val yDimIndex: Int = index / xSize
            twoDimensionalBlockData(xDimIndex)(yDimIndex) = blockData(index)
          }

          // 将块数据写入数组
          pixelValuesArr(bandIndex - 1)(xBlkIndex - xReadScale._1)(yBlkIndex - yReadScale._1) = twoDimensionalBlockData

        }
      }
    }

    pixelValuesArr
  }

  /**
   * 计算 a 和 b 的系数
   * @param sc  SparkContext
   * @param imgBlkPixel_rdd  图像像素块 rdd
   * @param imgBlkGeoArr  图像分块的几何信息
   * @return  a 和 b 系数
   */
  private def abParameterSingleBand(sc: SparkContext, imgBlkPixel_rdd: RDD[((Int, Int), Array[Array[Short]])],
                                    imgBlkGeoArr:Array[Array[(Int, Int, Int, Int)]]):
  RDD[((Int, Int), (Array[Array[Float]], Array[Array[Float]]))]= {

    val imgBlkMeanVar_rdd: RDD[((Int, Int), (Double, Double))] = calBlocksMeanVariance(imgBlkPixel_rdd)

    val bigImgMeaVar_rdd: (Double, Double) = calBigImageMeanVariance(imgBlkPixel_rdd, imgBlkMeanVar_rdd)

    val abPar: RDD[((Int, Int), (Array[Array[Float]], Array[Array[Float]]))] = abParameter(sc, bigImgMeaVar_rdd, imgBlkMeanVar_rdd, imgBlkGeoArr)
    abPar
  }

  /**
   * 线性化图像的分块结果
   * @param splitImgBlk
   * @return
   */
  private def lineSplitImgBlk(splitImgBlk: ImagesPixelArr):Array[Array[((Int, Int), Array[Array[Short]])]] = {

    val bandCounts = m_data.m_band_counts
    val xBlockCounts = m_data.m_xBlock_weight_counts
    val yBlockCounts = m_data.m_yBlock_weight_counts

    // 模板为某个块的具体数据内容
    val lineArr: Array[Array[((Int, Int), Array[Array[Short]])]] =
      new Array[Array[((Int, Int), Array[Array[Short]])]](bandCounts * xBlockCounts * yBlockCounts)

    for (bandIndex <- m_data.m_band_scale._1 to  m_data.m_band_scale._2) {
      val bandLineArr: Array[((Int, Int), Array[Array[Short]])] = new Array[((Int, Int), Array[Array[Short]])](xBlockCounts * yBlockCounts)

      for (x <- 0 until xBlockCounts) {
        for (y <- 0 until yBlockCounts) {
          bandLineArr(x * yBlockCounts + y) =((x, y), splitImgBlk(bandIndex - m_data.m_band_scale._1)(x)(y))
        }
      }

      lineArr(bandIndex - m_data.m_band_scale._1) = bandLineArr
    }

    lineArr
  }

  // 图像的像素数组，一维为波段，二三维分别为 x 和 y 方向的分块，四五维分别为图像块 x 和 y 方向块的位置，模板值为像素值
  type ImagesPixelArr = Array[Array[Array[Array[Array[Short]]]]]  // 要分波段，否则效率可能提不高
  // 图像的几何数据数组，一维为波段，二三维分别为 x 和 y 方向的分块，模板值为分块的 x 和 y 方向的像素个数
  type ImagesGeoArr = Array[Array[Array[(Int, Int)]]]

  type WeightMapRDD = RDD[((Int, Int, Int), Array[Array[Array[Array[Float]]]])]

  /**
   * 写匀光后的图像到磁盘上
   * @param desDataset  输出图像的 Dataset
   * @param bandIndex  要写的波段
   * @param cbdImgBlkSortArr  匀光后的已排序的数组
   * @param surDataset  输入图像的 Dataset
   * @param xMaxSize  分块的最大值
   * @param yMaxSize  分块的最大值
   * @param splitOverlap  分块重叠值
   * @param writeOverlap  写的重叠值
   */
  private def writeCbdImgBlks(desDataset: Dataset, bandIndex: Int, cbdImgBlkSortArr: Array[((Int, Int), Array[Array[Short]])],
                              surDataset: Dataset, xMaxSize: Int, yMaxSize: Int,
                              splitOverlap: Int, writeOverlap: Int): Unit = {

    val xWriteScale: (Int, Int) = (cbdImgBlkSortArr(0)._1._1, cbdImgBlkSortArr(cbdImgBlkSortArr.length - 1)._1._1)
    val yWriteScale: (Int, Int) = (cbdImgBlkSortArr(0)._1._2, cbdImgBlkSortArr(cbdImgBlkSortArr.length - 1)._1._2)
    //    val xBlockCounts = xWriteScale._2 - xWriteScale._1 + 1
    val yBlockCounts = yWriteScale._2 - yWriteScale._1 + 1

    for (xWrite <- xWriteScale._1 to xWriteScale._2) {
      for (yWrite <- yWriteScale._1 to yWriteScale._2) {

        val toWriteIndex = (xWrite - xWriteScale._1) * yBlockCounts + (yWrite - yWriteScale._1)
        val cbdImgBlk:Array[Array[Short]] = cbdImgBlkSortArr(toWriteIndex)._2

        val xToWriteSize = getDimensionSize(cbdImgBlk, 0)
        val yToWriteSize = getDimensionSize(cbdImgBlk, 1)

        // 1.处理每块写入的起始位置
        var xOff = xWrite * (xToWriteSize - splitOverlap)
        var yOff = yWrite * (yToWriteSize - splitOverlap)

        // 1.1 处理倒数第二行、倒数第二列的特殊数据
        if ((xWriteScale._2 - 1) == xWrite) {
          val xLastThirdSize = getDimensionSize(cbdImgBlkSortArr((xWrite - 2) * yBlockCounts)._2, 0)
          xOff = xWrite * (xLastThirdSize - splitOverlap)
        }
        if ((yWriteScale._2 - 1) == yWrite) {
          val yLastThirdSize = getDimensionSize(cbdImgBlkSortArr(yWrite - 2)._2, 1)
          yOff = yWrite * (yLastThirdSize - splitOverlap)
        }

        // 1.2 处理倒数第一行、倒数第一列的特殊数据
        if (xWriteScale._2 == xWrite) {
          val xLastThirdSize = getDimensionSize(cbdImgBlkSortArr((xWrite - 2) * yBlockCounts)._2, 0)
          val posX = (xWrite - 1) * (xLastThirdSize - splitOverlap)
          val xLastSecondSize = getDimensionSize(cbdImgBlkSortArr((xWrite - 1) * yBlockCounts)._2, 0)

          if (xLastThirdSize == xLastSecondSize) {
            xOff = posX + (xLastSecondSize - splitOverlap)
          } else {
            xOff = posX + (xLastSecondSize - xToWriteSize)
          }
        }

        if (yWriteScale._2 == yWrite) {
          val yLastThirdSize = getDimensionSize(cbdImgBlkSortArr(yWrite - 2)._2, 1)
          val posY = (yWrite - 1) * (yLastThirdSize - splitOverlap)
          val yLastSecondSize = getDimensionSize(cbdImgBlkSortArr(yWrite - 1)._2, 1)

          if (yLastThirdSize == yLastSecondSize) {
            yOff = posY + (yLastSecondSize - splitOverlap)
          } else {
            yOff = posY + (yLastSecondSize - yToWriteSize)
          }
        }

        // 2.处理每块写入的长和宽
        var xPixelSize = xToWriteSize - writeOverlap
        var yPixelSize = yToWriteSize - writeOverlap
        if (xMaxSize > xToWriteSize) {
          xPixelSize = xToWriteSize
        }
        if (yMaxSize > yToWriteSize) {
          yPixelSize = yToWriteSize
        }

        // 3.读取图片像素值，二维数据一维化，便于用 gdal 写数据
        //        val colorBalancedTwoDimArr: Array[Array[Short]] = cbdImgBlksArr(bandIndex - 1)(xWrite)(yWrite)
        val colorBalancedLineArr: Array[Short] = Array.ofDim[Short](xPixelSize * yPixelSize)
        for (index <- colorBalancedLineArr.indices) {
          val xDimIndex: Int = index % xPixelSize
          val yDimIndex: Int = index / xPixelSize
          colorBalancedLineArr(index) = cbdImgBlk(xDimIndex)(yDimIndex)
        }

        // 4.写入数据
        desDataset.GetRasterBand(bandIndex).WriteRaster(xOff, yOff, xPixelSize, yPixelSize, colorBalancedLineArr)
      }
    }

  }

  /**
   * 计算所有分块图像的均值和方差
   * @param imgBlk_rdd 图像分块 RDD
   * @return MeanVarMapRDD 图像分块的均值和方差 RDD
   */
  private def calBlocksMeanVariance(imgBlk_rdd: RDD[((Int, Int), Array[Array[Short]])]):RDD[((Int, Int), (Double, Double))]  = {

    val imgBlk_meanVar_rdd: RDD[((Int, Int), (Double, Double))] = imgBlk_rdd.map(imgBlk => {
      (imgBlk._1, getMeanVariance(imgBlk._2))
    })

    imgBlk_meanVar_rdd
  }

  /**
   * 依据图像的分块，和分块后的均值和方差（主要是均值），计算整幅图像的均值和方差
   *
   * @param pixelBlk_rdd  图像分块数组的像素值
   * @param meanVar_rdd   图像分块数组的均值和方差
   * @return Array[(Double, Double)] 整幅图像的均值和方差数组，_1 为均值，_2 为方差
   */
  private def calBigImageMeanVariance(pixelBlk_rdd: RDD[((Int, Int), Array[Array[Short]])],
                                      meanVar_rdd: RDD[((Int, Int), (Double, Double))]): (Double, Double) = {
    // 1.计算均值
    val meansSum:Double = meanVar_rdd.map(meanVar_band_map => {
      meanVar_band_map._2._1
    }).reduce((mean1, mean2) => {
      mean1 + mean2
    })
    val mean = meansSum / (m_data.m_xBlock_weight_counts * m_data.m_yBlock_weight_counts)

    // 2.计算方差
    val varianceSum:Double = pixelBlk_rdd.map(x => x._2).map(pixelLineArr => {
      var imgBlk_sumVariance: Double = 0.0
      pixelLineArr.foreach(xLine => {
        xLine.foreach(pixelValue => {
          imgBlk_sumVariance += (pixelValue - mean) * (pixelValue - mean)
        })
      })
      imgBlk_sumVariance
    }).reduce((variance1, variance2) => {
      variance1 + variance2
    })

    val variance = varianceSum / ((m_data.m_image_scale._1 * m_data.m_image_scale._2) - 1)

    (mean, variance)
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
  private def imgBlkPosNbh(x: Int, y: Int, xSize: Int, ySize: Int): Array[Array[Byte]] = {
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
   * @param x  像素块 x 方向的索引
   * @param y  像素块 y 方向的索引
   * @param xBlockCounts  像素块 x 方向的个数
   * @param yBlockCounts  像素块 y 方向的个数
   * @return  均值和方差的邻域数组
   */
  private def meanVarianceNbh(meanVarianceArr: Array[Array[(Double, Double)]],
                              x: Int, y: Int, xBlockCounts: Int, yBlockCounts: Int): Array[Array[(Double, Double)]] = {

    val weightXSize = m_data.m_weight_nbh._1
    val weightYSize = m_data.m_weight_nbh._2
    val meanVarianceNbhArr: Array[Array[(Double, Double)]] = Array.ofDim[(Double, Double)](weightXSize, weightYSize)  // 具体块的邻域均值和方差

    for (i <- 0 until weightXSize) {
      for (j <- 0 until weightYSize) {

        // 邻域位置转换为具体坐标，然后返回
        val corDir = posMapCorDir(i, j)  // 获取中心像素得到邻域坐标的方向
        val xNbh = x + corDir._1  // 获取邻域 x 坐标
        val yNbh = y + corDir._2  // 获取邻域 y 坐标

        var mean = -1.0  // 超过范围的均值由后续的邻域位置负责
        var variance = -1.0  //  超过范围的方差由后续的邻域位置负责
        if (((-1 < xNbh) && (xNbh < xBlockCounts)) && ((-1 < yNbh) && (yNbh < yBlockCounts))) {
          mean = meanVarianceArr(xNbh)(yNbh)._1
          variance = meanVarianceArr(xNbh)(yNbh)._2
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
  private def imgBlkXYSizeNbh(imageBlocksBandGeoArr: Array[Array[(Int, Int, Int, Int)]], x: Int, y: Int,
                              xBlockCounts: Int, yBlockCounts: Int): Array[Array[(Int, Int)]] = {
    val weightXSize = m_data.m_weight_nbh._1
    val weightYSize = m_data.m_weight_nbh._2
    val scaleSizeNbh: Array[Array[(Int, Int)]] = Array.ofDim[(Int, Int)](weightXSize, weightYSize)

    for (i <- 0 until weightXSize) {
      for (j <- 0 until weightYSize) {

        val corDir = posMapCorDir(i, j) // 获取中心像素得到邻域坐标的方向
        val xNbh = x + corDir._1 // 获取邻域 x 坐标
        val yNbh = y + corDir._2 // 获取邻域 y 坐标

        var xSize = 0  // 没在范围则设置为 0，由邻域位置保证
        var ySize = 0  // 没在范围则设置为 0，由邻域位置保证
        if (((-1 < xNbh) && (xNbh < xBlockCounts)) && ((-1 < yNbh) && (yNbh < yBlockCounts))) {
          xSize = imageBlocksBandGeoArr(xNbh)(yNbh)._3
          ySize = imageBlocksBandGeoArr(xNbh)(yNbh)._4
        }

        scaleSizeNbh(i)(j) = (xSize, ySize)
      }
    }

    scaleSizeNbh
  }

  /**
   * 图像块的权重邻域
   * @param imageBlocksGeoArr  图像分块的几何信息
   * @param x  块的索引
   * @param y  块的索引
   * @param xBlockCounts  总的块数
   * @param yBlockCounts  总的块数
   * @return
   */
  private def imgBlkWeightNbh(imageBlocksGeoArr: Array[Array[(Int, Int, Int, Int)]], x: Int, y: Int, xBlockCounts: Int, yBlockCounts: Int):
  Array[Array[Array[Array[Float]]]] = {
    // 计算每个块中周围的权重
    val weightXSize = m_data.m_weight_nbh._1
    val weightYSize = m_data.m_weight_nbh._2

    val xPixelSize = imageBlocksGeoArr(x)(y)._3
    val yPixelSize = imageBlocksGeoArr(x)(y)._4

    val weightBlocksArr: Array[Array[Array[Array[Float]]]] = Array.ofDim[Float](weightXSize, weightXSize, xPixelSize, yPixelSize)

    val weightPosNbhArr: Array[Array[Byte]] = imgBlkPosNbh(x, y, xBlockCounts, yBlockCounts)
    val imgBlkXYSizeNbhArr: Array[Array[(Int, Int)]] = imgBlkXYSizeNbh(imageBlocksGeoArr, x, y, xBlockCounts, yBlockCounts)

    // 具体计算每块图像的周围 9 块的每个像素的权重
    for (i <- 0 until xPixelSize) {
      for (j <- 0 until yPixelSize) {
        weightBlocksArr(0)(0)(i)(j) = weightPosNbhArr(0)(0) * 1 / sqrt(square(imgBlkXYSizeNbhArr(0)(0)._1 + i) + square(imgBlkXYSizeNbhArr(0)(0)._2 + j)).toFloat
        weightBlocksArr(0)(1)(i)(j) = weightPosNbhArr(0)(1) * 1 / sqrt(square(imgBlkXYSizeNbhArr(0)(1)._1 + i) + square(imgBlkXYSizeNbhArr(0)(1)._2 - j)).toFloat
        weightBlocksArr(0)(2)(i)(j) = weightPosNbhArr(0)(2) * 1 / sqrt(square(imgBlkXYSizeNbhArr(0)(2)._1 + i) + square(imgBlkXYSizeNbhArr(0)(2)._2 + 2 * imgBlkXYSizeNbhArr(1)(1)._2 - j)).toFloat

        weightBlocksArr(1)(0)(i)(j) = weightPosNbhArr(1)(0) * 1 / sqrt(square(imgBlkXYSizeNbhArr(1)(0)._1 - i) + square(imgBlkXYSizeNbhArr(1)(0)._2 + j)).toFloat
        weightBlocksArr(1)(1)(i)(j) = weightPosNbhArr(1)(1) * 1 / sqrt(square(imgBlkXYSizeNbhArr(1)(1)._1 - i) + square(imgBlkXYSizeNbhArr(1)(1)._2 - j)).toFloat
        weightBlocksArr(1)(2)(i)(j) = weightPosNbhArr(1)(2) * 1 / sqrt(square(imgBlkXYSizeNbhArr(1)(2)._1 - i) + square(imgBlkXYSizeNbhArr(1)(2)._2 + 2 * imgBlkXYSizeNbhArr(1)(1)._1 - j)).toFloat

        weightBlocksArr(2)(0)(i)(j) = weightPosNbhArr(2)(0) * 1 / sqrt(square(imgBlkXYSizeNbhArr(2)(0)._1 + 2 * imgBlkXYSizeNbhArr(1)(1)._1 - i) + square(imgBlkXYSizeNbhArr(2)(0)._2 + j)).toFloat
        weightBlocksArr(2)(1)(i)(j) = weightPosNbhArr(2)(1) * 1 / sqrt(square(imgBlkXYSizeNbhArr(2)(1)._1 + 2 * imgBlkXYSizeNbhArr(1)(1)._1 - i) + square(imgBlkXYSizeNbhArr(2)(1)._2 - j)).toFloat
        weightBlocksArr(2)(2)(i)(j) = weightPosNbhArr(2)(2) * 1 / sqrt(square(imgBlkXYSizeNbhArr(2)(2)._1 + 2 * imgBlkXYSizeNbhArr(1)(1)._1 - i) + square(imgBlkXYSizeNbhArr(2)(2)._2 + 2 * imgBlkXYSizeNbhArr(1)(1)._2 - j)).toFloat

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

    weightBlocksArr // 返回具体的权重邻域以及其定位
  }

  /**
   * 计算 a 和 b 参数
   * @param sc  SparkContext 系数
   * @param bigImgMeaVar  大图像的均值和方差
   * @param imgBlkMeanVar_rdd  分块图像的均值和方差 rdd
   * @param imgBlkGeoArr  图像分块的几何信息
   * @return
   */
  private def abParameter(sc: SparkContext, bigImgMeaVar: (Double, Double),
                          imgBlkMeanVar_rdd: RDD[((Int, Int), (Double, Double))], imgBlkGeoArr: Array[Array[(Int, Int, Int, Int)]]):
  RDD[((Int, Int), (Array[Array[Float]], Array[Array[Float]]))] = {
    val xBlockCounts = m_data.m_xBlock_weight_counts
    val yBlockCounts = m_data.m_yBlock_weight_counts

    // 1.将方差的邻域算到，先序列化，
    val imgBlkMeanVarNoSortArr: Array[((Int, Int), (Double, Double))] = imgBlkMeanVar_rdd.collect()
    val imgBlkMeanVarSortArr:Array[Array[(Double, Double)]] = Array.ofDim[(Double, Double)](xBlockCounts, yBlockCounts)
    imgBlkMeanVarNoSortArr.foreach(imgBlkMeanVarNoSort => {
      imgBlkMeanVarSortArr(imgBlkMeanVarNoSort._1._1)(imgBlkMeanVarNoSort._1._2) = imgBlkMeanVarNoSort._2
    })

    val xCalBlockCounts = m_data.m_xBlock_cal_counts
    val yCalBlockCounts = m_data.m_yBlock_cal_counts
    val calXYScale = new ArrayBuffer[(Int, Int)](xCalBlockCounts * yCalBlockCounts)
    for (x <- m_data.m_xBlock_cal_scale._1 to m_data.m_xBlock_cal_scale._2) {
      for (y <- m_data.m_yBlock_cal_scale._1 to m_data.m_yBlock_cal_scale._2) {
        calXYScale.append((x, y))
      }
    }

    val calXYScale_rdd: RDD[(Int, Int)] = sc.makeRDD(calXYScale)
    // 2.将权重的邻域算到
    val abParameterImgBlk: RDD[((Int, Int), (Array[Array[Float]], Array[Array[Float]]))] = calXYScale_rdd.map(calXYScale => {
      val weightXSize = m_data.m_weight_nbh._1
      val weightYSize = m_data.m_weight_nbh._2

      val xImgBlk = calXYScale._1
      val yImgBlk = calXYScale._2
      val xPixelSize = imgBlkGeoArr(xImgBlk)(yImgBlk)._3
      val yPixelSize = imgBlkGeoArr(xImgBlk)(yImgBlk)._4

      val aParameter: Array[Array[Float]] = Array.ofDim(xPixelSize, yPixelSize) // 计算的加权的方差比值权重，即线性系数 a
      val bParameter: Array[Array[Float]] = Array.ofDim(xPixelSize, yPixelSize) // 计算的加权的方差比值权重然后加上均值影响，即线性系数 b

      val weight_meanVar_nbh_arr: Array[Array[Byte]] = imgBlkPosNbh(xImgBlk, yImgBlk, xBlockCounts, yBlockCounts) // 获取当下块的 3 * 3 的邻域位置数组

      val bigImageMean: Float = bigImgMeaVar._1.toFloat // 整幅图像的均值
      val bigImageVariance: Float = bigImgMeaVar._2.toFloat // 整幅图像的方差

      // 包含当下块的 3 * 3 的邻域的权重
      val weightNbhArr: Array[Array[Array[Array[Float]]]] = imgBlkWeightNbh(imgBlkGeoArr, xImgBlk, yImgBlk, xBlockCounts, yBlockCounts)
      // 获取当下块的 3 * 3 的邻域均值和方差
      val meanVarianceNbhArr: Array[Array[(Double, Double)]] = meanVarianceNbh(imgBlkMeanVarSortArr, xImgBlk, yImgBlk, xBlockCounts, yBlockCounts)


      for (i <- 0 until xPixelSize) {
        for (j <- 0 until yPixelSize) {

          for (m <- 0 until weightXSize) {
            for (n <- 0 until weightYSize) {

              var meanImgBlk: Double = meanVarianceNbhArr(m)(n)._2
              if (0.0 == meanImgBlk) {
                meanImgBlk = bigImageVariance
              }

              //              val a1 = weight_meanVar_nbh_arr(m)(n)
              //              val a2 = weightNbhArr(m)(n)(i)(j)
              //              val a3 = bigImageVariance
              //              val a4 = meanImgBlk
              //              val b1 = bigImageMean
              //              val b2 = meanVarianceNbhArr(m)(n)._1
              //              val tempa = (a1 * a2 * a3 / a4).toFloat
              //              val tempb = (a1 * a2 * (b1 - a3 / a4 * b2)).toFloat
              //              aParameter(i)(j) += tempa // a 系数公式
              //              bParameter(i)(j) += tempb // b 系数公式

              aParameter(i)(j) += (weight_meanVar_nbh_arr(m)(n) * weightNbhArr(m)(n)(i)(j) * bigImageVariance / meanImgBlk).toFloat // a 系数公式
              bParameter(i)(j) += (weight_meanVar_nbh_arr(m)(n) * weightNbhArr(m)(n)(i)(j) *
                (bigImageMean - bigImageVariance / meanImgBlk * meanVarianceNbhArr(m)(n)._1)).toFloat // b 系数公式
            }
          }

        }
      }

      ((xImgBlk, yImgBlk) , (aParameter, bParameter))
    })

    abParameterImgBlk
  }

  /**
   * 计算匀光后的图像块
   * @param imageBlocksArr  分块后的图像块数组
   * @param abParameter_rdd  计算的 a 系数和 b 系数
   * @return  匀光后的分块图像
   */
  private def calColorBalancedImageBlocks(imageBlocksArr: Array[Array[Array[Array[Short]]]],
                                          abParameter_rdd: RDD[((Int, Int), (Array[Array[Float]], Array[Array[Float]]))]):
  RDD[((Int, Int), Array[Array[Short]])] = {

    val newImgBlkPixel_rdd: RDD[((Int, Int), Array[Array[Short]])] = abParameter_rdd.map(abParameter => {
      val x = abParameter._1._1
      val y = abParameter._1._2

      val oldImageBlock: Array[Array[Short]] = imageBlocksArr(x)(y)
      val xPixelSize = getDimensionSize(oldImageBlock, 0)
      val yPixelSize = getDimensionSize(oldImageBlock, 1)

      val colorBalancedImageBlock: Array[Array[Short]] = Array.ofDim[Short](xPixelSize, yPixelSize)

      val aParameter: Array[Array[Float]] = abParameter._2._1 // 计算的加权的方差比值权重，即线性系数 a
      val bParameter: Array[Array[Float]] = abParameter._2._2 // 计算的加权的方差比值权重然后加上均值影响，即线性系数 b

      for (i <- 0 until xPixelSize) {
        for (j <- 0 until yPixelSize) {
          colorBalancedImageBlock(i)(j) = (aParameter(i)(j) * oldImageBlock(i)(j) + bParameter(i)(j)).toShort
        }
      }

      ((x, y), colorBalancedImageBlock)
    })

    newImgBlkPixel_rdd
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
        -1 // 深度超出了数组的维度
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