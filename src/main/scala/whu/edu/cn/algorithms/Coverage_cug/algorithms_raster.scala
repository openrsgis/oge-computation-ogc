package whu.edu.cn.oge
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, MultibandTile, Tile}
import org.apache.spark.rdd.RDD
import whu.edu.cn.core.MathTools
import whu.edu.cn.core.MathTools.findminmaxValue
import whu.edu.cn.core.RDDTransformerUtil.paddingRDD
import whu.edu.cn.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey
import scala.collection.mutable.ListBuffer
import scala.math.sqrt

object algorithms_raster {
  //双边滤波
  def bilateralFilter(coverage: RDDImage, d: Int, sigmaSpace: Double, sigmaColor: Double, borderType: String): RDDImage= {
    val radius: Int = d/2
    val group:RDDImage= paddingRDD(coverage, radius,borderType)
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
        var sum = 0.0
        var weightSum = 0.0
        for (k <- 0 until d; l <- 0 until d) {
          val x = math.abs(i - radius+ k)
          val y = math.abs(j - radius+ l)
          val colorDiff = image.getDouble(j, i) - image.getDouble(y, x)
          val spaceDiff = math.sqrt((x - i) * (x - i) + (y - j) * (y - j))
          val weight = bilateral_kernel(spaceDiff, colorDiff, sigmaSpace, sigmaColor)
          sum += weight * image.getDouble(y, x)
          weightSum += weight

        }
        val roundedNumber: Double = BigDecimal(sum).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble
        newImage(i)(j) = roundedNumber

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
        band_ArrayTile(bandIndex) = bilateral_single(tile)
      }
      //组合各波段的运算结果
      (image._1, MultibandTile(band_ArrayTile.toList).crop(radius, radius, numCols - radius - 1, numRows - radius - 1))
    })
    (bilateralFilterRDD, coverage._2)
  }
  //高斯滤波
  def gaussianBlur(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), ksize: List[Int], sigmaX: Double, sigmaY: Double, borderType: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    //ksize：高斯核的大小，正奇数；sigmaX sigmaY：X和Y方向上的方差
    // 构建高斯核矩阵
    var kernel = Array.ofDim[Double](ksize(0), ksize(1))
    for (i <- 0 until ksize(0); j <- 0 until ksize(1)) {
      val x = math.abs(i - ksize(0) / 2) //模糊距离x
      val y = math.abs(j - ksize(1) / 2) //模糊距离y
      kernel(i)(j) = math.exp(-x*x/(2*sigmaX*sigmaX) - y*y/(2*sigmaY*sigmaY)) //距离中心像素的模糊距离
    }
    // 归一化高斯核函数
    val sum = kernel.flatten.sum
    kernel = kernel.map(_.map(_ / sum))

    val radius: Int = math.max(ksize(0) / 2, ksize(1) / 2)
    val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(coverage, radius,borderType)

    //遍历每个像素，对其进行高斯滤波
    def gaussian_single(image: Tile, kernel: Array[Array[Double]]): Tile = {
      val numRows = image.rows
      val numCols = image.cols
      val newImage = Array.ofDim[Double](numRows, numCols)
      for (i <- radius until numRows-radius; j <- radius until numCols-radius) {  //处理区域内的像素
        var sum = 0.0
        for (k <- 0 until ksize(0); l <- 0 until ksize(1)) {
          val x = i - ksize(0) / 2 + k
          val y = j - ksize(1) / 2 + l
          sum += kernel(k)(l) * image.get(y, x)
        }
        val roundedSum: Double = BigDecimal(sum).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble
        newImage(i)(j) = roundedSum
      }
      DoubleArrayTile(newImage.flatten, numCols, numRows)
    }
    //遍历延宽像素后的瓦片
    val GaussianBlurRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(image => {
      val numRows = image._2.rows
      val numCols = image._2.cols
      //遍历每个波段，调用gaussian_single函数进行高斯滤波
      val bandCount: Int = image._2.bandCount
      val band_ArrayTile = Array.ofDim[Tile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val tile: Tile = image._2.band(bandIndex)
        band_ArrayTile(bandIndex) = gaussian_single(tile, kernel)
      }
      //组合各波段的运算结果
      (image._1, MultibandTile(band_ArrayTile.toList).crop(radius, radius, numCols-radius-1, numRows-radius-1)) //需确保传递给它的 Tile 对象数量和顺序与所希望组合的波段顺序一致
    })
    (GaussianBlurRDD, coverage._2)
  }

  //直方图均衡化
  def histogramEqualization(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    //求整张影像像素总数
    val eachCount: RDD[Int] = coverage._1.map(t=>{
      t._2.rows * t._2.cols
    })
    val totalPixelCount: Int = eachCount.reduce(_+_)
    //定义一个查询字典，调用MathTools函数查询整张影像的最大最小值
    val Dictionary = MathTools.findminmaxValue(coverage)
    //计算单波段的像素值累计频率
    def PixelFrequency_single(image: IntArrayTile, bandIndex: Int): Array[Int] = {
      val Rows = image.rows
      val Cols = image.cols
      // 像素值的取值范围存储在bandMinMax字典中 Map[Int, (Int, Int)]
      // 灰度级数
      val (minValue,maxValue) = Dictionary(bandIndex)
      val levels: Int = maxValue - minValue + 1 //TODO 后面考虑是否有必要用Long，或根据情况判断
      // 计算每个像素灰度值的频率
      val histogram = Array.ofDim[Int](levels)
      for (i <- 0 until Rows; j <- 0 until Cols) {
        val pixel = image.get(j, i)
        histogram(pixel - minValue) += 1
      }
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
          PixelFrequency.append(((bandIndex, i + Dictionary(bandIndex)._1), bandPixelFrequecy(i)))
        }
      }
      PixelFrequency.toList
    })
    //整张影像的[(波段索引，像素值), 像素频率)]
    val bandPixelFrequencyToCount: RDD[((Int, Int), Int)] = bandPixelFrequency.reduceByKey(_ + _)
    //字典，方便查询整张影像的像素频率
    val collectDictionary: Map[(Int, Int), Int] = bandPixelFrequencyToCount.collect().toMap
    //计算均衡化后的每个像素值
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
        val (minValue,maxValue) = Dictionary(bandIndex)
        val levels: Int = maxValue - minValue + 1
        println(minValue, maxValue)
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

  //canny边缘提取
  def cannyEdgeDetection(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    def gradXTileCalculate(tile: Tile, radius: Int): Tile = {
      val rows = tile.rows
      val cols = tile.cols
      val result = Array.ofDim[Double](rows, cols)

      for (i <- radius until rows - radius; j <- radius until cols - radius) {
        val gradX = tile.get(j + 1, i + 1) - tile.get(j - 1, i + 1) + 2 * tile.get(j + 1, i) - 2 * tile.get(j - 1, i) + tile.get(j + 1, i - 1) - tile.get(j - 1, i - 1)
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
        val gradY = tile.get(j - 1, i - 1) - tile.get(j - 1, i + 1) + 2 * tile.get(j, i - 1) - 2 * tile.get(j, i + 1) + tile.get(j + 1, i - 1) - tile.get(j + 1, i + 1)
        result(i)(j) = gradY
      }
      DoubleArrayTile(result.flatten, cols, rows)
    }
    //计算每个Tile的梯度幅值
    def gradTileCalculate(tileX: Tile, tileY: Tile, radius: Int): Tile = {
      val rows = tileX.rows
      val cols = tileX.cols
      val result = Array.ofDim[Double](rows, cols)
      for (i <- radius until rows - radius; j <- radius until cols - radius) {
        val grad = math.sqrt(math.pow(tileX.get(j, i), 2) + math.pow(tileY.get(j, i), 2))
        result(i)(j) = grad
      }
      DoubleArrayTile(result.flatten, cols, rows)
    }
    //计算图像的梯度幅值，并输出梯度图
    def gradientCalculate(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      val radius: Int = 1
      val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(coverage, radius)
      val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = group._1.map(
        image => {
          val rows = image._2.rows
          val cols = image._2.cols
          val radius: Int = 1
          val bandCount: Int = image._2.bandCount
          val band_Array = Array.ofDim[Tile](bandCount)

          val tile: Tile = image._2.band(0)
          val gradX = gradXTileCalculate(tile, 1)
          val gradY = gradYTileCalculate(tile, 1)
          band_Array(0) = gradTileCalculate(gradX, gradY, 1)
          (image._1, MultibandTile(band_Array).crop(radius, radius, cols - radius - 1, rows - radius - 1))
        })
      (newRDD, coverage._2)
    }
    //计算图像在X方向上的梯度幅值，并输出梯度图
    def nonMaxSuppression(tile1: Tile, tile2: Tile, tile3: Tile) = {
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
          val currentGradX = gradX.get(j, i)
          val currentGradY = gradY.get(j, i)
          var weight = 0.0
          //如果梯度为0，直接结果赋值为0
          if (currentAmplitude == 0) {
            result.set(j, i, 0)
          }
          else {
            var grad1 = 0.0
            var grad2 = 0.0
            var grad3 = 0.0
            var grad4 = 0.0
            // y大于x方向梯度
            //            if(currentGradX!=0 && currentGradY!=0&&(math.abs(currentGradY) / math.abs(currentGradX)!=1))
            //              println("1")
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
            if (math.round(currentAmplitude) >= math.round(gradTemp1) && math.round(currentAmplitude) >= math.round(gradTemp2))
            // 中心点在其邻域内为极大值 ，在结果中保留其梯度值
              result.setDouble(j, i, currentAmplitude);
            else
            // 否则的话 ，在结果中置0
              result.setDouble(j, i, 0)


          }
        }
      }
      result
    }
    def doubleThresholdDetection(NMSResult: DoubleArrayTile, maxGradient: Double, radius: Int) = {
      //高阈值 TH×Max  TH=0.3
      val highThreshold = 0.3 * maxGradient
      //低阈值 TL×Max  TH=0.1
      val lowThreshold = 0.1 * maxGradient
      var count = 0
      var count2 = 0
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
            count += 1
          }
          else if (currentAmplitude < lowThreshold) {
            result.set(j, i, 0)
          }
          else if (currentAmplitude >= lowThreshold && currentAmplitude <= highThreshold) {
            //            println(1)
            // 定义一个存储八个方向的偏移量的数组
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
    val group: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = paddingRDD(coverage, radius)
    val InternRDD:RDD[(SpaceTimeBandKey, MultibandTile)]=group._1.map(
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
        var InternTile=Array.ofDim[Tile](3)
        InternTile(0)=Crop_gradx.band(0)
        InternTile(1)=Crop_grady.band(0)
        InternTile(2)=Crop_amplitude.band(0)
        (image._1,MultibandTile(InternTile))
      }

    )
    val InternRDDImage:RDDImage=(InternRDD,group._2)
    val PaddingVriable=paddingRDD(InternRDDImage, radius)
    val processedRDD: RDD[Array[Double]] = PaddingVriable._1.map(
      image => {

        val gradX: Tile = image._2.band(0)

        val gradY:Tile=image._2.band(1)


        val amplitude = image._2.band(2)


        val NMS: DoubleArrayTile = nonMaxSuppression(amplitude, gradX, gradY)
        NMS.toArrayDouble()

        //        (image._1, MultibandTile(band_Array).crop(radius, radius, cols - radius - 1, rows - radius - 1))
      })
    val maxGradient = processedRDD.map(_.max).reduce(math.max)
    println("经过非极大抑制后的最大幅值为：" + maxGradient)
    val newRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = PaddingVriable._1.map(
      image => {
        val cols = image._2.cols
        val rows = image._2.rows

        val gradX: Tile = image._2.band(0)
        val gradY: Tile = image._2.band(1)
        val amplitude = image._2.band(2)
        val NMS: DoubleArrayTile = nonMaxSuppression(amplitude, gradX, gradY)
        val cannyEdgeExtraction =doubleThresholdDetection(NMS, maxGradient, radius)
        val test=MultibandTile(cannyEdgeExtraction)

        (image._1, MultibandTile(cannyEdgeExtraction).crop(radius, radius, cols - radius - 1, rows - radius - 1))
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
  def linearTransformation(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), k: Double = 1.0, b: Int = 0): RDDImage = {

    def linear_trans_respectively(image: IntArrayTile, k: Double, b: Int, Min: Int, Max: Int): IntArrayTile = {
      //      print(k,b)
      val col = image.cols
      val row = image.rows

      val newImages: IntArrayTile = IntArrayTile.empty(col, row)
      for (i <- 0 until row; j <- 0 until col) {
        var pixel = image.get(j, i)

        var transformedPixel = (k * pixel + b).round.toInt


        if (transformedPixel < Min) {
          transformedPixel = Min
        }
        if (transformedPixel > Max) {
          transformedPixel = Max
        }
        //        print(pixel,transformedPixel,"\n")
        newImages.set(j, i, transformedPixel)
      }

      newImages
    }
    val MinMaxValue=findminmaxValue(coverage)
    val newImage: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(image => {
      val bandCount: Int = image._2.bandCount
      val band_intArrayTile = Array.ofDim[IntArrayTile](bandCount)
      for (bandIndex <- 0 until bandCount) {
        val Min = MinMaxValue(bandIndex)._1
        val Max = MinMaxValue(bandIndex)._2
        //        print(Min,Max)
        val tile: Tile = image._2.band(bandIndex)
        val bandTile: IntArrayTile = tile match {
          case intTile: IntArrayTile => intTile // 如果已经是 IntArrayTile，则直接使用
          case _ => IntArrayTile(tile.toArray, tile.cols, tile.rows) // 否则，将 Tile 转换为 IntArrayTile
        }

        band_intArrayTile(bandIndex) = linear_trans_respectively(bandTile, k, b, Min, Max)
      }
      (image._1, MultibandTile(band_intArrayTile.toList))
    })
    (newImage, coverage._2)
  }


    //假彩色合成
  def fakeColorCompose (coverage:RDDImage,BandRed:Int,BandBlue:Int,BandGreen:Int):RDDImage=
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
    val MinMaxMap=findminmaxValue(coverage)
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

      (x._1,MultibandTile(ColorTile))
    })
    (Changed_Image,coverage._2)
  }
   //标准差拉伸
  def standardDeviationStretching(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {


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

}
