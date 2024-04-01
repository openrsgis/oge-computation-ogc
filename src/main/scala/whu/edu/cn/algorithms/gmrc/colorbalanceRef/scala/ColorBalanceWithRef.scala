package whu.edu.cn.algorithms.gmrc.colorbalanceRef.scala



//import api.{MosaicCutApi, MosaicLineApi}
//import common.ImageXX
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.commons.math3.stat.descriptive.rank.Min
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import sun.util.logging.PlatformLogger.Level
import whu.edu.cn.algorithms.gmrc.colorbalanceRef.java.{CommonTools, ImageXX, MosaicLineApi, RebuildOpe}
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.dagId
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.File
import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer




class ColorBalanceWithRef extends Serializable {

  /**
   * 像素块数据类型
   * (起始高度，终止高度)， 宽度, 波段数, 像素的有效位数（0-8）,块的序号（从0开始），金字塔层数，
   * 原始影像像素块，顶层金字塔像素块
   */
  //type ImageBlock = ((Int, Int), Int, Int, Int, Int, Int, Array[Short], Array[Short])

  class ImageBlock extends Serializable {
    // todo 把 上面ImageBlock中的元素以成员变量的形式，定义到这里；当然听以后，需要修改相关代码 cqc
    var m_arrHeightRange : Array[Int] = _
    var m_nWidth : Int = 0
    var m_nChannel : Int = 0
    var m_nRealBitset : Int = 0
    var m_nIndex : Int = 0
    var m_nLevel : Int = 0
    var m_pBuf : Array[Short] = null
    //    var m_pGsBuf : Array[Short] = null
  }

  val MEMSIZE__ = 500000000
  val m_nRealByte = (MEMSIZE__ * 0.8/8).toLong

  val MAX_LEVEL = 256

  def InitGdal() : Unit = {
    gdal.AllRegister()
  }
  def CloseGdal() : Unit = {
    gdal.GDALDestroyDriverManager()
  }
  /**根据影像尺寸信息，获取金字塔层级及原始影像每块的高度
   * @param height：影像高度
   * @param width：影像宽度
   * @param nBands：影像波段数
   * @param nBitSize：影像像素位数
   * @param tempLevel：金字塔初始层级
   * @return：数组[分块的高度，金字塔层数]
   */
  def GetGaussBlockSize(height : Int, width : Int, nBands : Int, nBitSize : Int, tempLevel : Int ): Array[Int] ={
    val totSize = m_nRealByte
    val result = new Array[Int](2)
    var level = tempLevel
    val temp = (totSize / (width * nBands * nBitSize)).toInt
    if (temp > height){
      result(0) = height
      result(1) = level
      return result
    }
    var size = Math.pow(2.0, 1.0 * level).toInt
    var nStep = (temp / size).toInt
    while(nStep < 2){
      level = level -1
      size = Math.pow(2.0, level * 1.0).toInt
      nStep = (temp / size).toInt
    }
    val blockSize = nStep * size
    result(0) = blockSize
    result(1) = level
    return result
  }
  /**
   * 根据待处理影像（范围）去参考影像中取对应范围的区域，生成新的影像
   * @param pOrignal：待处理影像指针
   * @param pStandard：参考影像指针
   * @param pNewImage：新生成的影像指针
   * @param stProgress：当前进度的基准点
   * @param rgProgress：该步骤的进度总长
   * @return
   */
  def CutFromStandardaddprojection2(pOrignal : ImageXX, pStandard : ImageXX, pNewImage : ImageXX,
                                    stProgress : Double, rgProgress : Double) : Boolean = {
    val env1 = pOrignal.GetEnv()
    val env2 = pStandard.GetEnv()
    val w1 = pOrignal.GetWKT()
    val w2 = pStandard.GetWKT()
    val SRS1 = new SpatialReference(w1)
    val SRS2 = new SpatialReference(w2)
    val pCoorTransform = new CoordinateTransformation(SRS1, SRS2)
    if (null == pCoorTransform){
      println("CoordinateTransformation Created Failed!")
      return false
    }
    var x1 = env1(0)
    var y1 = env1(1)
    var x2 = env1(2)
    var y2 = env1(3)
    val pt1 =  pCoorTransform.TransformPoint(x1, y1)
    val pt2 = pCoorTransform.TransformPoint(x2, y2)
    x1 = pt1(0)
    y1 = pt1(1)
    x2 = pt2(0)
    y2 = pt2(1)
    val x = x1 - env2(0)
    val y = env2(3) - y2
    //    val y = y1 - env2(1)
    val nSumX = Math.ceil( (pt2(0) - pt1(0)) / pStandard.getM_dXRes ).toInt
    val nSumY = Math.ceil( (pt2(1) - pt1(1)) / pStandard.getM_dYRes ).toInt
    val I = (x / pStandard.getM_dXRes + 0.5).toInt
    val J =  pStandard.getM_nHeight - ( y / pStandard.getM_dYRes + 0.5).toInt - nSumY
    if (I < 0 || J < 0 || nSumX <= 0 || nSumY <= 0){
      println(" Ref Image out of Range!")
      return false
    }
    val level = 1
    val arrReuslt = GetGaussBlockSize(nSumY, nSumX, pStandard.getM_nChannels, pStandard.getM_nBitSize, level)
    val Blocksize = arrReuslt(0)
    val mOriginBuf = new Array[Short](Blocksize * nSumX*pStandard.getM_nChannels * pStandard.getM_nBitSize)

    val CountJ = J + nSumY
    val CountI = I + nSumX
    //    mOriginBuf = Array.fill[Byte]((m_nRealBitCount/8).toInt)(0)
    //    for (i <- 0 until(mOriginBuf.length)){
    //      mOriginBuf(i) = 0
    //    }

    var j = J;
    while(j < CountJ){
      var nY = Blocksize
      var nX = nSumX
      if (j + Blocksize > CountJ){
        nY = CountJ - j
      }
      pStandard.ReadBlock(I, j, nX, nY, mOriginBuf)
      //      CommonTools.bytesToShorts(mTranformBuf, mOriginBuf)
      //      CommonTools.shortsToBytes(mOriginBuf, mTranformBuf)
      pNewImage.WriteBlock(0, j - J, nX, nY,mOriginBuf)
      j += Blocksize
    }
    pNewImage.SetWKT(w1);
    return  true
  }



  /**
   * 根据影像金字塔第0层的尺寸，计算任意某曾的尺寸
   * @param heightOrWidth：金字塔第0层的高度或者宽度
   * @param level：所求金字塔的层级
   * @return：金字塔第level层级的尺寸
   */
  def GetGaussianSize(heightOrWidth : Int, level : Int): Int ={
    var temp = heightOrWidth
    for (i <- 0 until(level)){
      temp = (temp - 1) / 2
    }
    return temp
  }

  /**
   * 根据影像的高度、分块的大小以及块与块之间的重叠度，计算分块的数量
   * @param height：影像的高度
   * @param blocksize：分块的大小
   * @param overlap：块与块之间的重叠度
   * @return：分块数量
   */
  def GetImgBlockCount(height: Int, blocksize : Int, overlap : Int) : Int = {
    var nCount = 0;
    var nRowCount = 0
    var nStartIndex = 0
    var nEndIndex = 0
    var i : Int = 0
    while(i < height){
      nStartIndex = i - overlap
      nStartIndex = Math.max(0, nStartIndex)
      nEndIndex = (i + blocksize + overlap).toInt
      nEndIndex = Math.min(height, nEndIndex)
      nRowCount = nEndIndex - nStartIndex

      i = i + blocksize.toInt
      nCount += 1
    }
    return nCount
  }

  def GetRebuildBlockSize(height : Int, width : Int, nBands : Int, nBitSize : Int, level1 : Int): Long ={
    var level = level1
    val totSize = m_nRealByte
    val temp = totSize / (width * nBands * nBitSize)
    //影像太小，无需分块
    if (temp > height) return height

    var size = Math.pow(2.0, level * 1.0).toLong
    var nStep = (temp / size).toLong
    while ( {
      nStep < 2
    }) {
      level -= 1
      size = Math.pow(2.0, level * 1.0).toLong
      nStep = temp / size.toLong
    }
    val blockSize = nStep * size
    return blockSize
  }

  /**
   * 根据影像信息，强行拆分成多块，计算块的高度
   * @param height
   * @param width
   * @param nBands
   * @param nBitSize
   * @return
   */
  def GetImgBlockSize(height : Int, width : Int, nBands : Int, nBitSize : Int): Long ={
    val totSize = m_nRealByte
    val temp = totSize / (width * nBands * nBitSize)
    //影像太小，无需分块
    if (temp > height) return height

    return temp
  }

  /**
   * 影像分块
   * @param pOrignal：影像ImageXX
   * @param level：金字塔层数
   * @param realBitSize：影像像素有效位数
   * @return
   */
  def SplitImage(pOrignal: ImageXX, level : Int, realBitSize : Int):
  Array[ImageBlock] ={
    val height = pOrignal.getM_nHeight
    val width = pOrignal.getM_nWidth
    val nChannel = pOrignal.getM_nChannels
    //计算单个方向外扩高度
    var adjust = (Math.pow(2.0, level)).toInt
    if (adjust > 128){
      adjust = 128
    }else if (adjust < 64){
      adjust = 64
    }
    val nDis = 2 * adjust

    val blocksize = GetRebuildBlockSize(height, width, pOrignal.getM_nChannels, pOrignal.getM_nBitSize, level)

    val nBlockCount = GetImgBlockCount(height, blocksize.toInt, nDis)

    val nBlockHeight = blocksize + 2*nDis
    val nMemory = nBlockHeight * width * pOrignal.getM_nChannels*pOrignal.getM_nBitSize

    //    var gsArray = new Array[Array[Short]](level + 1)
    //    var th : Int = (blocksize + 2*nDis).toInt
    //    var tw : Int = width
    //    while(i <= level){
    //      gsArray(i) = new Array[Short](th*tw*3)
    //      th = (th - 1)/2
    //      tw = (tw - 1)/2
    //    }

    var nRowCount = 0
    var nStartIndex = 0
    var nEndIndex = 0
    var i : Int = 0
    var nIndex = 0

    var imageBlockArray = new Array[ImageBlock](nBlockCount)
    i = 0
    while(i < height){
      nStartIndex = i - nDis
      nStartIndex = Math.max(0, nStartIndex)
      nEndIndex = (i + blocksize + nDis).toInt
      nEndIndex = Math.min(height, nEndIndex)
      nRowCount = nEndIndex - nStartIndex

      imageBlockArray(nIndex) = new ImageBlock
      imageBlockArray(nIndex).m_arrHeightRange = new Array[Int](2)
      imageBlockArray(nIndex).m_arrHeightRange(0) = nStartIndex
      imageBlockArray(nIndex).m_arrHeightRange(1) = nEndIndex
      imageBlockArray(nIndex).m_nWidth = width
      imageBlockArray(nIndex).m_nChannel = pOrignal.getM_nChannels
      imageBlockArray(nIndex).m_nRealBitset = realBitSize
      imageBlockArray(nIndex).m_nIndex = nIndex
      imageBlockArray(nIndex).m_nLevel = level
      imageBlockArray(nIndex).m_pBuf = new Array[Short](nRowCount*width*pOrignal.getM_nBitSize*pOrignal.getM_nChannels)

      //val pBuf = new Array[Short](nMemory.toInt)
      pOrignal.ReadBlock(0, nStartIndex, width, nRowCount, imageBlockArray(nIndex).m_pBuf)


      val nSize = GetGaussianSize(nRowCount, level).toInt *  GetGaussianSize(width, level) * 3
      //imageBlockArray(nIndex).m_pGsBuf = new Array[Short](nSize)




      //        ((nStartIndex, nEndIndex), width, pOrignal.getM_nChannels, realBitSize, nIndex,level, pBuf, gsBuf)
      i = i + blocksize.toInt
      nIndex += 1
    }

    return imageBlockArray
  }

  def SplitImage(pOrignal: ImageXX):
  Array[ImageBlock] ={
    val height = pOrignal.getM_nHeight
    val width = pOrignal.getM_nWidth
    val nChannel = pOrignal.getM_nChannels

    val blocksize = GetImgBlockSize(height, width, pOrignal.getM_nChannels, pOrignal.getM_nBitSize)

    val nBlockHeight = blocksize
    val nMemory = nBlockHeight * width * pOrignal.getM_nChannels*pOrignal.getM_nBitSize
    val nBlockCount = GetImgBlockCount(height, blocksize.toInt, 0)


    var nRowCount = 0
    var nStartIndex = 0
    var nEndIndex = 0
    var i : Int = 0
    var nIndex = 0

    var imageBlockArray = new Array[ImageBlock](nBlockCount)
    i = 0
    while(i < height){
      nStartIndex = i
      nEndIndex = (i + blocksize ).toInt
      nEndIndex = Math.min(height, nEndIndex)
      nRowCount = nEndIndex - nStartIndex

      imageBlockArray(nIndex) = new ImageBlock
      imageBlockArray(nIndex).m_arrHeightRange = new Array[Int](2)
      imageBlockArray(nIndex).m_arrHeightRange(0) = nStartIndex
      imageBlockArray(nIndex).m_arrHeightRange(1) = nEndIndex
      imageBlockArray(nIndex).m_nWidth = width
      imageBlockArray(nIndex).m_nChannel = pOrignal.getM_nChannels
      imageBlockArray(nIndex).m_nRealBitset = 0
      imageBlockArray(nIndex).m_nIndex = nIndex
      imageBlockArray(nIndex).m_pBuf = new Array[Short](nRowCount*width*pOrignal.getM_nBitSize*pOrignal.getM_nChannels)

      //val pBuf = new Array[Short](nMemory.toInt)
      pOrignal.ReadBlock(0, nStartIndex, width, nRowCount, imageBlockArray(nIndex).m_pBuf)

      i = i + blocksize.toInt
      nIndex += 1
    }

    return imageBlockArray
  }

  /**
   *
   * @param Src
   * @param Dest
   * @param SrcW
   * @param SrcH
   * @param StrideS
   * @param DstW
   * @param DstH
   * @param StrideD
   * @param Channel
   * @return
   */
  def IM_DownSample8U_C1(Src : Array[Short], Dest : Array[Short], SrcW : Int, SrcH : Int,
                         StrideS: Int, DstW: Int, DstH: Int, StrideD: Int, Channel: Int): Boolean ={

    if ((Channel != 1) && (Channel != 3) && (Channel != 4)) return false
    var Sum1 = 0
    var Sum2 = 0
    var Sum3 = 0
    var Sum4 = 0
    var Sum5 = 0
    //    if (1 == Channel)
    //    {
    for (i <- 0 until(DstH))
    {
      val LinePD = i * StrideD
      var P1 = (2 * i) * StrideS
      var P2 = P1 + StrideS
      var P3 = P2 + StrideS
      var P4 = P3 + StrideS
      var P5 = P4 + StrideS
      Sum3 = Src(P1 + 0) + ((Src(P2 + 0) + Src(P4 + 0)) << 2) + Src(P3 + 0) * 6 + Src(P5 + 0)
      Sum4 = Src(P1 + 1) + ((Src(P2 + 1) + Src(P4 + 1)) << 2) + Src(P3 + 1) * 6 + Src(P5 + 1)
      Sum5 = Src(P1 + 2) + ((Src(P2 + 2) + Src(P4 + 2)) << 2) + Src(P3 + 2) * 6 + Src(P5 + 2)
      for (j <- 0 until(DstW))
      {
        Sum1 = Sum3
        Sum2 = Sum4
        Sum3 = Sum5
        Sum4 = Src(P1 + 3) + ((Src(P2 + 3) + Src(P4 + 3)) << 2) + Src(P3 + 3) * 6 + Src(P5 + 3)
        Sum5 = Src(P1 + 4) + ((Src(P2 + 4) + Src(P4 + 4)) << 2) + Src(P3 + 4) * 6 + Src(P5 + 4)

        val value = ((Sum1 + ((Sum2 + Sum4) << 2) + Sum3 * 6 + Sum5 + 128) >> 8).toShort
        //out1.write(s"${value}, ")
        Dest(j + LinePD) = value
        P1 += 2
        P2 += 2
        P3 += 2
        P4 += 2
        P5 += 2
      }

    }

    return true
  }

  /**
   * 像素块创建金字塔，新生成的金字塔像素块覆盖原始数据
   * @param pVAL				像素块指针
   * @param level				金字塔层级
   * @param nBKX				像素块宽度
   * @param nBKY				像素块高度
   * @param nChannel			影像波段数
   * @param GSr				存放高斯金字塔波段R的像素指针
   * @param GSg				存放高斯金字塔波段G的像素指针
   * @param GSb				存放高斯金字塔波段B的像素指针
   * @param Vr					存放拉普拉斯金字塔波段R的像素指针
   * @param Vg					存放拉普拉斯金字塔波段R的像素指针
   * @param Vb					存放拉普拉斯金字塔波段R的像素指针
   * @return 是否成功
   */
  def GaussianModel_Parallel(pVAL : Array[Short], level: Int, nBKY : Int, nBKX : Int, nChannel : Int,
                             GSr : Array[Array[Short]], GSg : Array[Array[Short]], GSb : Array[Array[Short]],
                             Vr : Array[Array[Short]], Vg : Array[Array[Short]], Vb : Array[Array[Short]], gsResult : Array[Short]): Boolean ={
    var index = 0
    var border = 0
    var th = nBKY
    var tw = nBKX
    val size = new Array[Int](2 * (level + 1) )

    for (i <- 0 to level){
      size(2 * i) = th
      size(2 * i + 1) = tw
      th = (th - 1) / 2
      tw = (tw - 1) / 2
    }
    for (i <- 0 to level){
      for (j <- 0 until(GSr(i).length)){
        GSr(i)(j) = 0
        GSg(i)(j) = 0
        GSb(i)(j) = 0
      }
    }
    for (i <- 0 until(level)){
      for (j <- 0 until(Vr(i).length)){
        Vr(i)(j) = 0
        Vg(i)(j) = 0
        Vb(i)(j) = 0
      }
    }

    var k = 0
    var height = 0
    var width = 0
    val weight = new Array[Double](5)
    for (i <- 0 until(5)){
      if (Math.abs(i - 2) == 2) weight(i) = 1.0 / 16
      else if(Math.abs(i - 2) == 1) weight(i) = 1.0 / 4
      else weight(i) = 3.0 / 8
    }
    /////////////////////////////////////////高斯金字塔层赋值
    border = size(0) * size(1)
    for (index <- 0 until(border)){
      val i = index / size(1)
      val j = index % size(1)
      GSr(0)(i*size(1) + j) = pVAL(i * size(1) * nChannel + nChannel * j )
      GSg(0)(i*size(1) + j) = pVAL(i * size(1) * nChannel + nChannel * j + 1)
      GSb(0)(i*size(1) + j) = pVAL(i * size(1) * nChannel + nChannel * j + 2)
    }

    ////////////////////////////////////////////高斯金字塔的层层赋值
    for (k <- 1 to(level))
    {
      height = size(2 * (k - 1)) + 2;//size是根据原始影像的大小计算得来的
      width = size(2 * (k - 1) + 1)+ 2;//每层的高斯都往外扩两行，为了处理边边角角的像元
      border = size(2 * (k - 1)) * size(2 * (k - 1) + 1)

      for (index <- 0 until(border))
      {
        val i = index / size(2 * (k - 1) + 1)
        val j = index % size(2 * (k - 1) + 1)
        Vr(k - 1)((i + 2) * width + j + 2) = GSr(k - 1)(i * size(2 * (k - 1) + 1) + j) //GSr[0]已经赋值过了
        Vg(k - 1)((i + 2) * width + j + 2) = GSg(k - 1)(i * size(2 * (k - 1) + 1) + j)
        Vb(k - 1)((i + 2) * width + j + 2) = GSb(k - 1)(i * size(2 * (k - 1) + 1) + j)
      }

      for (i <- 0 until(size(2 * (k - 1))))
      {
        Vr(k - 1)(i * width + 1) = Math.max((2 * Vr(k - 1)(i * width + 2) - Vr(k - 1)(i * width + 3)), 0).toShort
        Vg(k - 1)(i * width + 1) = Math.max((2 * Vg(k - 1)(i * width + 2) - Vg(k - 1)(i * width + 3)), 0).toShort
        Vb(k - 1)(i * width + 1) =  Math.max((2 * Vb(k - 1)(i * width + 2) - Vb(k - 1)(i * width + 3)), 0).toShort
        Vr(k - 1)(i * width + 0) =  Math.max((2 * Vr(k - 1)(i * width + 2) - Vr(k - 1)(i * width + 4)), 0).toShort
        Vg(k - 1)(i * width + 0) =  Math.max((2 * Vg(k - 1)(i * width + 2) - Vg(k - 1)(i * width + 4)), 0).toShort
        Vb(k - 1)(i * width + 0) =  Math.max((2 * Vb(k - 1)(i * width + 2) - Vb(k - 1)(i * width + 4)), 0).toShort
      }
      for (j <- 0 until(width))
      {
        Vr(k - 1)(1 * width + j) =  Math.max((2 * Vr(k - 1)(2 * width + j) - Vr(k - 1)(3 * width + j)), 0).toShort
        Vg(k - 1)(1 * width + j) =  Math.max((2 * Vg(k - 1)(2 * width + j) - Vg(k - 1)(3 * width + j)), 0).toShort
        Vb(k - 1)(1 * width + j) =  Math.max((2 * Vb(k - 1)(2 * width + j) - Vb(k - 1)(3 * width + j)), 0).toShort
        Vr(k - 1)(0 * width + j) =  Math.max((2 * Vr(k - 1)(2 * width + j) - Vr(k - 1)(4 * width + j)), 0).toShort
        Vg(k - 1)(0 * width + j) =  Math.max((2 * Vg(k - 1)(2 * width + j) - Vg(k - 1)(4 * width + j)), 0).toShort
        Vb(k - 1)(0 * width + j) =  Math.max((2 * Vb(k - 1)(2 * width + j) - Vb(k - 1)(4 * width + j)), 0).toShort
      }

      IM_DownSample8U_C1(Vr(k - 1), GSr(k), width, height, width, size(2 * k + 1), size(2 * k), size(2 * k + 1), 1)
      IM_DownSample8U_C1(Vg(k - 1), GSg(k), width, height, width, size(2 * k + 1), size(2 * k), size(2 * k + 1), 1)
      IM_DownSample8U_C1(Vb(k - 1), GSb(k), width, height, width, size(2 * k + 1), size(2 * k), size(2 * k + 1), 1)
    }

    border = size(2 * level) * size(2 * level + 1)
    for (index <- 0 until(border)){
      val i = index / size(2 * level + 1)
      val j = index % size(2 * level + 1)
      gsResult(i * size(2 * level + 1) * 3 + 3*j) = GSr(level)(i * size(2 * level + 1) + j)
      gsResult(i * size(2 * level + 1) * 3 + 3*j + 1) = GSg(level)(i * size(2 * level + 1) + j)
      gsResult(i * size(2 * level + 1) * 3 + 3*j + 2) = GSb(level)(i * size(2 * level + 1) + j)
    }
    return true
  }

  /**
   *
   * @param imageBlock:原始影像像素块信息
   * @return：
   * 像素块序号，顶层金字塔像素块，原始影像的起始、终止行号，原始影像的宽度, 影像金字塔层数
   */
  def GetGaussianImageTotalCol(imageBlock: ImageBlock) : (Int, Array[Short], Array[Int], Int, Int) = {

    val height = imageBlock.m_arrHeightRange(1) - imageBlock.m_arrHeightRange(0)
    val width = imageBlock.m_nWidth
    val nChannel = imageBlock.m_nChannel
    val level = imageBlock.m_nLevel
    var adjust = (Math.pow(2.0, level)).toInt
    if (adjust > 128){
      adjust = 128
    }else if (adjust < 64){
      adjust = 64
    }
    val nDis = 2 * adjust
    val gaussianW = GetGaussianSize(width, level)
    val gaussianH = GetGaussianSize(height, level)



    val size = new Array[Long](2 * (level + 1))
    var th = height
    var tw = width
    for (i <- 0 to level){
      size(2 * i) = th
      size(2 * i + 1) = tw
      th = (th - 1) / 2
      tw = (tw - 1) / 2
    }
    val GSr = new Array[Array[Short]](level + 1)
    val GSg = new Array[Array[Short]](level + 1)
    val GSb = new Array[Array[Short]](level + 1)
    for (i <- 0 to(level))
    {
      GSr(i) = new Array[Short]((size(2 * i) * size(2 * i + 1)).toInt)
      GSg(i) = new Array[Short]((size(2 * i) * size(2 * i + 1)).toInt)
      GSb(i) = new Array[Short]((size(2 * i) * size(2 * i + 1)).toInt)
    }

    val Vr = new Array[Array[Short]](level)
    val Vg = new Array[Array[Short]](level)
    val Vb = new Array[Array[Short]](level)
    for (k <- 0 until(level))
    {
      th = (size(2 * k) + 2).toInt;               //size[]表示的是每层金字塔的宽度和高度
      tw = (size(2 * k + 1) + 2).toInt;   //创建存储金字塔的宽度和高度增加一行和一列
      Vr(k) = new Array[Short]((th * tw).toInt)
      Vg(k) = new Array[Short]((th * tw).toInt)
      Vb(k) = new Array[Short]((th * tw).toInt)
    }

    val gsHeight = GetGaussianSize(height, level)
    val gsWidth = GetGaussianSize(width, level)
    var m_pGsBuf = new Array[Short]((gsHeight) * gsWidth * 3)

    //TODO 16位影像转8位

    GaussianModel_Parallel(imageBlock.m_pBuf, level,  height, width, 3,
      GSr, GSg, GSb, Vr, Vg, Vb, m_pGsBuf)

    return (imageBlock.m_nIndex, m_pGsBuf, imageBlock.m_arrHeightRange, imageBlock.m_nWidth, imageBlock.m_nLevel)
  }

  /**
   * 统计像素块的直方图
   * @param imageBlock
   * @return：（直方图，像素统计数量）
   */
  def StatisticsImgValidHist(imageBlock: ImageBlock) : (Array[Double], Double) =
  {
    val nBlockHeight = imageBlock.m_arrHeightRange(1) - imageBlock.m_arrHeightRange(0)
    val nBlockWidth = imageBlock.m_nWidth
    val input_histogram = new Array[Double](MAX_LEVEL * 3)
    var nInvalidCount = 0

    var red = 0
    var green = 0
    var blue = 0
    for (i <- 0 until(nBlockHeight) )
    {
      for (j <- 0 until(nBlockWidth))
      {
        red = imageBlock.m_pBuf(i * nBlockWidth * 3 + 3 * j)
        green = imageBlock.m_pBuf(i * nBlockWidth * 3 + 3 * j + 1)
        blue = imageBlock.m_pBuf(i * nBlockWidth * 3 + 3 * j + 2)
        if (0 == red + green + blue){
          nInvalidCount += 1
        }else{
          input_histogram(red) += 1
          input_histogram(MAX_LEVEL + green) += 1
          input_histogram(MAX_LEVEL * 2 + blue) += 1
        }

      }
    }

    return (input_histogram, nBlockHeight*nBlockWidth-nInvalidCount)
  }

  /**
   * 统计像素块的直方图
   * @param block:
   * @return:（直方图，像素统计数量）
   */
  def StatisticsImgValidHist(block : (Int, Array[Short], Array[Int], Int, Int)) : (Array[Double], Double) =
  {
    val nBlockHeight = GetGaussianSize(block._3(1) - block._3(0), block._5)
    val nBlockWidth = GetGaussianSize(block._4, block._5)
    val input_histogram = new Array[Double](MAX_LEVEL * 3)

    var red = 0
    var green = 0
    var blue = 0
    var nInvalidCount = 0
    for (i <- 0 until(nBlockHeight) )
    {
      for (j <- 0 until(nBlockWidth))
      {

        red = block._2(i * nBlockWidth * 3 + 3 * j)
        green = block._2(i * nBlockWidth * 3 + 3 * j + 1)
        blue = block._2(i * nBlockWidth * 3 + 3 * j + 2)
        if (0 == red + green + blue){
          nInvalidCount += 1
        }else{
          input_histogram(red) += 1
          input_histogram(MAX_LEVEL + green) += 1
          input_histogram(MAX_LEVEL * 2 + blue) += 1
        }

      }
    }

    return (input_histogram, nBlockHeight*nBlockWidth-nInvalidCount)
  }

  /**
   * 集中各个像素块直方图，计算归一化的累计直方图
   * @param arrayHist:各个像素块的直方图
   * @return
   */
  def CalculateImgHistUniform(arrayHist: Array[(Array[Double], Double)]) : Array[Double] = {
    val temp_cdf = new Array[Double](MAX_LEVEL * 3)
    var dSumCount : Double = 0.0
    for (item <- arrayHist ){
      dSumCount += item._2
    }
    for (item <- arrayHist){
      for (i <- 0 until(MAX_LEVEL)){
        temp_cdf(i) += item._1(i)/dSumCount
        temp_cdf(i + MAX_LEVEL) += item._1(i + MAX_LEVEL)/dSumCount
        temp_cdf(i + MAX_LEVEL * 2) += item._1(i + MAX_LEVEL * 2)/dSumCount
      }
    }

    var dR = 0.0
    var dG = 0.0
    var dB = 0.0
    for (i <- 1 until(MAX_LEVEL))
    {
      dR = temp_cdf(i - 1) + temp_cdf(i)
      dG = temp_cdf(i - 1 + MAX_LEVEL) + temp_cdf(i + MAX_LEVEL)
      dB = temp_cdf(i - 1 + MAX_LEVEL * 2) + temp_cdf(i + MAX_LEVEL * 2)
      temp_cdf(i) = dR
      temp_cdf(i + MAX_LEVEL) = dG
      temp_cdf(i + MAX_LEVEL * 2) = dB
    }
    return temp_cdf
  }

  /**
   * 根据颜色映射表对像素块像素进行重新赋值
   * @param block:
   * @param colorMap
   * @return
   */
  def ColorMatch(block: (Int, Array[Short], Array[Int], Int, Int), colorMap: Array[Int]) :
  (Int, Array[Short], Array[Int], Int, Int) = {
    val nRowCount = GetGaussianSize(block._3(1) - block._3(0), block._5)
    val nOriWidth = GetGaussianSize(block._4, block._5)
    var red = 0
    var green = 0
    var blue = 0
    var newR = 0
    var newG = 0
    var newB = 0
    var mTargetBuf = new Array[Short](nRowCount * nOriWidth * 3)
    for (m <- 0 until(nRowCount); n <- 0 until(nOriWidth))
    {
      red = block._2(m * nOriWidth * 3 + 3 * n)
      green = block._2(m * nOriWidth * 3 + 3 * n + 1)
      blue = block._2(m * nOriWidth * 3 + 3 * n + 2)

      newR = colorMap(red).toShort
      newG = colorMap(green + MAX_LEVEL).toShort
      newB = colorMap(blue + MAX_LEVEL * 2).toShort
      mTargetBuf(m * nOriWidth * 3 + 3 * n) = newR.toShort
      mTargetBuf(m * nOriWidth * 3 + 3 * n + 1) = newG.toShort
      mTargetBuf(m * nOriWidth * 3 + 3 * n + 2) = newB.toShort
    }
    return (block._1, mTargetBuf, block._3, block._4, block._5)
  }

  def ExportGaussian(arrBlock: Array[(Int, Array[Short], Array[Int], Int, Int)]): Boolean = {
    var nRowCount = 0
    var nStartIndex = 0
    var nEndIndex = 0
    var i : Int = 0
    var nIndex = 0
    val height = arrBlock(arrBlock.length-1)._3(1)
    val width = arrBlock(0)._4
    var nDis: Int = 0
    if (arrBlock.length >1){
      nDis = (arrBlock(0)._3(1) - arrBlock(1)._3(0))/2
    }
    val blocksize = arrBlock(0)._3(1) - arrBlock(0)._3(0) - nDis
    val level = arrBlock(0)._5
    val gaussianW = GetGaussianSize(width, level)
    val gaussianH = GetGaussianSize(blocksize + 2*nDis, level)
    val mTargetBuf = new Array[Short](gaussianH * gaussianW * 3)

    val sImgPath = "H:\\work\\2023\\pan\\spark\\test_data\\result\\gs11.tif"
    val sOutputDir = CommonTools.GetDirectory(sImgPath)

    //    for (item <- arrBlock ){
    //      val str = sOutputDir + "/gs_" + nIndex + ".tif"
    //      val tempHeight = GetGaussianSize(item._3(1) - item._3(0), level)
    //      val tempWidth = GetGaussianSize(item._4, level)
    //
    //      nIndex += 1
    //      val pNewImg = new ImageXX()
    //      pNewImg.CreateImage(str, tempHeight,tempWidth, 3, 1)
    //      pNewImg.WriteBlock(0, 0, tempWidth, tempHeight, item._2)
    //      pNewImg.ReleaseObj()
    //    }

    val pNewImg = new ImageXX()
    pNewImg.CreateImage(sImgPath, GetGaussianSize(height, level), GetGaussianSize(width, level), 3, 1)

    nIndex = 0
    while(i < height){
      nStartIndex = i - nDis
      nStartIndex = Math.max(0, nStartIndex)
      nEndIndex = (i + blocksize + nDis).toInt
      nEndIndex = Math.min(height, nEndIndex)
      nRowCount = nEndIndex - nStartIndex


      val y0 = GetGaussianSize(nStartIndex, level)
      val y1 = GetGaussianSize(i, level)
      val y2 = GetGaussianSize(nEndIndex, level)
      val nGaussianSpan = y1 - y0
      val blockHeight = GetGaussianSize(arrBlock(nIndex)._3(1) - arrBlock(nIndex)._3(0), level)
      val MaxValue= Math.min(y2, blockHeight+y1 - nGaussianSpan)

      for(m <- y1 until(MaxValue); n <- 0 until(gaussianW)) {
        mTargetBuf((m - y1) * gaussianW * 3 + 3 * n) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n)
        mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 1) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 1)
        mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 2) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 2)
      }


      //      if (1 == nIndex){
      //        for (m <- y1 until(y1 + nGaussianSpan); n <- 0 until(gaussianW)){
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 1) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 1)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 2) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 2)
      //        }
      //        for (m <- (y1 + nGaussianSpan) until(y2); n <- 0 until(gaussianW)){
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 1) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 1)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 2) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 2)
      //        }
      //        pNewImg.WriteBlock(0, y1 + 1, gaussianW, y2 - y1 - 1, mTargetBuf)
      //      }else{
      //        for(m <- y1 until(y2); n <- 0 until(gaussianW)){
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 1) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 1)
      //          mTargetBuf((m - y1) * gaussianW * 3 + 3 * n + 2) = arrBlock(nIndex)._2((m - y1 + nGaussianSpan) * gaussianW * 3 + 3 * n + 2)
      //        }

      pNewImg.WriteBlock(0, y1, gaussianW, y2 - y1, mTargetBuf)
      println(s"---------------------${i}-------------------")
      i = i + blocksize.toInt
      nIndex += 1
    }


    pNewImg.ReleaseObj()
    return true
  }

  /**
   * 实现原始影像的重建
   * @param imageBlock
   * @param colorMap
   * @return
   */
  def ImageRebuild(imageBlock: ImageBlock, colorMap: Array[Int]): (Int, Array[Short], Array[Int], Int, Int) = {
    val height = imageBlock.m_arrHeightRange(1) - imageBlock.m_arrHeightRange(0)
    val width = imageBlock.m_nWidth
    val level = imageBlock.m_nLevel
    val rebuildOpe = new RebuildOpe(level, height, width)
    rebuildOpe.SetBufData(imageBlock.m_pBuf, height, width)
    rebuildOpe.GaussianModel_Parallel()
    rebuildOpe.LaplacianManage()
    rebuildOpe.ColorMap(colorMap)
    rebuildOpe.BuildImage()
    return (imageBlock.m_nIndex, rebuildOpe.mBuf, imageBlock.m_arrHeightRange, imageBlock.m_nWidth, imageBlock.m_nLevel)
  }

  def ImageCollect(arrBlock : Array[(Int, Array[Short], Array[Int], Int, Int)], sImgFilePath: String): ImageXX = {
    var nRowCount = 0
    var nStartIndex = 0
    var nEndIndex = 0
    var i : Int = 0
    var nIndex = 0
    val height = arrBlock(arrBlock.length-1)._3(1)
    val width = arrBlock(0)._4
    var nDis: Int = 0
    if (arrBlock.length >1){
      nDis = (arrBlock(0)._3(1) - arrBlock(1)._3(0))/2
    }
    val blocksize = arrBlock(0)._3(1) - arrBlock(0)._3(0) - nDis

    val pNewImage = new ImageXX
    pNewImage.CreateImage(sImgFilePath, height, width, 3, 1)

    while (i < height)
    {
      nStartIndex = i - nDis
      nStartIndex = Math.max(nStartIndex, 0)
      //第一块向上外扩2倍

      nEndIndex = (i + blocksize + nDis).toInt
      nEndIndex = Math.min(nEndIndex, height)

      nRowCount = nEndIndex - nStartIndex
      val nSpan = i - nStartIndex

      pNewImage.WriteBlock(0, nStartIndex + nSpan, width, nRowCount - nSpan,
        arrBlock(nIndex)._2, nSpan)

      i += blocksize.toInt
      nIndex += 1
    }

    return pNewImage
  }


  def ColorBalanceFun(sc : SparkContext, inputImg:String, refImg:String, outputImg:String) : Boolean = {
    if(!CommonTools.IsFileExists(inputImg) || !CommonTools.IsFileExists(refImg)){
      println(s"Input Image File:${inputImg} or ${refImg} is not Exists!")
      return false
    }

    InitGdal()

    //步骤0-原始影像初始化
    val pMR = new ImageXX()
    var bReuslt = pMR.Init(inputImg)
    if(!bReuslt) return false
    val env = pMR.GetEnv()
    val wkt = pMR.GetWKT()
    val nBitSize = pMR.getM_nBitSize()
    val nChannels = pMR.getM_nChannels()
    val height1 = pMR.getM_nHeight()
    val width1 = pMR.getM_nWidth()
    //参考影像初始化
    val pST = new ImageXX()
    bReuslt = pST.Init(refImg)
    if(!bReuslt) return false
    //步骤1.1-计算金字塔层数
    val dLevel = CommonTools.CalculatePyramidLevel(pMR.getM_dXRes(), pMR.getM_dYRes, pST.getM_dXRes(), pST.getM_dYRes())
    val valueB = 1
    var valueA = dLevel.toInt + valueB
    //步骤1.1-修正金字塔层数
    val arrTemp = GetGaussBlockSize(pMR.getM_nHeight(), pMR.getM_nWidth(), pMR.getM_nChannels, pMR.getM_nBitSize, valueA)
    valueA = arrTemp(1)
    //计算参考影像需要重采样到与待处理影像（valueA-valueB）层金字塔相同分辨率，设置的分辨率倍数
    val RES = (Math.pow(2.0, valueA - valueB)).toInt
    //步骤2-根据待处理影像和参考影像范围判断参考影像是否完全包含待处理影像
    if (!CommonTools.IsWithIn(pMR.GetEnv(), pST.GetEnv())){
      System.out.println("The Reference Image Range Can not Contains the Target Image!")
      return false
    }
    //步骤3-根据待处理影像范围在参考影像中截取的影像
    val arrImgs = new Array[String](1)
    arrImgs(0) = inputImg
    val bResult = MosaicLineApi.instance.GenerateMosaicLine(arrImgs, CommonTools.GetDirectory(outputImg))
    val sMosaicLineFile = CommonTools.GetDirectory(outputImg) + "/project.shp"
    if (!bResult || !CommonTools.IsFileExists(sMosaicLineFile)) {
      System.out.println("Image Mosaic Line Generate Failed!")
      return false
    }

    val pStCut = new ImageXX
    val xRes = pST.getM_dXRes
    val yRes = pST.getM_dYRes
    pStCut.CreateImage("", env, pST.getM_dXRes, pST.getM_dYRes,
      pST.getM_nChannels, pST.getM_nBitSize)
    pST.ClipWithShp(pStCut, sMosaicLineFile);
    pStCut.SetWKT(pMR.GetWKT())
    //步骤4-参考影像重采样到与待处理影像（valueA-valueB）层金字塔相同分辨率
    val pStUniform = new ImageXX
    pStCut.ResampleImg(pStUniform, GetGaussianSize(height1, valueA), GetGaussianSize(width1, valueA))

    //步骤5-拆分参考影像
    val stImgBlockArray: Array[ImageBlock] = SplitImage(pStUniform)
    pStCut.ReleaseObj()
    pST.ReleaseObj()
    pStUniform.ReleaseObj()
    val stImgBlockRDD: RDD[ImageBlock] = sc.makeRDD(stImgBlockArray)

    //步骤6-统计参考影像分块的直方图
    val stImgBlockHist = stImgBlockRDD.map(StatisticsImgValidHist).collect()
    val stImgHistUniform = CalculateImgHistUniform(stImgBlockHist)


    //步骤7-统计待处理影像像素极值，获取影像像素有效位数
    pMR.ComputeMinMax()
    val nImgRealBitSize = CommonTools.GetOffsetBit(pMR.getM_nMaxValue)

    //步骤8-待处理影像分块
    val originImgBlockArray: Array[ImageBlock] = SplitImage(pMR, valueA, nImgRealBitSize)
//    System.out.println("originImgBlockArray--length: " + originImgBlockArray.length)

    //存储原始影像像素块信息
    val originImgBlockRDD: RDD[ImageBlock] = sc.makeRDD(originImgBlockArray)

    //步骤9-原始影像创建金字塔
    val oriGaussian = originImgBlockRDD.map(GetGaussianImageTotalCol).collect()
    val oriGaussianRDD : RDD[(Int, Array[Short], Array[Int], Int, Int)] = sc.makeRDD(oriGaussian)

    //步骤10-统计原始影像金字塔直方图
    val oriGaussianHist = oriGaussianRDD.map(StatisticsImgValidHist).collect()
    val oriGaussianHistUniform = CalculateImgHistUniform(oriGaussianHist)

    //步骤11-直方图匹配计算颜色映射表
    val mactching_func = new Array[Int](MAX_LEVEL * 3)
    CommonTools.MatchImgsHist(oriGaussianHistUniform, stImgHistUniform, mactching_func, MAX_LEVEL)

    //步骤12-颜色映射表给顶层金字塔影像匀色
    //val oriGaussianColoredBlock = oriGaussianRDD.map(tuple => ColorMatch(tuple, mactching_func)).collect()

    //步骤13-原始影像创建高斯金字塔，实现顶层金字塔匀色，并完成影像重建
    val rebuilImgBlock = originImgBlockRDD.map(tuple => ImageRebuild(tuple, mactching_func)).collect()

    //步骤14-重建后的分块影像合成整幅
    val pNewImage = ImageCollect(rebuilImgBlock, outputImg)
    pNewImage.SetWKT(pMR.GetWKT())
    val dGeotransform = pMR.GetTransform()
    pNewImage.SetTransform(dGeotransform)

    pMR.ReleaseObj()
    pNewImage.ReleaseObj()

    CloseGdal()

    println("color balance with ref end")

    return true
  }

  def InitImage(inputImg:String) : ImageXX = {
    val image = new ImageXX;
    if(image.Init(inputImg)) return image
    else return null
  }


}


object ColorBalanceWithRef {
  type CoverageMap = Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
    // 可以chat下如何设置日志打印，然后只打印需要的部分
    val sc = new SparkContext(sparkConf)

    val coverageCollection: CoverageMap = null
    colorBalanceRef(sc, coverageCollection)

    sc.stop()

  }

  def colorBalanceRef(sc: SparkContext, coverageCollection: CoverageMap): RDDImage = {
    val isTest: Boolean = false // 此变量为 true 时，本算子在本地可以正常正确的运行

    if (isTest) {

      // 可以chat下如何设置日志打印，然后只打印需要的部分
      val tool = new ColorBalanceWithRef
      val inputFile: String = "./data/testdata/colorbalanceRef/input/ZY302_TMS_E12.9_N52.4_20191205_L1A0000605840-Ortho_8bit.tiff"
      val refFile: String = "./data/testdata/colorbalanceRef/input/Euro.tif"
      val outputFile: String = "./data/testdata/colorbalanceRef/output/outfile.tiff"
      tool.ColorBalanceFun(sc, inputFile, refFile, outputFile)

      val output_file = new File(outputFile)
      val outputfile_absolute_path = output_file.getAbsolutePath
      val hadoop_file_path = "/" + outputfile_absolute_path
      println("out put file is: " + hadoop_file_path)
      makeChangedRasterRDDFromTif(sc, hadoop_file_path)

    } else {
      val inputFileArr: ArrayBuffer[String] = new ArrayBuffer[String]()

      coverageCollection.foreach(file_coverage => {
        val inputSaveFile = s"/mnt/storage/algorithmData/${dagId}_colorbalanceRef.tiff"
        saveRasterRDDToTif(file_coverage._2, inputSaveFile)
        inputFileArr.append(inputSaveFile)
      })

      val inputFleArray: Array[String] = inputFileArr.toArray
      val resInitFile = s"/mnt/storage/algorithmData/${dagId}_colorbalanceRef_output_temp.tiff"
      val tool = new ColorBalanceWithRef
      tool.ColorBalanceFun(sc, inputFleArray(0), inputFleArray(1), resInitFile)

      makeChangedRasterRDDFromTif(sc, resInitFile)
    }
  }
}
