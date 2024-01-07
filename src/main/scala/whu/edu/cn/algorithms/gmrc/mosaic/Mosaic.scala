/**
 * File Name: Mosaic.scala
 * Project Name: OGE
 * Module Name: OGE
 * Created On: 2023/11/20
 * Created By: Qunchao Cui
 * Description: 本文件进行镶嵌算子开发，本文件执行需要 gdal 库，本文件可以在 gdal 的 3.2.0 版本运行通过，其它版本执行
 * 可能会有故障
 */

package whu.edu.cn.algorithms.gmrc.mosaic

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal._
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.algorithms.gmrc.util.CommonTools
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.dagId
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.File
import java.lang.Math._
import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer


object Mosaic {
  type CoverageMap = Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]

  def main(args: Array[String]): Unit = {

    // 1.创建 spark 环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Mosaic")
    val sc = new SparkContext(sparkConf)

    val startTime = System.nanoTime()

    // 2.执行具体算法
    val coverageMap: CoverageMap = null
    splitMosaic(sc, coverageMap)

    val endTime = System.nanoTime()
    val costtime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
    println("\nspark cost time is: " + costtime.toString + "s")

    sc.stop()
  }

  /**
   * 注册 gdal
   */
  private def initGdal(): Unit = {
    gdal.AllRegister()
    ogr.RegisterAll()
  }

  /**
   * 销毁 gdal
   */
  private def destroyGdal(): Unit = {
    gdal.GDALDestroyDriverManager()
  }

  /**
   * 进行图像镶嵌操作
   *
   * @param sc        Spark 环境
   * @param coverageCollection 图像保存在这里
   * @param
   * @return 图像的RDD
   */
  def splitMosaic(sc: SparkContext, coverageCollection: CoverageMap): RDDImage = {
    val isTest: Boolean = false  // 此变量为 true 时，本算子在本地可以正常正确的运行

    if (isTest) {
      val inputImgArray = new Array[String](2)
//      inputImgArray(0) = "./data/testdata/mosaic/input/GF1_WFV1_E109.8_N29.6_20160208_L1A0001398813_ortho_8bit.tif"
//      inputImgArray(1) = "./data/testdata/mosaic/input/GF1_WFV1_E110.1_N31.3_20160208_L1A0001398820_ortho_8bit.tif"
//      val sOutoutFile = "./data/testdata/mosaic/output/mosaic_test.tif"

      inputImgArray(0) = "./data/testdata/mosaic/input_little/GF1_WFV1_E109.8_N29.6_20160208_L1A0001398813_ortho_8bit_little.tif"
      inputImgArray(1) = "./data/testdata/mosaic/input_little/GF1_WFV1_E110.1_N31.3_20160208_L1A0001398820_ortho_8bit_little.tif"
      val sOutoutFile = "./data/testdata/mosaic/output_little/mosaic_test.tif"

      val outputfile_relative_path: String = splitMosaic(sc, inputImgArray, sOutoutFile, 1)

      val output_file = new File(outputfile_relative_path)
      val outputfile_absolute_path = output_file.getAbsolutePath
      val hadoop_file_path = "/" + outputfile_absolute_path
      println("out put file is: " + hadoop_file_path)
      makeChangedRasterRDDFromTif(sc, hadoop_file_path)

    } else {
      val inputFileArr: ArrayBuffer[String] = new ArrayBuffer[String]()

      coverageCollection.foreach(file_coverage => {
        val inputSaveFile = s"/mnt/storage/algorithmData/${dagId}_mosaic.tiff"
        saveRasterRDDToTif(file_coverage._2, inputSaveFile)
        inputFileArr.append(inputSaveFile)
      })

      val inputFleArray: Array[String] = inputFileArr.toArray
      val resInitFile = s"/mnt/storage/algorithmData/${dagId}_mosaic_temp.tiff"
      val resFile = splitMosaic(sc, inputFleArray, resInitFile, 1)

      makeChangedRasterRDDFromTif(sc, resFile)
    }
  }

  /**
   * 进行图像镶嵌操作
   *
   * @param sc        Spark 环境
   * @param siFileArr 要输入的图像文件数组
   * @param diFile    要输出的图像文件名，文件名格式为 name_i_j.suf，i 和 j 为镶嵌图像输出拼接时的块索引
   * @param diFileDim 输出图像块的维度，如 1 则是一幅图像，2 则是四幅图像等
   * @return 是否执行成功
   */
  private def splitMosaic(sc: SparkContext, siFileArr: Array[String], diFile: String, diFileDim: Int): String = {
    initGdal()

    // 1.生成镶嵌线文件并解析
    val output_dir = CommonTools.GetDirectory(diFile)

    val bResult = MosaicLineLibrary.MOSAIC_LINE.GenerateMosaicLine(siFileArr, output_dir)
    val cut_line_file = output_dir + "\\project.shp"
    if (!bResult && !CommonTools.IsFileExists(cut_line_file)) {
      System.out.println("generate mosaic line failed, maybe input mosaic line shape file: " + cut_line_file + " is not exists")
    }

    val list_sql_java: util.List[String] = paraseShpFileToSqlList(cut_line_file)
    val list_sql_scala = list_sql_java.asScala
    val list_sql_rdd = sc.makeRDD(list_sql_scala)

    // 2.获取源图像的 Dataset
    val si_rdd: RDD[String] = sc.makeRDD(siFileArr)
    val si_dataset_rdd: RDD[(Boolean, Dataset)] = si_rdd.map(si_file => {
      var valid_ds: Boolean = true

      val dataset: Dataset = gdal.Open(si_file, gdalconstConstants.GA_ReadOnly)
      if ((null == dataset) || (0 == dataset.getRasterCount)) {
        println("Input Image File: [" + si_file + "] open gdal failed or raster count is 0")
        valid_ds = false
      }

      (valid_ds, dataset)
    })

    // 3.由源图像获取目标图像的 dataset rdd
    val di_dataset_rdd: RDD[(String, Dataset)] = splitGDALWarpOutputDs(sc, si_dataset_rdd, diFile, diFileDim)

    val outputfile_rdd: RDD[String] = di_dataset_rdd.map(di_dataset => {
      di_dataset._1
    })

    var outputfile_relative_path = outputfile_rdd.first()

    //     4.由源图像和目标图像进行镶嵌操作
    val unit: Unit = di_dataset_rdd.map(di_dataset => {
      val si_data_datasetArr = new Array[Dataset](siFileArr.length)
      for (i <- siFileArr.indices) {
        si_data_datasetArr(i) = gdal.Open(siFileArr(i), gdalconstConstants.GA_ReadOnly)
      }
      for (i <- si_data_datasetArr.indices) {
        val si_dataset = new Array[Dataset](1)
        si_dataset(0) = si_data_datasetArr(i)

        val option = new util.Vector[String]()
        //镶嵌线文件
        option.add("-cutline")
        option.add(cut_line_file)
        //设置sql语句选择镶嵌线中特定的feature
        option.add("-cwhere")
        option.add(list_sql_java.get(i))
        option.add("-crop_to_cutline")
        //设置羽化宽度
        option.add("-cblend")
        option.add("100")
        //设置影像无效值区域像素值
        option.add("-srcnodata")
        option.add("0 0 0")

        gdal.Warp(di_dataset._2, si_dataset, new WarpOptions(option))
      }

      for (i <- 0 until si_data_datasetArr.length) {
        si_data_datasetArr(i).delete()
      }

    }).collect()

    destroyGdal()

    println("mosaic end")

    outputfile_relative_path
  }

  /**
   * 根据输入影像列表、（x，y）方向的块数，获取Dataset的列表
   *
   * @param si_dataset_rdd ： 输入影像数组rdd
   * @param pszOutFile  ：   输出影像文件路径
   * @param nBlockCount ：  x、y方向的块个数
   * @return
   */
  private def splitGDALWarpOutputDs(sc: SparkContext, si_dataset_rdd: RDD[(Boolean, Dataset)], pszOutFile: String, nBlockCount: Integer): RDD[(String, Dataset)] = {
    // 1.获取源图像的Dataset，已有，为参数si_dataset_rdd

    // 2.获取源图像geo信息的rdd，及其属性
    val geo_trans_pro_rdd: RDD[(Array[Double], (Int, Int, String))] = si_dataset_rdd.map(dataset => {
      val geo_transform_arr = new Array[Double](6) // 无实际物理意义，仅传递数据
      var si_rasterCount = 0
      var si_dataType = 0
      var si_projectionRef = ""

      if (dataset._1) {
        val adfThisGeoTransform = dataset._2.GetGeoTransform
        val nWidth = dataset._2.getRasterXSize
        val nHeight = dataset._2.getRasterYSize

        geo_transform_arr(0) = adfThisGeoTransform(0) // 原点的X坐标
        geo_transform_arr(2) = geo_transform_arr(0) + nWidth * adfThisGeoTransform(1) // 右下角X坐标
        geo_transform_arr(3) = adfThisGeoTransform(3) // 原点的Y坐标
        geo_transform_arr(1) = geo_transform_arr(3) + nHeight * adfThisGeoTransform(5) // 右下角Y坐标
        geo_transform_arr(4) = adfThisGeoTransform(1) // X 方向分辨率
        geo_transform_arr(5) = adfThisGeoTransform(5) // Y 方向分辨率

        si_rasterCount = dataset._2.getRasterCount
        si_dataType = dataset._2.GetRasterBand(1).getDataType
        si_projectionRef = dataset._2.GetProjectionRef

      }

      (geo_transform_arr, (si_rasterCount, si_dataType, si_projectionRef))
    })

    // 3.获取源图像geo信息的的最大值和最小值
    val trans_pro_ret: (Array[Double], (Int, Int, String)) = geo_trans_pro_rdd.reduce((tp1, tp2) => {
      val trans_pro_arr = new Array[Double](6)
      trans_pro_arr(0) = min(tp1._1(0), tp2._1(0)) // X 最小值
      trans_pro_arr(1) = min(tp1._1(1), tp2._1(1)) // Y 最小值
      trans_pro_arr(2) = max(tp1._1(2), tp2._1(2)) // X 最大值
      trans_pro_arr(3) = max(tp1._1(3), tp2._1(3)) // Y 最大值
      trans_pro_arr(4) = min(tp1._1(4), tp2._1(4)) // X 方向分辨率
      trans_pro_arr(5) = min(abs(tp1._1(5)), abs(tp2._1(5))) // Y 方向分辨率

      (trans_pro_arr, tp2._2)
    })

    // 4.将第一步算的最大值设置到转换系数中
    val adfDstGeoTransform = new Array[Double](6)
    adfDstGeoTransform(0) = trans_pro_ret._1(0)
    adfDstGeoTransform(1) = trans_pro_ret._1(4)
    adfDstGeoTransform(2) = 0.0
    adfDstGeoTransform(3) = trans_pro_ret._1(3)
    adfDstGeoTransform(4) = 0.0
    adfDstGeoTransform(5) = -1 * trans_pro_ret._1(5)

    // 5.获取 x 和 y 方向的像素个数，即总的 width 和 height；计算每个分块长和宽
    val x_pixel_count = ((trans_pro_ret._1(2) - trans_pro_ret._1(0)) / trans_pro_ret._1(4) + 0.5).toInt
    val y_pixel_count = ((trans_pro_ret._1(3) - trans_pro_ret._1(1)) / trans_pro_ret._1(5) + 0.5).toInt
    val height = y_pixel_count / nBlockCount
    val width = x_pixel_count / nBlockCount

    // 6.计算每块的坐标转换信息，这个也是分块的逻辑；并转换为 分块信息的RDD
    val di_geo_transform_arr = new Array[(Array[Double], (Int, Int))](nBlockCount * nBlockCount)
    var count = 0
    for (i <- 0 until nBlockCount) {
      // 5.2 计算每块的开始和结束坐标
      val dUpLeftY = adfDstGeoTransform(3) + i * height * adfDstGeoTransform(5)
      for (j <- 0 until nBlockCount) {
        val dUpLeftX = adfDstGeoTransform(0) + j * width * adfDstGeoTransform(1)
        // 5.3 计算坐标转换信息
        val dGeoTransform = new Array[Double](6)
        dGeoTransform(0) = dUpLeftX
        dGeoTransform(1) = adfDstGeoTransform(1)
        dGeoTransform(2) = 0.0
        dGeoTransform(3) = dUpLeftY
        dGeoTransform(4) = 0.0
        dGeoTransform(5) = adfDstGeoTransform(5)

        di_geo_transform_arr(count) = (dGeoTransform, (i, j))
        count += 1
      }
    }

    // 7.根据分快信息，计算目标分块的Dataset的RDD
    val di_geo_transform_rdd: RDD[(Array[Double], (Int, Int))] = sc.makeRDD(di_geo_transform_arr)

    val sOutputDir = CommonTools.GetDirectory(pszOutFile)
    val sName = CommonTools.GetNameWithoutExt(pszOutFile)
    val sExt = CommonTools.GetExt(pszOutFile)

    val di_dataset_rdd: RDD[(String, Dataset)] = di_geo_transform_rdd.map(di_geo_transform => {
      var sFilePath = ""
      var di_dataset: Dataset = null

      val hDriver: Driver = gdal.GetDriverByName("GTiff")
//      val hDriver: Driver = gdal.GetDriverByName("MEM")
      if (null != hDriver) {
        sFilePath = sOutputDir + "\\" + sName + "_" + di_geo_transform._2._1 + "_" + di_geo_transform._2._2 + sExt
        di_dataset = hDriver.Create(sFilePath, width, height, trans_pro_ret._2._1, trans_pro_ret._2._2)
        if (null == di_dataset) {
          println("di dataset is null")
        } else {
          di_dataset.SetGeoTransform(di_geo_transform._1)
          di_dataset.SetProjection(trans_pro_ret._2._3)
        }
      }

      (sFilePath, di_dataset)
    })

    di_dataset_rdd
  }

  /**
   * 获取 shp 文件 0 层的字段名称到列表中，并返回
   *
   * @param shpFile shp 文件路径
   * @return 获取的 0 层字段名称
   */
  private def paraseShpFileToSqlList(shpFile: String): util.List[String] = {
    val ds = ogr.Open(shpFile)
    if (null == ds) return null
    val layer = ds.GetLayer(0)
    if (null == layer) return null

    val listSql = new util.ArrayList[String]
    var feature = layer.GetNextFeature
    while (feature != null) {
      val sSql = "FID = " + feature.GetFID
      listSql.add(sSql)

      feature.delete()
      feature = layer.GetNextFeature
    }

    ds.delete()
    listSql
  }
}
