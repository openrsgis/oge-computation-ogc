/**
 * File Name: GeoCorrection.scala
 * Project Name: OGE
 * Module Name: OGE
 * Created On: 2023/11/20
 * Created By: Qunchao Cui
 * Description: 本文件进行几何校正的算子开发
 * 本文件进行几何校正算子开发和验证
 */

package whu.edu.cn.algorithms.gmrc.geocorrection

import com.sun.jna.{Pointer, StringArray}
import org.apache.spark.{SparkConf, SparkContext}
import GdalCorrectionLibrary.GDAL_CORRECTION_LIBRARY
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.trigger.Trigger.dagId
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.File


object GeoCorrection {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
    val sc = new SparkContext(sparkConf)

    val startTime = System.nanoTime()
    val inputImage: RDDImage = null

    geometricCorrection(sc, inputImage)

    val endTime = System.nanoTime()
    val costtime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
    println("")
    println(" spark cost time is: " + costtime.toString + "s")

    sc.stop()
  }

  /**
   * * 图像几何校正
   * 使用本算子前，需要确保待校正文件的路径和输出文件夹合法且存在，且待校正文件有对应的同名 RPC 文件存在
   * @param sc spark 上下文环境
   * @param coverage 图像存在该内存中
   */
  def geometricCorrection(sc: SparkContext, coverage:  RDDImage): RDDImage = {
    val isTest = false  // 此变量为 true 时，程序可以正常正确运行

    if (isTest) {
      val inputImgPath: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__1.tiff")
      val outputImgPath: String = new String("./data/testdata/geometric_correction/output/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__1_output.tiff")

      whu_geometric_correction(inputImgPath, outputImgPath)

      val outputfile_absolute_path = new File(outputImgPath).getAbsolutePath
      val hadoop_file_path = "/" + outputfile_absolute_path
      makeChangedRasterRDDFromTif(sc, hadoop_file_path)
    } else {
      val inputSavePath = s"/mnt/storage/${dagId}_geometricCorrection.tiff"
      saveRasterRDDToTif(coverage,inputSavePath)

      val resFile = s"/mnt/storage/${dagId}_geometricCorrection_temp.tiff"
      whu_geometric_correction(inputSavePath, resFile)

      makeChangedRasterRDDFromTif(sc, resFile)
    }
  }

  /**
   * 几何校正
   *
   * @param inputFile  待校正图像
   * @param outputFile 校正后的图像
   */
  private def whu_geometric_correction(inputFile: String, outputFile: String): String = {
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALAllRegister()

    // 1.获取 RPC 数据
    GDAL_CORRECTION_LIBRARY.whu_gdal_CPLSetConfigOption("GDAL_FILENAME_IS_UTF8", "NO")
    val inputDataset: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALOpen(inputFile, 0)
    val rpcMedaData: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GetMetaData(inputDataset, "RPC")
    val rpcInfo: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALExtractRPCInfo(rpcMedaData)

    // 2.RPC校正
    val resultImg = whu_geometric_correction_rpc_separate_dll(inputDataset, outputFile, rpcInfo, 0, null, 1, "GTiff")

    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALClose(inputDataset)

    resultImg
  }

  /**
   * 几何校正，RPC校正，gdal库分开版本
   *
   * @param inputDataset
   * @param outputFile
   * @param rpcInfo
   * @param dfPixErrThreshold
   * @param paOptions
   * @param eResampleMethod
   * @param format
   * @return
   */
  private def whu_geometric_correction_rpc_separate_dll(inputDataset: Pointer, outputFile: String, rpcInfo: Pointer, dfPixErrThreshold: Double, paOptions: StringArray, eResampleMethod: Int, format: String): String = {

    val bandCount: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GetRasterCount(inputDataset)
    val transformArg: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALCreateRPCTransformer(rpcInfo, 0, dfPixErrThreshold, null)
    val adfGeoTransform: Array[Double] = new Array[Double](6)
    val adfExtent: Array[Double] = new Array[Double](4)
    val pixels: Array[Int] = new Array[Int](1)
    val lines: Array[Int] = new Array[Int](1)
    val ret: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALSuggestedWarpOutput2(inputDataset, transformArg, adfGeoTransform, pixels, lines, adfExtent, 0)
    if (0 != ret) {
      println("whu_gdal_GDALSuggestedWarpOutput2 error")
      return ""
    }

    val dataType: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GetRasterDataType(inputDataset)
    val outputDataset: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_Create(format, outputFile, pixels(0), lines(0), bandCount, dataType)
    GDAL_CORRECTION_LIBRARY.whu_gdal_SetGeoTransform(outputDataset, adfGeoTransform)

    val WKT_WGS84: String = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9108\"]],AUTHORITY[\"EPSG\",\"4326\"]]"
    GDAL_CORRECTION_LIBRARY.whu_gdal_SetProjection(outputDataset, WKT_WGS84)
    val geoToPixelTransform = GDAL_CORRECTION_LIBRARY.whu_gdal_CreateGeoToPixelTransform(transformArg, adfGeoTransform)
    val warp_options: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALCreateWarpOptions(inputDataset, outputDataset, dataType,
      eResampleMethod, geoToPixelTransform, bandCount)

    GDAL_CORRECTION_LIBRARY.whu_gdal_Warp_Correctioin(warp_options, 0, 0, pixels(0), lines(0))

    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALExtractRPCInfoClose(rpcInfo)
    GDAL_CORRECTION_LIBRARY.whu_gdal_DestroyGeoToPixelTransform(geoToPixelTransform)
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALDestroyWarpOptions(warp_options)
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALDestroyRPCTransformer(transformArg)
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALClose(outputDataset)

    outputFile
  }
}

//object GeoCorrection {
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
//    val sc = new SparkContext(sparkConf)
//
//    val startTime = System.nanoTime()
//    val inputImage: RDDImage = null
//
//    geometricCorrection(sc, inputImage)
//
//    val endTime = System.nanoTime()
//    val costtime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
//    println("")
//    println(" spark cost time is: " + costtime.toString + "s")
//
//    sc.stop()
//  }
//
//  def geometricCorrection(sc: SparkContext, coverage:  RDDImage): RDDImage = {
//    val geoCorrection = new GeoCorrection
//    geoCorrection.geometricCorrection(sc, coverage)
//  }
//}