/**
 * File Name: whu_geometric_correction_alg.scala
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

import java.io.File
import java.nio.file.Paths

import GdalCorrectionLibrary.GDAL_CORRECTION_LIBRARY


class whu_geometric_correction_alg {

  /**
   * * 图像几何校正
   * 使用本算子前，需要确保待校正文件的路径和输出文件夹合法且存在，且待校正文件有对应的同名 RPC 文件存在
   * @param sc spark 上下文环境
   * @param inputFileArr 待校正的文件路径名称数组
   * @param outPutDir 校正后图像保存的文件夹
   * @param outputSuf  是否对输出图像添加除文件属性外的后缀，默认为 true，即当待校正文件为 1.tif 时，
   *                   校正后为1_0.tif，默认从 _0 开始
   */
  def whu_geometric_correction(sc: SparkContext, inputFileArr: Array[String], outPutDir: String, outputSuf: Boolean = true): Unit = {
    val input_file_rdd = sc.makeRDD(inputFileArr)

    if (outputSuf) {
      val output_file_suf_arr = new Array[String](inputFileArr.length)
      for (i <- inputFileArr.indices) {
        output_file_suf_arr(i) = "_" + i.toString
      }
      val output_file_suf_rdd = sc.makeRDD(output_file_suf_arr)

      val input_file_path_output_suf_rdd = input_file_rdd.zip(output_file_suf_rdd)

      val input_output_file_rdd = input_file_path_output_suf_rdd.map(input_file_path_output_suf => {
        val input_file = new File(input_file_path_output_suf._1)
        val input_file_name = input_file.getName

        var output_file_name = ""
        val last_dot_index = input_file_name.lastIndexOf(".")
        if (last_dot_index >= 0) {
          val filePrefix = input_file_name.substring(0, last_dot_index)
          val fileSuffix = input_file_name.substring(last_dot_index)

          output_file_name = filePrefix + input_file_path_output_suf._2
          output_file_name += fileSuffix
        } else {
          output_file_name = input_file_name + input_file_path_output_suf._2
        }

        val output_file_path = Paths.get(outPutDir, output_file_name).toString

        (input_file_path_output_suf._1, output_file_path)
      })

      input_output_file_rdd.foreach(input_output_file => {
        val geo_correction =  new whu_geometric_correction_alg()
        geo_correction.whu_geometric_correction(input_output_file._1, input_output_file._2)
      })
    } else {
      val input_output_file_rdd = input_file_rdd.map(input_file_path => {
        val input_file = new File(input_file_path)
        val input_file_name = input_file.getName

        val output_file_path = Paths.get(outPutDir, input_file_name).toString

        (input_file_path, output_file_path)
      })

      input_output_file_rdd.foreach(input_output_file => {
        val geo_correction =  new whu_geometric_correction_alg()
        geo_correction.whu_geometric_correction(input_output_file._1, input_output_file._2)
      })
    }
  }

  /**
   * 几何校正
   * @param inputFile  待校正图像
   * @param outputFile 校正后的图像
   */
  private def whu_geometric_correction(inputFile: String, outputFile: String): Unit = {
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALAllRegister()

    // 1.获取 RPC 数据
    GDAL_CORRECTION_LIBRARY.whu_gdal_CPLSetConfigOption("GDAL_FILENAME_IS_UTF8", "NO")
    val inputDataset: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALOpen(inputFile, 0)
    val rpcMedaData: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GetMetaData(inputDataset, "RPC")
    val rpcInfo: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALExtractRPCInfo(rpcMedaData)

    // 2.RPC校正
    whu_geometric_correction_rpc_separate_dll(inputDataset, outputFile, rpcInfo, 0, null, 1, "GTiff")

    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALClose(inputDataset)
  }

  /**
   * 几何校正，RPC校正，gdal库分开版本
   * @param inputDataset
   * @param outFile
   * @param rpcInfo
   * @param dfPixErrThreshold
   * @param paOptions
   * @param eResampleMethod
   * @param format
   * @return
   */
  private def whu_geometric_correction_rpc_separate_dll(inputDataset: Pointer, outFile: String, rpcInfo: Pointer, dfPixErrThreshold: Double,
                         paOptions: StringArray, eResampleMethod: Int, format: String): Int = {

    val bandCount: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GetRasterCount(inputDataset)
    val transformArg: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALCreateRPCTransformer(rpcInfo, 0, dfPixErrThreshold, null)
    val adfGeoTransform: Array[Double] = new Array[Double](6)
    val adfExtent: Array[Double] = new Array[Double](4)
    val pixels: Array[Int] = new Array[Int](1)
    val lines: Array[Int] = new Array[Int](1)
    val ret: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALSuggestedWarpOutput2(inputDataset, transformArg, adfGeoTransform, pixels, lines, adfExtent, 0)
    if (0 != ret) {
      println("whu_gdal_GDALSuggestedWarpOutput2 error")
      return ret;
    }

    val dataType: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GetRasterDataType(inputDataset)
    val outputDataset: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_Create(format, outFile, pixels(0), lines(0), bandCount, dataType)
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

    return 0;
  }

  // RPC校正，gdal库合并版本，可以只用
  private def whu_geometric_correction_rpc_part_dll(inputDataset: Pointer, outFile: String, rpcInfo: Pointer, dfPixErrThreshold: Double,
                                 paOptions: StringArray, eResampleMethod: Int, format: String): Int = {
    GDAL_CORRECTION_LIBRARY.whu_gdal_imageWarpByRPC(inputDataset, outFile, rpcInfo, dfPixErrThreshold, paOptions, eResampleMethod, format)
  }

  // RPC校正，gdal库整个版本，可以直接使用
  private def whu_geometric_correction_rpc_all_dll(inputFile: String, outputFile: String): Unit = {
    GDAL_CORRECTION_LIBRARY.whu_gdal_correction_all(inputFile, outputFile)
  }
}

object whu_geometric_correction_alg {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("whu_correction")
    val sc = new SparkContext(sparkConf)

    val inputImgPath1: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__1.tiff")
    val inputImgPath2: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__2.tiff")
    val inputImgPath3: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__3.tiff")
    val input_file_arr = new Array[String](1)
    input_file_arr(0) = inputImgPath1
//    input_file_arr(1) = inputImgPath2
//    input_file_arr(2) = inputImgPath3

    val outputImgPath: String = new String("./data/testdata/geometric_correction/output")
    val startTime = System.nanoTime()
    val correction_alg = new whu_geometric_correction_alg()
    correction_alg.whu_geometric_correction(sc, input_file_arr, outputImgPath)
    val endTime = System.nanoTime()
    val costtime = ((endTime.toDouble - startTime.toDouble) / 1e6d) / 1000
    println("")
    println(" spark cost time is: " + costtime.toString + "s")

    sc.stop()
  }
}
