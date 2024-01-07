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
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.dagId
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.File
import java.nio.file.Paths
import scala.collection.mutable.ArrayBuffer


object GeoCorrection {
  type CoverageMap = Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Correction")
    val sc = new SparkContext(sparkConf)

    val startTime = System.nanoTime()
    val inputImage: CoverageMap = null

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
  def geometricCorrection(sc: SparkContext, coverageCollection: CoverageMap): RDDImage = {
    val isTest = false  // 此变量为 true 时，本算子在本地可以正常正确运行

    if (isTest) {
      val inputImgPath1: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__1.tiff")
//      val inputImgPath2: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__2.tiff")
//      val inputImgPath3: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300__3.tiff")
      val inputDEM: String = new String("./data/testdata/geometric_correction/input/GF1_WFV3_E115.6_N38.9_20160529_L1A0001610300-DEM.tif")
      val outputImgPath: String = new String("./data/testdata/geometric_correction/output/")

      val inputFileArr = new Array[String](2)
      inputFileArr(0) = inputImgPath1
//      inputFileArr(1) = inputImgPath2
//      inputFileArr(2) = inputImgPath3
      inputFileArr(inputFileArr.length - 1) = ""
      val outputFileArr: Array[String] = geoCor_spark(sc, inputFileArr, outputImgPath)
//      whu_geometric_correction(inputImgPath, null, outputImgPath)

      val outputfile_absolute_path = new File(outputFileArr(0)).getAbsolutePath
      val hadoop_file_path = "/" + outputfile_absolute_path
      println("hadoop file is: " + hadoop_file_path)
      makeChangedRasterRDDFromTif(sc, hadoop_file_path)
    } else {
      val inputFileArr: ArrayBuffer[String] = new ArrayBuffer[String]()

      coverageCollection.foreach(file_coverage => {
        val inputSaveFile = s"/mnt/storage/algorithmData/${dagId}_geo_correc_by_dem_or_rpc.tiff"
        saveRasterRDDToTif(file_coverage._2, inputSaveFile)
        inputFileArr.append(inputSaveFile)
      })

      val inputFleArray: Array[String] = inputFileArr.toArray
      val outDir = s"/mnt/storage/algorithmData/"
      val outputFileArr: Array[String] = geoCor_spark(sc, inputFleArray, outDir)

//      val inputSavePath = s"/mnt/storage/algorithmData/${dagId}_geometricCorrection.tiff"
//      saveRasterRDDToTif(coverage,inputSavePath)
//      whu_geometric_correction(inputSavePath, null, resFile)

      makeChangedRasterRDDFromTif(sc, outputFileArr(0))
    }
  }

  private def geoCor_spark(sc: SparkContext, inputFileArr: Array[String], outPutDir: String): Array[String] = {
    val input_dem_file = inputFileArr.last
    val input_image_file = inputFileArr.dropRight(1)
    val input_file_rdd = sc.makeRDD(input_image_file)

    val input_output_file_rdd = input_file_rdd.map(input_file_path => {
      val input_file = new File(input_file_path)
      val input_file_name = dagId + "_" + input_file.getName

      val output_file_path = Paths.get(outPutDir, input_file_name).toString

      (input_file_path, output_file_path)
    })

    val outFile: RDD[String] = input_output_file_rdd.map(input_output_file => {
      whu_geometric_correction(input_output_file._1, input_dem_file, input_output_file._2)
    })

    outFile.collect()
  }

  /**
   * 几何校正
   *
   * @param inputFile  待校正图像
   * @param inputDEM DEM路径，不输入是要设置为null
   * @param outputFile 校正后的图像
   */
  private def whu_geometric_correction(inputFile: String, inputDEM: String, outputFile: String): String = {
    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALAllRegister()

    // 1.获取 RPC 数据
    GDAL_CORRECTION_LIBRARY.whu_gdal_CPLSetConfigOption("GDAL_FILENAME_IS_UTF8", "NO")
    val inputDataset: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALOpen(inputFile, 0)
    val rpcMedaData: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GetMetaData(inputDataset, "RPC")
    val rpcInfo: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALExtractRPCInfo(rpcMedaData)

    // 2.RPC校正
    var rpcOptions: Pointer = null
    if ((null != inputDEM) && ("" != inputDEM)) {
      rpcOptions = GDAL_CORRECTION_LIBRARY.whu_gdal_CSLSetNameValue(rpcOptions, "RPC_DEM", inputDEM)
      println("geometric correction by DEM")
    }
    val resultImg = whu_geometric_correction_rpc_separate_dll(inputDataset, outputFile, rpcInfo, 0, rpcOptions, 1, "GTiff")

    GDAL_CORRECTION_LIBRARY.whu_gdal_GDALClose(inputDataset)

    println("geometric correction end")

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
  private def whu_geometric_correction_rpc_separate_dll(inputDataset: Pointer, outputFile: String, rpcInfo: Pointer, dfPixErrThreshold: Double, paOptions: Pointer, eResampleMethod: Int, format: String): String = {

    val bandCount: Int = GDAL_CORRECTION_LIBRARY.whu_gdal_GetRasterCount(inputDataset)
    val transformArg: Pointer = GDAL_CORRECTION_LIBRARY.whu_gdal_GDALCreateRPCTransformer(rpcInfo, 0, dfPixErrThreshold, paOptions)
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