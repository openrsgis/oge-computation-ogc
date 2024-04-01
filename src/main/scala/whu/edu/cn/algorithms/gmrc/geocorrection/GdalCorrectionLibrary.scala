/**
 * File Name: GdalCorrectionLibrary.java
 * Project Name: OGE
 * Module Name: OGE
 * Created On: 2023/11/20
 * Created By: Qunchao Cui
 * Description: 本文件提供进行几何校正开发的 JNA 接口
 */

package whu.edu.cn.algorithms.gmrc.geocorrection

import com.sun.jna.{Library, Native, Pointer, StringArray}

object GdalCorrectionLibrary {
  val GDAL_CORRECTION_LIBRARY: GdalCorrectionLibrary = Native.load("./lib/dll/geocorrection/libgdalgeocor.so", classOf[GdalCorrectionLibrary])
//  val GDAL_CORRECTION_LIBRARY: GdalCorrectionLibrary = Native.load("./lib/dll/geocorrection/WhuGdalDll_dem.dll", classOf[GdalCorrectionLibrary])
}

trait GdalCorrectionLibrary extends Library {

  def whu_gdal_CSLSetNameValue(rpcOptions: Pointer, dem: String, demPath: String): Pointer

  def whu_gdal_GDALAllRegister(): Unit

  def whu_gdal_CPLSetConfigOption(key: String, value: String): Unit

  def whu_gdal_GDALOpen(filePath: String, access: Int): Pointer

  def whu_gdal_GetMetaData(dataset: Pointer, domain: String): Pointer

  def whu_gdal_GDALExtractRPCInfo(rpcMetaData: Pointer): Pointer

  def whu_gdal_GDALExtractRPCInfoClose(rpcInfo: Pointer): Unit

  def whu_gdal_GetRasterCount(dataset: Pointer): Int

  def whu_gdal_GDALCreateRPCTransformer(rpcInfo: Pointer, isTrue: Int, dfPixErrThreshold: Double, papOptions: Pointer): Pointer

  def whu_gdal_GDALSuggestedWarpOutput2(inputDataset: Pointer, pTransformArg: Pointer, padfGeoTransformOut: Array[Double], pnPixels: Array[Int], pnLines: Array[Int], padfExtent: Array[Double], nOptions: Int): Int

  def whu_gdal_GetRasterDataType(dataset: Pointer): Int

  def whu_gdal_Create(format: String, pszName: String, nXSize: Int, nYSize: Int, nBands: Int, eType: Int): Pointer

  def whu_gdal_SetGeoTransform(dataset: Pointer, adfGeoTransform: Array[Double]): Int

  def whu_gdal_SetProjection(dataset: Pointer, projection: String): Int

  def whu_gdal_CreateGeoToPixelTransform(GDALTransformerArg: Pointer, padfGeotransform: Array[Double]): Pointer

  def whu_gdal_GDALCreateWarpOptions(inputDs: Pointer, outputDs: Pointer, dataType: Int, eResampleMethod: Int, transformArg: Pointer, bandCount: Int): Pointer

  def whu_gdal_Warp_Correctioin(wartOptions: Pointer, nDstXOff: Int, nDstYOff: Int, nDstXSize: Int, nDstYSize: Int): Unit

  def whu_gdal_DestroyGeoToPixelTransform(transfromArg: Pointer): Unit

  def whu_gdal_GDALDestroyWarpOptions(options: Pointer): Unit

  def whu_gdal_GDALDestroyRPCTransformer(transformArg: Pointer): Unit

  def whu_gdal_GDALClose(dataset: Pointer): Int

  // 这是整个 dll 的接口，可以在Spark中正常正确的运行
  // 此接口不提供，否则Spark会卡顿，同时报错
  //    void whu_gdal_GDALDestroyDriverManager();
  def whu_gdal_correction_all(inputImgPath: String, outputImgPath: String): Int

  // 这是部分 dll 的接口，可以在Spark中正常正确的运行
  def whu_gdal_imageWarpByRPC(inputDataset: Pointer, outFile: String, rpcInfo: Pointer, dfPixErrThreshold: Double, paOptions: StringArray, eResampleMethod: Int, format: String): Int
}