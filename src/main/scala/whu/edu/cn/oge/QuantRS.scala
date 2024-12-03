package whu.edu.cn.oge
import com.alibaba.fastjson.{JSON, JSONObject}
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.RS.Utils
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.Others.{hbaseHost, tempFilePath}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.Coverage.{loadFileFromCase, loadTxtFromCase, loadTxtFromUpload}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageUtil.removeZeroFromCoverage
import whu.edu.cn.util.PostSender.{sendShelvedPost, shelvePost}
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, makeFeatureRDDFromShp, saveFeatureRDDToShp, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}
import whu.edu.cn.util.ClientUtil

import scala.collection.immutable.Map
import scala.collection.mutable.{Map => UMap}
import scala.collection.{immutable, mutable}
import java.io.File
import java.time.LocalDate
import org.apache.hadoop.fs.{FileSystem, Path => hPath}
import org.apache.hadoop.conf.Configuration
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
object QuantRS {
  val algorithmData=GlobalConfig.QuantConf.Quant_DataPath
  val host = GlobalConfig.QuantConf.Quant_HOST
  val userName = GlobalConfig.QuantConf.Quant_USERNAME
  val password = GlobalConfig.QuantConf.Quant_PASSWORD
  val port = GlobalConfig.QuantConf.Quant_PORT
  val acPath = GlobalConfig.QuantConf.Quant_ACpath
  val tmpPath = GlobalConfig.Others.tempFilePath

  /**
   * 虚拟星座30米
   *
   * @param sc
   * @param LAI
   * @param FAPAR
   * @param NDVI
   * @param FVC
   * @param ALBEDO
   * @return
   */

  def imaginaryConstellations(implicit sc: SparkContext,
                                LAI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FAPAR: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                NDVI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FVC: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              ALBEDO: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // RDD落地
    val LAIPath = algorithmData + "LAI_" + time + ".tif"
    val FAPARPath = algorithmData + "FAPAR_" + time + ".tif"
    val NDVIPath = algorithmData + "NDVI_" + time + ".tif"
    val FVCPath = algorithmData + "FVC_" + time + ".tif"
    val ALBEDOPath = algorithmData + "ALBEDO_" + time + ".tif"

    saveRasterRDDToTif(LAI, LAIPath)
    saveRasterRDDToTif(FAPAR, FAPARPath)
    saveRasterRDDToTif(NDVI, NDVIPath)
    saveRasterRDDToTif(FVC,FVCPath)
    saveRasterRDDToTif(ALBEDO,ALBEDOPath)
    val outputTiffPath = algorithmData+"ref30_result_"+ time + ".tif"
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""cd /mnt/storage/htTeam/ref_rec_30;bash /mnt/storage/htTeam/ref_rec_30/ref_rec_30_v1.sh   $LAIPath $FAPARPath $NDVIPath  $FVCPath $ALBEDOPath $outputTiffPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val result=makeChangedRasterRDDFromTif(sc, outputTiffPath)
    // 解决黑边值为255影响渲染的问题
    removeZeroFromCoverage(Coverage.addNum(result, 1))
  }

  /**
   * 虚拟星座500米
   *
   * @param sc
   * @param MOD09A1
   * @param LAI
   * @param FAPAR
   * @param NDVI
   * @param EVI
   * @param FVC
   * @param GPP
   * @param NPP
   * @param ALBEDO
   * @param COPY
   * @return
   */
  def reflectanceReconstruction(implicit sc: SparkContext,
                                MOD09A1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                LAI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FAPAR: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                NDVI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                EVI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FVC: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                GPP: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                NPP: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                ALBEDO: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                COPY: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // RDD落地
    val MOD09A1Path = algorithmData + "MOD09A1500_" + time + ".tif"
    val LAIPath = algorithmData + "LAI500_" + time + ".tif"
    val FAPARPath = algorithmData + "FAPAR500_" + time + ".tif"
    val NDVIPath = algorithmData + "NDVI500_" + time + ".tif"
    val EVIPath = algorithmData + "EVI500_" + time + ".tif"
    val FVCPath = algorithmData + "FVC500_" + time + ".tif"
    val GPPPath = algorithmData + "GPP500_" + time + ".tif"
    val NPPPath = algorithmData + "NPP500_" + time + ".tif"
    val ALBEDOPath = algorithmData + "ALBEDO500_" + time + ".tif"
    val COPYPath = algorithmData + "COPY500_" + time + ".tif"
    saveRasterRDDToTif(MOD09A1, MOD09A1Path)
    saveRasterRDDToTif(LAI, LAIPath)
    saveRasterRDDToTif(FAPAR, FAPARPath)
    saveRasterRDDToTif(NDVI, NDVIPath)
    saveRasterRDDToTif(EVI, EVIPath)
    saveRasterRDDToTif(FVC, FVCPath)
    saveRasterRDDToTif(GPP, GPPPath)
    saveRasterRDDToTif(NPP, NPPPath)
    saveRasterRDDToTif(ALBEDO, ALBEDOPath)
    saveRasterRDDToTif(COPY, COPYPath)
    val outputTiffPath = algorithmData + "ref500_result_" + time + ".tif"
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""cd /mnt/storage/htTeam/ref_rec_500;bash /mnt/storage/htTeam/ref_rec_500/ref_rec_500_v1.sh   $MOD09A1Path $LAIPath $FAPARPath $NDVIPath $EVIPath  $FVCPath $GPPPath $NPPPath $ALBEDOPath $COPYPath  $outputTiffPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val result = makeChangedRasterRDDFromTif(sc, outputTiffPath)
    // 解决黑边值为255影响渲染的问题
    removeZeroFromCoverage(Coverage.addNum(result, 1))
  }
  /**
   * MERSI反照率计算
   * @param sc
   * @param TOAReflectance
   * @param solarZenith
   * @param solarAzimuth
   * @param sensorZenith
   * @param sensorAzimuth
   * @param cloudMask
   * @param timeStamp
   * @param localnoonCoefs
   * @param parameters
   * @param bands
   * @return
   */
  def surfaceAlbedoLocalNoon(implicit sc: SparkContext,
                              TOAReflectance: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              solarZenith: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              solarAzimuth: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              sensorZenith: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              sensorAzimuth: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              cloudMask: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              timeStamp: String,
                              localnoonCoefs: String,
                              parameters: String,
                              bands: Int,userId: String, dagId: String)
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // RDD落地
    val TOAReflectancePath = algorithmData + "TOAReflectance_" + time + ".tif"
    val solarZenithPath = algorithmData + "solarZenith_" + time + ".tif"
    val solarAzimuthPath = algorithmData + "solarAzimuth_" + time + ".tif"
    val sensorZenithPath = algorithmData + "sensorZenith_" + time + ".tif"
    val sensorAzimuthPath = algorithmData + "sensorAzimuth_" + time + ".tif"
    val cloudMaskPath = algorithmData + "cloudMask_" + time + ".tif"
    saveRasterRDDToTif(TOAReflectance, TOAReflectancePath)
    saveRasterRDDToTif(solarZenith, solarZenithPath)
    saveRasterRDDToTif(solarAzimuth, solarAzimuthPath)
    saveRasterRDDToTif(sensorZenith, sensorZenithPath)
    saveRasterRDDToTif(sensorAzimuth, sensorAzimuthPath)
    saveRasterRDDToTif(cloudMask,cloudMaskPath)

    def loadPath(filePath: String, userId: String, dagId: String): String = {
      if (filePath.startsWith("myData/")) {
        loadTxtFromUpload(filePath, userId, dagId, "others")
      } else if (filePath.startsWith("OGE_Case_Data/")) {
        loadTxtFromCase(filePath, dagId)
      } else {
        "" // 返回空字符串或其他默认值
      }
    }

    val localnoonCoefsPath = loadPath(localnoonCoefs, userId, dagId)
    val parametersPath = loadPath(parameters, userId, dagId)

//    val localnoonCoefsPath = loadTxtFromUpload(localnoonCoefs, userId, dagId, "others")
//    val parametersPath = loadTxtFromUpload(parameters, userId, dagId, "others")
    val writePath = "/mnt/storage/htTeam/data"
    val writeName = "surfaceAlbedoLocalNoon_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""cd /mnt/storage/htTeam/albedo_MERSI;./Surface_Albedo_LocalNoon_Cal $TOAReflectancePath $solarZenithPath $solarAzimuthPath $sensorZenithPath $sensorAzimuthPath $cloudMaskPath $timeStamp $localnoonCoefsPath $parametersPath $bands $writePath  $writeName""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, algorithmData + writeName)
  }

  /**
   *  HI-GLASS反照率
   * @param sc
   * @param InputTiffs
   * @param Metadata
   * @param BinaryData
   * @return
   */
  def HiGlassAlbedo(implicit sc: SparkContext,
                    InputTiffs: immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                    Metadata:String,
                    BinaryData: String,
                    userID: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) ={
    // 配置本地文件系统
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    val fs = FileSystem.get(conf)

    // 指定创建目录
    val time = System.currentTimeMillis()
    val currentDate1 = LocalDate.now()

          // 输入文件路径
    val folderPath = new hPath(algorithmData + "LC09_L1TP_" + time)
    // 输出结果路径
    val outputPath = new hPath(algorithmData + "HI-GLASS_result_" + time)
    val baseName = folderPath.getName

    // 创建文件夹存放影像
    if (!fs.exists(folderPath) && !fs.exists(outputPath)) {
      fs.mkdirs(folderPath)
      fs.mkdirs(outputPath)
      println(s"影像文件夹创建成功：$folderPath")
      println(s"结果文件夹创建成功：$outputPath")
    } else {
      println(s"文件夹已存在")
    }

    // 影像落地
    var i = 1
    for (tiff <- InputTiffs) {
      // 影像落地为tif
      val tiffPath = folderPath.toString + "/" + baseName + s"_B$i.TIF"
      saveRasterRDDToTif(tiff._2, tiffPath)
      i = i + 1
    }
    val utilsAC = new Utils
    // 元数据&bin文件落地
    val txtPath = folderPath.toString + "/" + baseName + "_MTL.txt"
    val binPath = folderPath. toString + "/" +"sw_bsa_coefs.bin"
// bos
var txtBosPath: String = s"${userID}/$Metadata"
var binBosPath: String = s"${userID}/$BinaryData"
val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    clientUtil.Download(txtBosPath, txtPath)
    clientUtil.Download(binBosPath, binPath)

    // 启动反照率程序
    val writeName = "albedo" + time + "_out.tif"
    val resultPath = outputPath.toString +"/"+writeName
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate cv && python /mnt/storage/htTeam/albedo_HIGLASS/task_albedo.py --input_data_dir $folderPath --output_file $resultPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val result = makeChangedRasterRDDFromTif(sc, resultPath)
    // 解决黑边值为255影响渲染的问题
    removeZeroFromCoverage(Coverage.addNum(result, 1))
  }


  /**
   *  大气校正算子
   * @param sc
   * @param tgtTiff
   * @param LstTiff
   * @param GMTED2Tiff
   * @param sensorType
   * @param xlsPath
   * @param userID
   * @return
   */
  def AtmoCorrection(implicit sc: SparkContext,
                     tgtTiff: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     LstTiff: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     GMTED2Tiff: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     sensorType: String = "GF1",
                     xlsPath: String,
                     userID: String,
                     dagId:String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val sensorTypeInput: String = UMap(
      "GF1" -> "GF1",
      "GF6" -> "GF6"
    ).getOrElse(sensorType, "GF1")
    val time = System.currentTimeMillis()
    // RDD落地，目前使用的测试数据命名固定，不可修改，不可加时间戳
    val tgtTiffPath = algorithmData + "GF1_WFV3_E107.1_N28.9_20230524_L1A0007296754_Addmetadata_ORT_106E_29N_CR.tif"
    val lstTiffPath  = algorithmData + "GF1_WFV3_E107.1_N28.9_20230524_L1A0007296754_Addmetadata_ORT_106E_29N_CR_dst.tif"
    val GMTED2TiffPath =  tmpPath +  "GMTED2km.tif"
    saveRasterRDDToTif(tgtTiff, tgtTiffPath)
    saveRasterRDDToTif(LstTiff, lstTiffPath)
    saveRasterRDDToTif(GMTED2Tiff, GMTED2TiffPath)
    // xlx落地
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    var xlsBosPath: String = s"${userID}/$xlsPath"

    if (sensorTypeInput.equals("GF1")){
      var LocalPath = algorithmData + "GF1_WFV_SRF.xls"
      if (xlsPath.startsWith("myData/")){
        clientUtil.Download(xlsBosPath, LocalPath)
      } else if (xlsPath.startsWith("OGE_Case_Data/")){
        loadFileFromCase(xlsPath,"GF1_WFV_SRF" , "xls",  dagId)
      }
    } else if (sensorTypeInput.equals("GF6")){
      var LocalPath = algorithmData + "GF6_WFV_SRF.xlsx"
      if (xlsPath.startsWith("myData/")) {
        clientUtil.Download(xlsBosPath, LocalPath)
      } else if (xlsPath.startsWith("OGE_Case_Data/")) {
        loadFileFromCase(xlsPath, "GF6_WFV_SRF","xlsx",dagId)
      }
    }
    // 启动大气校正程序
    val writeName = algorithmData + "atmoCorrection" + time + "_out.tif"
    val auxPath = tmpPath.substring(0, tmpPath.length() -1)
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate produce_GF_ref;cd /mnt/storage/htTeam/AtmoCorrection;./dist/produce_GF_ref_with_dst $tgtTiffPath $writeName $lstTiffPath $auxPath""".stripMargin
      println(s"st = $st")
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    makeChangedRasterRDDFromTif(sc, writeName)
  }

}