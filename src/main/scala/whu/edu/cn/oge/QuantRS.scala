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
import whu.edu.cn.oge.Coverage.loadTxtFromUpload
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageUtil.removeZeroFromCoverage
import whu.edu.cn.util.PostSender.{sendShelvedPost, shelvePost}
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, makeFeatureRDDFromShp, saveFeatureRDDToShp, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import scala.collection.immutable.Map
import scala.collection.mutable.Map
import scala.collection.{immutable, mutable}
import java.io.File
import java.time.LocalDate
import org.apache.hadoop.fs.{FileSystem, Path => hPath}
object QuantRS {
  val algorithmData=GlobalConfig.QuantConf.Quant_DataPath
  val host = GlobalConfig.QuantConf.Quant_HOST
  val userName = GlobalConfig.QuantConf.Quant_USERNAME
  val password = GlobalConfig.QuantConf.Quant_PASSWORD
  val port = GlobalConfig.QuantConf.Quant_PORT

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
        raw"""bash /mnt/storage/htTeam/ref_rec_30/ref_rec_30_v1.sh   $LAIPath $FAPARPath $NDVIPath  $FVCPath $ALBEDOPath $outputTiffPath""".stripMargin

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
        raw"""bash /mnt/storage/htTeam/ref_rec_500/ref_rec_500_v1.sh   $MOD09A1Path $LAIPath $FAPARPath $NDVIPath $EVIPath  $FVCPath $GPPPath $NPPPath $ALBEDOPath $COPYPath  $outputTiffPath""".stripMargin

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
                              bands: Int)
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

    val writeName = "surfaceAlbedoLocalNoon_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw""""/mnt/storage/htTeam/albedo_MERSI/Surface_Albedo_LocalNoon_Cal" $TOAReflectancePath $solarZenithPath $solarAzimuthPath $sensorZenithPath $sensorAzimuthPath $cloudMaskPath $timeStamp $localnoonCoefs $parameters $bands $algorithmData $writeName """.stripMargin

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
                    BinaryData: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) ={
    // 配置本地文件系统
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    val fs = FileSystem.get(conf)

    // 指定创建目录
    val time = System.currentTimeMillis()
    val currentDate1 = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val formattedDate1 = currentDate1.format(formatter)

    val folderPath = new hPath(algorithmData + "LC09_L1TP_" + time)
    val outputPath = new hPath(algorithmData + "HI-GLASS_result_" + time)
    val baseName = folderPath.getFileName.toString

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
      val currentDate2 = LocalDate.now()
      val formattedDate2 = currentDate2.format(formatter)
      // 影像落地为tif
      val tiffPath = folderPath.toString + "/" + baseName + s"_B$i.TIF"
      saveRasterRDDToTif(tiff._2, tiffPath)
      i = i + 1
    }
    val utilsAC = new Utils
    // 元数据落地
    val targetPath1 = folderPath.toString + "/" + baseName + "_MTL.txt"
    utilsAC.saveTXT(Metadata, targetPath)
    // bin文件落地
    val targetPath2 = folderPath. toString + "/" +"sw_bsa_coefs.bin"
    utilsAC.saveTXT(BinaryData, targetPath2)

    // 启动反照率程序
    val writeName = "albedo" + time + "_out.tif"
    val resultPath = outputPath.toString +"/"+writeName
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate cv && /mnt/storage/htTeam/albedo_HIGLASS/task_albedo.py --input_data $folderPath --output_file $resultPath""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }




}

