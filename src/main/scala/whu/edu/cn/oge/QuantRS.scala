package whu.edu.cn.oge
import com.alibaba.fastjson.{JSON, JSONObject}
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.Others.tempFilePath
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
object QuantRS {
  val algorithmData=GlobalConfig.QuantConf.Quant_DataPath
  val host = GlobalConfig.QuantConf.Quant_HOST
  val userName = GlobalConfig.QuantConf.Quant_USERNAME
  val password = GlobalConfig.QuantConf.Quant_PASSWORD
  val port = GlobalConfig.QuantConf.Quant_PORT

  def imaginaryConstellations(implicit sc: SparkContext,
                                MOD09A1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                LAI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FAPAR: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                NDVI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                EVI: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                FVC: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                              ALBEDO: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    val outputTiffPath = algorithmData + "otbEdgeExtraction_" + time + ".tif"
    val writePath = algorithmData + "otbEdgeExtraction_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""bash /mnt/storage/OGE_ref_rec/ref_rec_tile/dist/ref_rec_tile_minio.sh""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, outputTiffPath)
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


}

