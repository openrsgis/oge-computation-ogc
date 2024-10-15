package whu.edu.cn.algorithms.AI

import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.feature.FeatureCollection
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.config.GlobalConfig.Others.tempFilePath
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.SAGA.{host, password, port, userName}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.ClientUtil
import whu.edu.cn.util.CoverageUtil.{getnoDataAccordingtoCellType, removeZeroFromCoverage}
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import java.io.File

object ML {
  def main(args: Array[String]): Unit = {

  }



  val algorithmData = GlobalConfig.SAGAConf.SAGA_DATA
  val algorithmDockerData = GlobalConfig.SAGAConf.SAGA_DOCKERDATA
  //  val algorithmCode = GlobalConfig.SAGAConf.SAGA_ALGORITHMCODE
  val host = GlobalConfig.SAGAConf.SAGA_HOST
  val userName = GlobalConfig.SAGAConf.SAGA_USERNAME
  val password = GlobalConfig.SAGAConf.SAGA_PASSWORD
  val port = GlobalConfig.SAGAConf.SAGA_PORT

  def deleteDirectory(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles.foreach(deleteDirectory)
    }
    dir.delete()
  }
  def ANNClassification(implicit sc: SparkContext,coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), sampleFiles: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // 保存图像
    val savePath = s"${tempFilePath}${Trigger.dagId}_test.tiff"
    saveRasterRDDToTif(coverage,savePath)


    val resultPath = s"${tempFilePath}${Trigger.dagId}test_result.tiff"
    // 样本数据创建用户文件夹
    val userFolder = s"$tempFilePath${Trigger.userId}"
    val folder = new File(userFolder)
    folder.mkdir()
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    //下载文件
    sampleFiles.foreach(f =>{
      val fileName = f.split('/')(1)
      val filePath = s"$userFolder/$fileName"
      val path = s"${Trigger.userId}/$f"
      println(path)

      clientUtil.Download(path, filePath)
    })

    // 给每个文件加路径前缀
    val sampleFiles1 = sampleFiles.map(f =>{
      s"$userFolder/" + f.split('/')(1)
    })
    val samplePaths = sampleFiles1.mkString(" ")

    versouSshUtil(host, userName, password, port)
    // nodata的处理很麻烦的一点是如果匹配不到怎么办？
    val nodata = getnoDataAccordingtoCellType(coverage._2.cellType)

    val st = s"conda activate cv && python /root/ann/ann.py --imagePath $savePath --samplePath $samplePaths --savePath $resultPath --noData $nodata"

    Trigger.tempFileList.append(savePath)
    Trigger.tempFileList.append(resultPath)

    println(s"st = $st")
    runCmd(st, "UTF-8")
    println("Success")
    //    Thread.sleep(200)
    val result = removeZeroFromCoverage(makeChangedRasterRDDFromTif(sc,resultPath))


    deleteDirectory(folder)
    result
  }

  def SVMClassification(implicit sc: SparkContext,coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), sampleFiles: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // 保存图像
    val savePath = s"${tempFilePath}${Trigger.dagId}_test.tiff"
    saveRasterRDDToTif(coverage,savePath)


    val resultPath = s"${tempFilePath}${Trigger.dagId}test_result.tiff"
    // 样本数据创建用户文件夹
    val userFolder = s"$tempFilePath${Trigger.userId}"
    val folder = new File(userFolder)
    folder.mkdir()
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    //下载文件
    sampleFiles.foreach(f => {
      val fileName = f.split('/')(1)
      val filePath = s"$userFolder/$fileName"
      val path = s"${Trigger.userId}/$f"
      println(path)

      clientUtil.Download(path, filePath)
    })

    // 给每个文件加路径前缀
    val sampleFiles1 = sampleFiles.map(f =>{
      s"$userFolder/" + f.split('/')(1)
    })
    val samplePaths = sampleFiles1.mkString(" ")

    versouSshUtil(host, userName, password, port)
    // nodata的处理很麻烦的一点是如果匹配不到怎么办？
    //    val nodata = getnoDataAccordingtoCellType(coverage._2.cellType)

    val st = s"conda activate cv && python /root/svm/svm.py --imagePath $savePath --samplePath $samplePaths --savePath $resultPath "

    Trigger.tempFileList.append(savePath)
    Trigger.tempFileList.append(resultPath)

    println(s"st = $st")
    runCmd(st, "UTF-8")
    println("Success")
    //    Thread.sleep(200)
    val result = makeChangedRasterRDDFromTif(sc,resultPath)


    deleteDirectory(folder)
    result
  }
}