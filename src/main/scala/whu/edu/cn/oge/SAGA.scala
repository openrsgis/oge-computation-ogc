package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import scala.collection.mutable.Map

object SAGA {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
  }

  val algorithmData = GlobalConfig.SAGAConf.SAGA_DATA
  val algorithmDockerData = GlobalConfig.SAGAConf.SAGA_DOCKERDATA
//  val algorithmCode = GlobalConfig.SAGAConf.SAGA_ALGORITHMCODE
  val host = GlobalConfig.SAGAConf.SAGA_HOST
  val userName = GlobalConfig.SAGAConf.SAGA_USERNAME
  val password = GlobalConfig.SAGAConf.SAGA_PASSWORD
  val port = GlobalConfig.SAGAConf.SAGA_PORT

  def sagaHistogramMatching(implicit sc: SparkContext,
                            grid: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            referenceGrid: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            method: Int = 1,
                            nclasses: Int = 100,
                            maxSamples: Int = 1000000):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val methodInput: Int = Map(
      0 -> 0,
      1 -> 1,
    ).getOrElse(method, 1)

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath1 = algorithmData + "sagaHistogramMatchingGrid_" + time + ".tif"
    val outputTiffPath2 = algorithmData + "sagaHistogramMatchingReference_" + time + ".tif"
    val writePath = algorithmData + "sagaHistogramMatching_" + time + "_out.tif"
    saveRasterRDDToTif(grid, outputTiffPath1)
    saveRasterRDDToTif(referenceGrid, outputTiffPath2)
    // docker路径
    val dockerTiffPath1 = algorithmDockerData + "sagaHistogramMatchingGrid_" + time + ".tif"
    val dockerTiffPath2 = algorithmDockerData + "sagaHistogramMatchingReference_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "sagaHistogramMatching_" + time + "_out.tif"

    try {
      versouSshUtil(host, userName, password, port)
      val st =
//        raw"""docker run -v /mnt/SAGA/sagaData/:/tmp/saga -it saga-gis /bin/bash;saga_cmd grid_calculus 21 -GRID "$dockerTiffPath1" -REFERENCE "$dockerTiffPath2" -MATCHED "$writeDockerPath" -METHOD $methodInput -NCLASSES $nclasses -MAXSAMPLES $maxSamples""".stripMargin val st =
        raw"""start docker 567ea3ad13c2;docker exec -it saga_cmd grid_calculus 21 -GRID "$dockerTiffPath1" -REFERENCE "$dockerTiffPath2" -MATCHED "$writeDockerPath" -METHOD $methodInput -NCLASSES $nclasses -MAXSAMPLES $maxSamples""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }


}
