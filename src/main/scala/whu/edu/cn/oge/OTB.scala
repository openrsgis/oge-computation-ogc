package whu.edu.cn.oge

import java.io.{BufferedReader, BufferedWriter, FileWriter, InputStreamReader, OutputStreamWriter, PrintWriter}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal.ZFactor
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, _}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger.{dagId, runMain, workTaskJson}
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._
import whu.edu.cn.oge.Feature._
import whu.edu.cn.config.GlobalConfig

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.io.Source

object OTB {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
  }

  val algorithmData = GlobalConfig.OTBConf.OTB_DATA
  val algorithmCode = GlobalConfig.OTBConf.OTB_ALGORITHMCODE
  val host = GlobalConfig.OTBConf.OTB_HOST
  val userName = GlobalConfig.OTBConf.OTB_USERNAME
  val password = GlobalConfig.OTBConf.OTB_PASSWORD
  val port = GlobalConfig.OTBConf.OTB_PORT

  /**
   * The Moving Average is a simple data averaging algorithm.
   *
   * @param sc        Alias object for SparkContext.
   * @param input     The input image on which the features are computed.
   * @param channel   The selected channel index.
   * @param filter    Choice of edge feature, which contains gradient, sobel, touzi.
   * @return          Output image containing the edge features.
   */
  def otbEdgeExtraction(implicit sc: SparkContext,
                  input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                        channel : Int = 1,
                        filter: String = "gradient"):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()

    val outputTiffPath = algorithmData + "otbEdgeExtraction_" + time + ".tif"
    val writePath = algorithmData + "otbEdgeExtraction_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"""conda activate otb;${algorithmCode}python otb_edgeextraction.py --input "$outputTiffPath" --channel $channel --filter "$filter" --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }

}
