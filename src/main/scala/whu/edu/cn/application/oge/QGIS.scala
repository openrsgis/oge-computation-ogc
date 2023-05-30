package whu.edu.cn.application.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom._
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._

import scala.collection.mutable.Map

object QGIS {
  def main(args: Array[String]): Unit = {

  }

  /**
   * 计算坡向
   *
   * @param sc      Spark上下文环境
   * @param input   输入的RasterRDD
   * @param zFactor Z因子
   * @return 输入的RasterRDD
   */
  //输入输出都是TIF的
  //
  def aspect(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), zFactor: Double = 1.0): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    //RDD转换为TIF，QGIS可以直接使用
    val time = System.currentTimeMillis()
    // TODO 两个aspect要改
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + ".tif"
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    // TODO 参数必须检查
    // 1 默认值  在参数里面加 = XXX
    // 2 有枚举值限制
//    var zFactorInput: Double = null
//    zFactor match {
//      case 1.0 => zFactorInput = 1.0
//      case 2.0 => zFactorInput = 2.0
//      case 3.0 => zFactorInput = 3.0
//      case _ => zFactorInput = 1.0
//    }


    //连接服务器，调用QGIS算子
    // TODO 这里要改
    try {
      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
      val st = "conda activate qgis" +
        "\n" +
        "cd /home/geocube/oge/oge-server/dag-boot/qgis" +
        "\n" +
        "python algorithmCode/aspect.py" +
        " --in-layer \"" +
        outputTiffPath +
        "\" --z-factor " +
        zFactor +
        " --out-layer \"" +
        writePath +
        "\""
      System.out.println("st = " + st)
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    // QGIS生成了一个TIF, 如果前后的分辨率、投影坐标系、范围都没变，可以直接复用这个代码
    makeRasterRDDFromTif(sc, input, writePath)
    // 如果变了
    makeChangedRasterRDDFromTif(sc, writePath)
  }

  def randomPointsAlongLine(implicit sc: SparkContext, input: RDD[(String, (Geometry, Map[String, Any]))], pointsNumber: Int = 1, minDistance: Double = 0.0): RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/randomPointsAlongLine_" + time + ".shp"
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/randomPointsAlongLine_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)

    //如果不符合参数要求，抛出RuntimeException异常
    //throw RuntimeException("Arg does not follow the rule.")

    //连接服务器，调用QGIS算子
    try {
      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
      val st = "conda activate qgis" +
        "\n" +
        "cd /home/geocube/oge/oge-server/dag-boot/qgis" +
        "\n" +
        "python algorithmCode/aspect.py" +
        " --in-layer \"" +
        outputTiffPath +
        "\" --points-number " +
        pointsNumber +
        "\" --min-distance " +
        minDistance +
        " --out-layer \"" +
        writePath +
        "\""
      System.out.println("st = " + st)
      runCmd(st, "UTF-8")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeFeatureRDDFromShp(sc, writePath)
  }
}