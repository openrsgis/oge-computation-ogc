package whu.edu.cn.application.oge

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleCellType, Raster, Tile}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom._
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil._
import whu.edu.cn.util.SSHClientUtil._
import whu.edu.cn.util.ShapeUtil
import whu.edu.cn.util.ShapeUtil.readShp

import java.text.SimpleDateFormat
import scala.collection.mutable.Map
import scala.collection.JavaConverters._

object QGIS {
  def main(args: Array[String]): Unit = {

  }

  //输入输出都是TIF的
  def aspect(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), zFactor: Float): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    //RDD转换为TIF，QGIS可以直接使用
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + ".tif"
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    //如果不符合参数要求，抛出RuntimeException异常
    //throw RuntimeException("Args do not follow the rules.")

    //连接服务器，调用QGIS算子
    try {
      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
      val st = "conda activate qgis" +
        "\n" +
        "cd /home/geocube/oge/oge-server/dag-boot/" +
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

    //QGIS生成了一个TIF, 如果前后的分辨率、投影坐标系、范围都没变，可以直接复用这个代码
    makeRasterRDDFromTif(sc, input, writePath)
  }

  def aspect2(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), zFactor: Float): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    //RDD转换为TIF，QGIS可以直接使用
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + ".tif"
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)

    //如果不符合参数要求，抛出RuntimeException异常
    //throw RuntimeException("Arg does not follow the rule.")

    //连接服务器，调用QGIS算子
    try {
      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
      val st = "conda activate qgis" +
        "\n" +
        "cd /home/geocube/oge/oge-server/dag-boot/" +
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

    //如果变了
    makeChangedRasterRDDFromTif(sc, writePath)
  }

  def randomPointsAlongLine(implicit sc: SparkContext, input: RDD[(String, (Geometry, Map[String, Any]))], pointsNumber: Int = 1, minDistance: Double = 0.0): RDD[(String, (Geometry, Map[String, Any]))] = {
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/randomPointsAlongLine_" + time + ".shp"
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.shp"
    saveFeatureRDDToShp(input, outputTiffPath)

    //如果不符合参数要求，抛出RuntimeException异常
    //throw RuntimeException("Arg does not follow the rule.")

    //连接服务器，调用QGIS算子
    try {
      versouSshUtil("125.220.153.26", "geocube", "ypfamily608", 22)
      val st = "conda activate qgis" +
        "\n" +
        "cd /home/geocube/oge/oge-server/dag-boot/" +
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