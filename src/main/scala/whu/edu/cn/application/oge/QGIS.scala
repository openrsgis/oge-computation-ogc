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
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.tif"

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
    val hadoopPath = "file://" + writePath
    val inputRdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(hadoopPath))(sc)
    val tiled = inputRdd.tileToLayout(DoubleCellType, layout, Bilinear)
    val srcLayout = input._2.layout
    val srcExtent = input._2.extent
    val srcCrs = input._2.crs
    val srcBounds = input._2.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
    val metaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "Aspect"), t._2)
    })
    (tiledOut, metaData)
  }

  def aspect2(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), zFactor: Float): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    //RDD转换为TIF，QGIS可以直接使用
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.tif"

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
    val hadoopPath = "file://" + writePath
    val inputRdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(hadoopPath))(sc)
    val localLayoutScheme = FloatingLayoutScheme(256)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    val srcLayout = metadata.layout
    val srcExtent = metadata.extent
    val srcCrs = metadata.crs
    val srcBounds = metadata.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, date), SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, date))
    val metaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "Aspect"), t._2)
    })
    (tiledOut, metaData)
  }

  def randomPointsAlongLine(implicit sc: SparkContext, input: RDD[(String, (Geometry, Map[String, Any]))], pointsNumber: Int = 1, minDistance: Double = 0.0): RDD[(String, (Geometry, Map[String, Any]))] = {
    val data = input.map(t=>{
      t._2._2 + (ShapeUtil.DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/randomPointsAlongLine_" + time + ".shp"
    ShapeUtil.createShp(outputTiffPath, "utf-8", classOf[LineString], data)
    val writePath = "/home/geocube/oge/oge-server/dag-boot/qgis/algorithmData/aspect_" + time + "_out.shp"

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
    readShp(sc, writePath, "utf-8")
  }
}