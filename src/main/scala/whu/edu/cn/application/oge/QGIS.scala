package whu.edu.cn.application.oge

import geotrellis.layer.{Bounds, SpaceTimeKey, TileLayerMetadata}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{DoubleCellType, Raster, Tile}
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.withTilerMethods
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.SSHClientUtil._

import java.text.SimpleDateFormat

object QGIS {
  def main(args: Array[String]): Unit = {

  }

  def aspect(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), zFactor: Float): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
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


















}