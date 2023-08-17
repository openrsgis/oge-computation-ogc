package whu.edu.cn.util

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream, OutputStreamWriter, PrintWriter}

import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.raster.{DoubleCellType, MultibandTile, Raster, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.store.hadoop.{HadoopGeoTiffRDD, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, LineString}
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.ShapeFileUtil.readShp
import java.text.SimpleDateFormat
import java.time.ZoneOffset
import java.util
import java.io.BufferedReader
import java.io.InputStreamReader



import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

object RDDTransformerUtil {

  var output:String = _

  def saveRasterRDDToTif(input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), outputTiffPath: String): Unit = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    println("成功落地tif")
  }

  def demo(data: Array[String]):Unit={
    val pb = new ProcessBuilder("D:\\python\\python310\\python.exe", "E:\\oge\\sat\\TopographicCorrectionT.py")
    pb.redirectInput(ProcessBuilder.Redirect.PIPE)
    pb.redirectOutput(ProcessBuilder.Redirect.PIPE)
    pb.redirectError(ProcessBuilder.Redirect.PIPE)
    val process = pb.start



    // 获取子进程的输入输出流
    val inputStream = process.getInputStream
    val outputStream = process.getOutputStream
    val writer = new PrintWriter(new OutputStreamWriter(outputStream), true)
    for(st <- data){
      writer.println(st)
    }
    writer.close()
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    val errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    var line =errorReader.readLine()
    while (line !=null)
    {
      println(line)
      output = line
      line = errorReader.readLine()
    }
    process.destroy()
    // 等待子进程结束并检查退出代码
    val exitCode = process.waitFor
    System.out.println("Exited with error code " + exitCode)
    reader.close()
    return output
  }


  def makeRasterRDDFromTif(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                           sourceTiffpath: String) = {
    val hadoopPath = "file://" + sourceTiffpath
    val layout = input._2.layout
    val inputRdd = sc.hadoopMultibandGeoTiffRDD(new Path(hadoopPath))
    val tiled = inputRdd.tileToLayout(input._2.cellType, layout, Bilinear)
    val srcLayout = input._2.layout
    val srcExtent = input._2.extent
    val srcCrs = input._2.crs
    val cellType = input._2.cellType
    val srcBounds = input._2.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), ListBuffer("Grass")), t._2)
    })
    println("成功读取tif")
    (tiledOut, metaData)
  }



  def makeChangedRasterRDDFromTif(sc: SparkContext, sourceTiffpath: String) = {
    val hadoopPath = "file://" + sourceTiffpath
    val inputRdd = sc.hadoopMultibandGeoTiffRDD(new Path(hadoopPath))
    val localLayoutScheme = FloatingLayoutScheme(256)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    val cellType = metadata.cellType
    val srcLayout = metadata.layout
    val srcExtent = metadata.extent
    val srcCrs = metadata.crs
    val srcBounds = metadata.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, date), SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, date))
    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), ListBuffer("Aspect")), t._2)
    })


    println("成功读取tif")
    (tiledOut, metaData)
  }

  def saveFeatureRDDToShp(input: RDD[(String, (Geometry, mutable.Map[String, Any]))], outputShpPath: String): Unit = {
    val data = input.map(t => {
      t._2._2 + (ShapeFileUtil.DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava
    ShapeFileUtil.createShp(outputShpPath, "utf-8", classOf[LineString], data)
    println("成功落地shp")
  }

  def makeFeatureRDDFromShp(sc: SparkContext, sourceShpPath: String) = {
    val featureRDD = readShp(sc, sourceShpPath, "utf-8")
    println("成功读取shp")
    featureRDD
  }

}
