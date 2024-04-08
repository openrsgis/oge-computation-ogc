package whu.edu.cn.util

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.raster.{CellValue, DoubleConstantNoDataCellType, IntCellType, MultibandTile, Raster}
import geotrellis.spark.store.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.ShapeFileUtil.readShp

import java.io._
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RDDTransformerUtil {

  var output:String = _

  /**
   * 将Raster RDD保存为Tiff文件
   */
  def saveRasterRDDToTif(input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), outputTiffPath: String): Unit = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    println("成功落地tif: " + outputTiffPath)
  }

  /**
   * 保存RasterRDD的元数据
   */
  def savaRasterMetaData(input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), outputMetaPath: String): Unit = {
    val fileOut = new FileOutputStream(outputMetaPath)
    val out = new ObjectOutputStream(fileOut)
    out.writeObject(input._2)
    out.close()
    fileOut.close()
    println("缓存栅格元数据成功: " + outputMetaPath)
  };

  /**
   * 读取tiff文件的元数据
   */
  def readRasterMetaData(metaDataPath: String): TileLayerMetadata[SpaceTimeKey] = {
    val fileIn = new FileInputStream(metaDataPath)
    val in = new ObjectInputStream(fileIn)
    val metadataRead = in.readObject().asInstanceOf[TileLayerMetadata[SpaceTimeKey]]
    metadataRead
  }

  /**
   * 基于Tiff文件创建RDD，需要提供RDD的信息
   */
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
    (tiledOut, metaData)
  }

  /**
   * 基于元数据从tiff文件中构建RDD，需要提供影像的元数据信息
   */
  def makeRasterRDDFromTifBasedMeta(sc: SparkContext, myMetaData: TileLayerMetadata[SpaceTimeKey],
                           sourceTiffpath: String) = {
    val hadoopPath = "file://" + sourceTiffpath
    val inputRdd = sc.hadoopMultibandGeoTiffRDD(new Path(hadoopPath))
    val layout = myMetaData.layout
    var srcCellType = myMetaData.cellType;
    if (srcCellType == null) {
      srcCellType = IntCellType
    }
    val tiled = inputRdd.tileToLayout(srcCellType, layout, NearestNeighbor)
    val srcLayout = myMetaData.layout
    val srcExtent = myMetaData.extent
    val srcCrs = myMetaData.crs
    val cellType = myMetaData.cellType
    val srcBounds = myMetaData.bounds
    val dateKey = srcBounds.get.minKey.temporalKey
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, dateKey),
      SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, dateKey))
    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, dateKey), ListBuffer("B1","B2","B3","B4","B5","B6","B7","B8","B9").take(t._2.bandCount)), t._2)
    })
    println("成功读取tif: " + sourceTiffpath)
    println((tiledOut, metaData))
    (tiledOut, metaData)
  }

  /**
   * 基于元数据从tiff文件中构建RDD，不提供相关元数据信息，系统自定义地进行构建
   */
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
      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), ListBuffer("B")), t._2)
    })
    println("成功读取tif: " + sourceTiffpath)
    println((tiledOut, metaData))
    (tiledOut, metaData)
  }

  def saveFeatureRDDToShp(input: RDD[(String, (Geometry, mutable.Map[String, Any]))], outputShpPath: String): Unit = {
    val data = input.map(t => {
      t._2._2 + (ShapeFileUtil.DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava

    val geomClass = input.first()._2._1.getClass
    ShapeFileUtil.createShp(outputShpPath, "utf-8", geomClass, data)
    println("成功落地shp")
  }

  def makeFeatureRDDFromShp(sc: SparkContext, sourceShpPath: String) = {
    val featureRDD = readShp(sc, sourceShpPath, "utf-8")
    println("成功读取shp")
    featureRDD
  }

  def demo(data: Array[String]): Unit = {
    val pb = new ProcessBuilder("D:\\python\\python310\\python.exe", "E:\\oge\\sat\\TopographicCorrectionT.py")
    pb.redirectInput(ProcessBuilder.Redirect.PIPE)
    pb.redirectOutput(ProcessBuilder.Redirect.PIPE)
    pb.redirectError(ProcessBuilder.Redirect.PIPE)
    val process = pb.start


    // 获取子进程的输入输出流
    val inputStream = process.getInputStream
    val outputStream = process.getOutputStream
    val writer = new PrintWriter(new OutputStreamWriter(outputStream), true)
    for (st <- data) {
      writer.println(st)
    }
    writer.close()
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    val errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    var line = errorReader.readLine()
    while (line != null) {
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

}
