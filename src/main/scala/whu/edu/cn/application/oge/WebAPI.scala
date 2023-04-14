package whu.edu.cn.application.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffReader, SinglebandGeoTiff}
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleCellType, DoubleConstantNoDataCellType, MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.math.min
import whu.edu.cn.application.oge.HttpRequest.{sendGet, sendPost, writeTIFF}
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.ogc.entity.process.CoverageMediaType

import java.text.SimpleDateFormat

object WebAPI {
  def main(args: Array[String]): Unit = {
  }

  /**
   * RDD转tif
   * @param input (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]))
   * @return url of the tif
   */
  def tileRDD2Tiff(input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), coverageType:String) : String = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val coverageTypeEnum = CoverageMediaType.valueOf(coverageType)
    coverageTypeEnum match{
      case CoverageMediaType.GEOTIFF => {
        val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
        GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
        // TODO 这里应该返回一个href
        outputTiffPath
      }
      case CoverageMediaType.PNG => {
        // TODO 这里将RDD转换为PNG
        null
      }
      case CoverageMediaType.BINARY =>{
        // TODO 这里将RDD转换为Binary
        null
      }
    }

  }
// TODO geotiff转RDD
  def tiff2RDD(implicit sc: SparkContext, tifPath: String, coverageType:String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageTypeEnum = CoverageMediaType.valueOf(coverageType)
    writeTIFF(tifPath, "E:\\LaoK\\data2\\test.tif")
    coverageTypeEnum match{
      case CoverageMediaType.GEOTIFF => {
        // TODO geotiff转RDD
        null
      }
      case CoverageMediaType.PNG => {
        // TODO png转RDD
        null
      }
      case CoverageMediaType.BINARY=>{
        // TODO Binary 转RDD
        null
      }
    }
    null
//    val inputRdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(tifPath))(sc)
//    val tiled = inputRdd.tileToLayout(DoubleCellType, layout, Bilinear)
//    val srcLayout = input._2.layout
//    val srcExtent = input._2.extent
//    val srcCrs = input._2.crs
//    val srcBounds = input._2.bounds
//    val now = "1000-01-01 00:00:00"
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = sdf.parse(now).getTime
//    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
//    val metaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, newBounds)
//    val tiledOut = tiled.map(t => {
//      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "Slope"), t._2)
//    })
//    (tiledOut, metaData)
  }

  //卢宾宾老师算子
  def gwr(): Unit = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("data", "http://125.220.153.22:8027/test_data/gwr-example.shp")
    bodyChildren.put("attachment", Array("http://125.220.153.22:8027/test_data/gwr-example.shx", "http://125.220.153.22:8027/test_data/gwr-example.dbf"))
    body.put("f", bodyChildren)
    body.put("d", "y")
    body.put("i", "x1,x2,x3")
    body.put("k", "bisquare")
    body.put("t", "true")
    body.put("s", "true")
    body.put("a", "CV")
    body.put("r", "result.shp")
    body.put("h", "result.txt")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/gwr", param)
    println(s)
  }
  //陈玉敏老师算子
  def slope(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), Z_factor: Double): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "slope.tif")
    body.put("Z_factor", Z_factor)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/slope", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "Slope"), t._2)
    })
    (tiledOut, metaData)
  }

  def aspect(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), Z_factor: Double): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "aspect.tif")
    body.put("Z_factor", Z_factor)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/aspect", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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

  def hillShade(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), Z_factor: Double, Azimuth: Double, Vertical_angle: Double): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "hillShade.tif")
    body.put("Z_factor", Z_factor)
    body.put("Azimuth", Azimuth)
    body.put("Vertical_angle", Vertical_angle)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/hillShade", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "HillShade"), t._2)
    })
    (tiledOut, metaData)
  }

  def relief(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), Z_factor: Double): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "relief.tif")
    body.put("Z_factor", Z_factor)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/relief", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "Relief"), t._2)
    })
    (tiledOut, metaData)
  }

  def ruggednessIndex(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), Z_factor: Double): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "ruggednessIndex.tif")
    body.put("Z_factor", Z_factor)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/ruggednessIndex", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "RuggednessIndex"), t._2)
    })
    (tiledOut, metaData)
  }

  def cellBalance(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "cellBalance.tif")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/cellBalance", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "CellBalance"), t._2)
    })
    (tiledOut, metaData)
  }

  def flowAccumulationTD(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "flowAccumulationTD.tif")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/flowAccumulationTD", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "FlowAccumulationTD"), t._2)
    })
    (tiledOut, metaData)
  }

  def flowPathLength(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "flowPathLength.tif")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/flowPathLength", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "FlowPathLength"), t._2)
    })
    (tiledOut, metaData)
  }

  def slopeLength(implicit sc: SparkContext, input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    val time = System.currentTimeMillis()
    val outputTiffPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/webapi/webapi_" + time + ".tif"
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.26:8093/webapi/webapi_" + time + ".tif")
    body.put("input", bodyChildren)
    body.put("output", "slopeLength.tif")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/cym/slopeLength", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    val path = jsonObject.getJSONObject("result").getJSONObject("Result").getString("href")
    val writePath = "/home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
    writeTIFF(path, writePath)
    val hadoopPath = "file:///home/geocube/oge/oge-server/dag-boot/webapi/webapi_" + time + ".tif"
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
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "SlopeLength"), t._2)
    })
    (tiledOut, metaData)
  }

  //水文算子接口
  def hargreaves(inputTemperature: String, inputStation: String, startTime: String, endTime: String, timeStep: Long): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/"+ inputTemperature +".csv")
    body.put("inputTemperature", bodyChildren)
    val bodyChildren2: JSONObject = new JSONObject()
    bodyChildren2.put("href", "http://125.220.153.22:8027/test_data/" + inputStation + ".geojson")
    body.put("inputStation", bodyChildren2)
    body.put("startTime", startTime)
    body.put("endTime", endTime)
    body.put("timeStep", timeStep)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/hargreaves", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def topModel(inputPrecipEvapFile: String, inputTopoIndex: String, startTime: String, endTime: String, timeStep: Long, rate: Double,
               recession: Int, tMax: Int, iterception: Int, waterShedArea: Int): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/" + "TI_raster" + ".txt")
    body.put("inputTopoIndex", bodyChildren)
    val bodyChildren2: JSONObject = new JSONObject()
    bodyChildren2.put("href", "http://125.220.153.22:8027/test_data/" + "prec_pet" + ".csv")
    body.put("inputPrecipEvapFile", bodyChildren2)
    body.put("rate", rate)
    body.put("recession", recession)
    body.put("tMax", tMax)
    body.put("iterception", iterception)
    body.put("waterShedArea", waterShedArea)
    body.put("startTime", startTime)
    body.put("endTime", endTime)
    body.put("timeStep", timeStep)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/topmodel", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def SWMM5(input: String): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/" + "Example2-Post" + ".inp")
    body.put("inputInp", bodyChildren)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/swmm5", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  //定量遥感产品生产算子
  def virtualConstellation(): Unit = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801.tif")
    bodyChildren.put("attachment", Array("http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_MTD_TL.xml",
      "http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_MTD_MSIL1C.xml"))
    body.put("input", bodyChildren)
    body.put("bands", "B02,B03,B04,B08")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ard/virtualConstellation", param)
    println(s)
  }

  //湖南省定量遥感案例
  def calCrop(year: String, quarter: String, feature: String = null, sort: String, input: String = null): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", year)
    body.put("Quarter", quarter)
    body.put("Sort", sort)
    body.put("Region", "changsha")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/crop", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def calVegIndex(quarter: String, year: String): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", year)
    body.put("Quarter", quarter)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVegIndex", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def calVegCoverage(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    body.put("Quarter", "2")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVegCoverage", param)
    println(s)
  }

  def calNPP(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    body.put("Quarter", "1")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calNPP", param)
    println(s)
  }

  def calVEI(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVEI", param)
    println(s)
  }
}
