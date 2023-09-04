package whu.edu.cn.oge

import geotrellis.layer.{Bounds, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ColorMap, ColorRamp, Raster, Tile}
import geotrellis.raster.render.{Exact, Png, RGBA}
import geotrellis.spark.{TileLayerRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import whu.edu.cn.geocube.application.conjoint.Overlap.overlappedGeoObjects
import whu.edu.cn.geocube.application.spetralindices.NDBI.ndbi
import whu.edu.cn.geocube.application.spetralindices.NDVI.ndvi
import whu.edu.cn.geocube.application.spetralindices.NDWI.{ndwi, ndwiRadar}
import whu.edu.cn.geocube.application.timeseries.WaterChangeDetection.waterChangeDetectionUsingNDWIProduct
import whu.edu.cn.geocube.core.cube.CubeRDD._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.vector.{FeatureRDD, GeoObject, GeoObjectRDD}
import whu.edu.cn.geocube.core.entity.BiDimensionKey.LocationTimeGenderKey
import whu.edu.cn.geocube.core.entity.{BiQueryParams, QueryParams}
import whu.edu.cn.geocube.core.io.Output.saveAsGeojson
import whu.edu.cn.geocube.core.tabular.query.DistributedQueryTabularRecords.getBITabulars
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.geocube.util.{PostgresqlService, TileUtil}

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.io.Source

object Cube {
  /**
   * 根据提供的数据列表、时间范围、空间范围生成RDD，用字典形式记录
   *
   * @param productList 需要生成Cube的产品列表
   * @param startTime   起始时间
   * @param endTime     结束时间
   * @param geom        空间范围
   * @return 返回字典类型 Map[String, Any] cube data map
   */
  def load(implicit sc: SparkContext, productList: String = null, dateTime: String = null, geom: String = null, bandList: String = null): mutable.Map[String, Any] = {
    val products = if(productList!=null) productList.replace("[", "").replace("]", "").split(",") else null
    val geomList = if(geom!=null) geom.replace("[", "").replace("]", "").split(",").map(t => {
      t.toDouble
    }) else null
    val dateTimeArray = if(dateTime!=null) dateTime.replace("[", "").replace("]", "").split(",") else null
    val startTime = if(dateTimeArray.length==2) dateTimeArray(0) else null
    val endTime = if(dateTimeArray.length==2) dateTimeArray(1) else null
    val bands = if(bandList!=null) bandList.replace("[", "").replace("]", "").split(",") else null
    val result = mutable.Map[String, Any]()
    for (i <- 0 until products.length) {
      if (products(i).contains("EO")) {
        val queryParams = new QueryParams
        queryParams.setCubeId("27")
        queryParams.setLevel("4000")
        queryParams.setRasterProductName(products(i))
        queryParams.setExtent(geomList(0), geomList(1), geomList(2), geomList(3))
        queryParams.setTime(startTime, endTime)
        queryParams.setMeasurements(bands)
        val rasterRdd: RasterRDD = getData(sc, queryParams)
        result += (products(i) -> rasterRdd)
      }
      else if (products(i).contains("Vector")) {
        val queryParams = new QueryParams
        queryParams.setCubeId("27")
        queryParams.setCRS("WGS84")
        queryParams.setVectorProductName(products(i))
        queryParams.setExtent(geomList(0), geomList(1), geomList(2), geomList(3))
        queryParams.setTime(startTime, endTime)
        val featureRdd: FeatureRDD = getData(sc, queryParams)
        result += (products(i) -> featureRdd)
      }
      else if (products(i).contains("Tabular")) {
        val tabularRdd: RDD[(LocationTimeGenderKey, Int)] = getBITabulars(sc, new BiQueryParams)
        result += (products(i) -> tabularRdd)
      }
    }
    result
  }

  /**
   * 计算输入产品名的NDWI，每一景影像都做
   *
   * @param input   cube data
   * @param product 计算数据产品名
   * @param name    输出结果名
   * @return cube data map
   */
  def NDWI(input: mutable.Map[String, Any], product: String, name: String): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val ndwiRdd: RasterRDD = ndwi(rasterRdd)
        input += (name -> ndwiRdd)
        input
      }
    }
  }

  /**
   * 计算输入产品名的NDVI，每一景影像都做
   *
   * @param input   cube data
   * @param product 计算数据产品名
   * @param name    输出结果名
   * @return cube data map
   */
  def NDVI(input: mutable.Map[String, Any], product: String, name: String): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val ndviRdd: RasterRDD = ndvi(rasterRdd)
        input += (name -> ndviRdd)
        input
      }
    }
  }

  /**
   * 计算输入产品名的NDBI，每一景影像都做
   *
   * @param input   cube data
   * @param product 计算数据产品名
   * @param name    输出结果名
   * @return cube data map
   */
  def NDBI(input: mutable.Map[String, Any], product: String, name: String): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val ndbiRdd: RasterRDD = ndbi(rasterRdd)
        input += (name -> ndbiRdd)
        input
      }
    }
  }

  def binarization(input: mutable.Map[String, Any], product: String, name: String, threshold: Double): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val binarizationRdd: RasterRDD = new RasterRDD(rasterRdd.rddPrev.map(t=>(t._1, t._2.mapDouble(pixel => {
          if (pixel > threshold) 255.0
          else if (pixel >= -1) 0.0
          else Double.NaN
        }))), rasterRdd.meta)
        input += (name -> binarizationRdd)
        input
      }
    }
  }

  /**
   * 根据指定的两个特定时间点的产品影像计算变化检测
   *
   * @param input        cube data
   * @param product      计算数据产品名
   * @param certainTimes 指定时间点
   * @param name         输出结果名
   * @return cube data map
   */
  def WaterChangeDetection(input: mutable.Map[String, Any], product: String, certainTimes: String, name: String): mutable.Map[String, Any] = {
    input(product) match {
      case rasterRdd: RasterRDD => {
        val certainTimesList = certainTimes.replace("[", "").replace("]", "").split(",")
        val changedRdd: RasterRDD = waterChangeDetectionUsingNDWIProduct(rasterRdd, certainTimesList)
        input += (name -> changedRdd)
        input
      }
    }
  }

  /**
   * 对栅格和矢量做叠置分析，输出矢量的筛选结果
   *
   * @param input  cube data
   * @param raster 栅格数据
   * @param vector 矢量数据
   * @param name   输出结果名字
   * @return cube data map
   */
  def OverlayAnalysis(input: mutable.Map[String, Any], rasterOrTabular: String, vector: String, name: String): mutable.Map[String, Any] = {
    input(rasterOrTabular) match {
      case changedRdd: RasterRDD => {
        input(vector) match {
          case featureRdd: FeatureRDD => {
            val gridLayerGeoObjectRdd: RDD[(SpatialKey, Iterable[GeoObject])] = featureRdd.map(x => (x._1.spatialKey, x._2))
            val affectedGeoObjectRdd: GeoObjectRDD = overlappedGeoObjects(changedRdd, gridLayerGeoObjectRdd).cache()
            input += (name -> affectedGeoObjectRdd)
            input
          }
        }
      }
      case tabularRdd: RDD[(LocationTimeGenderKey, Int)] => {
        input(vector) match {
          case affectedGeoObjectRdd: GeoObjectRDD => {
            val affectedGeoObjectArray: Array[GeoObject] = affectedGeoObjectRdd.collect()
            val affectedFeatures = affectedGeoObjectRdd.map(x => x.feature).collect()
            //            val affectedGeoObjectKeys: Array[String] = affectedGeoObjectArray.map(_.id)
            //            val locationKeys: Array[Int] = new PostgresqlService().getLocationKeysByFeatureIds(affectedGeoObjectKeys)
            //            val affectedTabularArray: Array[(LocationTimeGenderKey, Int)] = tabularRdd.filter(x => locationKeys.contains(x._1.locationKey)).collect()
            //            val affectedPopulations = affectedTabularArray.map(_._2).sum
            val affectedGeoNames: ArrayBuffer[String] = new ArrayBuffer[String]()
            affectedFeatures.foreach { features =>
              val affectedGeoName = features.getAttribute("geo_name").asInstanceOf[String]
              affectedGeoNames.append(affectedGeoName)
            }
            print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedGeoNames.length + " " + " are impacted including: ")
            affectedGeoNames.foreach(x => print(x + " "));
            println()
            //            println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- " + affectedPopulations + " people are affected")
            input += (name -> affectedGeoNames)
            input
          }
        }
      }
    }
  }

  /**
   *
   * @param input    input cube data map
   * @param products 要显示的产品列表
   * @return cube data map
   */
  def visualize(implicit sc: SparkContext, cube: mutable.Map[String, Any], products: String, fileName: String = null) = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val result = mutable.Map[String, Any]()
    val vectorCubeList = new ArrayBuffer[Map[String, Any]]()
    val rasterCubeList = new ArrayBuffer[Map[String, Any]]()
    val rasterList = new ArrayBuffer[Map[String, Any]]()
    val vectorList = new ArrayBuffer[Map[String, Any]]()
    val tableList = new ArrayBuffer[Map[String, Any]]()
    val executorOutputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogeoutput/"
    val productList = products.replace("[", "").replace("]", "").split(",")
    val time = System.currentTimeMillis();
    for (product <- productList) {
      cube(product) match {
        case affectedGeoObjectRdd: GeoObjectRDD => {
          //save vector
          val affectedFeatures = affectedGeoObjectRdd.map(x => x.feature).collect()
          val outputVectorPath = executorOutputDir + "oge_flooded_" + time + ".geojson"
          saveAsGeojson(affectedFeatures, outputVectorPath)
          vectorCubeList.append(Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_flooded_" + time + ".geojson")))
        }
        case changedRdd: RasterRDD => {
          if (product == "Change_Product") {
            val cellType = changedRdd.meta.tileLayerMetadata.cellType
            val srcLayout = changedRdd.meta.tileLayerMetadata.layout
            val srcExtent = changedRdd.meta.tileLayerMetadata.extent
            val srcCrs = changedRdd.meta.tileLayerMetadata.crs
            val srcBounds = Bounds(changedRdd.meta.tileLayerMetadata.bounds.get._1.spatialKey, changedRdd.meta.tileLayerMetadata.bounds.get._2.spatialKey)
            val changedMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, srcBounds)

            val changedStitchRdd: TileLayerRDD[SpatialKey] = ContextRDD(changedRdd.rddPrev.map(t => (t._1.spaceTimeKey.spatialKey, t._2)), changedMetaData)
            val stitched = changedStitchRdd.stitch()
            val extentRet = stitched.extent
            val outputRasterPath = executorOutputDir + "oge_flooded_" + time + ".png"
            rasterCubeList.append(Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_flooded_" + time + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
            stitched.tile.renderPng().write(outputRasterPath)
          }
          if (product == "Binarization_Product") {
            val ndwiRdd = changedRdd.rddPrev.groupBy(_._1.spaceTimeKey.instant)
            val ndwiInfo = ndwiRdd.map({x =>
              //stitch extent-series tiles of each time instant to pngs
              val metadata = changedRdd.meta.tileLayerMetadata
              val layout = metadata.layout
              val crs = metadata.crs
              val instant = x._1
              val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spaceTimeKey.spatialKey, ele._2))
              val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

              val colorRamp = ColorRamp(
                0xD76B27FF,
                0xE68F2DFF,
                0xF9B737FF,
                0xF5CF7DFF,
                0xF0E7BBFF,
                0xEDECEAFF,
                0xC8E1E7FF,
                0xADD8EAFF,
                0x7FB8D4FF,
                0x4EA3C8FF,
                0x2586ABFF
              )
              var accum = 0.0
              tileLayerArray.foreach { x =>
                val tile = x._2
                tile.foreachDouble { x =>
                  if (x == 255.0) accum += x
                }
              }
              val sdf = new SimpleDateFormat("yyyy-MM-dd");
              val str = sdf.format(instant);
              System.out.println(str);
              //val outputRasterPath = executorOutputDir + "ndwi_" + str + ".png"
              val extentRet = stitched.extent
              //rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "ndwi_" + str + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
              val stitchedPng:Png = stitched.tile.renderPng(colorRamp)
              //stitched.tile.renderPng(colorRamp).write(outputRasterPath)
              //generate ndwi thematic product
              val outputTiffPath = executorOutputDir  + "_ndwi_" + instant + ".TIF"
              GeoTiff(stitched, crs).write(outputTiffPath)
              (stitchedPng, str, extentRet)
            })
            ndwiInfo.collect().foreach(t=>{
              val str = t._2
              val extentRet = t._3
              val outputRasterPath = executorOutputDir + "ndwi_" + str + "_" + time + ".png"
              t._1.write(outputRasterPath)
              rasterCubeList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "ndwi_" + str + "_" + time + ".png"), "extent" -> Array(extentRet.ymin, extentRet.xmin, extentRet.ymax, extentRet.xmax)))
            })
          }
          if (product == "LC08_L1TP_ARD_EO") {
            val ndwiRdd = changedRdd.rddPrev.groupBy(_._1.spaceTimeKey.instant)
            val ndwiInfo = ndwiRdd.map({x =>
              //stitch extent-series tiles of each time instant to pngs
              val metadata = changedRdd.meta.tileLayerMetadata
              val layout = metadata.layout
              val crs = metadata.crs
              val instant = x._1
              val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spaceTimeKey.spatialKey, ele._2))
              val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

              val sdf = new SimpleDateFormat("yyyy-MM-dd");
              val str = sdf.format(instant);
              System.out.println(str);
              val outputTiffPath = executorOutputDir + str + "_ndwi_" + instant + ".TIF"
              GeoTiff(stitched, crs).write(outputTiffPath)
              str
            })
            ndwiInfo.count()
          }
        }
      }
    }
    result += ("vectorCube" -> vectorCubeList)
    result += ("rasterCube" -> rasterCubeList)
    result += ("raster" -> rasterCubeList)
    result += ("vector" -> rasterCubeList)
    result += ("table" -> rasterCubeList)
    val jsonStr: String = write(result)
    val writeFile = new File(fileName)
    val writer = new BufferedWriter(new FileWriter(writeFile))
    writer.write(jsonStr)
    writer.close()
  }
}