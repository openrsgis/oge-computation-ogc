package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import redis.clients.jedis.Jedis
import whu.edu.cn.application.oge.ImageTrigger.{fileName, originTaskID, rdd_list_image, workID, workTaskJSON, zIndexStrArray}
import whu.edu.cn.application.oge.WebAPI._
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.jsonparser.{JsonToArg, JsonToArgLocal}
import whu.edu.cn.util.{JedisConnectionFactory, ZCurveUtil}

import java.io.File
import scala.collection.mutable.Map
import scala.collection.immutable
import scala.io.Source
import scala.language.postfixOps

object Trigger {
  var rdd_list_image: Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = Map.empty[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])]
  var rdd_list_image_waitingForMosaic: Map[String, RDD[RawTile]] = Map.empty[String, RDD[RawTile]]
  var rdd_list_table: Map[String, String] = Map.empty[String, String]
  var rdd_list_feature_API: Map[String, String] = Map.empty[String, String]
  var list_kernel: Map[String, Kernel] = Map.empty[String, Kernel]
  var rdd_list_feature: Map[String, Any] = Map.empty[String, Any]
  var imageLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]
  var filterEqual: Map[String, (String, String)] = Map.empty[String, (String, String)]
  var filterAnd: Map[String, Array[String]] = Map.empty[String, Array[String]]

  var rdd_list_cube: Map[String, Map[String, Any]] = Map.empty[String, Map[String, Any]]
  var cubeLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]

  var level: Int = _
  var windowRange: String = _
  var layerID: Int = 0
  var fileName: String = _
  var oorB: Int = _

  def argOrNot(args: Map[String, String], name: String): String = {
    if (args.contains(name)) {
      args(name)
    }
    else {
      null
    }
  }

  def func(implicit sc: SparkContext, UUID: String, name: String, args: Map[String, String]): Unit = {
    name match {


      // ImageTrigger
      case "classificationDLCUG" => {
        rdd_list_image += (
          UUID -> Image.classificationDLCUG(
            image1 = rdd_list_image(args("coverage1")),
            image2 = rdd_list_image(args("coverage2"))
          )
          )
      }


      case "map" => {
        level = argOrNot(args, "level").toInt
        windowRange = argOrNot(args, "windowRange")
      }
      case "Service.getCoverageCollection" => {
        imageLoad += (UUID -> (argOrNot(args, "productID"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
      }
      case "Service.getCoverage" => {
        imageLoad += (UUID -> (argOrNot(args, "productID"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
      }
      case "Filter.equals" => {
        filterEqual += (UUID -> (argOrNot(args, "leftField"), argOrNot(args, "rightValue")))
      }
      case "Filter.and" => {
        filterAnd += (UUID -> argOrNot(args, "filters").replace("[", "").replace("]", "").split(","))
      }
      case "CoverageCollection.subCollection" => {
        var crs: String = null
        var measurementName: String = null
        val filter = argOrNot(args, "filter")
        if (filterAnd.contains(filter)) {
          val filters = filterAnd(filter)
          for (i <- filters.indices) {
            if ("crs".equals(filterEqual(filters(i))._1)) {
              crs = filterEqual(filters(i))._2
            }
            if ("measurementName".equals(filterEqual(filters(i))._1)) {
              measurementName = filterEqual(filters(i))._2
            }
          }
        }
        else if (filterEqual.contains(filter)) {
          if ("crs".equals(filterEqual(filter)._1)) {
            crs = filterEqual(filter)._2
          }
          if ("measurementName".equals(filterEqual(filter)._1)) {
            measurementName = filterEqual(filter)._2
          }
        }
        if (oorB == 0) {
          val loadInit = Image.load(sc, productName = imageLoad(argOrNot(args, "input"))._1, measurementName = measurementName, dateTime = imageLoad(argOrNot(args, "input"))._2,
            geom = windowRange, geom2 = imageLoad(argOrNot(args, "input"))._3, crs = crs, level = level)
          rdd_list_image += (UUID -> loadInit._1)
          rdd_list_image_waitingForMosaic += (UUID -> loadInit._2)
        }
        else {
          val loadInit = Image.load(sc, productName = imageLoad(argOrNot(args, "input"))._1, measurementName = measurementName, dateTime = imageLoad(argOrNot(args, "input"))._2,
            geom = imageLoad(argOrNot(args, "input"))._3, crs = crs, level = -1)
          rdd_list_image += (UUID -> loadInit._1)
          rdd_list_image_waitingForMosaic += (UUID -> loadInit._2)
        }
      }
      case "CoverageCollection.mosaic" => {
        rdd_list_image += (UUID -> Image.mosaic(sc, tileRDDReP = rdd_list_image_waitingForMosaic(argOrNot(args, "coverageCollection")), method = argOrNot(args, "method")))
      }
      case "Service.getTable" => {
        rdd_list_table += (UUID -> argOrNot(args, "productID"))
      }
      case "Service.getFeatureCollection" => {
        rdd_list_feature_API += (UUID -> argOrNot(args, "productID"))
      }

      //Algorithm
      case "Algorithm.hargreaves" => {
        rdd_list_table += (UUID -> hargreaves(inputTemperature = rdd_list_table(argOrNot(args, "inputTemperature")), inputStation = rdd_list_feature_API(argOrNot(args, "inputStation")),
          startTime = argOrNot(args, "startTime"), endTime = argOrNot(args, "endTime"), timeStep = argOrNot(args, "timeStep").toLong))
      }
      case "Algorithm.topmodel" => {
        rdd_list_table += (UUID -> topModel(inputPrecipEvapFile = rdd_list_table(argOrNot(args, "inputPrecipEvapFile")), inputTopoIndex = rdd_list_table(argOrNot(args, "inputTopoIndex")),
          startTime = argOrNot(args, "startTime"), endTime = argOrNot(args, "endTime"), timeStep = argOrNot(args, "timeStep").toLong,
          rate = argOrNot(args, "rate").toDouble, recession = argOrNot(args, "recession").toInt, iterception = argOrNot(args, "iterception").toInt,
          waterShedArea = argOrNot(args, "waterShedArea").toInt, tMax = argOrNot(args, "tMax").toInt))
      }
      case "Algorithm.swmm" => {
        rdd_list_table += (UUID -> SWMM5(input = rdd_list_table(argOrNot(args, "input"))))
      }

      //Table
      case "Table.getDownloadUrl" => {
        Table.getDownloadUrl(url = rdd_list_table(argOrNot(args, "input")), fileName = fileName)
      }
      case "Table.addStyles" => {
        Table.getDownloadUrl(url = rdd_list_table(argOrNot(args, "input")), fileName = fileName)
      }

      //Coverage
      case "Coverage.subtract" => {
        rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.add" => {
        rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.divide" => {
        rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.multiply" => {
        rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.binarization" => {
        rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverage")), threshold = args("threshold").toInt))
      }
      case "Coverage.and" => {
        rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.or" => {
        rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.not" => {
        rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.ceil" => {
        rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.floor" => {
        rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.sin" => {
        rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.cos" => {
        rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.sinh" => {
        rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.cosh" => {
        rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.asin" => {
        rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.acos" => {
        rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.atan" => {
        rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.atan2" => {
        rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.eq" => {
        rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.gt" => {
        rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.gte" => {
        rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.addBands" => {
        val names: List[String] = List("B3")
        rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2")), names = names))
      }
      case "Coverage.slope" => {
        rdd_list_image += (UUID -> slope(sc, input = rdd_list_image(args("input")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "Coverage.aspect" => {
        rdd_list_image += (UUID -> aspect(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "Coverage.hillShade" => {
        rdd_list_image += (UUID -> hillShade(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble, Azimuth = argOrNot(args, "Azimuth").toDouble, Vertical_angle = argOrNot(args, "Vertical_angle").toDouble))
      }
      case "Coverage.relief" => {
        rdd_list_image += (UUID -> relief(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "Coverage.ruggednessIndex" => {
        rdd_list_image += (UUID -> ruggednessIndex(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "Coverage.cellBalance" => {
        rdd_list_image += (UUID -> cellBalance(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "Coverage.flowAccumulationTD" => {
        rdd_list_image += (UUID -> flowAccumulationTD(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "Coverage.flowPathLength" => {
        rdd_list_image += (UUID -> flowPathLength(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "Coverage.slopeLength" => {
        rdd_list_image += (UUID -> slopeLength(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "Coverage.calCrop" => {
        calCrop(year = argOrNot(args, "year"), quarter = argOrNot(args, "quarter"), sort = argOrNot(args, "sort"))
      }
      case "Coverage.bandNames" => {
        val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverage")))
        println("******************test bandNames***********************")
        println(bandNames)
        println(bandNames.length)
      }
      case "Kernel.fixed" => {
        val kernel = Kernel.fixed(weights = args("weights"))
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.square" => {
        val kernel = Kernel.square(radius = args("radius").toInt, normalize = args("normalize").toBoolean, value = args("value").toDouble)
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.prewitt" => {
        val kernel = Kernel.prewitt(axis = args("axis"))
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.kirsch" => {
        val kernel = Kernel.kirsch(axis = args("axis"))
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.sobel" => {
        val kernel = Kernel.sobel(axis = args("axis"))
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }

      case "Kernel.laplacian4" => {
        val kernel = Kernel.laplacian4()
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.laplacian8" => {
        val kernel = Kernel.laplacian8()
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.laplacian8" => {
        val kernel = Kernel.laplacian8()
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Kernel.add" => {
        val kernel = Kernel.add(kernel1 = list_kernel(args("kernel1")), kernel2 = list_kernel(args("kernel2")))
        list_kernel += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      }
      case "Coverage.abs" => {
        rdd_list_image += (UUID -> Image.abs(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.neq" => {
        rdd_list_image += (UUID -> Image.neq(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.signum" => {
        rdd_list_image += (UUID -> Image.signum(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.bandTypes" => {
        val bandTypes: immutable.Map[String, String] = Image.bandTypes(image = rdd_list_image(args("coverage")))
        println(bandTypes)
        println(bandTypes.size)
      }
      case "Coverage.rename" => {
        rdd_list_image += (UUID -> Image.rename(image = rdd_list_image(args("coverage")), name = args("name")))
      }
      case "Coverage.pow" => {
        rdd_list_image += (UUID -> Image.pow(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.mini" => {
        rdd_list_image += (UUID -> Image.mini(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.maxi" => {
        rdd_list_image += (UUID -> Image.maxi(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
      }
      case "Coverage.focalMean" => {
        rdd_list_image += (UUID -> Image.focalMean(image = rdd_list_image(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      }
      case "Coverage.focalMedian" => {
        rdd_list_image += (UUID -> Image.focalMedian(image = rdd_list_image(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      }
      case "Coverage.focalMin" => {
        rdd_list_image += (UUID -> Image.focalMin(image = rdd_list_image(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      }
      case "Coverage.focalMax" => {
        rdd_list_image += (UUID -> Image.focalMax(image = rdd_list_image(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      }
      case "Coverage.focalMode" => {
        rdd_list_image += (UUID -> Image.focalMode(image = rdd_list_image(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      }
      case "Coverage.convolve" => {
        rdd_list_image += (UUID -> Image.convolve(image = rdd_list_image(args("coverage")), kernel = list_kernel(args("kernel"))))
      }
      case "Coverage.projection" => {
        val projection: String = Image.projection(image = rdd_list_image(args("coverage")))
        println(projection)
      }
      case "Coverage.histogram" => {
        val hist = Image.histogram(image = rdd_list_image(args("coverage")))
        println(hist)
      }
      case "Coverage.resample" => {
        rdd_list_image += (UUID -> Image.resample(image = rdd_list_image(args("coverage")), level = args("level").toInt, mode = args("mode")))
      }
      case "Coverage.gradient" => {
        rdd_list_image += (UUID -> Image.gradient(image = rdd_list_image(args("coverage"))))
      }
      case "Coverage.clamp" => {
        rdd_list_image += (UUID -> Image.clamp(image = rdd_list_image(args("coverage")), low = args("low").toInt, high = args("high").toInt))
      }
      case "Coverage.rgbToHsv" => {
        rdd_list_image += (UUID -> Image.rgbToHsv(imageRed = rdd_list_image(args("coverageRed")), imageGreen = rdd_list_image(args("coverageGreen")), imageBlue = rdd_list_image(args("coverageBlue"))))
      }
      case "Coverage.hsvToRgb" => {
        rdd_list_image += (UUID -> Image.hsvToRgb(imageHue = rdd_list_image(args("coverageHue")), imageSaturation = rdd_list_image(args("coverageSaturation")), imageValue = rdd_list_image(args("coverageValue"))))
      }

      case "Coverage.addStyles" => {
        if (oorB == 0) {
          Image.visualizeOnTheFly(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
            method = argOrNot(args, "method"), palette = argOrNot(args, "palette"), layerID = layerID, fileName = fileName, level = level)
          layerID = layerID + 1
        }
        else {
          Image.visualizeBatch(sc, image = rdd_list_image(args("input")), layerID = layerID, fileName = fileName)
          layerID = layerID + 1
        }
      }

      //CoverageCollection
      case "CoverageCollection.subtract" => {
        rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.add" => {
        rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.divide" => {
        rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.multiply" => {
        rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.binarization" => {
        rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverageCollection")), threshold = args("threshold").toInt))
      }
      case "CoverageCollection.and" => {
        rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.or" => {
        rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.not" => {
        rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.ceil" => {
        rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.floor" => {
        rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.sin" => {
        rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.cos" => {
        rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.sinh" => {
        rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.cosh" => {
        rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.asin" => {
        rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.acos" => {
        rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.atan" => {
        rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.atan2" => {
        rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.eq" => {
        rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.gt" => {
        rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.gte" => {
        rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
      }
      case "CoverageCollection.addBands" => {
        val names: List[String] = List("B3")
        rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2")), names = names))
      }
      case "CoverageCollection.slope" => {
        rdd_list_image += (UUID -> slope(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "CoverageCollection.aspect" => {
        rdd_list_image += (UUID -> aspect(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "CoverageCollection.hillShade" => {
        rdd_list_image += (UUID -> hillShade(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble, Azimuth = argOrNot(args, "Azimuth").toDouble, Vertical_angle = argOrNot(args, "Vertical_angle").toDouble))
      }
      case "CoverageCollection.relief" => {
        rdd_list_image += (UUID -> relief(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "CoverageCollection.ruggednessIndex" => {
        rdd_list_image += (UUID -> ruggednessIndex(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
      }
      case "CoverageCollection.cellBalance" => {
        rdd_list_image += (UUID -> cellBalance(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.flowAccumulationTD" => {
        rdd_list_image += (UUID -> flowAccumulationTD(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.flowPathLength" => {
        rdd_list_image += (UUID -> flowPathLength(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.slopeLength" => {
        rdd_list_image += (UUID -> slopeLength(sc, input = rdd_list_image(args("coverageCollection"))))
      }
      case "CoverageCollection.calCrop" => {
        calCrop(year = argOrNot(args, "year"), quarter = argOrNot(args, "quarter"), sort = argOrNot(args, "sort"))
      }
      case "CoverageCollection.PAP" => {
        rdd_list_image += (UUID -> Image.PAP(image = rdd_list_image(args("coverageCollection")), time = argOrNot(args, "time"), n = argOrNot(args, "n").toInt))
      }
      case "CoverageCollection.bandNames" => {
        val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverageCollection")))
        println("******************test bandNames***********************")
        println(bandNames)
        println(bandNames.length)
      }
      case "CoverageCollection.addStyles" => {
        if (oorB == 0) {
          Image.visualizeOnTheFly(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
            method = argOrNot(args, "method"), palette = argOrNot(args, "palette"), layerID = layerID, fileName = fileName, level = level)
          layerID = layerID + 1
        }
        else {
          Image.visualizeBatch(sc, image = rdd_list_image(args("input")), layerID = layerID, fileName = fileName)
          layerID = layerID + 1
        }
      }

      //Feature
      case "Feature.load" => {
        var dateTime = argOrNot(args, "dateTime")
        if (dateTime == "null")
          dateTime = null
        println("dateTime:" + dateTime)
        if (dateTime != null) {
          if (argOrNot(args, "crs") != null)
            rdd_list_feature += (UUID -> Feature.load(sc, args("productName"), args("dateTime"), args("crs")))
          else
            rdd_list_feature += (UUID -> Feature.load(sc, args("productName"), args("dateTime")))
        }
        else
          rdd_list_feature += (UUID -> Feature.load(sc, args("productName")))
      }
      case "Feature.point" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.point(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.point(sc, args("coors"), args("properties")))
      }
      case "Feature.lineString" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.lineString(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.lineString(sc, args("coors"), args("properties")))
      }
      case "Feature.linearRing" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.linearRing(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.linearRing(sc, args("coors"), args("properties")))
      }
      case "Feature.polygon" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.polygon(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.polygon(sc, args("coors"), args("properties")))
      }
      case "Feature.multiPoint" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.multiPoint(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.multiPoint(sc, args("coors"), args("properties")))
      }
      case "Feature.multiLineString" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.multiLineString(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.multiLineString(sc, args("coors"), args("properties")))
      }
      case "Feature.multiPolygon" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.multiPolygon(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.multiPolygon(sc, args("coors"), args("properties")))
      }
      case "Feature.geometry" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.geometry(sc, args("coors"), args("properties"), args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.geometry(sc, args("coors"), args("properties")))
      }
      case "Feature.area" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.area(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.area(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.bounds" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.bounds(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.bounds(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.centroid" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.centroid(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.centroid(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.buffer" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.buffer(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("distance").toDouble, args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.buffer(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("distance").toDouble))
      }
      case "Feature.convexHull" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.convexHull(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.convexHull(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.coordinates" => {
        rdd_list_feature += (UUID -> Feature.coordinates(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.reproject" => {
        rdd_list_feature += (UUID -> Feature.reproject(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("tarCrsCode")))
      }
      case "Feature.isUnbounded" => {
        rdd_list_feature += (UUID -> Feature.isUnbounded(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.getType" => {
        rdd_list_feature += (UUID -> Feature.getType(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.projection" => {
        rdd_list_feature += (UUID -> Feature.projection(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.toGeoJSONString" => {
        rdd_list_feature += (UUID -> Feature.toGeoJSONString(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.getLength" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.getLength(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.getLength(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.geometries" => {
        rdd_list_feature += (UUID -> Feature.geometries(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.dissolve" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.dissolve(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.dissolve(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.contains" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.contains(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.contains(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.containedIn" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.containedIn(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.containedIn(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.disjoint" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.disjoint(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.disjoint(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.distance" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.distance(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.distance(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.difference" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.difference(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.difference(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.intersection" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.intersection(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.intersection(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.intersects" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.intersects(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.intersects(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.symmetricDifference" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.symmetricDifference(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.symmetricDifference(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.union" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.union(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.union(sc, rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.withDistance" => {
        if (argOrNot(args, "crs") != null)
          rdd_list_feature += (UUID -> Feature.withDistance(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("distance").toDouble, args("crs")))
        else
          rdd_list_feature += (UUID -> Feature.withDistance(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
            rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("distance").toDouble))
      }
      case "Feature.copyProperties" => {
        val propertyList = args("properties").replace("[", "").replace("]", "")
          .replace("\"", "").split(",").toList
        rdd_list_feature += (UUID -> Feature.copyProperties(rdd_list_feature(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
          rdd_list_feature(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], propertyList))
      }
      case "Feature.get" => {
        rdd_list_feature += (UUID -> Feature.get(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("property")))
      }
      case "Feature.getNumber" => {
        rdd_list_feature += (UUID -> Feature.getNumber(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("property")))
      }
      case "Feature.getString" => {
        rdd_list_feature += (UUID -> Feature.getString(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("property")))
      }
      case "Feature.getArray" => {
        rdd_list_feature += (UUID -> Feature.getArray(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("property")))
      }
      case "Feature.propertyNames" => {
        rdd_list_feature += (UUID -> Feature.propertyNames(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.set" => {
        rdd_list_feature += (UUID -> Feature.set(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("property")))
      }
      case "Feature.setGeometry" => {
        rdd_list_feature += (UUID -> Feature.setGeometry(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
          rdd_list_feature(args("geometry")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.setGeometry" => {
        rdd_list_feature += (UUID -> Feature.setGeometry(rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
          rdd_list_feature(args("geometry")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }
      case "Feature.inverseDistanceWeighted" => {
        rdd_list_image += (UUID -> Feature.inverseDistanceWeighted(sc, rdd_list_feature(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],
          args("propertyName"), rdd_list_feature(args("maskGeom")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]]))
      }

      //Cube
      case "Service.getCollections" => {
        cubeLoad += (UUID -> (argOrNot(args, "productIDs"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
      }
      case "Collections.toCube" => {
        rdd_list_cube += (UUID -> Cube.load(sc, productList = cubeLoad(args("input"))._1, dateTime = cubeLoad(args("input"))._2, geom = cubeLoad(args("input"))._3, bandList = argOrNot(args, "bands")))
      }
      case "Cube.NDWI" => {
        rdd_list_cube += (UUID -> Cube.NDWI(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"), name = argOrNot(args, "name")))
      }
      case "Cube.binarization" => {
        rdd_list_cube += (UUID -> Cube.binarization(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"), name = argOrNot(args, "name"),
          threshold = argOrNot(args, "threshold").toDouble))
      }
      case "Cube.subtract" => {
        rdd_list_cube += (UUID -> Cube.WaterChangeDetection(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"),
          certainTimes = argOrNot(args, "timeList"), name = argOrNot(args, "name")))
      }
      case "Cube.overlayAnalysis" => {
        rdd_list_cube += (UUID -> Cube.OverlayAnalysis(input = rdd_list_cube(args("input")), rasterOrTabular = argOrNot(args, "raster"), vector = argOrNot(args, "vector"), name = argOrNot(args, "name")))
      }
      case "Cube.addStyles" => {
        Cube.visualize(sc, cube = rdd_list_cube(args("cube")), products = argOrNot(args, "products"))
      }
    }
  }

  def lamda(implicit sc: SparkContext, list: List[Tuple3[String, String, Map[String, String]]]) = {
    for (i <- list.indices) {
      func(sc, list(i)._1, list(i)._2, list(i)._3)
    }
  }


  def main(args: Array[String]): Unit = {
    workID = "1234567890123" // 告知boot业务编号，应当由命令行参数获取，on-the-fly


    workTaskJSON = {
      val fileSource = Source.fromFile(
        "src/main/scala/whu/edu/cn/application/oge/modis.json")
      fileName = "datas/out.txt" // TODO
      val line: String = fileSource.mkString
      fileSource.close()
      line
    } // 任务要用的 JSON,应当由命令行参数获取


    originTaskID = "0000000000000"
    // 点击整个run的唯一标识，来自boot

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    runMain(sc, workTaskJSON, workID, originTaskID)


    Thread.sleep(1000000)

    sc.stop()
  }

  def runMain(implicit sc: SparkContext,
              curWorkTaskJSON: String,
              curWorkID: String,
              curOriginTaskID: String): Unit = {

    /* sc,workTaskJson,workID,originTaskID */

    workTaskJSON = curWorkTaskJSON
    workID = curWorkID
    originTaskID = curOriginTaskID

    val time1: Long = System.currentTimeMillis()


    val jsonObject = JSON.parseObject(workTaskJSON)
    println(jsonObject)

    oorB = jsonObject.getString("oorB").toInt
    if (oorB == 0) {
      val map = jsonObject.getJSONObject("map")
      level = map.getString("level").toInt //2
      windowRange = map.getString("spatialRange") //1


      // 通过on the fly的范围1和层级2找到该层级包含的所有前端瓦片
      println("windowRange = " + windowRange)

      val lonLatsOfWindow: Array[Double] = windowRange
        .substring(1, windowRange.length - 1).split(",").map(_.toDouble)

      println("lonLatsOfWindow = " + lonLatsOfWindow.mkString("Array(", ", ", ")"))

      val jedis: Jedis = JedisConnectionFactory.getJedis
      val key: String = ImageTrigger.originTaskID + ":solvedTile:" + level
      jedis.select(1)

      val xMinOfTile: Int = ZCurveUtil.lon2Tile(lonLatsOfWindow.head, level)
      val xMaxOfTile: Int = ZCurveUtil.lon2Tile(lonLatsOfWindow(2), level)

      val yMinOfTile: Int = ZCurveUtil.lat2Tile(lonLatsOfWindow.last, level)
      val yMaxOfTile: Int = ZCurveUtil.lat2Tile(lonLatsOfWindow(1), level)
      System.out.println("xMinOfTile - xMaxOfTile" + xMinOfTile + "-" + xMaxOfTile)
      System.out.println("yMinOfTile - yMaxOfTile" + yMinOfTile + "-" + yMaxOfTile)

      // z曲线编码后的索引字符串
      //TODO 从redis 找到并剔除这些瓦片中已经算过的，之前缓存在redis中的瓦片编号

      // 等价于两层循环
      for (y <- yMinOfTile to yMaxOfTile; x <- xMinOfTile to xMaxOfTile
           if !jedis.sismember(key, ZCurveUtil.xyToZCurve(Array[Int](x, y), level))
        // 排除 redis 已经存在的前端瓦片编码
           ) { // redis里存在当前的索引

        // Redis 里没有的前端瓦片编码
        val zIndexStr: String = ZCurveUtil.xyToZCurve(Array[Int](x, y), level)

        zIndexStrArray.append(zIndexStr)

        // 将这些新的瓦片编号存到 Redis
        //        jedis.sadd(key, zIndexStr)

      }
    }
    if (zIndexStrArray.isEmpty) {
      //      throw new RuntimeException("窗口范围无明显变化，没有新的瓦片待计算")
      println("窗口范围无明显变化，没有新的瓦片待计算")
      return
    }
    println("***********************************************************")


    val a = if (sc.master == "local[*]") {
      JsonToArgLocal.trans(jsonObject)
    }
    else {
      JsonToArg.trans(jsonObject)
    }
    println("a.size = " + a.size)
    a.foreach(println(_))

    if (a.head._3.contains("productID")) {
      if (a.head._3("productID") != "GF2") {
        lamda(sc, a) // TODO
      }
      else {
        if (oorB == 0) {

        }
        else {

        }
      }
    }
    else {
      lamda(sc, a)
    }

    val time2 = System.currentTimeMillis()
    println(time2 - time1)

  }
}