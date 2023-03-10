package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.application.oge.Tiffheader_parse.RawTile
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.jsonparser.{JsonToArg, JsonToArgLocal}
import org.locationtech.jts.geom._
import whu.edu.cn.application.oge.WebAPI._

import scala.collection.mutable.{ListBuffer, Map}
import scala.collection.immutable
import scala.io.Source

object ImageTrigger {
  var rdd_list_image: Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = Map.empty[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])]
  var rdd_list_image_waitingForMosaic: Map[String, RDD[RawTile]] = Map.empty[String, RDD[RawTile]]
  var rdd_list_table: Map[String, String] = Map.empty[String, String]
  var rdd_list_feature_API: Map[String, String] = Map.empty[String, String]
  var list_kernel: Map[String, Kernel] = Map.empty[String, Kernel]
  var imageLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]
  var filterEqual: Map[String, (String, String)] = Map.empty[String, (String, String)]
  var filterAnd: Map[String, Array[String]] = Map.empty[String, Array[String]]
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
    if (name == "Service.getCoverageCollection") {
      imageLoad += (UUID -> (argOrNot(args, "productID"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
    }
    if (name == "Service.getCoverage") {
      imageLoad += (UUID -> (argOrNot(args, "productID"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
    }
    if (name == "Filter.equals") {
      filterEqual += (UUID -> (argOrNot(args, "leftField"), argOrNot(args, "rightValue")))
    }
    if (name == "Filter.and") {
      filterAnd += (UUID -> argOrNot(args, "filters").replace("[", "").replace("]", "").split(","))
    }
    if (name == "CoverageCollection.subCollection") {
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
    if (name == "CoverageCollection.mosaic") {
      rdd_list_image += (UUID -> Image.mosaic(sc, tileRDDReP = rdd_list_image_waitingForMosaic(argOrNot(args, "coverageCollection")), method = argOrNot(args, "method")))
    }
    if (name == "Service.getTable") {
      rdd_list_table += (UUID -> argOrNot(args, "productID"))
    }
    if (name == "Service.getFeatureCollection") {
      rdd_list_feature_API += (UUID -> argOrNot(args, "productID"))
    }

    //Algorithm
    if (name == "Algorithm.hargreaves") {
      rdd_list_table += (UUID -> hargreaves(inputTemperature = rdd_list_table(argOrNot(args, "inputTemperature")), inputStation = rdd_list_feature_API(argOrNot(args, "inputStation")),
        startTime = argOrNot(args, "startTime"), endTime = argOrNot(args, "endTime"), timeStep = argOrNot(args, "timeStep").toLong))
    }
    if (name == "Algorithm.topmodel") {
      rdd_list_table += (UUID -> topModel(inputPrecipEvapFile = rdd_list_table(argOrNot(args, "inputPrecipEvapFile")), inputTopoIndex = rdd_list_table(argOrNot(args, "inputTopoIndex")),
        startTime = argOrNot(args, "startTime"), endTime = argOrNot(args, "endTime"), timeStep = argOrNot(args, "timeStep").toLong,
        rate = argOrNot(args, "rate").toDouble, recession = argOrNot(args, "recession").toInt, iterception = argOrNot(args, "iterception").toInt,
        waterShedArea = argOrNot(args, "waterShedArea").toInt, tMax = argOrNot(args, "tMax").toInt))
    }
    if (name == "Algorithm.swmm") {
      rdd_list_table += (UUID -> SWMM5(input = rdd_list_table(argOrNot(args, "input"))))
    }

    //Table
    if (name == "Table.getDownloadUrl") {
      Table.getDownloadUrl(url = rdd_list_table(argOrNot(args, "input")), fileName = fileName)
    }
    if (name == "Table.addStyles") {
      Table.getDownloadUrl(url = rdd_list_table(argOrNot(args, "input")), fileName = fileName)
    }

    //Coverage
    if (name == "Coverage.subtract") {
      rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.add") {
      rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.divide") {
      rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.multiply") {
      rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.binarization") {
      rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverage")), threshold = args("threshold").toInt))
    }
    if (name == "Coverage.and") {
      rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.or") {
      rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.not") {
      rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.ceil") {
      rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.floor") {
      rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.sin") {
      rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.cos") {
      rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.sinh") {
      rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.cosh") {
      rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.asin") {
      rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.acos") {
      rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.atan") {
      rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.atan2") {
      rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.eq") {
      rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.gt") {
      rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.gte") {
      rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.addBands") {
      val names: List[String] = List("B3")
      rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2")), names = names))
    }
    if (name == "Coverage.bandNames") {
      val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverage")))
      println("******************test bandNames***********************")
      println(bandNames)
      println(bandNames.length)
    }

    if (name == "Kernel.fixed") {
      val kernel = Kernel.fixed(weights = args("weights"))
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.square") {
      val kernel = Kernel.square(radius = args("radius").toInt, normalize = args("normalize").toBoolean, value = args("value").toDouble)
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.prewitt") {
      val kernel = Kernel.prewitt(axis = args("axis"))
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.kirsch") {
      val kernel = Kernel.kirsch(axis = args("axis"))
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.sobel") {
      val kernel = Kernel.sobel(axis = args("axis"))
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }

    if (name == "Kernel.laplacian4") {
      val kernel = Kernel.laplacian4()
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.laplacian8") {
      val kernel = Kernel.laplacian8()
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.laplacian8") {
      val kernel = Kernel.laplacian8()
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Kernel.add") {
      val kernel = Kernel.add(kernel1 = list_kernel(args("kernel1")), kernel2 = list_kernel(args("kernel2")))
      list_kernel += (UUID -> kernel)
      print(kernel.tile.asciiDraw())
    }
    if (name == "Coverage.abs") {
      rdd_list_image += (UUID -> Image.abs(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.neq") {
      rdd_list_image += (UUID -> Image.neq(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
    }
    if (name == "Coverage.signum") {
      rdd_list_image += (UUID -> Image.signum(image = rdd_list_image(args("coverage"))))
    }
    if (name == "Coverage.bandTypes") {
      val bandTypes: immutable.Map[String, String] = Image.bandTypes(image = rdd_list_image(args("coverage")))
      println(bandTypes)
      println(bandTypes.size)
    }
    if (name == "Coverage.rename") {
      rdd_list_image += (UUID -> Image.rename(image = rdd_list_image(args("coverage")), name = args("name")))
    }
    if (name == "Coverage.projection") {
      val projection: String = Image.projection(image = rdd_list_image(args("coverage")))
      println(projection)
    }
    if (name == "Coverage.histogram") {
      val hist = Image.histogram(image = rdd_list_image(args("coverage")))
      println(hist)
    }
    if (name == "Coverage.resample") {
      rdd_list_image += (UUID -> Image.resample(image = rdd_list_image(args("coverage")), level = args("level").toInt, mode = args("mode")))
    }

    if (name == "Coverage.slope") {
      rdd_list_image += (UUID -> slope(sc, input = rdd_list_image(args("input")), Z_factor = argOrNot(args, "Z_factor").toDouble))
    }
    if (name == "Coverage.addStyles") {
      if (oorB == 0) {
        Image.visualizeOnTheFly(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
          method = argOrNot(args, "method"), palette = argOrNot(args, "palette"), layerID = layerID, fileName = fileName)
        layerID = layerID + 1
      }
      else {
        Image.visualizeBatch(sc, image = rdd_list_image(args("input")), layerID = layerID, fileName = fileName)
        layerID = layerID + 1
      }
    }

    //CoverageCollection
    if (name == "CoverageCollection.subtract") {
      rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.add") {
      rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.divide") {
      rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.multiply") {
      rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.binarization") {
      rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverageCollection")), threshold = args("threshold").toInt))
    }
    if (name == "CoverageCollection.and") {
      rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.or") {
      rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.not") {
      rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.ceil") {
      rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.floor") {
      rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.sin") {
      rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.cos") {
      rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.sinh") {
      rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.cosh") {
      rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.asin") {
      rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.acos") {
      rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.atan") {
      rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.atan2") {
      rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.eq") {
      rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.gt") {
      rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.gte") {
      rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
    }
    if (name == "CoverageCollection.addBands") {
      val names: List[String] = List("B3")
      rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2")), names = names))
    }
    if (name == "CoverageCollection.slope") {
      rdd_list_image += (UUID -> slope(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
    }
    if (name == "CoverageCollection.aspect") {
      rdd_list_image += (UUID -> aspect(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
    }
    if (name == "CoverageCollection.hillShade") {
      rdd_list_image += (UUID -> hillShade(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble, Azimuth = argOrNot(args, "Azimuth").toDouble, Vertical_angle = argOrNot(args, "Vertical_angle").toDouble))
    }
    if (name == "CoverageCollection.relief") {
      rdd_list_image += (UUID -> relief(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
    }
    if (name == "CoverageCollection.ruggednessIndex") {
      rdd_list_image += (UUID -> ruggednessIndex(sc, input = rdd_list_image(args("coverageCollection")), Z_factor = argOrNot(args, "Z_factor").toDouble))
    }
    if (name == "CoverageCollection.cellBalance") {
      rdd_list_image += (UUID -> cellBalance(sc, input = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.flowAccumulationTD") {
      rdd_list_image += (UUID -> flowAccumulationTD(sc, input = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.flowPathLength") {
      rdd_list_image += (UUID -> flowPathLength(sc, input = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.slopeLength") {
      rdd_list_image += (UUID -> slopeLength(sc, input = rdd_list_image(args("coverageCollection"))))
    }
    if (name == "CoverageCollection.calCrop") {
      calCrop(year = argOrNot(args, "year"), quarter = argOrNot(args, "quarter"), sort = argOrNot(args, "sort"))
    }
    if (name == "CoverageCollection.PAP") {
      rdd_list_image += (UUID -> Image.PAP(image = rdd_list_image(args("coverageCollection")), time = argOrNot(args, "time"), n = argOrNot(args, "n").toInt))
    }
    if (name == "CoverageCollection.bandNames") {
      val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverageCollection")))
      println("******************test bandNames***********************")
      println(bandNames)
      println(bandNames.length)
    }
    if (name == "CoverageCollection.addStyles") {
      if (oorB == 0) {
        Image.visualizeOnTheFly(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
          method = argOrNot(args, "method"), palette = argOrNot(args, "palette"), layerID = layerID, fileName = fileName)
        layerID = layerID + 1
      }
      else {
        Image.visualizeBatch(sc, image = rdd_list_image(args("input")), layerID = layerID, fileName = fileName)
        layerID = layerID + 1
      }
    }
  }

  def lamda(implicit sc: SparkContext, list: List[Tuple3[String, String, Map[String, String]]]) = {
    for (i <- list.indices) {
      func(sc, list(i)._1, list(i)._2, list(i)._3)
    }
  }

  def main(args: Array[String]): Unit = {
    val time1 = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("query")
    val sc = new SparkContext(conf)

    val fileSource = Source.fromFile("src/main/scala/whu/edu/cn/application/oge/climatepilot/PAP.json")
    fileName = "/home/geocube/oge/on-the-fly/out.txt"
    val line: String = fileSource.mkString
    fileSource.close()
    val jsonObject = JSON.parseObject(line)
    println(jsonObject.size())
    println(jsonObject)

    oorB = jsonObject.getString("oorB").toInt
    if (oorB == 0) {
      val map = jsonObject.getJSONObject("map")
      level = map.getString("level").toInt
      windowRange = map.getString("spatialRange")
    }

    val a = JsonToArgLocal.trans(jsonObject)
    println(a.size)
    a.foreach(println(_))

    if (a.head._3.contains("productID")) {
      if (a.head._3("productID") != "GF2") {
        lamda(sc, a)
      }
      else {
        if (oorB == 0) {
          Image.deepLearningOnTheFly(sc, level, geom = windowRange, geom2 = a.head._3("bbox"), fileName = fileName)
        }
        else {
          Image.deepLearning(sc, geom = a.head._3("bbox"), fileName = fileName)
        }
      }
    }
    else {
      lamda(sc, a)
    }

    val time2 = System.currentTimeMillis()
    println(time2 - time1)
  }
  /*def main(args: Array[String]): Unit = {
    val time1 = System.currentTimeMillis()
    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    var a: List[Tuple3[String, String, Map[String, String]]] = List.empty[Tuple3[String, String, Map[String, String]]]
    a = a :+ Tuple3("00000", "Service.getCoverageCollection", Map("productID" -> "ASTER_GDEM_DEM30", "baseUrl" -> "http://localhost"))
    a = a :+ Tuple3("00004", "Filter.equals", Map("rightValue" -> "EPSG:4326", "leftField" -> "crs"))
    a = a :+ Tuple3("0000", "CoverageCollection.subCollection", Map("filter" -> "00004", "input" -> "00000"))
    a = a :+ Tuple3("000", "CoverageCollection.mosaic", Map("method" -> "min", "coverageCollection" -> "0000"))
//    a = a :+ Tuple3("00", "CoverageCollection.slope", Map("Z_factor" -> "1", "coverageCollection" -> "000"))
    a = a :+ Tuple3("0", "CoverageCollection.addStyles", Map("input" -> "000", "max" -> "255", "min" -> "0"))

    println(a.size)
    a.foreach(println(_))
    fileName = "/home/geocube/oge/on-the-fly/out.txt"
    level = 11
    windowRange = "[113.17588,30.379332,114.573944,30.71761]"
    if(a(0)._3.contains("productID")) {
      if (a(0)._3("productID") != "GF2") {
        lamda(sc, a)
      }
      else {
        Image.deepLearning(sc, a(0)._3("bbox"))
      }
    }
    else{
      lamda(sc, a)
    }

    val time2 = System.currentTimeMillis()
    println(time2 - time1)
    sc.stop()
  }*/
}
