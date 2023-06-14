//package whu.edu.cn.oge
//
//import com.alibaba.fastjson.JSON
//import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
//import geotrellis.raster.Tile
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import whu.edu.cn.oge.Tiffheader_parse.RawTile
//import whu.edu.cn.entity.SpaceTimeBandKey
//import whu.edu.cn.jsonparser.JsonToArg
//import whu.edu.cn.modelbuilder.ModelBuilderJsonToScala
//
//import scala.collection.mutable.Map
//import scala.io.Source
//
//object TriggerModelBuilder {
//  var rdd_list_image: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map.empty[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
//  var rdd_list_image_waitingForMosaic: Map[String, RDD[RawTile]] = Map.empty[String, RDD[RawTile]]
//  var imageLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]
//  var filterEqual: Map[String, (String, String)] = Map.empty[String, (String, String)]
//  var filterAnd: Map[String, Array[String]] = Map.empty[String, Array[String]]
//
//  var rdd_list_cube: Map[String, Map[String, Any]] = Map.empty[String, Map[String, Any]]
//  var cubeLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]
//  var layerID: Int = 0
//
//  def argOrNot(args: Map[String, String], name: String): String = {
//    if (args.contains(name)) {
//      args(name)
//    }
//    else {
//      null
//    }
//  }
//
//  def func(implicit sc: SparkContext, UUID: String, name: String, args: Map[String, String]): Unit = {
//    //Coverage
//    if (name == "Service.getCoverageCollection") {
//      imageLoad += (UUID -> (argOrNot(args, "productID"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
//    }
//    if (name == "Filter.equals") {
//      filterEqual += (UUID -> (argOrNot(args, "leftField"), argOrNot(args, "rightValue")))
//    }
//    if (name == "Filter.and") {
//      filterAnd += (UUID -> argOrNot(args, "filters").replace("[", "").replace("]", "").split(","))
//    }
//    if (name == "CoverageCollection.subCollection") {
//      var crs: String = null
//      var measurementName: String = null
//      val filter = argOrNot(args, "filter")
//      if (filterAnd.contains(filter)) {
//        val filters = filterAnd(filter)
//        for (i <- filters.indices) {
//          if ("crs".equals(filterEqual(filters(i))._1)) {
//            crs = filterEqual(filters(i))._2
//          }
//          if ("measurementName".equals(filterEqual(filters(i))._1)) {
//            measurementName = filterEqual(filters(i))._2
//          }
//        }
//      }
//      else if (filterEqual.contains(filter)) {
//        if ("crs".equals(filterEqual(filter)._1)) {
//          crs = filterEqual(filter)._2
//        }
//        if ("measurementName".equals(filterEqual(filter)._1)) {
//          measurementName = filterEqual(filter)._2
//        }
//      }
//      val loadInit = Image.load(sc, productName = imageLoad(argOrNot(args, "input"))._1, measurementName = measurementName, dateTime = imageLoad(argOrNot(args, "input"))._2,
//        geom = imageLoad(argOrNot(args, "input"))._3, crs = crs)
//      rdd_list_image += (UUID -> loadInit._1)
//      rdd_list_image_waitingForMosaic += (UUID -> loadInit._2)
//    }
//    if (name == "CoverageCollection.mosaic") {
//      rdd_list_image += (UUID -> Image.mosaic(sc, tileRDDReP = rdd_list_image_waitingForMosaic(argOrNot(args, "coverageCollection")), method = argOrNot(args, "method")))
//    }
//    if (name == "Coverage.subtract") {
//      rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.add") {
//      rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.divide") {
//      rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.multiply") {
//      rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.binarization") {
//      rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverage")), threshold = args("threshold").toInt))
//    }
//    if (name == "Coverage.and") {
//      rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.or") {
//      rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.not") {
//      rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.ceil") {
//      rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.floor") {
//      rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.sin") {
//      rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.cos") {
//      rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.sinh") {
//      rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.cosh") {
//      rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.asin") {
//      rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.acos") {
//      rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.atan") {
//      rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverage"))))
//    }
//    if (name == "Coverage.atan2") {
//      rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.eq") {
//      rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.gt") {
//      rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.gte") {
//      rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2"))))
//    }
//    if (name == "Coverage.addBands") {
//      val names: List[String] = List("B3")
//      rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverage1")), image2 = rdd_list_image(args("coverage2")), names = names))
//    }
//    if (name == "Coverage.bandNames") {
//      val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverage")))
//      println("******************test bandNames***********************")
//      println(bandNames)
//      println(bandNames.length)
//    }
//    if (name == "Coverage.addStyles") {
//      Image.visualize(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
//        method = argOrNot(args, "method"), palette = argOrNot(args, "palette"),layerID = layerID)
//      layerID = layerID + 1
//    }
//
//    //CoverageCollection
//    if (name == "CoverageCollection.subtract") {
//      rdd_list_image += (UUID -> Image.subtract(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.add") {
//      rdd_list_image += (UUID -> Image.add(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.divide") {
//      rdd_list_image += (UUID -> Image.divide(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.multiply") {
//      rdd_list_image += (UUID -> Image.multiply(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.binarization") {
//      rdd_list_image += (UUID -> Image.binarization(image = rdd_list_image(args("coverageCollection")), threshold = args("threshold").toInt))
//    }
//    if (name == "CoverageCollection.and") {
//      rdd_list_image += (UUID -> Image.and(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.or") {
//      rdd_list_image += (UUID -> Image.or(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.not") {
//      rdd_list_image += (UUID -> Image.not(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.ceil") {
//      rdd_list_image += (UUID -> Image.ceil(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.floor") {
//      rdd_list_image += (UUID -> Image.floor(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.sin") {
//      rdd_list_image += (UUID -> Image.sin(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.cos") {
//      rdd_list_image += (UUID -> Image.cos(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.sinh") {
//      rdd_list_image += (UUID -> Image.sinh(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.cosh") {
//      rdd_list_image += (UUID -> Image.cosh(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.asin") {
//      rdd_list_image += (UUID -> Image.asin(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.acos") {
//      rdd_list_image += (UUID -> Image.acos(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.atan") {
//      rdd_list_image += (UUID -> Image.atan(image = rdd_list_image(args("coverageCollection"))))
//    }
//    if (name == "CoverageCollection.atan2") {
//      rdd_list_image += (UUID -> Image.atan2(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.eq") {
//      rdd_list_image += (UUID -> Image.eq(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.gt") {
//      rdd_list_image += (UUID -> Image.gt(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.gte") {
//      rdd_list_image += (UUID -> Image.gte(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2"))))
//    }
//    if (name == "CoverageCollection.addBands") {
//      val names: List[String] = List("B3")
//      rdd_list_image += (UUID -> Image.addBands(image1 = rdd_list_image(args("coverageCollection1")), image2 = rdd_list_image(args("coverageCollection2")), names = names))
//    }
//    if (name == "CoverageCollection.bandNames") {
//      val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverageCollection")))
//      println("******************test bandNames***********************")
//      println(bandNames)
//      println(bandNames.length)
//    }
//    if (name == "CoverageCollection.addStyles") {
//      Image.visualize(sc, image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
//        method = argOrNot(args, "method"), palette = argOrNot(args, "palette"), layerID = layerID)
//      layerID = layerID + 1
//    }
//
//    //Cube
//    if (name == "Service.getCollections") {
//      cubeLoad += (UUID -> (argOrNot(args, "productIDs"), argOrNot(args, "datetime"), argOrNot(args, "bbox")))
//    }
//    if (name == "Collections.toCube") {
//      rdd_list_cube += (UUID -> Cube.load(sc, productList = cubeLoad(args("input"))._1, dateTime = cubeLoad(args("input"))._2, geom = cubeLoad(args("input"))._3, bandList = argOrNot(args,"bands")))
//    }
//    if (name == "Cube.NDWI") {
//      rdd_list_cube += (UUID -> Cube.NDWI(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"), name = argOrNot(args, "name")))
//    }
//    if(name == "Cube.binarization") {
//      rdd_list_cube += (UUID -> Cube.binarization(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"), name = argOrNot(args, "name"),
//        threshold = argOrNot(args, "threshold").toDouble))
//    }
//    if (name == "Cube.subtract") {
//      rdd_list_cube += (UUID -> Cube.WaterChangeDetection(input = rdd_list_cube(args("input")), product = argOrNot(args, "product"),
//        certainTimes = argOrNot(args, "timeList"), name = argOrNot(args, "name")))
//    }
//    if (name == "Cube.overlayAnalysis") {
//      rdd_list_cube += (UUID -> Cube.OverlayAnalysis(input = rdd_list_cube(args("input")), rasterOrTabular = argOrNot(args, "raster"), vector = argOrNot(args, "vector"),name = argOrNot(args, "name")))
//    }
//    if (name == "Cube.addStyles") {
//      Cube.visualize(sc, cube = rdd_list_cube(args("cube")), products = argOrNot(args, "products"))
//    }
//  }
//
//  def lamda(implicit sc: SparkContext, list: List[Tuple3[String, String, Map[String, String]]]) = {
//    for (i <- list.indices) {
//      func(sc, list(i)._1, list(i)._2, list(i)._3)
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    //    val time1 = System.currentTimeMillis()
//    //    val conf = new SparkConf()
//    //      .setAppName("GeoCube-Dianmu Hurrican Flood Analysis")
//    //      .setMaster("local[*]")
//    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    //      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//    //      .set("spark.kryoserializer.buffer.max", "512m")
//    //      .set("spark.rpc.message.maxSize", "1024")
//    //    val sc = new SparkContext(conf)
//    //
//    //    val line: String = Source.fromFile("C:\\Users\\dell\\Desktop\\testJsonCubeFloodAnalysis.json").mkString
//    //    val jsonObject = JSON.parseObject(line)
//    //    println(jsonObject.size())
//    //    println(jsonObject)
//    //
//    //    val a = JsonToArg.trans(jsonObject)
//    //    println(a.size)
//    //    a.foreach(println(_))
//    //
//    //    lamda(sc, a)
//    //
//    //    val time2 = System.currentTimeMillis()
//    //    println("算子运行时间为："+(time2 - time1))
//    val time1 = System.currentTimeMillis()
//    val conf = new SparkConf()
////      .setMaster("local[*]")
//      .setAppName("OGE-Computation")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//    val sc = new SparkContext(conf)
//
//    val line: String = Source.fromFile("/home/geocube/oge/oge-server/dag-boot/testJson.json").mkString
////    val line: String = Source.fromFile("src/main/scala/whu/edu/cn/application/oge/testJsonModelBuilderFloodAnalysis.json").mkString
//    val jsonObject = JSON.parseObject(line)
//    println(jsonObject.size())
//    println(jsonObject)
//
//    val modelBuilder = new ModelBuilderJsonToScala()
//    val a = modelBuilder.JsonToScala(jsonObject).getDAGMap
//    println(a.size)
//    a.foreach(println(_))
//
//    lamda(sc, a)
//
//    val time2 = System.currentTimeMillis()
//    println(time2 - time1)
//  }
//}