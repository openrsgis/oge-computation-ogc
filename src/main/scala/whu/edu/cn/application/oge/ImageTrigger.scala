package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.application.oge.Tiffheader_parse.RawTile
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.jsonparser.{JsonToArg, JsonToArgLocal}

import scala.collection.mutable.Map
import scala.io.Source

object ImageTrigger {
  var rdd_list_image: Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = Map.empty[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])]
  var rdd_list_image_waitingForMosaic: Map[String, RDD[RawTile]] = Map.empty[String, RDD[RawTile]]
  var imageLoad: Map[String, (String, String, String)] = Map.empty[String, (String, String, String)]
  var filterEqual: Map[String, (String, String)] = Map.empty[String, (String, String)]
  var filterAnd: Map[String, Array[String]] = Map.empty[String, Array[String]]
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
    if (name == "Filter.equals") {
      filterEqual += (UUID -> (argOrNot(args, "leftField"), argOrNot(args, "rightValue")))
    }
    if (name == "Filter.and") {
      filterAnd += (UUID -> argOrNot(args, "filters").replace("[", "").replace("]", "").split(","))
    }
    if (name == "CoverageCollection.subCollection") {
      var crs:String  = null
      var measurementName: String = null
      val filter = argOrNot(args, "filter")
      if(filterAnd.contains(filter)) {
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
      else if (filterEqual.contains(filter)){
        if ("crs".equals(filterEqual(filter)._1)) {
          crs = filterEqual(filter)._2
        }
        if ("measurementName".equals(filterEqual(filter)._1)) {
          measurementName = filterEqual(filter)._2
        }
      }
      val loadInit = Image.load(sc, productName = imageLoad(argOrNot(args,"input"))._1, measurementName = measurementName, dateTime = imageLoad(argOrNot(args,"input"))._2,
        geom = imageLoad(argOrNot(args,"input"))._3, crs = crs)
      rdd_list_image += (UUID -> loadInit._1)
      rdd_list_image_waitingForMosaic += (UUID -> loadInit._2)
    }
    if(name == "CoverageCollection.mosaic") {
      rdd_list_image += (UUID -> Image.mosaic(sc, tileRDDReP = rdd_list_image_waitingForMosaic(argOrNot(args, "coverageCollection")), method = argOrNot(args, "method")))
    }
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
    if (name == "Coverage.addStyles") {
      Image.visualize(image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
        method = argOrNot(args, "method"), palette = argOrNot(args, "palette"))
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
    if (name == "CoverageCollection.bandNames") {
      val bandNames: List[String] = Image.bandNames(image = rdd_list_image(args("coverageCollection")))
      println("******************test bandNames***********************")
      println(bandNames)
      println(bandNames.length)
    }
    if (name == "CoverageCollection.addStyles") {
      Image.visualize(image = rdd_list_image(args("input")), min = args("min").toInt, max = args("max").toInt,
        method = argOrNot(args, "method"), palette = argOrNot(args, "palette"))
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
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    val line: String = Source.fromFile("src/main/scala/whu/edu/cn/application/oge/testCOG.json").mkString
    val jsonObject = JSON.parseObject(line)
    println(jsonObject.size())
    println(jsonObject)

    val a = JsonToArgLocal.trans(jsonObject)
    println(a.size)
    a.foreach(println(_))

    if (a(0)._3("productID") != "GF2") {
      lamda(sc, a)
    }
    else {
      Image.deepLearning(a(0)._3("bbox"))
    }

    val time2 = System.currentTimeMillis()
    println(time2 - time1)
  }
}