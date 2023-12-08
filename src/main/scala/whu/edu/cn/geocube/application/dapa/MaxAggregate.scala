package whu.edu.cn.geocube.application.dapa

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import geotrellis.layer._
import geotrellis.raster.{Tile, _}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render.{ColorRamp, RGB}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.view.Info

import scala.collection.mutable.ListBuffer
import scala.sys.process._


/**
 * Generate max fields value from bands.
 *
 *
 */
object MaxAggregate {
  /**
    * Use List[(SpaceTimeKey,Tile)] as input
    * find the max pixel of each tile
    * @param TileRDDList
    * @return tile
    */
  def generateMaxTile(TileRDDList:List[(SpaceTimeKey, Tile)]):Tile = {
    Max(TileRDDList.map({x=>x._2}))
  }
  /**
   * This Max function is used in Jupyter Notebook, e.g. cloud free ndvi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param threshold base threshold default 0
   *
   * @return results Info containing thematic max aggregate png path, time, product type and tiff path
   */
  def max(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double,outputDir: String): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Max aggregate task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Max aggregate task is running ...")
    val analysisBegin = System.currentTimeMillis()

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val bandTileLayerRddWithMeta:(RDD[(String,(SpaceTimeKey,Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
    (tileLayerRddWithMeta._1.map(x=>(x._1.measurementName, (x._1.spaceTimeKey, x._2))), tileLayerRddWithMeta._2)

//    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata


    val bandAndSpatialTemporalRdd:RDD[(String, (SpaceTimeKey, Tile))] = bandTileLayerRddWithMeta._1

    var maxRdd:RDD[(String,Iterable[(SpatialKey,Tile)])] = bandAndSpatialTemporalRdd
        .groupByKey()//group by band to get a spatial-time-series RDD, i.e. RDD[(band, Iterable((SpaceTimeKey, Tile)))]
        .map{ x =>
        val band = x._1
        var reslist = ListBuffer[(SpatialKey, Tile)]()
        var tile:Tile = null
        x._2.groupBy(_._1.spatialKey)//group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
          .foreach({ y =>
          val list = y._2.toList  // List[(SpaceTimeKey, Tile)]
          if(list.length == 1) {
            println("only one instant,just return")
            tile = list.head._2
          }else{
            println("find mix pixel through tiles and generate")
            tile = generateMaxTile(list)
          }
          reslist.append((y._1,tile))
        })
        (band,reslist.toList)
      }


    val results:RDD[Info] = maxRdd.map{x =>
      //stitch extent-series tiles of each band to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val band = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

//      val colorRamp = ColorRamp(
//        0xB96230FF,
//        0xDB9842FF,
//        0xDFAC6CFF,
//        0xE3C193FF,
//        0xE6D6BEFF,
//        0xE4E7C4FF,
//        0xD9E2B2FF,
//        0xBBCA7AFF,
//        0x9EBD4DFF,
//        0x569543FF
//      )
      val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
        .stops(100)
        .setAlphaGradient(0xFF, 0xAA)
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()
//      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
      val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(executorOutputDir + uuid + "_max_" + band + ".png")

      //generate ndvi thematic product
      val outputTiffPath = outputDir + uuid + "_max_" + band + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
//      val outputThematicPngPath = outputDir + uuid + "_max_thematic" + band + ".png"
//      val stdout = new StringBuilder
//      val stderr = new StringBuilder
//      Seq("/home/geocube/qgis/run.sh", "-t", "max", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)

      new Info("this is example,no specific path", 1000, "Max Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
  }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }


  /**
   * This Max function is used in web service and web platform.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param outputDir
   *
   * @return
   */
  def max(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           outputDir: String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val bandTileLayerRddWithMeta:(RDD[(String,(SpaceTimeKey,Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.measurementName, (x._1.spaceTimeKey, x._2))), tileLayerRddWithMeta._2)

    //    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata


    val bandAndSpatialTemporalRdd:RDD[(String, (SpaceTimeKey, Tile))] = bandTileLayerRddWithMeta._1

    var maxRdd:RDD[(String,Iterable[(SpatialKey,Tile)])] =  bandAndSpatialTemporalRdd
        .groupByKey()//group by band to get a spatial-time-series RDD, i.e. RDD[(band, Iterable((SpaceTimeKey, Tile)))]
        .map{ x =>
        val band = x._1
        var reslist = ListBuffer[(SpatialKey, Tile)]()
        var tile:Tile = null
        x._2.groupBy(_._1.spatialKey)//group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
          .foreach({ y =>
          val list = y._2.toList  // List[(SpaceTimeKey, Tile)]
          if(list.length == 1) {
            println("only one instant,just return")
            tile = list.head._2
          }else{
            println("find mix pixel through tiles and generate")
            tile = generateMaxTile(list)
          }
          reslist.append((y._1,tile))
//          reslist+=((y._1,tile))
//          reslist:+(y._1,tile)
        })
        println(reslist.length)
        (band,reslist.toList)
      }
    println(maxRdd.count())

    //stitch extent-series tiles of each band to pngs
    maxRdd.foreach{x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val band = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val extentRet = stitched.extent
      println("<--------" + band + ": " + extentRet + "-------->")
      val colorRamp = ColorRamp(
        0xB96230AA,
        0xDB9842AA,
        0xDFAC6CAA,
        0xE3C193AA,
        0xE6D6BEAA,
        0xE4E7C4AA,
        0xD9E2B2AA,
        0xBBCA7AAA,
        0x9EBD4DAA,
        0x569543AA
      )

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputPath: String = executorOutputDir + "Max_" + band + ".png"
      stitched.tile.renderPng(colorRamp).write(outputPath)
      val scpPngCommand: String = "scp " + outputPath + " geocube@gisweb1:" + outputDir

      val outputMetaPath: String = executorOutputDir + "Max_" + band + ".json"
      val objectMapper =new ObjectMapper()
      val node: ObjectNode = objectMapper.createObjectNode()
      node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
      node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
      node.put("band", band)
      node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
      val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir

      scpPngCommand.!
      scpMetaCommand1.!
    }

  }



  def main(args: Array[String]): Unit = {
    /**
     * Using raster tile array as input
     */
    /*//parse the web request params
    val rasterProductNames = args(0).split(",")
    val extent = args(1).split(",").map(_.toDouble)
    val startTime = args(2) + " 00:00:00.000"
    val endTime = args(3) + " 00:00:00.000"
    val outputDir = args(4)
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(Array("Red", "Near-Infrared")) //该条件为NDVI分析固定条件
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndvi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("NDVI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    ndvi(sc, tileLayerArrayWithMeta, 0.3, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/

    /**
     * Using raster tile RDD as input
     */
    //parse the web request params
    val cubeId = args(0)
    val rasterProductNames = args(1).split(",")
    val extent = args(2).split(",").map(_.toDouble)
    val startTime = args(3)
    val endTime = args(4)
    val outputDir = args(5)
    val bandNames = args(6).split(",")
    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
    println("time: " + (startTime, endTime))
    println(bandNames.foreach({b=>println(b)}))
    val emptyBands:Array[String] = Array[String]()
    val conf = new SparkConf()
      .setAppName("NDVI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    if(!args(6).isEmpty){
      println("非空参数")
      queryParams.setMeasurements(bandNames)
    }else{
      println("空参数")
      queryParams.setMeasurements(emptyBands)
    }
    println(queryParams.getGridCodes)
    println(queryParams.getStartTime)
    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()
    //max
    val analysisBegin = System.currentTimeMillis()
    max(tileLayerRddWithMeta,0.3,outputDir)
    max(tileLayerRddWithMeta, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))

    /**
     * API test
     */
    /*//query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,30.073457222586285,113.9181165740333,30.9597805438586)
    queryParams.setTime("2018-07-01 00:00:00.000", "2018-12-01 00:00:00.000")
    queryParams.setMeasurements(Array("Red", "Near-Infrared"))
    //queryParams.setLevel("4000")
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val queryEnd = System.currentTimeMillis()
    println("tiles length: " + tileLayerArrayWithMeta._1.length)
    //ndvi
    val analysisBegin = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("NDVI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    ndvi(sc, tileLayerArrayWithMeta, 0.3, "/home/geocube/environment_test/geocube_core_jar/")
    val analysisEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))
    println("Analysis time of " + tileLayerArrayWithMeta._1.length * 1024 * 1024 + " observations: " + (analysisEnd - analysisBegin))*/

  }

}

