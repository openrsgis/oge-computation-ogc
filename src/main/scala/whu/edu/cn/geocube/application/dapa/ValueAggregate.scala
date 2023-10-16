package whu.edu.cn.geocube.application.dapa

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import geotrellis.layer._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{Tile, _}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark._
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}
import whu.edu.cn.geocube.core.entity.{GcMeasurement, QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.util.PostgresqlUtil

import scala.sys.process._


/**
 * Generate max fields value from bands.
 *
 *
 */
object ValueAggregate {
  /**
    * Use List[(SpaceTimeKey,Tile)] as input
    * find the max pixel of each tile
    * @param TileRDDList
    * @return tile
    */
  def generateMaxTile(TileRDDList:RDD[(SpatialKey, Tile)],metadata: TileLayerMetadata[SpaceTimeKey],bbox:String,outputDir:String):Raster[Tile] = {
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)

    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(TileRDDList, spatialMetadata)
    val stitched: Raster[Tile] = tileLayerRdd.stitch()
    val srcExtent = stitched.extent
    var tile:Tile = null
    val coordinates = bbox.split(",")
    val requestedExtent = new Extent(coordinates(0).toDouble, coordinates(1).toDouble, coordinates(2).toDouble, coordinates(3).toDouble)
    if(requestedExtent.intersects(srcExtent)){
      println("intersected")
      val intersection = requestedExtent.intersection(srcExtent).get
      val colMin = (intersection.xmin - srcExtent.xmin)/stitched.cellSize.width
      val colMax = (intersection.xmax - srcExtent.xmin)/stitched.cellSize.width
      val rowMin = (intersection.ymin - srcExtent.ymin)/stitched.cellSize.height
      val rowMax = (intersection.ymax - srcExtent.ymin)/stitched.cellSize.height
      tile = stitched.tile.crop(colMin.toInt, stitched.rows - rowMax.toInt -1, colMax.toInt, stitched.rows - rowMin.toInt - 1) //Array[Byte]
    }
    val slopeTile = tile.slope(spatialMetadata.cellSize, 1/(111*math.cos(requestedExtent.center.getY)), None)
    val aspectTile = tile.aspect(spatialMetadata.cellSize, None)
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()
    //      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
    val uuid = UUID.randomUUID
    slopeTile.renderPng(colorRamp).write(executorOutputDir + uuid + "_slope.png")
    aspectTile.renderPng(colorRamp).write(executorOutputDir + uuid + "_aspect.png")
    val SoutputTiffPath = outputDir + uuid + "_slope.TIF"
    val AoutputTiffPath = outputDir + uuid + "_aspect.TIF"
    val slopeRaster: Raster[Tile] =Raster(slopeTile,requestedExtent)
    val aspectRaster: Raster[Tile] =Raster(aspectTile,requestedExtent)
    GeoTiff(slopeRaster, spatialMetadata.crs).write(SoutputTiffPath)
    GeoTiff(aspectRaster, spatialMetadata.crs).write(AoutputTiffPath)
    Raster(tile,requestedExtent)

  }
  /**
    * Use List[(SpaceTimeKey,Tile)] as input
    * find the max pixel of each tile
    * @param TileRDDList
    * @return tile
    */
  def generateValue(TileRDDList:RDD[(Long,Iterable[(String,(SpaceTimeKey, Tile))])],metadata: TileLayerMetadata[SpaceTimeKey],bbox:String):Double = {
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)

    val middleRdd:RDD[(SpatialKey,Tile)] = TileRDDList.values.flatMap(x=>x.map(ele=>(ele._2._1.spatialKey,ele._2._2)))
    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(middleRdd, spatialMetadata)
    val stitched: Raster[Tile] = tileLayerRdd.stitch()
    val srcExtent = stitched.extent
    var tile:Tile = null
    var value:Double =0.0
    val coordinates = bbox.split(",")
    if(srcExtent.intersects(coordinates(0).toDouble, coordinates(1).toDouble)){
      println("intersected")
      val col = math.floor((coordinates(0).toDouble - srcExtent.xmin))/stitched.cellSize.width
      val row = stitched.rows - 1 - math.floor((coordinates(0).toDouble - srcExtent.ymin))/stitched.cellSize.height

      value = tile.getDouble(col.toInt,row.toInt)

    }

    value
  }
  /**
    * Use List[(SpaceTimeKey,Tile)] as input
    * find the max pixel of each tile
    * @param TileRDDList
    * @return tile
    */
  def generateValues(TileRDDList:RDD[(Long,Iterable[(String,(SpaceTimeKey, Tile))])],metadata: TileLayerMetadata[SpaceTimeKey],bbox:String):((Double,Double),Double) = {
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)

    val middleRdd:RDD[(SpatialKey,Tile)] = TileRDDList.values.flatMap(x=>x.map(ele=>(ele._2._1.spatialKey,ele._2._2)))
    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(middleRdd, spatialMetadata)
    val stitched: Raster[Tile] = tileLayerRdd.stitch()
    val srcExtent = stitched.extent
    var tile:Tile = null
    var average:Double = 0.0
    val coordinates = bbox.split(",")
    val requestedExtent = new Extent(coordinates(0).toDouble, coordinates(1).toDouble, coordinates(2).toDouble, coordinates(3).toDouble)
    if(requestedExtent.intersects(srcExtent)){
      println("intersected")
      val intersection = requestedExtent.intersection(srcExtent).get
      val colMin = (intersection.xmin - srcExtent.xmin)/stitched.cellSize.width
      val colMax = (intersection.xmax - srcExtent.xmin)/stitched.cellSize.width
      val rowMin = (intersection.ymin - srcExtent.ymin)/stitched.cellSize.height
      val rowMax = (intersection.ymax - srcExtent.ymin)/stitched.cellSize.height
      tile = stitched.tile.crop(colMin.toInt, stitched.rows - rowMax.toInt -1, colMax.toInt, stitched.rows - rowMin.toInt - 1) //Array[Byte]
      var accum = 0.0
      tile.foreachDouble(pixel=>
        accum+=pixel
      )
      average = accum / (colMax-colMin)/(rowMax - rowMin)

    }

    (tile.findMinMaxDouble,average)
  }

  /**
   * This Max function is used in Jupyter Notebook, e.g. cloud free ndvi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param tileLayerRddWithMeta a rdd of queried tiles
   * @param outputDir
   * @param point
   * @param bandName
   *
   * @return results Info containing thematic max aggregate png path, time, product type and tiff path
   */
  def Point(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           outputDir: String,point:String,bandName:String): Unit = {
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

    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])]  = bandAndSpatialTemporalRdd.filter({x=>x._1.equals(bandName)}).map(y=>(y._2._1.instant,y._2)).groupByKey()
    temporalGroupRdd.foreach { x =>
      val metadata = srcMetadata
      val layout = metadata.layout
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele => (ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)
      val srcExtent = stitched.extent

      var value:Double =0.0
      val coordinates = point.split(",").map(_.toDouble)
      if(srcExtent.intersects(coordinates(0), coordinates(1))){
        println("intersected")
        val col = math.floor((coordinates(0) - srcExtent.xmin) / stitched.cellSize.width).toInt
        val row = stitched.rows - 1 - math.floor((coordinates(1) - srcExtent.ymin) / stitched.cellSize.height).toInt
        value = stitched.tile.getDouble(col,row)

      }
      val sdf = new SimpleDateFormat("yyyy_MM_dd")
      val date = new Date(instant)
      val instantRet = sdf.format(date)
      println("<--------" + instantRet + ": " + point + "-------->")

      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()



      val outputMetaPath: String = executorOutputDir +  bandName+ "_"  + instantRet + ".json"
      val objectMapper =new ObjectMapper()
      val node: ObjectNode = objectMapper.createObjectNode()
      node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
      node.put("time", instantRet)
      node.put("field",bandName)
      node.put("value",value)
      node.put("point", coordinates(0) + "," + coordinates(1) )
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
      val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir

      scpMetaCommand1.!
    }

//    val value = generateValue(bandAndSpatialTemporalRdd.filter({x=>x._1.equals(bandName)}).groupBy(x=>x._2._1.instant),srcMetadata,point)
//
//    println("<--------ValueAggregate" + bandName + ": " + point + "-------->")
//
//
//    val executorOutputDir = outputDir
//    val executorOutputFile = new File(executorOutputDir)
//    if (!executorOutputFile.exists()) executorOutputFile.mkdir()
//
//
//
//    val outputMetaPath = executorOutputDir +  bandName+ "_"  + ".json"
//    val objectMapper =new ObjectMapper()
//    val node = objectMapper.createObjectNode()
//    val localDataRoot = GlobalConfig.localDataRoot
//    val httpDataRoot = GlobalConfig.httpDataRoot
//    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
//    node.put("field",bandName)
//    node.put("value",value)
//    node.put("point", point )
//    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
//    val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir
//
//    scpMetaCommand1.!


    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
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
  def Area(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           outputDir: String,bbox:String,bandName:String): Unit = {
    println("Task is running ...")
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)
//    val latiRad = (tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerRddWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val bandTileLayerRddWithMeta:(RDD[(String,(SpaceTimeKey,Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (tileLayerRddWithMeta._1.map(x=>(x._1.measurementName, (x._1.spaceTimeKey, x._2))), tileLayerRddWithMeta._2)

    //    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tranTileLayerRddWithMeta._1
    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata


    val bandAndSpatialTemporalRdd:RDD[(String, (SpaceTimeKey, Tile))] = bandTileLayerRddWithMeta._1

//    val rasterTileRdd = bandAndSpatialTemporalRdd.filter({x=>x._1.equals(bandName)}).groupBy(y=>y._2._1.instant)

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])]  = bandAndSpatialTemporalRdd.filter({x=>x._1.equals(bandName)}).map(y=>(y._2._1.instant,y._2)).groupByKey()
    val value = generateValues(bandAndSpatialTemporalRdd.filter({x=>x._1.equals(bandName)}).groupBy(x=>x._2._1.instant),srcMetadata,bbox)
    generateMaxTile(bandAndSpatialTemporalRdd.values.map({x=>(x._1.spatialKey,x._2)}),srcMetadata,bbox,outputDir)
    println("<--------ValueAggregate" + bandName + ": " + bbox + "-------->")


    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()



    val outputMetaPath = executorOutputDir +  bandName+ "_"  + ".json"
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("field",bandName)
    node.put("value",value.toString())
    node.put("point", bbox )
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)
    val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir

    scpMetaCommand1.!


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
//    println("extent: " + (extent(0), extent(1), extent(2), extent(3)))
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

    if(args(2).split(",").length==4){

      queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    }else if(args(2).split(",").length==2){

      queryParams.setExtent(extent(0)-0.05, extent(1)-0.05, extent(0)+0.05, extent(1)+0.05)
    }


    queryParams.setTime(startTime, endTime)
    if(!args(6).isEmpty){
      println("非空参数")
      queryParams.setMeasurements(bandNames)
    }else{
      println("空参数")
      queryParams.setMeasurements(emptyBands)
    }

    //queryParams.setLevel("4000") //default 4000 in this version
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    val queryEnd = System.currentTimeMillis()
    //max
    val analysisBegin = System.currentTimeMillis()
    if(!args(6).isEmpty){
      println("非空参数")
      bandNames.foreach({band=>
        if(args(2).split(",").length==4){
          Area(tileLayerRddWithMeta, outputDir,args(2),band)
        }else if(args(2).split(",").length==2){
          Point(tileLayerRddWithMeta,outputDir,args(2),band)
        }
      })
    }else{
      println("空参数")
      val newBandNames = GcMeasurement.getMeasurementByProdName(cubeId,rasterProductNames.head,PostgresqlUtil.url,PostgresqlUtil.user,PostgresqlUtil.password)
      newBandNames.foreach({band=>
        if(args(2).split(",").length==4){
          Area(tileLayerRddWithMeta, outputDir,args(2),band)
        }else if(args(2).split(",").length==2){
          Point(tileLayerRddWithMeta,outputDir,args(2),band)
        }
      })
    }


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

