package whu.edu.cn.geocube.application.gdc

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{CellGrid, MultibandTile, Raster, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.JsonMethods.parse
import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.entity._
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.geocube.util.NetcdfUtil.{isAddDimensionCombineSame, isAddDimensionSame, rasterRDD2Netcdf}
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter


object gdcCoverage {
  /**
   * get coverage in certain format
   *
   * @param queryParams the query params
   * @param scaleSize   scale size
   * @param scaleAxes   scale axes
   * @param scaleFactor scale factor
   * @param outputDir   the output directory
   * @param imageFormat the format of coverage
   * @return the path of the coverage
   */
  def getCoveragePng(sc: SparkContext, queryParams: QueryParams, outputDir: String, imageFormat: String,
                     scaleSize: Array[java.lang.Integer] = null, scaleAxes: Array[java.lang.Double] = null,
                     scaleFactor: java.lang.Double = null): String = {
    //    val conf = new SparkConf()
    //      .setMaster("local[*]")
    //      .setAppName("gdcCoverage")
    //      .set("spark.driver.allowMultipleContexts", "true")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    //    val sc = new SparkContext(conf)
    var rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, queryParams)
    val crs = rasterTileLayerRdd._2.tileLayerMetadata.crs
    val measurements = queryParams.getMeasurements
    var outputPath: String = null
    if (imageFormat.equals("netcdf")) {
      outputPath = rasterRDD2Netcdf(measurements, rasterTileLayerRdd, queryParams, scaleSize, scaleAxes, scaleFactor, outputDir)
    }
    //    val postgresqlService = new PostgresqlService
    //    val startTime = queryParams.startTime
    //    val endTime = queryParams.endTime

    // 多余维度只取第一个
    //    val dimensionsRDD = rasterTileLayerRdd._1.map(v => (v._1.spaceTimeKey.time, v._1.measurementName, v._1.additionalDimensions))
    val minTime = rasterTileLayerRdd._2.tileLayerMetadata.bounds.get.minKey.time.toInstant.toEpochMilli
    //    val minTime = dimensionsRDD.map(_._1).distinct().map(zonedDateTime => {
    //      //      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z") // 定义日期时间格式
    //      //      zonedDateTime.format(formatter) // 将 ZonedDateTime 转换为字符串
    //      zonedDateTime.toInstant.toEpochMilli
    //    }).sortBy(identity).first()
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    rasterTileLayerRdd = (rasterTileLayerRdd.filter {
      rdd => {
        //        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")
        //        val zonedDateTime: ZonedDateTime = ZonedDateTime.parse(minTime, formatter)
        //        rdd._1.spaceTimeKey.time == zonedDateTime && isAddDimensionSame(rdd._1.additionalDimensions)
        rdd._1.spaceTimeKey.time.toInstant.toEpochMilli == minTime && isAddDimensionSame(rdd._1.additionalDimensions)
      }
    }, rasterTileLayerRdd._2)
    if (measurements.length > 1) {
      val stitchedTile = getMultiBandStitchRDD(measurements, rasterTileLayerRdd)
      outputPath = stitchRDD2ImageFile(queryParams, scaleSize, scaleAxes, scaleFactor, stitchedTile, outputDir, imageFormat, crs)
    } else {
      val stitchedTile = getSingleStitchRDD(rasterTileLayerRdd)
      outputPath = stitchRDD2ImageFile(queryParams, scaleSize, scaleAxes, scaleFactor, stitchedTile, outputDir, imageFormat, crs)
    }
    outputPath
  }

  /**
   * transform the stitchedRDD to the image file
   *
   * @param queryParams the query params
   * @param scaleSize   scale size
   * @param scaleAxes   scale axes
   * @param scaleFactor scale factor
   * @param stitched    stitched tile rdd
   * @param outputDir   the output directory
   * @param imageFormat the format of coverage: png or tif
   * @param crs         the crs of the coverage
   * @tparam T Tile or MultibandTile
   * @return the path of output file
   */
  def stitchRDD2ImageFile[T <: CellGrid[Int]](queryParams: QueryParams, scaleSize: Array[java.lang.Integer],
                                              scaleAxes: Array[java.lang.Double], scaleFactor: java.lang.Double,
                                              stitched: Raster[T], outputDir: String, imageFormat: String, crs: CRS): String = {
    val stitchedTile = stitched.tile
    var stitchedExtent = stitched.extent
    val queryExtentCoordinates = queryParams.extentCoordinates
    val cropExtent = getCropExtent(queryExtentCoordinates, stitchedExtent, stitched)
    val scaleExtent = scaleCoverage(stitchedTile, scaleSize, scaleAxes, scaleFactor)
    stitchedTile match {
      case t: Tile =>
        var stitchedSingleTile: Tile = t
        if (cropExtent != null) {
          stitchedSingleTile = t.crop(cropExtent._1, cropExtent._2, cropExtent._3, cropExtent._4)
          stitchedExtent = cropExtent._5
        }
        if (scaleExtent != null) {
          stitchedSingleTile = stitchedSingleTile.resample(scaleExtent._1, scaleExtent._2)
        }
        stitchRDDWriteFile(stitchedSingleTile, outputDir, imageFormat, stitchedExtent, crs)
      case mt: MultibandTile =>
        var stitchedMultiBandTile: MultibandTile = mt
        if (cropExtent != null) {
          stitchedMultiBandTile = mt.crop(cropExtent._1, cropExtent._2, cropExtent._3, cropExtent._4)
          stitchedExtent = cropExtent._5
        }
        if (scaleExtent != null) {
          stitchedMultiBandTile = stitchedMultiBandTile.resample(scaleExtent._1, scaleExtent._2)
        }
        stitchRDDWriteFile(stitchedMultiBandTile, outputDir, imageFormat, stitchedExtent, crs)
      case _ =>
        null
    }
  }

  /**
   * transform the rasterTileLayerRdd to stitched single band rdd
   *
   * @param rasterTileLayerRdd rasterTileLayerRdd
   * @return stitched tile
   */
  def getSingleStitchRDD(rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Raster[Tile] = {
    val rasterTileRdd = rasterTileLayerRdd.map(x => (x._1.spaceTimeKey.spatialKey, x._2))
    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)
    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(rasterTileRdd, spatialMetadata)
    tileLayerRdd.stitch()
  }

  /**
   * transform the rasterTileLayerRdd to stitched multiband rdd
   *
   * @param measurements       the bands list
   * @param rasterTileLayerRdd rasterTileLayerRdd
   * @return stitched tile
   */
  def getMultiBandStitchRDD(measurements: ArrayBuffer[String], rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])): Raster[MultibandTile] = {
    val rasterMultiBandTileRdd: RDD[(SpaceTimeKey, MultibandTile)] = rasterTileLayerRdd.map(x => (x._1.spaceTimeKey, x._1.measurementName, x._2)).groupBy(_._1).map {
      x => {
        val tilePair = x._2.toArray
        val multiBandTiles: ArrayBuffer[Tile] = new ArrayBuffer[Tile]()
        measurements.foreach { measurement => {
          tilePair.foreach { ele =>
            if (ele._2.equals(measurement)) {
              multiBandTiles.append(ele._3)
            }
          }
        }
        }
        (x._1, MultibandTile(multiBandTiles))
      }
    }
    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      metadata.cellType,
      metadata.layout,
      metadata.extent,
      metadata.crs,
      metadata.bounds.get.toSpatial)
    val tileLayerRdd: MultibandTileLayerRDD[SpatialKey] = ContextRDD(rasterMultiBandTileRdd.map { x => (x._1.spatialKey, x._2) }, spatialMetadata)
    tileLayerRdd.stitch()
  }

  /**
   * write the stitchedRDD in file
   *
   * @param stitchedTile   the stitched tile
   * @param outputDir      the output directory
   * @param imageFormat    the format of image: png or tif
   * @param stitchedExtent the extent of stitched tile
   * @param crs            the crs
   * @tparam T Tile or MultibandTile
   * @return the path of file
   */
  def stitchRDDWriteFile[T <: CellGrid[Int]](stitchedTile: T, outputDir: String, imageFormat: String, stitchedExtent: Extent, crs: CRS): String = {
    val time = System.currentTimeMillis()
    val sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val date = new Date(time)
    val instantRet = sdf.format(date)
    val outputImagePath = outputDir + "Coverage_" + instantRet + "." + imageFormat
    val outputFile = new File(outputDir)
    if (!outputFile.exists()) {
      outputFile.mkdir()
    }
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)

    if (imageFormat.equals("png")) {
      stitchedTile match {
        case t: Tile =>
          t.renderPng(colorRamp).write(outputImagePath)
        case mt: MultibandTile =>
          mt.renderPng().write(outputImagePath)
      }
    }
    if (imageFormat.equals("tif")) {
      stitchedTile match {
        case t: Tile =>
          GeoTiff(t, stitchedExtent, crs).write(outputImagePath)
        case mt: MultibandTile =>
          GeoTiff(mt, stitchedExtent, crs).write(outputImagePath)
      }
    }
    outputImagePath
  }

  /**
   * scale a coverage
   *
   * @param tile        拼接后的瓦片[Tile]
   * @param scaleSize   长宽缩放后的大小 [200, 300]
   * @param scaleAxes   长宽各自的缩放倍数 [1.5, 2.0]
   * @param scaleFactor 整体缩放倍数 2.0
   * @return resultTile
   */
  def scaleCoverage[T <: CellGrid[Int]](tile: T, scaleSize: Array[java.lang.Integer], scaleAxes: Array[java.lang.Double], scaleFactor: java.lang.Double): (Int, Int) = {
    var xScale = tile.cols
    var yScale = tile.rows
    if (scaleSize == null && scaleAxes == null && scaleFactor == null) {
      return null
    }
    if (scaleSize != null) {
      if (scaleSize(0) != null) {
        xScale = scaleSize(0)
      }
      if (scaleSize(1) != null) {
        yScale = scaleSize(1)
      }
    }
    if (scaleAxes != null) {
      if (scaleAxes(0) != null) {
        xScale = scala.math.round(scaleAxes(0) * xScale).toInt
      }
      if (scaleAxes(1) != null) {
        yScale = scala.math.round(scaleAxes(1) * yScale).toInt
      }
    }
    if (scaleFactor != null) {
      xScale = scala.math.round(scaleFactor * xScale).toInt
      yScale = scala.math.round(scaleFactor * yScale).toInt
    }
    (xScale, yScale)
  }

  /**
   * get the crop extent, if the extent is not changed return null
   *
   * @param queryExtentCoordinates the coordinates of query extent
   * @param srcExtent              the cropTile extent
   * @param cropTile               the tile will be croped
   * @tparam T Tile or MultibandTile
   * @return the crop extent
   */
  def getCropExtent[T <: CellGrid[Int]](queryExtentCoordinates: ArrayBuffer[Double], srcExtent: Extent, cropTile: Raster[T]): (Int, Int, Int, Int, Extent) = {
    if (queryExtentCoordinates(0) != srcExtent.xmin || queryExtentCoordinates(1) != srcExtent.ymin
      || queryExtentCoordinates(2) != srcExtent.xmax || queryExtentCoordinates(3) != srcExtent.ymax) {
      val requestedExtent = new Extent(queryExtentCoordinates(0), queryExtentCoordinates(1), queryExtentCoordinates(2), queryExtentCoordinates(3))
      if (requestedExtent.intersects(srcExtent)) {
        val intersection = requestedExtent.intersection(srcExtent).get
        val colMin = (intersection.xmin - srcExtent.xmin) / cropTile.cellSize.width
        val colMax = (intersection.xmax - srcExtent.xmin) / cropTile.cellSize.width
        val rowMin = (intersection.ymin - srcExtent.ymin) / cropTile.cellSize.height
        val rowMax = (intersection.ymax - srcExtent.ymin) / cropTile.cellSize.height
        (colMin.toInt, cropTile.rows - rowMax.toInt - 1, colMax.toInt, cropTile.rows - rowMin.toInt - 1, intersection.extent)
      } else null
    } else null
  }


  def main(args: Array[String]): Unit = {
    //     val extent2 =  reprojectExtent("hdfs://gisweb1:9000/tb19/SENTINEL/S2A_MSIL2A_20190109T110431_N0211_R094_T31UES_20190109T121746-B3.tif")
    //    val cubeId = args(0)
    //    val rasterProductName = args(1)
    //    val extent = args(2)
    //    val startTime = args(3)
    //    val endTime =  args(4)
    //    val measurement = args(5)
    //    val bbox = args(6)
    //    val coordinates = args(7)
    //    val outputDir = args(8)
    //    val imageFormat = args(9)
    //    val cubeId = "29"
    //    val cubeId = "30"
    //    val cubeId = "31"
    //    val cubeId = "33"
    //    val cubeId = "35"
    //    val rasterProductName = "Testbed_soilgrid_WGS84"
    //    val rasterProductName = "ht3e"
    //    val rasterProductName = "hsvs"
    //    val rasterProductName = "SENTINEL-2 Level-2A MSI"
    //    //    val extent = "-27.0499992370605469,32.9500007629394389,45.0499980531890003,73.5499992370605469"
    //    //    val extent = "-20,35,23,37"
    ////    val extent = "-30,32,0,50"
    ////    val extent = "-7,33,0,38"
    ////    val extent = "4.77,51.61,5.08,51.76"
    //    val extent = "4.42,51.63,4.8,51.84"
    ////    val extent = "-12,32.1,5.9,44"
    //    //    val extent = "3.65472,51.34007,5.27169,52.09142"
    //    //    val startTime = "2016-10-28 08:59:59"
    //    //    val endTime = "2016-10-28 09:00:01"
    ////    val startTime = "2020-09-15 08:59:59"
    ////    val endTime = "2020-09-15 09:00:01"
    //    val startTime = "2019-01-13 08:59:59"
    //    val endTime = "2019-01-19 09:00:01"
    //    //    val startTime = "1978-01-01 07:00:00"
    //    //    val endTime = "1978-01-01 22:23:32"
    //    //    val measurement = "DEM"
    //    val bbox = "null args"
    //    //    val bbox = "3.65472,51.34007,5.27169,52.09142"
    //    val coordinates = "null args"
    /////////////////////////Sentinel-2//////////////////////////////////////
    //    val cubeId = "35"
    //    val rasterProductName = "SENTINEL-2 Level-2A MSI"
    //    val extent = "4.42,51.63,4.8,51.84"
    ////    val extent = "4.695,51.716,4.7245,51.7392"
    //    val startTime = "2019-01-17 08:59:59"
    //    val endTime = "2019-01-19 09:00:01"
    //    val measurements: Array[String] = Array("B3")
    //    val bbox = "null args"
    //    val coordinates = "null args"
    ////////////////////////////////////////////////////////////////

    /////////////////////////Soil//////////////////////////////////////
//        val cubeId = "36"
//        val rasterProductName ="Testbed_soilgrid_WGS84"
//        val extent = "4.08,51.60,5.71,52.29"
//        val startTime = "2016-10-27 08:00:00"
//        val endTime = "2016-10-29 08:00:00"
//        val bbox = "null args"
//        val coordinates = "null args"
//        val measurements: Array[String] = Array("clay")
    ////////////////////////////////////////////////////////////////

    /////////////////////////Copernicus_EU_DEM//////////////////////////////////////
      val cubeId = "29"
      val rasterProductName ="Copernicus_EU_DEM"
      val extent = "0,35,2,37"
      val startTime = "1977-01-01 08:00:00"
      val endTime = "1978-01-01 18:00:00"
      val bbox = "null args"
      val coordinates = "null args"
//      val measurements: Array[String] = Array("clay")
    ////////////////////////////////////////////////////////////////

    /////////////////////////hsvs//////////////////////////////////////
//    val cubeId = "33"
//    val rasterProductName = "hsvs"
//    val extent = "3.7,51.34,5.27,52.09"
//    val startTime = "2020-09-16 05:00:00"
//    val endTime = "2020-09-16 06:00:00"
//    val bbox = "null args"
//    val coordinates = "null args"
//    val measurements: Array[String] = Array("Divergence")
    ////////////////////////////////////////////////////////////////

    ///////////////////////sentinel-2 - test //////////////////////////////////////
    //    val cubeId = "37"
    //    val rasterProductName = "SENTINEL-2 Level-2A MSI"
    //    val extent = "4.42,51.63,4.8,51.84"
    //    //    val extent = "3.88,51.52,4.187,51.759"
    //    //    val extent = "4.695,51.716,4.7245,51.7392"
    //    //    val startTime = "2019-03-16 08:59:59"
    //    //    val endTime = "2019-03-18 09:00:01"
    //    val startTime = "2019-01-18 20:59:59"
    //    val endTime = "2019-01-19 21:00:01"
    //    val measurements: Array[String] = Array("B8")
    //    val bbox = "null args"
    //    val coordinates = "null args"
    ////////////////////////////////////////////////////////////////

    //    val outputDir = "E:\\LaoK\\data2\\GDC_API\\"
    //    val imageFormat = "tif"
    //    //    val imageFormat = "netcdf"
        val queryParams = new QueryParams()
    //    //
        queryParams.setCubeId(cubeId)
        queryParams.setRasterProductName(rasterProductName)
    //
        queryParams.setExtent(extent.split(",")(0).toDouble, extent.split(",")(1).toDouble, extent.split(",")(2).toDouble, extent.split(",")(3).toDouble)
        queryParams.setTime(startTime, endTime)
    //    //    //    //    val measurements : Array[String] = Array(measurement)
    //    //    //
    //    //    //    //    val measurements: Array[String] = Array("cec_0-5cm_mean")
//        queryParams.setMeasurements(measurements)
    //    val subsetQuery = new SubsetQuery()
    //    //    subsetQuery.setPoint(Integer.valueOf(1000))
    //    //    subsetQuery.setPoint(Integer.valueOf(925))
    //    //    subsetQuery.setLowerPoint(Integer.valueOf(925))
    //    //    subsetQuery.setHighPoint(Integer.valueOf(1000))
    //    //    subsetQuery.setAxisName("preasure")
    //    //    subsetQuery.setIsNumber(true)
    //    //    subsetQuery.setInterval(true)
    //    //    subsetQuery.setInterval(false)
    //    //    queryParams.setSubsetQuery(ArrayBuffer[SubsetQuery](subsetQuery))
    //    //    var gisUtil = new GISUtil()
    //    //    val wkt = gisUtil.DoubleToWKT(extent.split(",")(0).toDouble, extent.split(",")(1).toDouble,extent.split(",")(2).toDouble,extent.split(",")(3).toDouble)
    //    //    getGeoTrellisTilesCode(wkt, )
    //    //query and access
    //    val timeBegin = System.currentTimeMillis()
    //    //    val pngPath =  getCoveragePng(queryParams,bbox,coordinates,outputDir,imageFormat)
    //    //    val scaleSize: Array[java.lang.Integer] = Array[java.lang.Integer](4440, 4440)
    //    val scaleAxes: Array[java.lang.Double] = Array[java.lang.Double](null, 2)
    //    //    val pngPath =  getCoveragePng(queryParams, scaleSize, null, null, outputDir,imageFormat)
    //    val pngPath = getCoveragePng(queryParams, null, null, null, outputDir, imageFormat)
    //
    //    val timeEnd = System.currentTimeMillis()
    //    println("time cost: " + (timeEnd - timeBegin))
    //    case class City(name: String, funActivity: String, latitude: Double)
    //    val bengaluru = City("Bengaluru", "South Indian food", 12.97)
    //    implicit val cityRW = upickle.default.macroRW[City]
    //    val a = upickle.default.write(bengaluru)
    //    queryParams.setPolygon(null)
    //    //    val a = upickle.default.write(queryParams)
    //    //    val string = write(queryParams)
    //    val workflowCollectionParam = new WorkflowCollectionParam()
    //    workflowCollectionParam.queryParams = queryParams
    //
    //
    //    //    val a = writePretty(queryParams)
    //    val a = workflowCollectionParam.toJSONString
    //    //    val s = writePretty(workflowCollectionParam)
    //    //    val string = """{"rasterProductName":"SENTINEL-2 Level-2A MSI","platform":"","instruments":[],"measurements":[\"B8\"],\"CRS\":\"\",\"tileSize\":\"\",\"cellRes\":\"\",\"level\":\"\",\"phenomenonTime\":\"\",\"startTime\":\"2019-01-18 20:59:59\",\"endTime\":\"2019-01-19 21:00:01\",\"gridCodes\":[\"246\",\"244\",\"222\",\"247\",\"245\",\"223\",\"418\",\"416\",\"394\",\"419\",\"417\",\"395\",\"422\",\"420\",\"398\"],\"cityCodes\":[],\"cityNames\":[]}""".replace("\\","")
    //    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    //    val queryParams2 = parse(a).extract[WorkflowCollectionParam]
    //    println(a)
    //    println(s)

    //////////////////////////////////////////////////////////////
        val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("gdcCoverage")
          .set("spark.driver.allowMultipleContexts", "true")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        val sc = new SparkContext(conf)

//    val conf = new SparkConf()
//      .setAppName("Get Coverage")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//    val sc = new SparkContext(conf)
    val outputDir = "E:\\LaoK\\data2\\GDC_API\\"
//    val outputDir = args(0)
//    val collectionParamStr = args(1)
//    implicit val formats: Formats = Serialization.formats(NoTypeHints)
//    val workflowCollectionParam: WorkflowCollectionParam = parse(collectionParamStr).extract[WorkflowCollectionParam]
//    // val workflowCollectionParam: WorkflowCollectionParam = JSON.parseObject(collectionParamStr, classOf[WorkflowCollectionParam])
//    val queryParams = workflowCollectionParam.queryParams
//    queryParams.updatePolygon()
//    var scaleFactor: java.lang.Double = null
//    workflowCollectionParam.scaleFactor match {
//      case Some(value) =>
//        scaleFactor = java.lang.Double.valueOf(value)
//      case None =>
//    }
//    var scaleSize: Array[java.lang.Integer] = null
//    if (workflowCollectionParam.getScaleSize != null && (!workflowCollectionParam.getScaleSize.isEmpty)) {
//      scaleSize = workflowCollectionParam.getScaleSize.map(java.lang.Integer.valueOf)
//    }
//    var scaleAxes: Array[java.lang.Double] = null
//    if (workflowCollectionParam.getScaleAxes != null && (!workflowCollectionParam.getScaleAxes.isEmpty)) {
//      scaleAxes = workflowCollectionParam.getScaleAxes.map(java.lang.Double.valueOf)
//    }
//    getCoveragePng(sc, queryParams, outputDir, workflowCollectionParam.getImageFormat, scaleSize,
//      scaleAxes, scaleFactor)

    getCoveragePng(sc, queryParams, outputDir, "tif", null,
      null, null)
  }


  def runGetCoverage(implicit sc: SparkContext, outputDir: String, collectionParamStr: String): Unit = {
    //    val conf = new SparkConf()
    //      .setAppName("Get Coverage")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    //    val sc = new SparkContext(conf)
    //    //    val outputDir = "E:\\LaoK\\data2\\GDC_API\\"
//    val outputDir = args(0)
//    val collectionParamStr = args(1)
    println(outputDir)
    println(collectionParamStr)
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val workflowCollectionParam: WorkflowCollectionParam = parse(collectionParamStr).extract[WorkflowCollectionParam]
    // val workflowCollectionParam: WorkflowCollectionParam = JSON.parseObject(collectionParamStr, classOf[WorkflowCollectionParam])
    val queryParams = workflowCollectionParam.queryParams
    queryParams.updatePolygon()
    var scaleFactor: java.lang.Double = null
    workflowCollectionParam.scaleFactor match {
      case Some(value) =>
        scaleFactor = java.lang.Double.valueOf(value)
      case None =>
    }
    var scaleSize: Array[java.lang.Integer] = null
    if (workflowCollectionParam.getScaleSize != null && (!workflowCollectionParam.getScaleSize.isEmpty)) {
      scaleSize = workflowCollectionParam.getScaleSize.map(java.lang.Integer.valueOf)
    }
    var scaleAxes: Array[java.lang.Double] = null
    if (workflowCollectionParam.getScaleAxes != null && (!workflowCollectionParam.getScaleAxes.isEmpty)) {
      scaleAxes = workflowCollectionParam.getScaleAxes.map(java.lang.Double.valueOf)
    }
    println("workflowCollectionParam:--------" )
    getCoveragePng(sc, queryParams, outputDir, workflowCollectionParam.getImageFormat, scaleSize,
      scaleAxes, scaleFactor)
  }


  //    getCoveragePng(sc, queryParams, "E:\\LaoK\\data2\\GDC_API\\", "tif", null,
  //      null, null)
  //  }
}
