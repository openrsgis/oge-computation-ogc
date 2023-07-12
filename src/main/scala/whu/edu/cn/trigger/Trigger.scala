package whu.edu.cn.trigger

import com.alibaba.fastjson.{JSON, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import redis.clients.jedis.Jedis
import whu.edu.cn.entity.OGEClassType.OGEClassType
import whu.edu.cn.entity.{CoverageCollectionMetadata, OGEClassType, RawTile, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.oge._
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.{GlobalConstantUtil, JedisUtil, ZCurveUtil}

import scala.collection.{immutable, mutable}
import scala.io.{BufferedSource, Source}
import scala.util.Random

object Trigger {
  var optimizedDagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]
  var coverageCollectionMetadata: mutable.Map[String, CoverageCollectionMetadata] = mutable.Map.empty[String, CoverageCollectionMetadata]
  var lazyFunc: mutable.Map[String, (String, mutable.Map[String, String])] = mutable.Map.empty[String, (String, mutable.Map[String, String])]
  var coverageCollectionRddList: mutable.Map[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]] = mutable.Map.empty[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]]
  var coverageRddList: mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]


  // TODO lrx: 以下为未检验

  var tableRddList: mutable.Map[String, String] = mutable.Map.empty[String, String]
  var kernelRddList: mutable.Map[String, geotrellis.raster.mapalgebra.focal.Kernel] = mutable.Map.empty[String, geotrellis.raster.mapalgebra.focal.Kernel]
  var featureRddList: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

  var cubeRDDList: mutable.Map[String, mutable.Map[String, Any]] = mutable.Map.empty[String, mutable.Map[String, Any]]
  var cubeLoad: mutable.Map[String, (String, String, String)] = mutable.Map.empty[String, (String, String, String)]


  var level: Int = _
  var layerName: String = _
  var windowExtent: Extent = _
  var isBatch: Int = _
  // 此次计算工作的任务json
  var workTaskJson: String = _
  // DAG-ID
  var dagId: String = _
  val zIndexStrArray = new mutable.ArrayBuffer[String]

  def isOptionalArg(args: mutable.Map[String, String], name: String): String = {
    if (args.contains(name)) {
      args(name)
    }
    else {
      null
    }
  }

  def isActioned(implicit sc: SparkContext, UUID: String, typeEnum: OGEClassType): Unit = {
    typeEnum match {
      case OGEClassType.CoverageCollection =>
        if (!coverageCollectionRddList.contains(UUID)) {
          val metadata: CoverageCollectionMetadata = coverageCollectionMetadata(UUID)
          coverageCollectionRddList += (UUID -> CoverageCollection.load(sc, productName = metadata.productName, sensorName = metadata.sensorName, measurementName = metadata.measurementName, startTime = metadata.startTime, endTime = metadata.endTime, extent = metadata.extent, crs = metadata.crs, level = level))
        }
    }
  }

  def action(): Unit = {

  }

  @throws(classOf[Throwable])
  def func(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {


    try {

      funcName match {

        // Service
        case "Service.getCoverageCollection" =>
          lazyFunc += (UUID -> (funcName, args))
          coverageCollectionMetadata += (UUID -> Service.getCoverageCollection(args("productID"), dateTime = isOptionalArg(args, "datetime"), extent = isOptionalArg(args, "bbox")))
        case "Service.getCoverage" =>
          coverageRddList += (UUID -> Service.getCoverage(sc, isOptionalArg(args, "coverageID"), level = level))
        case "Service.getTable" =>
          tableRddList += (UUID -> isOptionalArg(args, "productID"))
        case "Service.getFeatureCollection" =>
          featureRddList += (UUID -> isOptionalArg(args, "productID"))

        // Filter // TODO lrx: 待完善Filter类的函数
        case "Filter.equals" =>
          lazyFunc += (UUID -> (funcName, args))
        case "Filter.and" =>
          lazyFunc += (UUID -> (funcName, args))

        // Collection
        case "Collection.map" =>
          if (lazyFunc(args("collection"))._1 == "Service.getCoverageCollection") {
            isActioned(sc, args("collection"), OGEClassType.CoverageCollection)
            coverageCollectionRddList += (UUID -> CoverageCollection.map(sc, coverageCollection = coverageCollectionRddList(args("collection")), baseAlgorithm = args("baseAlgorithm")))
          }

        // CoverageCollection
        case "CoverageCollection.filter" =>
          coverageCollectionMetadata += (UUID -> CoverageCollection.filter(filter = args("filter"), collection = coverageCollectionMetadata(args("collection"))))
        case "CoverageCollection.mosaic" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.mosaic(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.mean" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.mean(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.min" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.min(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.max" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.max(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.sum" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.sum(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.or" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.or(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.and" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.and(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.median" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.median(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.mode" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.mode(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.addStyles" =>
          if (isBatch == 0) {
            isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
            val visParam: VisualizationParam = new VisualizationParam
            visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
            CoverageCollection.visualizeOnTheFly(sc, coverageCollection = coverageCollectionRddList(args("coverageCollection")), visParam = visParam)
          }
          else {
            CoverageCollection.visualizeBatch(sc, coverageCollection = coverageCollectionRddList(args("coverageCollection")))
          }

      // TODO lrx: 这里要改造
      // Table
      case "Table.getDownloadUrl" =>
        Table.getDownloadUrl(url = tableRddList(isOptionalArg(args, "input")), fileName = "fileName")
      case "Table.addStyles" =>
        Table.getDownloadUrl(url = tableRddList(isOptionalArg(args, "input")), fileName = " fileName")

      // Coverage
      case "Coverage.subtract" =>
        coverageRddList += (UUID -> Coverage.subtract(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.add" =>
        coverageRddList += (UUID -> Coverage.add(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.mod" =>
        coverageRddList += (UUID -> Coverage.mod(coverage1 = coverageRddList(args("coverage1")), coverage2 =
          coverageRddList(args("coverage2"))))
      case "Coverage.divide" =>
        coverageRddList += (UUID -> Coverage.divide(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.multiply" =>
        coverageRddList += (UUID -> Coverage.multiply(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.multiply" =>
        coverageRddList += (UUID -> Coverage.multiply(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.normalizedDifference" =>
        coverageRddList += (UUID -> Coverage.normalizedDifference(coverageRddList(args("coverage")), bandNames = args("bandNames").substring(1, args("bandNames").length - 1).split(",").toList))
      case "Coverage.binarization" =>
        coverageRddList += (UUID -> Coverage.binarization(coverage = coverageRddList(args("coverage")), threshold = args("threshold").toInt))
      case "Coverage.and" =>
        coverageRddList += (UUID -> Coverage.and(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.xor" =>
        coverageRddList += (UUID -> Coverage.xor(coverage1 = coverageRddList(args("coverage1")), coverage2 =
          coverageRddList(args("coverage2"))))
      case "Coverage.or" =>
        coverageRddList += (UUID -> Coverage.or(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.not" =>
        coverageRddList += (UUID -> Coverage.not(coverage = coverageRddList(args("coverage"))))
      case "Coverage.ceil" =>
        coverageRddList += (UUID -> Coverage.ceil(coverage = coverageRddList(args("coverage"))))
      case "Coverage.floor" =>
        coverageRddList += (UUID -> Coverage.floor(coverage = coverageRddList(args("coverage"))))
      case "Coverage.sin" =>
        coverageRddList += (UUID -> Coverage.sin(coverage = coverageRddList(args("coverage"))))
      case "Coverage.cos" =>
        coverageRddList += (UUID -> Coverage.cos(coverage = coverageRddList(args("coverage"))))
      case "Coverage.sinh" =>
        coverageRddList += (UUID -> Coverage.sinh(coverage = coverageRddList(args("coverage"))))
      case "Coverage.cosh" =>
        coverageRddList += (UUID -> Coverage.cosh(coverage = coverageRddList(args("coverage"))))
      case "Coverage.asin" =>
        coverageRddList += (UUID -> Coverage.asin(coverage = coverageRddList(args("coverage"))))
      case "Coverage.acos" =>
        coverageRddList += (UUID -> Coverage.acos(coverage = coverageRddList(args("coverage"))))
      case "Coverage.atan" =>
        coverageRddList += (UUID -> Coverage.atan(coverage = coverageRddList(args("coverage"))))
      case "Coverage.atan2" =>
        coverageRddList += (UUID -> Coverage.atan2(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.eq" =>
        coverageRddList += (UUID -> Coverage.eq(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.gt" =>
        coverageRddList += (UUID -> Coverage.gt(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.gte" =>
        coverageRddList += (UUID -> Coverage.gte(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.selectBands" =>
        coverageRddList += (UUID -> Coverage.selectBands(coverage = coverageRddList(args("coverage")), bands = args("bands").substring(1, args("bands").length - 1).split(",").toList))
      case "Coverage.addBands" =>
        val names: List[String] = args("names").split(",").toList
        coverageRddList += (UUID -> Coverage.addBands(dstCoverage = coverageRddList(args("coverage1")), srcCoverage =
          coverageRddList(args("coverage2")), names = names))
      case "Coverage.bandNames" =>
        val bandNames: List[String] = Coverage.bandNames(coverage = coverageRddList(args("coverage")))
        println("******************test bandNames***********************")
        println(bandNames)
        println(bandNames.length)

      case "Coverage.abs" =>
        coverageRddList += (UUID -> Coverage.abs(coverage = coverageRddList(args("coverage"))))
      case "Coverage.neq" =>
        coverageRddList += (UUID -> Coverage.neq(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.signum" =>
        coverageRddList += (UUID -> Coverage.signum(coverage = coverageRddList(args("coverage"))))
      case "Coverage.bandTypes" =>
        val bandTypes: immutable.Map[String, String] = Coverage.bandTypes(coverage = coverageRddList(args("coverage")))
      case "Coverage.rename" =>
        coverageRddList += (UUID -> Coverage.rename(coverage = coverageRddList(args("coverage")), name = args("name").split(",").toList))
      case "Coverage.pow" =>
        coverageRddList += (UUID -> Coverage.pow(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.mini" =>
        coverageRddList += (UUID -> Coverage.mini(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.maxi" =>
        coverageRddList += (UUID -> Coverage.maxi(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
      case "Coverage.focalMean" =>
        coverageRddList += (UUID -> Coverage.focalMean(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      case "Coverage.focalMedian" =>
        coverageRddList += (UUID -> Coverage.focalMedian(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      case "Coverage.focalMin" =>
        coverageRddList += (UUID -> Coverage.focalMin(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      case "Coverage.focalMax" =>
        coverageRddList += (UUID -> Coverage.focalMax(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      case "Coverage.focalMode" =>
        coverageRddList += (UUID -> Coverage.focalMode(coverage = coverageRddList(args("coverage")), kernelType = args("kernelType"), radius = args("radius").toInt))
      case "Coverage.convolve" =>
        coverageRddList += (UUID -> Coverage.convolve(coverage = coverageRddList(args("coverage")), kernel = kernelRddList(args("kernel"))))
      case "Coverage.projection" =>
        val projection: String = Coverage.projection(coverage = coverageRddList(args("coverage")))
        println(projection)
        case "Coverage.remap" =>
          coverageRddList+=(UUID ->Coverage.remap(coverage = coverageRddList(args("coverage")), args("from").slice(1,
            args("from").length-1).split(',').toList.map(_.toInt), args("to").slice(1,
            args("to").length - 1).split(',').toList.map(_.toDouble),Option(isOptionalArg(args,"defaultValue")
            .toDouble)))
        case "Coverage.polynomial" =>
          coverageRddList+=(UUID -> Coverage.polynomial(coverage = coverageRddList(args("coverage")),args("l").slice(1,
            args("l").length - 1).split(',').toList.map(_.toDouble)))
      //      case "Coverage.histogram" =>
      //        val hist = Coverage.histogram(coverage = coverageRddList(args("coverage")))
      //        println(hist)
      //      case "Coverage.reproject" =>
      //        coverageRddList += (UUID -> Coverage.reproject(coverage = coverageRddList(args("coverage")), newProjectionCode = args("crsCode").toInt, resolution = args("resolution").toInt))
        case "Coverage.resample" =>
          coverageRddList += (UUID -> Coverage.resample(coverage = coverageRddList(args("coverage")), mode = args("mode")))
        case "Coverage.gradient" =>
          coverageRddList += (UUID -> Coverage.gradient(coverage = coverageRddList(args("coverage"))))
        case "Coverage.clip" =>
          coverageRddList += (UUID -> Coverage.clip(coverage = coverageRddList(args("coverage")), geom = featureRddList(args("geom")).asInstanceOf[Geometry]))
        case "Coverage.clamp" =>
          coverageRddList += (UUID -> Coverage.clamp(coverage = coverageRddList(args("coverage")), low = args("low").toInt, high = args("high").toInt))
      case "Coverage.rgbToHsv" =>
        coverageRddList += (UUID -> Coverage.rgbToHsv(coverage=coverageRddList(args("coverage"))))
      case "Coverage.hsvToRgb" =>
        coverageRddList += (UUID -> Coverage.hsvToRgb(coverage=coverageRddList(args("coverage"))))
      case "Coverage.entropy" =>
        coverageRddList += (UUID -> Coverage.entropy(coverage = coverageRddList(args("coverage")),"square", radius = args("radius").toInt))
      //      case "Coverage.NDVI" =>
      //        coverageRddList += (UUID -> Coverage.NDVI(NIR = coverageRddList(args("NIR")), Red = coverageRddList(args("Red"))))
      //      case "Coverage.cbrt" =>
      //        coverageRddList += (UUID -> Coverage.cbrt(coverage = coverageRddList(args("coverage"))))
      case "Coverage.metadata" =>
        val metadataString = Coverage.metadata(coverage = coverageRddList(args("coverage")))
        println(metadataString)
      case "Coverage.mask" =>
        coverageRddList += (UUID -> Coverage.mask(coverageRddList(args("coverage1")), coverageRddList(args
        ("coverage2")), args("readMask").toInt, args("writeMask").toInt))
      //        println(metadataString)
      case "Coverage.toInt8" =>
        coverageRddList += (UUID -> Coverage.toInt8(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toUint8" =>
        coverageRddList += (UUID -> Coverage.toUint8(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toInt16" =>
        coverageRddList += (UUID -> Coverage.toInt16(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toUint16" =>
        coverageRddList += (UUID -> Coverage.toUint16(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toInt32" =>
        coverageRddList += (UUID -> Coverage.toInt32(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toFloat" =>
        coverageRddList += (UUID -> Coverage.toFloat(coverage = coverageRddList(args("coverage"))))
      case "Coverage.toDouble" =>
        coverageRddList += (UUID -> Coverage.toDouble(coverage = coverageRddList(args("coverage"))))

      // Kernel
      case "Kernel.fixed" =>
        val kernel = Kernel.fixed(weights = args("weights"))
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.square" =>
        val kernel = Kernel.square(radius = args("radius").toInt, normalize = args("normalize").toBoolean, value = args("value").toDouble)
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.prewitt" =>
        val kernel = Kernel.prewitt(axis = args("axis"))
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.kirsch" =>
        val kernel = Kernel.kirsch(axis = args("axis"))
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.sobel" =>
        val kernel = Kernel.sobel(axis = args("axis"))
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())

      case "Kernel.laplacian4" =>
        val kernel = Kernel.laplacian4()
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.laplacian8" =>
        val kernel = Kernel.laplacian8()
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.laplacian8" =>
        val kernel = Kernel.laplacian8()
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())
      case "Kernel.add" =>
        val kernel = Kernel.add(kernel1 = kernelRddList(args("kernel1")), kernel2 = kernelRddList(args("kernel2")))
        kernelRddList += (UUID -> kernel)
        print(kernel.tile.asciiDraw())

      // Terrain
      //      case "Terrain.slope" =>
      //        coverageRddList += (UUID -> Terrain.slope(coverage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
      //      case "Terrain.aspect" =>
      //        coverageRddList += (UUID -> Terrain.aspect(coverage = coverageRddList(args("coverage")), radius = args("radius").toInt))

      case "Coverage.addStyles" =>
        if (isBatch == 0) {
          val visParam: VisualizationParam = new VisualizationParam
          visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))


          Coverage.visualizeOnTheFly(sc, coverage = coverageRddList(args("coverage")), visParam = visParam)
        }
        else {
          Coverage.visualizeBatch(sc, coverage = coverageRddList(args("coverage")))
        }


      //Feature
      case "Feature.load" =>
        var dateTime = isOptionalArg(args, "dateTime")
        if (dateTime == "null")
          dateTime = null
        println("dateTime:" + dateTime)
        if (dateTime != null) {
          if (isOptionalArg(args, "crs") != null)
            featureRddList += (UUID -> Feature.load(sc, args("productName"), args("dateTime"), args("crs")))
          else
            featureRddList += (UUID -> Feature.load(sc, args("productName"), args("dateTime")))
        }
        else
          featureRddList += (UUID -> Feature.load(sc, args("productName")))
      case "Feature.point" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.point(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.point(sc, args("coors"), args("properties")))
      case "Feature.lineString" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.lineString(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.lineString(sc, args("coors"), args("properties")))
      case "Feature.linearRing" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.linearRing(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.linearRing(sc, args("coors"), args("properties")))
      case "Feature.polygon" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.polygon(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.polygon(sc, args("coors"), args("properties")))
      case "Feature.multiPoint" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.multiPoint(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.multiPoint(sc, args("coors"), args("properties")))
      case "Feature.multiLineString" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.multiLineString(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.multiLineString(sc, args("coors"), args("properties")))
      case "Feature.multiPolygon" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.multiPolygon(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.multiPolygon(sc, args("coors"), args("properties")))
      case "Feature.geometry" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.geometry(sc, args("coors"), args("properties"), args("crs")))
        else
          featureRddList += (UUID -> Feature.geometry(sc, args("coors"), args("properties")))
      case "Feature.area" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.bounds" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.bounds(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.bounds(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.centroid" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.centroid(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.centroid(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.buffer" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.buffer(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble, args("crs")))
        else
          featureRddList += (UUID -> Feature.buffer(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble))
      case "Feature.convexHull" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.convexHull(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.convexHull(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.coordinates" =>
        featureRddList += (UUID -> Feature.coordinates(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.reproject" =>
        featureRddList += (UUID -> Feature.reproject(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("tarCrsCode")))
      case "Feature.isUnbounded" =>
        featureRddList += (UUID -> Feature.isUnbounded(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.getType" =>
        featureRddList += (UUID -> Feature.getType(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.projection" =>
        featureRddList += (UUID -> Feature.projection(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.toGeoJSONString" =>
        featureRddList += (UUID -> Feature.toGeoJSONString(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.getLength" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.getLength(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.getLength(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.geometries" =>
        featureRddList += (UUID -> Feature.geometries(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.dissolve" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.contains" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.containedIn" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.disjoint" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.distance" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.difference" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.difference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.difference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.intersection" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.intersection(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.intersection(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.intersects" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.intersects(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.intersects(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.symmetricDifference" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.symmetricDifference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.symmetricDifference(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.union" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.union(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
        else
          featureRddList += (UUID -> Feature.union(sc, featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.withDistance" =>
        if (isOptionalArg(args, "crs") != null)
          featureRddList += (UUID -> Feature.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble, args("crs")))
        else
          featureRddList += (UUID -> Feature.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble))
      case "Feature.copyProperties" =>
        val propertyList = args("properties").replace("[", "").replace("]", "")
          .replace("\"", "").split(",").toList
        featureRddList += (UUID -> Feature.copyProperties(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
          featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], propertyList))
      case "Feature.get" =>
        featureRddList += (UUID -> Feature.get(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
      case "Feature.getNumber" =>
        featureRddList += (UUID -> Feature.getNumber(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
      case "Feature.getString" =>
        featureRddList += (UUID -> Feature.getString(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
      case "Feature.getArray" =>
        featureRddList += (UUID -> Feature.getArray(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
      case "Feature.propertyNames" =>
        featureRddList += (UUID -> Feature.propertyNames(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.set" =>
        featureRddList += (UUID -> Feature.set(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
      case "Feature.setGeometry" =>
        featureRddList += (UUID -> Feature.setGeometry(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
          featureRddList(args("geometry")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      case "Feature.setGeometry" =>
        featureRddList += (UUID -> Feature.setGeometry(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
          featureRddList(args("geometry")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
      //      case "Feature.inverseDistanceWeighted" =>
      //        coverageRddList += (UUID -> Feature.inverseDistanceWeighted(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
      //          args("propertyName"), featureRddList(args("maskGeom")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))

        //Cube
        case "Service.getCollections" =>
          cubeLoad += (UUID -> (isOptionalArg(args, "productIDs"), isOptionalArg(args, "datetime"), isOptionalArg(args, "bbox")))
        case "Collections.toCube" =>
          cubeRDDList += (UUID -> Cube.load(sc, productList = cubeLoad(args("input"))._1, dateTime = cubeLoad(args("input"))._2, geom = cubeLoad(args("input"))._3, bandList = isOptionalArg(args, "bands")))
        case "Cube.NDWI" =>
          cubeRDDList += (UUID -> Cube.NDWI(input = cubeRDDList(args("input")), product = isOptionalArg(args, "product"), name = isOptionalArg(args, "name")))
        case "Cube.binarization" =>
          cubeRDDList += (UUID -> Cube.binarization(input = cubeRDDList(args("input")), product = isOptionalArg(args, "product"), name = isOptionalArg(args, "name"),
            threshold = isOptionalArg(args, "threshold").toDouble))
        case "Cube.subtract" =>
          cubeRDDList += (UUID -> Cube.WaterChangeDetection(input = cubeRDDList(args("input")), product = isOptionalArg(args, "product"),
            certainTimes = isOptionalArg(args, "timeList"), name = isOptionalArg(args, "name")))
        case "Cube.overlayAnalysis" =>
          cubeRDDList += (UUID -> Cube.OverlayAnalysis(input = cubeRDDList(args("input")), rasterOrTabular = isOptionalArg(args, "raster"), vector = isOptionalArg(args, "vector"), name = isOptionalArg(args, "name")))
        case "Cube.addStyles" =>
          Cube.visualize(sc, cube = cubeRDDList(args("cube")), products = isOptionalArg(args, "products"))
      }

    } catch {
      case e: Throwable => throw e
    } finally {
      // 清空list
      Trigger.optimizedDagMap.clear()
      Trigger.coverageCollectionMetadata.clear()
      Trigger.lazyFunc.clear()
      Trigger.coverageCollectionRddList.clear()
      Trigger.coverageRddList.clear()
      Trigger.zIndexStrArray.clear()
      JsonToArg.dagMap.clear()
      // TODO forDece: 以下为未检验
      Trigger.tableRddList.clear()
      Trigger.kernelRddList.clear()
      Trigger.featureRddList.clear()
      Trigger.cubeRDDList.clear()
      Trigger.cubeLoad.clear()
    }
  }

  def lambda(implicit sc: SparkContext, list: mutable.ArrayBuffer[Tuple3[String, String, mutable.Map[String, String]]]): Unit = {
    for (i <- list.indices) {
      try {
        func(sc, list(i)._1, list(i)._2, list(i)._3)
      } catch {
        case e: Throwable =>
          throw new Exception("Error occur in lambda: " +
            "UUID = " + list(i)._1 + "\t" +
            "funcName = " + list(i)._2 + "\n" +
            "innerErrorType = " + e.getClass + "\n" +
            "innerErrorInfo = " + e.getMessage + "\n" +
            e.getStackTrace.mkString("StackTrace:(\n", "\n", "\n)"))
      }
    }
  }

  def optimizedDAG(list: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    val duplicates: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = removeDuplicates(list)
    val checked: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = checkMap(duplicates)
    checked
  }

  def checkMap(collection: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    var result: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = collection
    for ((_, second, third) <- result) {
      if (second == "Collection.map") {
        for (tuple <- third) {
          if (tuple._1 == "baseAlgorithm") {
            for ((f, s, t) <- collection) {
              if (f == tuple._2) {
                lazyFunc += (f -> (s, t))
                result = result.filter(t => t._1 != f)
              }
            }
          }
        }
      }
    }
    result
  }

  def removeDuplicates(collection: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    val seen: mutable.HashMap[(String, mutable.Map[String, String]), String] = mutable.HashMap[(String, mutable.Map[String, String]), String]()
    val result: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]()
    val duplicates: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer[(String, String)]()

    for ((first, second, third) <- collection) {
      val key: (String, mutable.Map[String, String]) = (second, third)
      if (seen.contains(key)) {
        duplicates += Tuple2(first, seen(key))
      } else {
        seen(key) = first
        result += ((first, second, third))
      }
    }

    for ((_, _, third) <- result) {
      for (duplicate <- duplicates) {
        for (tuple <- third) {
          if (tuple._2 == duplicate._1) {
            third.remove(tuple._1)
            third.put(tuple._1, duplicate._2)
          }
        }
      }
    }
    result
  }


  def runMain(implicit sc: SparkContext,
              curWorkTaskJson: String,
              curDagId: String): Unit = {

    /* sc,workTaskJson,workID,originTaskID */
    workTaskJson = curWorkTaskJson
    dagId = curDagId

    val time1: Long = System.currentTimeMillis()

    val jsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(jsonObject)

    isBatch = jsonObject.getString("isBatch").toInt
    if (isBatch == 0) {
      layerName = jsonObject.getString("layerName")
      val map: JSONObject = jsonObject.getJSONObject("map")
      level = map.getString("level").toInt
      val spatialRange: Array[Double] = map.getString("spatialRange")
        .substring(1, map.getString("spatialRange").length - 1).split(",").map(_.toDouble)
      println("spatialRange = " + spatialRange.mkString("Array(", ", ", ")"))

      windowExtent = new Extent(spatialRange.head, spatialRange(1), spatialRange(2), spatialRange(3))

      val jedis: Jedis = new JedisUtil().getJedis
      val key: String = dagId + ":solvedTile:" + level
      jedis.select(1)

      val xMinOfTile: Int = ZCurveUtil.lon2Tile(windowExtent.xmin, level)
      val xMaxOfTile: Int = ZCurveUtil.lon2Tile(windowExtent.xmax, level)
      val yMinOfTile: Int = ZCurveUtil.lat2Tile(windowExtent.ymax, level)
      val yMaxOfTile: Int = ZCurveUtil.lat2Tile(windowExtent.ymin, level)

      System.out.println("xMinOfTile - xMaxOfTile: " + xMinOfTile + " - " + xMaxOfTile)
      System.out.println("yMinOfTile - yMaxOfTile: " + yMinOfTile + " - " + yMaxOfTile)

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


    /*val DAGList: List[(String, String, mutable.Map[String, String])] = */ if (sc.master.contains("local")) {
      JsonToArg.jsonAlgorithms = "src/main/scala/whu/edu/cn/jsonparser/algorithms_ogc.json"
      JsonToArg.trans(jsonObject, "0")
    }
    else {
      JsonToArg.trans(jsonObject, "0")
    }
    println("JsonToArg.dagMap.size = " + JsonToArg.dagMap)
    JsonToArg.dagMap.foreach(DAGList => {
      println("************优化前的DAG*************")
      println(DAGList._1)
      DAGList._2.foreach(println(_))
      println("************优化后的DAG*************")
      val optimizedDAGList: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = optimizedDAG(DAGList._2)
      optimizedDagMap += (DAGList._1 -> optimizedDAGList)
      optimizedDAGList.foreach(println(_))
    })


    try {
      lambda(sc, optimizedDagMap("0"))
    } catch {
      case e: Throwable =>
        val errorJson = new JSONObject
        errorJson.put("error", e.toString)

        // 回调服务，通过 boot 告知前端：
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("workID", Trigger.dagId)
        outJsonObject.put("json", errorJson)

        println("Error json = " + outJsonObject)
        sendPost(GlobalConstantUtil.DAG_ROOT_URL + "/deliverUrl",
          outJsonObject.toJSONString)

        // 打印至后端控制台
        e.printStackTrace()
    } finally {
      val time2: Long = System.currentTimeMillis()
      println(time2 - time1)

    }


  }


  def main(args: Array[String]): Unit = {

    workTaskJson = {
      val fileSource: BufferedSource = Source.fromFile("src/main/scala/whu/edu/cn/testjson/debug.json")
      val line: String = fileSource.mkString
      fileSource.close()
      line
    } // 任务要用的 JSON,应当由命令行参数获取

    dagId = Random.nextInt().toString
    // 点击整个run的唯一标识，来自boot

    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    runMain(sc, workTaskJson, dagId)

    //    Thread.sleep(1000000)

    sc.stop()
  }
}
