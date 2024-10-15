package whu.edu.cn.trigger


import whu.edu.cn.algorithms.SpatialStats.GWModels
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.{LinearRegression, SpatialDurbinModel, SpatialErrorModel, SpatialLagModel}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import com.alibaba.fastjson.{JSON, JSONObject}
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.vector.Extent
import io.minio.{MinioClient, PutObjectArgs}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import redis.clients.jedis.Jedis
import whu.edu.cn.algorithms.AI.ML
import whu.edu.cn.algorithms.terrain.calculator
import whu.edu.cn.entity.OGEClassType.OGEClassType
import whu.edu.cn.entity.{BatchParam, CoverageCollectionMetadata, OGEClassType, RawTile, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.oge._
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.{JedisUtil, PostSender, ZCurveUtil}
import whu.edu.cn.algorithms.ImageProcess.algorithms_Image.{GLCM, IHSFusion, PCA, RandomForestTrainAndRegress, bilateralFilter, broveyFusion, cannyEdgeDetection, catTwoCoverage, dilate, erosion, falseColorComposite, gaussianBlur, histogramBin, histogramEqualization, kMeans, linearTransformation, panSharp, reduceRegion, reduction, standardDeviationCalculation, standardDeviationStretching}

import java.io.ByteArrayInputStream
import scala.collection.{immutable, mutable}
import scala.io.{BufferedSource, Source}
import scala.util.Random
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.gmrc.geocorrection.GeoCorrection
import whu.edu.cn.algorithms.gmrc.mosaic.Mosaic
import whu.edu.cn.oge.Sheet.CsvData
import whu.edu.cn.algorithms.gmrc.colorbalance.ColorBalance
import whu.edu.cn.algorithms.gmrc.colorbalanceRef.scala.ColorBalanceWithRef
import whu.edu.cn.entity.cube.CubeTileKey
import whu.edu.cn.oge.CoverageArray.{CoverageList, funcArgs, funcNameList, process}
import whu.edu.cn.algorithms.MLlib.algorithms.{randomForestClassifierModel,logisticRegressionClassifierModel,decisionTreeClassifierModel,gbtClassifierClassifierModel,multilayerPerceptronClassifierModel,linearSVCClassifierModel,naiveBayesClassifierModel,fmClassifierModel,oneVsRestClassifierModel,modelClassify,
  randomForestRegressionModel,linearRegressionModel,generalizedLinearRegressionModel,decisionTreeRegressionModel,gbtRegressionModel,isotonicRegressionModel,fmRegressionModel,modelRegress,
  kMeans => mlKMeans, latentDirichletAllocation, bisectingKMeans, gaussianMixture,
  multiclassClassificationEvaluator,clusteringEvaluator,multilabelClassificationEvaluator,binaryClassificationEvaluator,regressionEvaluator,rankingEvaluator}


import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

object Trigger {
  var optimizedDagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]
  var coverageCollectionMetadata: mutable.Map[String, CoverageCollectionMetadata] = mutable.Map.empty[String, CoverageCollectionMetadata]
  var coverageArrayMetadata: ListBuffer[CoverageCollectionMetadata] = ListBuffer.empty[CoverageCollectionMetadata]
  var lazyFunc: mutable.Map[String, (String, mutable.Map[String, String])] = mutable.Map.empty[String, (String, mutable.Map[String, String])]
  var coverageCollectionRddList: mutable.Map[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]] = mutable.Map.empty[String, immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]]
  var coverageRddList: mutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
  var bandList:mutable.Map[String,immutable.Map[Int,Double]]=mutable.Map.empty[String,immutable.Map[Int,Double]]
  var doubleList: mutable.Map[String, Double] = mutable.Map.empty[String, Double]
  var stringList: mutable.Map[String, String] = mutable.Map.empty[String, String]
  var intList: mutable.Map[String, Int] = mutable.Map.empty[String, Int]
  var SheetList: mutable.Map[String,CsvData] = mutable.Map.empty[String,CsvData]
  var coverageParamsList: mutable.Map[String, Array[(String, String)]] = mutable.Map.empty[String, Array[(String, String)]]

  // TODO lrx: 以下为未检验

  var tableRddList: mutable.Map[String, String] = mutable.Map.empty[String, String]
  var kernelRddList: mutable.Map[String, geotrellis.raster.mapalgebra.focal.Kernel] = mutable.Map.empty[String, geotrellis.raster.mapalgebra.focal.Kernel]
  var mlmodelRddList: mutable.Map[String, org.apache.spark.ml.PipelineModel] = mutable.Map.empty[String, org.apache.spark.ml.PipelineModel]
  var featureRddList: mutable.Map[String, Any] = mutable.Map.empty[String, Any]
  var grassResultList: mutable.Map[String, Any] = mutable.Map.empty[String, Any] //GRASS部分返回String类的算子
  //  var cubeRDDList: mutable.Map[String, mutable.Map[String, Any]] = mutable.Map.empty[String, mutable.Map[String, Any]]
  var cubeRDDList: mutable.Map[String, Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]] = mutable.Map.empty[String, Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])]]

  var cubeLoad: mutable.Map[String, (String, String, String)] = mutable.Map.empty[String, (String, String, String)]
  var outputInformationList:mutable.ListBuffer[JSONObject] = mutable.ListBuffer.empty[JSONObject]

  var userId: String = _
  var level: Int = _
  var layerName: String = _
  var windowExtent: Extent = _
  var isBatch: Int = _
  // 此次计算工作的任务json
  var workTaskJson: String = _

  // 此次计算工作的来源,"main"为来自通用版本，"edu"为来自教育版
  var workType: String = _
  // DAG-ID
  var dagId: String = _
  var dagMd5: String = _
  val zIndexStrArray = new mutable.ArrayBuffer[String]

  // 批计算参数
  val batchParam: BatchParam = new BatchParam

  // Onthefly输出计算层级
  var ontheFlyLevel: Int = _
  //用来标识读取上传文件的编号的自增标识符
  var file_id: Long = 0
  val tempFileList = new ListBuffer[String]

  //任务来源
  var dagType = ""
  //任务结果文件名
  var outputFile = ""

  var ProcessName : String = _

  var coverageReadFromUploadFile : Boolean = false

  def isOptionalArg(args: mutable.Map[String, String], name: String): String = {
    if (args.contains(name)) {
      args(name)
    }
    else {
      null
    }
  }

  // 实际的CoverageCollection的加载函数
  def isActioned(implicit sc: SparkContext, UUID: String, typeEnum: OGEClassType): Unit = {
    typeEnum match {
      case OGEClassType.CoverageCollection =>
        if (!coverageCollectionRddList.contains(UUID)) {
          val metadata: CoverageCollectionMetadata = coverageCollectionMetadata(UUID)
          coverageCollectionRddList += (UUID -> CoverageCollection.load(sc, productName = metadata.productName, sensorName = metadata.sensorName, measurementName = metadata.measurementName, startTime = metadata.startTime.toString, endTime = metadata.endTime.toString, extent = metadata.extent, crs = metadata.crs, level = level, cloudCoverMin = metadata.getCloudCoverMin(), cloudCoverMax = metadata.getCloudCoverMax()))
        }
      case OGEClassType.CoverageArray =>
        val metadata: CoverageCollectionMetadata = coverageArrayMetadata.head
        coverageParamsList += (UUID -> CoverageArray.getLoadParams(productName = metadata.productName, sensorName = metadata.sensorName, measurementName = metadata.measurementName, startTime = metadata.startTime.toString, endTime = metadata.endTime.toString, extent = metadata.extent, crs = metadata.crs, level = level, cloudCoverMin = metadata.getCloudCoverMin(), cloudCoverMax = metadata.getCloudCoverMax()))
    }
  }



  def getValue(name:String):(String,String)={
    if(doubleList.contains(name)){
      (doubleList(name).toString,"double")
    }else if(intList.contains(name)){
      (intList(name).toString,"int")
    }else if(stringList.contains(name)){
      (stringList(name),"String")
    }else{
      throw new Exception("No such element in this calculation")
    }
  }

  def getCoverageListFromArgs(coverageNames: String): List[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] ={
    // String转List
    val names = coverageNames.replace("[", "").replace("]", "").split(',')
    val coverages = mutable.ListBuffer.empty[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]

    for(name <- names){
      coverages.append(coverageRddList(name))
    }
    coverages.toList
  }


  @throws(classOf[Throwable])
  def func(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    try {
      val tempNoticeJson = new JSONObject
      println("args:", funcName + args)
      funcName match {

        //Others
        case "Service.printString" =>
          val temp = getValue(args("object"))
          val res =temp._1
          val valueType=temp._2
          Service.print(res,args("name"),valueType)
        case "Service.printNumber" =>
          val temp = getValue(args("object"))
          val res = temp._1
          val valueType = temp._2
          Service.print(res, args("name"), valueType)
        case "Service.printList" =>
          val temp = getValue(args("object"))
          val res = temp._1
          val valueType = temp._2
          Service.print(res, args("name"), valueType)
        case "Service.printSheet" =>
          val sheet = SheetList(args("object"))
          Sheet.printSheet(sheet, args("name"))
        // Service
        case "Service.getCoverageCollection" =>
          lazyFunc += (UUID -> (funcName, args))
          coverageCollectionMetadata += (UUID -> Service.getCoverageCollection(args("productID"), dateTime = isOptionalArg(args, "datetime"), extent = isOptionalArg(args, "bbox"), cloudCoverMin = if(isOptionalArg(args, "cloudCoverMin") == null) 0 else isOptionalArg(args, "cloudCoverMin").toFloat, cloudCoverMax = if(isOptionalArg(args, "cloudCoverMax") == null) 100 else isOptionalArg(args, "cloudCoverMax").toFloat))
        case "Service.getCoverageArray" =>
          lazyFunc += (UUID -> (funcName, args))
          coverageArrayMetadata += Service.getCoverageCollection(args("productID"), dateTime = isOptionalArg(args, "datetime"), extent = isOptionalArg(args, "bbox"), cloudCoverMin = if(isOptionalArg(args, "cloudCoverMin") == null) 0 else isOptionalArg(args, "cloudCoverMin").toFloat, cloudCoverMax = if(isOptionalArg(args, "cloudCoverMax") == null) 100 else isOptionalArg(args, "cloudCoverMax").toFloat)
        case "Service.getCoverage" =>
          if(args("coverageID").startsWith("myData/") || args("coverageID").startsWith("result/")){
            coverageReadFromUploadFile = true
            coverageRddList += (UUID -> Coverage.loadCoverageFromUpload(sc, args("coverageID"), userId, dagId))
          } else if(args("coverageID").startsWith("OGE_Case_Data/")){
            coverageReadFromUploadFile = true
            coverageRddList += (UUID -> Coverage.loadCoverageFromCaseData(sc, args("coverageID"),  dagId))
          }
          else {
            coverageReadFromUploadFile = false
            coverageRddList += (UUID -> Service.getCoverage(sc, args("coverageID"), args("productID"), level = level))
          }
        case "Service.getCube" =>
          cubeRDDList += (UUID -> Service.getCube(sc, args("cubeId"), args("products"), args("bands"), args("time"), args("extent"), args("tms"), args("resolution")))
        case "Service.getTable" =>
          tableRddList += (UUID -> isOptionalArg(args, "productID"))
        case "Service.getFeatureCollection" =>
          featureRddList += (UUID -> isOptionalArg(args, "productID"))
        case "Service.getFeature" =>
          if(args("featureId").startsWith("myData/")){
            if(args.contains("crs"))
              featureRddList += (UUID -> Feature.loadFeatureFromUpload(sc, args("featureId"), userId, dagId, isOptionalArg(args, "crs")))
            else
              featureRddList += (UUID -> Feature.loadFeatureFromUpload(sc, args("featureId"), userId, dagId, "EPSG:4326"))
          } else {
            featureRddList += (UUID -> Service.getFeature(sc, args("featureId"), isOptionalArg(args, "dataTime"), isOptionalArg(args, "crs")))
          }
        case "Service.getSheet" =>
          if (args("sheetID").startsWith("myData/")) {
            SheetList += (UUID -> Sheet.loadCSVFromUpload(sc, args("sheetID"), userId, dagId))
          }
        // Filter // TODO lrx: 待完善Filter类的函数
        case "Filter.equals" =>
          lazyFunc += (UUID -> (funcName, args))
        case "Filter.and" =>
          lazyFunc += (UUID -> (funcName, args))
        case "Filter.date" =>
          lazyFunc += (UUID -> (funcName, args))
        case "Filter.bounds" =>
          lazyFunc += (UUID -> (funcName, args))

        // Sheet
        case "Sheet.getcellValue" =>
          stringList += (UUID -> Sheet.getcellValue(SheetList(args("sheet")), args("row").toInt, args("col").toInt))
        case "Sheet.slice" =>
          SheetList += (UUID -> Sheet.slice(SheetList(args("sheet")), args("sliceRows").toBoolean, args("start").toInt, args("end").toInt))
        case "Sheet.filterByHeader" =>
          SheetList += (UUID -> Sheet.filterByHeader(SheetList(args("sheet")), args("condition"), args("value")))

        // CoverageCollection
        case "CoverageCollection.filter" =>
          coverageCollectionMetadata += (UUID -> CoverageCollection.filter(filter = args("filter"), collection = coverageCollectionMetadata(args("collection"))))
        case "CoverageCollection.mergeCoverages" =>
          coverageCollectionRddList += (UUID -> CoverageCollection.mergeCoverages(getCoverageListFromArgs(args("coverages")),args("names").replace("[", "").replace("]", "").split(',').toList))
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
        case "CoverageCollection.cat" =>
          isActioned(sc, args("coverageCollection"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> CoverageCollection.cat(coverageCollectionRddList(args("coverageCollection"))))
        case "CoverageCollection.map" =>
        //          if (lazyFunc(args("collection"))._1 == "Service.getCoverageCollection") {
        //            isActioned(sc, args("collection"), OGEClassType.CoverageCollection)
        //            coverageCollectionRddList += (UUID -> CoverageCollection.map(sc, coverageCollection = coverageCollectionRddList(args("collection")), baseAlgorithm = args("baseAlgorithm")))
        //          }
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

        case "CoverageArray.add" =>
          funcNameList += "CoverageArray.add"
          funcArgs += List(coverageRddList(args("coverage")))
        case "CoverageArray.subtract" =>
          funcNameList += "CoverageArray.subtract"
          funcArgs += List(coverageRddList(args("coverage")))
        case "CoverageArray.divide" =>
          funcNameList += "CoverageArray.divide"
          funcArgs += List(coverageRddList(args("coverage")))
        case "CoverageArray.multiply" =>
          funcNameList += "CoverageArray.multiply"
          funcArgs += List(coverageRddList(args("coverage")))
        case "CoverageArray.addNum" =>
          funcNameList += "CoverageArray.addNum"
          funcArgs += List(args("i").toDouble)
        case "CoverageArray.subtractNum" =>
          funcNameList += "CoverageArray.subtractNum"
          funcArgs += List(args("i").toDouble)
        case "CoverageArray.multiplyNum" =>
          funcNameList += "CoverageArray.multiplyNum"
          funcArgs += List(args("i").toDouble)
        case "CoverageArray.normalizedDifference" =>
          funcNameList += "CoverageArray.normalizedDifference"
          funcArgs += List(args("bandNames").substring(1, args("bandNames").length - 1).split(",").toList)
        case "CoverageArray.toInt8" =>
          funcNameList += "CoverageArray.toInt8"
          funcArgs += List.empty
        case "CoverageArray.toUint8" =>
          funcNameList += "CoverageArray.toUint8"
          funcArgs += List.empty
        case "CoverageArray.toInt16" =>
          funcNameList += "CoverageArray.toInt16"
          funcArgs += List.empty
        case "CoverageArray.toUint16" =>
          funcNameList += "CoverageArray.toUint16"
          funcArgs += List.empty
        case "CoverageArray.toInt32" =>
          funcNameList += "CoverageArray.toInt32"
          funcArgs += List.empty
        case "CoverageArray.toFloat" =>
          funcNameList += "CoverageArray.toFloat"
          funcArgs += List.empty
        case "CoverageArray.toDouble" =>
          funcNameList += "CoverageArray.toDouble"
          funcArgs += List.empty
        //        case "CoverageArray.clipRasterByMaskLayerByGDAL" =>
        //          funcNameList += "CoverageArray.clipRasterByMaskLayerByGDAL"
        //          funcArgs += List(sc, )
        case "CoverageArray.addStyles" =>
          val visParam: VisualizationParam = new VisualizationParam
          visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
          if (isBatch == 0) {
            isActioned(sc, args("coverageArray"), OGEClassType.CoverageArray)
            val coverageArray: Array[(String, String)] = coverageParamsList(args("coverageArray"))
            funcNameList += "CoverageArray.visualizeOnTheFly"
            coverageArray.zipWithIndex.foreach {case (element, index) =>
              val coverage = Coverage.load(sc, element._1, element._2, level)
              val firstCoverage: (Int, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])) = (0 -> coverage)
              CoverageList += firstCoverage
              funcArgs += List(visParam, index)
              for (i <- funcNameList.indices) {
                process(funcNameList(i), funcArgs(i), i)
              }
              funcArgs.remove(funcArgs.length - 1)
              CoverageList.clear()
            }
          } else {
            funcNameList += "CoverageArray.addStyles"
            funcArgs += List(visParam)
          }

        case "CoverageArray.export" =>
          funcNameList += "CoverageArray.visualizeBatch"
          isActioned(sc, args("coverageArray"), OGEClassType.CoverageArray)
          val coverageArray: Array[(String, String)] = coverageParamsList(args("coverageArray"))
          coverageArray.zipWithIndex.foreach {case (element, index) =>
            val coverage = Coverage.load(sc, element._1, element._2, level)
            val firstCoverage: (Int, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])) = (0 -> coverage)
            CoverageList += firstCoverage
            println("index: ", index)
            funcArgs += List(batchParam, Trigger.dagId + index)
            for (i <- funcNameList.indices) {
              process(funcNameList(i), funcArgs(i), i)
            }
            funcArgs.remove(funcArgs.length - 1)
            CoverageList.clear()
          }


        // TODO lrx: 这里要改造
        // Table
        case "Table.getDownloadUrl" =>
          Table.getDownloadUrl(url = tableRddList(isOptionalArg(args, "input")), fileName = "fileName")
        case "Table.addStyles" =>
          Table.getDownloadUrl(url = tableRddList(isOptionalArg(args, "input")), fileName = " fileName")
        case "Algorithm.hargreaves" =>
          Table.hargreaves(args("inputTemperature"), args("inputStation"), args("startTime"), args("endTime"), args("timeStep").toLong)
        case "Algorithm.topmodel" =>
          Table.topModel(args("inputPrecipEvapFile"), args("inputTopoIndex"), args("startTime"), args("endTime"), args("timeStep").toLong, args("rate").toDouble,
            args("recession").toInt, args("tMax").toInt, args("iterception").toInt, args("waterShedArea").toInt)
        case "Algorithm.SWMM5" =>
          Table.SWMM5(null)

        // Sheet
        case "Sheet.getcellValue" =>
          stringList += (UUID -> Sheet.getcellValue(SheetList(args("sheet")), args("row").toInt, args("col").toInt))
        case "Sheet.slice" =>
          SheetList += (UUID -> Sheet.slice(SheetList(args("sheet")), args("sliceRows").toBoolean, args("start").toInt, args("end").toInt))
        case "Sheet.filterByHeader" =>
          SheetList += (UUID -> Sheet.filterByHeader(SheetList(args("sheet")), args("condition"), args("value")))
        case "Sheet.toPoint" =>
          val lat_col: String = args.getOrElse("lat_column", "lat")
          val lon_col: String = args.getOrElse("lon_column", "lon")
          featureRddList += (UUID -> Sheet.sheetToPoint(sc, SheetList(args("sheet")),lat_col, lon_col))
        case "Sheet.pointToSheet" =>
          SheetList += (UUID -> Sheet.pointToSheet(featureRddList(args("point")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))


        // Coverage
        case "Coverage.export" =>
          Coverage.visualizeBatch(sc, coverage = coverageRddList(args("coverage")), batchParam = batchParam, dagId)
        case "Coverage.area" =>
          doubleList += (UUID -> Coverage.area(coverage = coverageRddList(args("coverage")),ValList = args("valueRange").substring(1, args("valueRange").length - 1).split(",").toList,resolution = args("resolution").toDouble))
        case "Coverage.geoDetector" =>
          stringList += (UUID ->Coverage.geoDetector(depVar_In = coverageRddList(args("depVar_In")),
            facVar_name_In = args("facVar_name_In"),
            facVar_In = coverageRddList(args("facVar_In")),
            norExtent_sta = args("norExtent_sta").toDouble,norExtent_end = args("norExtent_end").toDouble,NaN_value = args("NaN_value").toDouble))
        case "Coverage.rasterUnion" =>
          coverageRddList += (UUID -> Coverage.rasterUnion(coverage1 = coverageRddList(args("coverage1")),coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.reclass" =>
          // 移除外部方括号并使用正则表达式分割字符串
          val pattern = "\\],\\["
          val subStrings = args("rules").stripPrefix("[").stripSuffix("]").split(pattern)
          // 使用 map 函数将每个子字符串解析成列表
          val rules_res = subStrings.map { subString =>
            val elements = subString.split(",").map { element =>
              element.stripPrefix("[").stripSuffix("]").toDouble
            }
            elements.toList
          }.map {
            case List(a, b, c) => (a, b, c)
          }.toList
          coverageRddList += (UUID -> Coverage.reclass(coverage = coverageRddList(args("coverage")),
            rules = rules_res,
            NaN_value= args("NaN_value").toDouble))
        case "Coverage.date" =>
          stringList += (UUID -> Coverage.date(coverage = coverageRddList(args("coverage"))))
        case "Coverage.subtract" =>
          coverageRddList += (UUID -> Coverage.subtract(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.subtractNum" =>
          coverageRddList += (UUID -> Coverage.subtractNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
        case "Coverage.cat" =>
          coverageRddList += (UUID -> Coverage.cat(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.add" =>
          coverageRddList += (UUID -> Coverage.add(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.addNum" =>
          coverageRddList += (UUID -> Coverage.addNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
        case "Coverage.mod" =>
          coverageRddList += (UUID -> Coverage.mod(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.modNum" =>
          coverageRddList += (UUID -> Coverage.modNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
        case "Coverage.divide" =>
          coverageRddList += (UUID -> Coverage.divide(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.divideNum" =>
          coverageRddList += (UUID -> Coverage.divideNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
        case "Coverage.round" =>
          coverageRddList += (UUID -> Coverage.round(coverage = coverageRddList(args("coverage"))))
        case "Coverage.multiply" =>
          coverageRddList += (UUID -> Coverage.multiply(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.multiplyNum" =>
          coverageRddList += (UUID -> Coverage.multiplyNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
        case "Coverage.normalizedDifference" =>
          coverageRddList += (UUID -> Coverage.normalizedDifference(coverageRddList(args("coverage")), bandNames = args("bandNames").substring(1, args("bandNames").length - 1).split(",").toList))
        case "Coverage.binarization" =>
          coverageRddList += (UUID -> Coverage.binarization(coverage = coverageRddList(args("coverage")), threshold = args("threshold").toDouble))
        case "Coverage.and" =>
          coverageRddList += (UUID -> Coverage.and(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.or" =>
          coverageRddList += (UUID -> Coverage.or(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.not" =>
          coverageRddList += (UUID -> Coverage.not(coverage = coverageRddList(args("coverage1"))))
        case "Coverage.bitwiseAnd" =>
          coverageRddList += (UUID -> Coverage.bitwiseAnd(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.bitwiseXor" =>
          coverageRddList += (UUID -> Coverage.bitwiseXor(coverage1 = coverageRddList(args("coverage1")), coverage2 =
            coverageRddList(args("coverage2"))))
        case "Coverage.bitwiseOr" =>
          coverageRddList += (UUID -> Coverage.bitwiseOr(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.bitwiseNot" =>
          coverageRddList += (UUID -> Coverage.bitwiseNot(coverage = coverageRddList(args("coverage"))))
        case "Coverage.ceil" =>
          coverageRddList += (UUID -> Coverage.ceil(coverage = coverageRddList(args("coverage"))))
        case "Coverage.floor" =>
          coverageRddList += (UUID -> Coverage.floor(coverage = coverageRddList(args("coverage"))))
        case "Coverage.sin" =>
          coverageRddList += (UUID -> Coverage.sin(coverage = coverageRddList(args("coverage"))))
        case "Coverage.tan" =>
          coverageRddList += (UUID -> Coverage.tan(coverage = coverageRddList(args("coverage"))))
        case "Coverage.tanh" =>
          coverageRddList += (UUID -> Coverage.tanh(coverage = coverageRddList(args("coverage"))))
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
        case "Coverage.standardDeviationCalculation" =>
          bandList += (UUID -> standardDeviationCalculation(coverage = coverageRddList(args("coverage"))))
        case "Coverage.cannyEdgeDetection" =>
          coverageRddList += (UUID -> cannyEdgeDetection(coverage = coverageRddList(args("coverage")),lowCoefficient=args("lowCoefficient").toDouble,highCoefficient=args("highCoefficient").toDouble))
        case "Coverage.histogramEqualization" =>
          coverageRddList += (UUID -> histogramEqualization(coverage = coverageRddList(args("coverage"))))
        case "Coverage.reduction" =>
          coverageRddList += (UUID -> reduction(coverage = coverageRddList(args("coverage")),option = args("option").toInt))
        case "Coverage.broveyFusion" =>
          coverageRddList += (UUID -> broveyFusion(multispectral = coverageRddList(args("multispectral")),panchromatic = coverageRddList(args("panchromatic"))))
        case "Coverage.standardDeviationStretching" =>
          coverageRddList += (UUID -> standardDeviationStretching(coverage = coverageRddList(args("coverage"))))
        case "Coverage.bilateralFilter" =>
          coverageRddList += (UUID -> bilateralFilter(coverage = coverageRddList(args("coverage")),d=args("d").toInt,sigmaSpace = args("sigmaSpace").toDouble,sigmaColor = args("sigmaColor").toDouble,borderType = args("borderType").toString))
        case "Coverage.gaussianBlur" =>
          coverageRddList += (UUID -> gaussianBlur(coverage = coverageRddList(args("coverage")),d=args("d").toInt, sigmaX = args("sigmaX").toDouble, sigmaY = args("sigmaY").toDouble, borderType = args("borderType")))
        case "Coverage.falseColorComposite" =>
          coverageRddList += (UUID -> falseColorComposite(coverage = coverageRddList(args("coverage")), BandRed = args("BandRed").toInt, BandGreen = args("BandGreen").toInt, BandBlue = args("BandBlue").toInt))
        case "Coverage.linearTransformation" =>
          coverageRddList += (UUID -> linearTransformation(coverage = coverageRddList(args("coverage")), k = args("k").toDouble, b = args("b").toInt))
        case "Coverage.erosion" =>
          coverageRddList += (UUID -> erosion(coverage = coverageRddList(args("coverage")), k = args("k").toInt))
        case "Coverage.dilate" =>
          coverageRddList += (UUID -> dilate(coverage = coverageRddList(args("coverage")), length = args("length").toInt))
        case "Coverage.GLCM" =>
          coverageRddList += (UUID -> GLCM(coverage = coverageRddList(args("coverage")), d = args("d").toInt,dist = args("dist").toInt,orient =args("orient").toInt,greyLevels = args("greyLevels").toInt ,feature=args("feature").toString,borderType = args("borderType").toString))
        case "Coverage.PCA" =>
          coverageRddList += (UUID -> PCA(coverage = coverageRddList(args("coverage")), num = args("num").toInt))
        case "Coverage.kMeans" =>
          coverageRddList += (UUID -> kMeans(coverage = coverageRddList(args("coverage")), k = args("k").toInt, seed =args("seed").toLong , maxIter =args("maxIter").toInt,distanceMeasure =args("distanceMeasure").toString))
        case "Coverage.panSharp" =>
          val bandListString :List[String] =args("bands").substring(1,args("bands").length - 1).split(",").toList
          val bandList=bandListString.map(s=>scala.util.Try(s.toShort)).filter(_.isSuccess).map(_.get)
          val weightListString:List[String]=args("weightList").substring(1,args("weightList").length - 1).split(",").toList
          val weightList=weightListString.map(s=>scala.util.Try(s.toDouble)).filter(_.isSuccess).map(_.get)

          coverageRddList += (UUID -> panSharp(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2")), method = args("method").toString, bandList = bandList, weightList = weightList))
        case "Coverage.catTwoCoverage" =>
          coverageRddList += (UUID -> catTwoCoverage(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.reflectanceReconstruction" =>
          coverageRddList += (UUID -> QuantRS.reflectanceReconstruction(sc, coverageRddList(args("MOD09A1")), coverageRddList(args("LAI")), coverageRddList(args("FAPAR")), coverageRddList(args("NDVI")), coverageRddList(args("EVI")), coverageRddList(args("FVC")), coverageRddList(args("GPP")), coverageRddList(args("NPP")), coverageRddList(args("ALBEDO")), coverageRddList(args("COPY"))))
        case "Coverage.IHSFusion" =>
          coverageRddList += (UUID -> catTwoCoverage(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
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
        case "Coverage.lt" =>
          coverageRddList += (UUID -> Coverage.lt(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.lte" =>
          coverageRddList += (UUID -> Coverage.lte(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.log" =>
          coverageRddList += (UUID -> Coverage.log(coverage = coverageRddList(args("coverage"))))
        case "Coverage.log10" =>
          coverageRddList += (UUID -> Coverage.log10(coverage = coverageRddList(args("coverage"))))
        case "Coverage.selectBands" =>
          coverageRddList += (UUID -> Coverage.selectBands(coverage = coverageRddList(args("coverage")), bands = args("bands").substring(1, args("bands").length - 1).split(",").toList))
        case "Coverage.addBands" =>
          val names: List[String] = args("names").split(",").toList
          coverageRddList += (UUID -> Coverage.addBands(dstCoverage = coverageRddList(args("coverage1")), srcCoverage =
            coverageRddList(args("coverage2")), names = names))
        case "Coverage.bandNames" =>
          stringList += (UUID -> Coverage.bandNames(coverage = coverageRddList(args("coverage"))))
        case "Coverage.bandNum" =>
          intList += (UUID -> Coverage.bandNum(coverage = coverageRddList(args("coverage"))))
        case "Coverage.abs" =>
          coverageRddList += (UUID -> Coverage.abs(coverage = coverageRddList(args("coverage"))))
        case "Coverage.neq" =>
          coverageRddList += (UUID -> Coverage.neq(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.signum" =>
          coverageRddList += (UUID -> Coverage.signum(coverage = coverageRddList(args("coverage"))))
        case "Coverage.bandTypes" =>
          stringList += (UUID -> Coverage.bandTypes(coverage = coverageRddList(args("coverage"))))
        case "Coverage.rename" =>
          coverageRddList += (UUID -> Coverage.rename(coverage = coverageRddList(args("coverage")), name = args("name").split(",").toList))
        case "Coverage.pow" =>
          coverageRddList += (UUID -> Coverage.pow(coverage1 = coverageRddList(args("coverage1")), coverage2 = coverageRddList(args("coverage2"))))
        case "Coverage.powNum" =>
          coverageRddList += (UUID -> Coverage.powNum(coverage = coverageRddList(args("coverage")), i = args("i").toDouble))
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
          stringList += (UUID -> Coverage.projection(coverage = coverageRddList(args("coverage"))))
        case "Coverage.remap" =>
          coverageRddList += (UUID -> Coverage.remap(coverage = coverageRddList(args("coverage")), args("from").slice(1, args("from").length - 1).split(',').toList.map(_.toInt),
            args("to").slice(1, args("to").length - 1).split(',').toList.map(_.toDouble), isOptionalArg(args, "defaultValue")))
        case "Coverage.polynomial" =>
          coverageRddList += (UUID -> Coverage.polynomial(coverage = coverageRddList(args("coverage")), args("l").slice(1, args("l").length - 1).split(',').toList.map(_.toDouble)))
        case "Coverage.slice" =>
          coverageRddList += (UUID -> Coverage.slice(coverage = coverageRddList(args("coverage")), start = args("start").toInt, end = args("end").toInt))
        case "Coverage.histogram" =>
          stringList += (UUID -> Coverage.histogram(coverage = coverageRddList(args("coverage")), scale = args("scale").toDouble).toString())
        case "Coverage.reproject" =>
          coverageRddList += (UUID -> Coverage.reproject(coverage = coverageRddList(args("coverage")), crs = CRS.fromEpsgCode(args("crsCode").toInt), scale = args("resolution").toDouble))
        case "Coverage.resample" =>
          coverageRddList += (UUID -> Coverage.resample(coverage = coverageRddList(args("coverage")), level = args("level").toDouble, mode = args("mode")))
        case "Coverage.gradient" =>
          coverageRddList += (UUID -> Coverage.gradient(coverage = coverageRddList(args("coverage"))))
        case "Coverage.clip" =>
          coverageRddList += (UUID -> Coverage.clip(coverage = coverageRddList(args("coverage")), geom = featureRddList(args("geom")).asInstanceOf[Geometry]))
        case "Coverage.clamp" =>
          coverageRddList += (UUID -> Coverage.clamp(coverage = coverageRddList(args("coverage")), low = args("low").toInt, high = args("high").toInt))
        case "Coverage.rgbToHsv" =>
          coverageRddList += (UUID -> Coverage.rgbToHsv(coverage = coverageRddList(args("coverage"))))
        case "Coverage.hsvToRgb" =>
          coverageRddList += (UUID -> Coverage.hsvToRgb(coverage = coverageRddList(args("coverage"))))
        case "Coverage.entropy" =>
          coverageRddList += (UUID -> Coverage.entropy(coverage = coverageRddList(args("coverage")), "square", radius = args("radius").toInt))
        //      case "Coverage.NDVI" =>
        //        coverageRddList += (UUID -> Coverage.NDVI(NIR = coverageRddList(args("NIR")), Red = coverageRddList(args("Red"))))
        case "Coverage.cbrt" =>
          coverageRddList += (UUID -> Coverage.cbrt(coverage = coverageRddList(args("coverage"))))
        case "Coverage.sqrt" =>
          coverageRddList += (UUID -> Coverage.sqrt(coverage = coverageRddList(args("coverage"))))
        case "Coverage.metadata" =>
          stringList += (UUID -> Coverage.metadata(coverage = coverageRddList(args("coverage"))))
        case "Coverage.mask" =>
          coverageRddList += (UUID -> Coverage.mask(coverageRddList(args("coverage1")), coverageRddList(args("coverage2")), args("readMask").toInt, args("writeMask").toInt))
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
        //   QGIS
        case "Coverage.warpGeoreByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalWarpGeore(sc, coverageRddList(args("input")), GCPs = args("GCPs"), resampleMethod = args("resampleMethod"), userId, dagId))
        case "Coverage.sieveByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalSieve(sc, input = coverageRddList(args("input")), threshold = args("threshold").toInt, eightConnectedness = args("eightConnectedness"), noMask = args("noMask"), maskLayer = args("maskLayer"), extra = args("extra")))
        case "Coverage.aspectByQGIS" =>
          coverageRddList += (UUID -> QGIS.nativeAspect(sc, input = coverageRddList(args("input")), zFactor = args("zFactor").toDouble))
        case "Coverage.slopeByQGIS" =>
          coverageRddList += (UUID -> QGIS.nativeSlope(sc, input = coverageRddList(args("input")), zFactor = args("zFactor").toDouble))
        case "Coverage.rescaleRasterByQGIS" =>
          coverageRddList += (UUID -> QGIS.nativeRescaleRaster(sc, input = coverageRddList(args("input")), minimum = args("minimum").toDouble, maximum = args("maximum").toDouble, band = args("band").toInt))
        case "Coverage.ruggednessIndexByQGIS" =>
          coverageRddList += (UUID -> QGIS.nativeRuggednessIndex(sc, input = coverageRddList(args("input")), zFactor = args("zFactor").toDouble))
        case "Feature.projectPointsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeProjectPoints(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], distance = args("distance").toDouble, bearing = args("bearing").toDouble))
        case "Feature.addFieldByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAddField(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], fieldType = args("fieldType"), fieldPrecision = args("fieldPrecision").toDouble, fieldName = args("fieldName"), fieldLength = args("fieldLength").toDouble))
        case "Feature.addXYFieldByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAddXYField(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], crs = args("crs"), prefix = args("prefix")))
        case "Feature.affineTransformByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAffineTransform(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], scaleX = args("scaleX").toDouble, scaleZ = args("scaleZ").toDouble, rotationZ = args("rotationZ").toDouble, scaleY = args("scaleY").toDouble, scaleM = args("scaleM").toDouble, deltaM = args("deltaM").toDouble, deltaX = args("deltaX").toDouble, deltaY = args("deltaY").toDouble, deltaZ = args("deltaZ").toDouble))
        case "Feature.antimeridianSplitByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAntimeridianSplit(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.arrayOffsetLinesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeArrayOffsetLines(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], segments = args("segments").toDouble, joinStyle = args("joinStyle"), offset = args("offset").toDouble, count = args("count").toDouble, miterLimit = args("miterLimit").toDouble))
        case "Feature.translatedFeaturesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeTranslatedFeatures(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], count = args("count").toDouble, deltaM = args("deltaM").toDouble, deltaX = args("deltaX").toDouble, deltaY = args("deltaY").toDouble, deltaZ = args("deltaZ").toDouble))
        case "Feature.assignProjectionByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAssignProjection(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], crs = args("crs")))
        case "Feature.offsetLineByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeOffsetLine(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], segments = args("segments").toInt, distance = args("distance").toDouble, joinStyle = args("joinStyle"), miterLimit = args("miterLimit").toDouble))
        case "Feature.pointsAlongLinesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePointsAlongLines(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], startOffset = args("startOffset").toDouble, distance = args("distance").toDouble, endOffset = args("endOffset").toDouble))
        case "Feature.polygonizeByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePolygonize(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], keepFields = args("keepFields")))
        case "Feature.polygonsToLinesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePolygonsToLines(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.randomPointsInPolygonsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRandomPointsInPolygons(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], minDistance = args("minDistance").toDouble, includePolygonAttributes = args("includePolygonAttributes"), maxTriesPerPoint = args("maxTriesPerPoint").toInt, pointsNumber = args("pointsNumber").toInt, minDistanceGlobal = args("minDistanceGlobal").toDouble))
        case "Feature.shortestPathPointToPointByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeShortestPathPointToPoint(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], valueForward = args("valueForward"), valueBoth = args("valueBoth"), startPoint = args("startPoint"), defaultDirection = args("defaultDirection"), strategy = args("strategy"), tolerance = args("tolerance").toDouble, defaultSpeed = args("defaultSpeed").toDouble, directionField = args("directionField"), endPoint = args("endPoint"), valueBackward = args("valueBackward"), speedField = args("speedField")))
        case "Feature.rasterSamplingByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRasterSampling(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], rasterCopy = coverageRddList(args("rasterCopy")), columnPrefix = args("columnPrefix")))
        case "Feature.randomPointsOnLinesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRandomPointsOnLines(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], minDistance = args("minDistance").toDouble, includeLineAttributes = args("includeLineAttributes"), maxTriesPerPoint = args("maxTriesPerPoint").toInt, pointsNumber = args("pointsNumber").toInt, minDistanceGlobal = args("minDistanceGlobal").toDouble))
        case "Feature.rotateFeaturesByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRotateFeatures(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], anchor = args("anchor"), angle = args("angle").toDouble))
        case "Feature.simplifyByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeSimplify(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], method = args("method"), tolerance = args("tolerance").toDouble))
        case "Feature.smoothByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeSmooth(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], maxAngle = args("maxAngle").toDouble, iterations = args("iterations").toInt, offset = args("offset").toDouble))
        case "Feature.swapXYByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeSwapXY(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.transectQGIS" =>
          featureRddList += (UUID -> QGIS.nativeTransect(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], side = args("side"), length = args("length").toDouble, angle = args("angle").toDouble))
        case "Feature.translateGeometryByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeTranslateGeometry(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], delta_x = args("delta_x").toDouble, delta_y = args("delta_y").toDouble, delta_z = args("delta_z").toDouble, delta_m = args("delta_m").toDouble))
        case "Feature.convertGeometryTypeByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeConvertGeometryType(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], geometryType = args("geometryType")))
        case "Feature.linesToPolygonsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeLinesToPolygons(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.pointsDisplacementByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePointsDisplacement(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], proximity = args("proximity").toDouble, distance = args("distance").toDouble, horizontal = args("horizontal")))
        case "Feature.randomPointsAlongLineByQGIS" =>
          featureRddList += (UUID -> QGIS.nativaRandomPointsAlongLine(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], pointsNumber = args("pointsNumber").toInt, minDistance = args("minDistance").toDouble))
        case "Feature.randomPointsInLayerBoundsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRandomPointsInLayerBounds(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], pointsNumber = args("pointsNumber").toInt, minDistance = args("minDistance").toDouble))
        case "Feature.angleToNearestByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeAngleToNearest(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], referenceLayer = featureRddList(args("referenceLayer")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], maxDistance = args("maxDistance").toDouble, fieldName = args("fieldName"), applySymbology = args("applySymbology")))
        case "Feature.boundaryByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeBoundary(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.miniEnclosingCircleByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeMiniEnclosingCircle(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], segments = args("segments").toInt))
        case "Feature.multiRingConstantBufferByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeMultiRingConstantBuffer(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], rings = args("rings").toInt, distance = args("distance").toDouble))
        case "Feature.orientedMinimumBoundingBoxByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeOrientedMinimumBoundingBox(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.pointOnSurfaceByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePointOnSurface(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], allParts = args("allParts")))
        case "Feature.poleOfInaccessibilityByQGIS" =>
          featureRddList += (UUID -> QGIS.nativePoleOfInaccessibility(sc, input = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], tolerance = args("tolerance").toDouble))
        case "Feature.rectanglesOvalsDiamondsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeRectanglesOvalsDiamonds(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], rotation = args("rotation").toDouble, shape = args("shape"), segments = args("segments").toInt, width = args("width").toDouble, height = args("height").toDouble))
        case "Feature.singleSidedBufferByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeSingleSidedBuffer(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], side = args("side"), distance = args("distance").toDouble, segments = args("segments").toInt, joinStyle = args("joinStyle"), miterLimit = args("miterLimit").toDouble))
        case "Feature.taperedBufferByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeTaperedBuffer(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], segments = args("segments").toInt, startWidth = args("startWidth").toDouble, endWidth = args("endWidth").toDouble))
        case "Feature.wedgeBuffersByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeWedgeBuffers(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], innerRadius = args("innerRadius").toDouble, outerRadius = args("outerRadius").toDouble, width = args("width").toDouble, azimuth = args("azimuth").toDouble))
        case "Feature.concaveHullByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeConcaveHull(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], noMultigeometry = args("noMultigeometry"), holes = args("holes"), alpha = args("alpha").toDouble))
        case "Feature.delaunayTriangulationByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeDelaunayTriangulation(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.voronoiPolygonsByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeVoronoiPolygons(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], buffer = args("buffer").toDouble))
        case "Feature.extractFromLocationByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeExtractFromLocation(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],featureRddList(args("intersect")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], predicate = args("predicate")))
        case "Feature.intersectionByQGIS" =>
          featureRddList += (UUID -> QGIS.nativeIntersection(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],featureRddList(args("overlay")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], inputFields = args("inputFields"),overlayFields = args("overlayFields"),overlayFieldsPrefix = args("overlayFieldsPrefix"),gridSize = args("gridSize").toDouble))
        case "Coverage.edgeExtractionByOTB" =>
          coverageRddList += (UUID -> OTB.otbEdgeExtraction(sc, coverageRddList(args("input")), channel = args("channel").toInt, filter = args("filter")))
        case "Coverage.dimensionalityReductionByOTB" =>
          coverageRddList += (UUID -> OTB.otbDimensionalityReduction(sc, coverageRddList(args("input")), method = args("method"), rescale = args("rescale")))
        case "Coverage.SFSTextureExtractionByOTB" =>
          coverageRddList += (UUID -> OTB.otbSFSTextureExtraction(sc, coverageRddList(args("input")), channel = args("channel").toInt, spectralThreshold = args("spectralThreshold").toDouble, spatialThreshold = args("spatialThreshold").toInt, numberOfDirection = args("numberOfDirection").toInt, alpha = args("alpha").toDouble, ratioMaximumConsiderationNumber = args("ratioMaximumConsiderationNumber").toInt))
        case "Coverage.multivariateAlterationDetectorByOTB" =>
          coverageRddList += (UUID -> OTB.otbMultivariateAlterationDetector(sc, coverageRddList(args("inputBefore")), coverageRddList(args("inputAfter"))))
        case "Coverage.radiometricIndicesByOTB" =>
          coverageRddList += (UUID -> OTB.otbRadiometricIndices(sc, coverageRddList(args("input")), channelsBlue = args("channelsBlue").toInt, channelsGreen = args("channelsGreen").toInt, channelsRed = args("channelsRed").toInt, channelsNir = args("channelsNir").toInt, channelsMir = args("channelsMir").toInt))
        case "Coverage.obtainUTMZoneFromGeoPointByOTB" =>
          OTB.otbObtainUTMZoneFromGeoPoint(sc, lat = args("lat").toDouble, lon = args("lon").toDouble)
        case "Coverage.largeScaleMeanShiftVectorByOTB" =>   //输入为栅格输出为矢量算哪一类？？
          featureRddList += (UUID -> OTB.otbLargeScaleMeanShiftVector(sc, coverageRddList(args("input")), spatialr = args("spatialr").toInt, ranger = args("ranger").toFloat, minsize = args("minsize").toInt,tilesizex = args("tilesizex").toInt,tilesizey=args("tilesizey").toInt))
        case "Coverage.largeScaleMeanShiftRasterByOTB" =>
          coverageRddList += (UUID -> OTB.otbLargeScaleMeanShiftRaster(sc, coverageRddList(args("input")), spatialr = args("spatialr").toInt, ranger = args("ranger").toFloat, minsize = args("minsize").toInt,tilesizex = args("tilesizex").toInt,tilesizey=args("tilesizey").toInt))
        case "Coverage.meanShiftSmoothingByOTB" =>
          coverageCollectionRddList += (UUID -> OTB.otbMeanShiftSmoothing(sc, coverageRddList(args("input")), spatialr = args("spatialr").toInt, ranger = args("ranger").toFloat, thres = args("thres").toFloat,maxiter = args("maxiter").toInt,rangeramp=args("rangeramp").toFloat))
        case "Coverage.localStatisticExtractionByOTB" =>
          coverageRddList += (UUID -> OTB.otbLocalStatisticExtraction(sc, coverageRddList(args("input")), channel = args("channel").toInt, radius = args("radius").toInt))
        case "Coverage.segmentationMeanshiftRasterByOTB" =>
          coverageRddList += (UUID -> OTB.otbSegmentationMeanshiftRaster(sc, coverageRddList(args("input")), spatialr = args("spatialr").toInt, ranger = args("ranger").toFloat, thres = args("thres").toFloat, maxiter = args("maxiter").toInt, minsize = args("minsize").toInt))
        case "Coverage.segmentationMeanshiftVectorByOTB" =>
          featureRddList += (UUID -> OTB.otbSegmentationMeanshiftVector(sc, coverageRddList(args("input")), spatialr = args("spatialr").toInt, ranger = args("ranger").toFloat, thres = args("thres").toFloat, maxiter = args("maxiter").toInt, minsize = args("minsize").toInt,neighbor = args("neighbor").toBoolean, stitch = args("stitch").toBoolean, v_minsize=args("v_minsize").toInt, simplify=args("simplify").toFloat, tilesize=args("tilesize").toInt, startlabel=args("startlabel").toInt))
        case "Coverage.segmentationWatershedRasterByOTB" =>
          coverageRddList += (UUID -> OTB.otbSegmentationWatershedRaster(sc, coverageRddList(args("input")), threshold = args("threshold").toFloat, level = args("level").toFloat))
        case "Coverage.segmentationWatershedVectorByOTB" =>
          featureRddList += (UUID -> OTB.otbSegmentationWatershedVector(sc, coverageRddList(args("input")), threshold = args("threshold").toFloat, level = args("level").toFloat, neighbor = args("neighbor").toBoolean, stitch = args("stitch").toBoolean, v_minsize=args("v_minsize").toInt, simplify=args("simplify").toFloat, tilesize=args("tilesize").toInt, startlabel=args("startlabel").toInt))
        case "Coverage.segmentationMprofilesdRasterByOTB" =>
          coverageRddList += (UUID -> OTB.otbSegmentationMprofilesdRaster(sc, coverageRddList(args("input")), size = args("size").toInt, start = args("start").toInt, step = args("step").toInt, sigma = args("sigma").toFloat))
        case "Coverage.segmentationMprofilesdVectorByOTB" =>
          featureRddList += (UUID -> OTB.otbSegmentationMprofilesVector(sc, coverageRddList(args("input")), size = args("size").toInt, start = args("start").toInt, step = args("step").toInt, sigma = args("sigma").toFloat, neighbor = args("neighbor").toBoolean, stitch = args("stitch").toBoolean, v_minsize=args("v_minsize").toInt, simplify=args("simplify").toFloat, tilesize=args("tilesize").toInt, startlabel=args("startlabel").toInt))
        case "Coverage.trainImagesRegressionLibSvmByOTB" =>
          stringList += (UUID -> OTB.otbTrainImagesRegressionLibSvm(sc, input_predict = coverageCollectionRddList(args("input_predict")),input_label = coverageCollectionRddList(args("input_predict")), ratio = args("ratio").toFloat, kernel = args("kernel"), model = args("model"), costc = args("costc").toFloat, gamma = args("gamma").toFloat, coefficient = args("coefficient").toFloat, degree = args("degree").toInt, costnu = args("costnu").toFloat, opt = args("opt").toBoolean, prob = args("prob").toBoolean, epsilon = args("epsilon").toFloat))
        case "Coverage.trainImagesRegressionDtByOTB" =>
          stringList += (UUID -> OTB.otbTrainImagesRegressionDt(sc, input_predict = coverageCollectionRddList(args("input_predict")),input_label = coverageCollectionRddList(args("input_predict")), ratio = args("ratio").toFloat, max = args("max").toInt, min = args("min").toInt, ra = args("ra").toFloat, cat = args("cat").toInt, r = args("r").toBoolean, t = args("t").toBoolean))
        case "Coverage.trainImagesRegressionRfByOTB" =>
          stringList += (UUID -> OTB.otbTrainImagesRegressionRf(sc, input_predict = coverageCollectionRddList(args("input_predict")),input_label = coverageCollectionRddList(args("input_predict")), ratio = args("ratio").toFloat, max = args("max").toInt, min = args("min").toInt, ra = args("ra").toFloat, cat = args("cat").toInt, var_ = args("var_").toInt, nbtrees = args("nbtrees").toInt, acc = args("acc").toFloat))
        case "Coverage.trainImagesRegressionKnnbyOTB" =>
          stringList += (UUID -> OTB.otbTrainImagesRegressionKnn(sc, input_predict = coverageCollectionRddList(args("input_predict")),input_label = coverageCollectionRddList(args("input_predict")), ratio = args("ratio").toFloat, number = args("max").toInt, rule = args("min")))
        case "Coverage.trainImagesRegressionSharkRfByOTB" =>
          stringList += (UUID -> OTB.otbTrainImagesRegressionSharkrf(sc, input_predict = coverageCollectionRddList(args("input_predict")),input_label = coverageCollectionRddList(args("input_predict")), ratio = args("ratio").toFloat, nbtrees = args("nbtrees").toInt, nodesize = args("nodesize").toInt, mtry = args("mtry").toInt, oobr = args("oobr").toFloat))
        case "Feature.ComputeOGRLayersFeaturesStatisticsByOTB" =>
          featureRddList += (UUID -> OTB.otbComputeOGRLayersFeaturesStatistics(sc, featureRddList(args("inshp")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], feat = args("feat").split(",").toList))
        case "Coverage.KMeansClassificationByOTB" =>
          coverageRddList += (UUID -> OTB.otbKMeansClassification(sc, coverageRddList(args("in")), nc = args("nc").toInt, ts = args("ts").toInt, maxit = args("maxit").toInt, centroidsIn = args("centroids.in"), centroidsOut = args("centroids.out"), ram = args("ram").toInt, sampler = args("sampler").split(",").toList, samplerPeriodicJitter = args("samplerPeriodicJitter").toInt, vm = args("vm"), nodatalabel = args("nodatalabel").toInt, cleanup = args("cleanup").toBoolean, rand = args("rand").toInt))
        case "Coverage.SOMClassificationByOTB" =>
          coverageRddList += (UUID -> OTB.otbSOMClassification(sc, coverageRddList(args("in")), vm = args("vm"), tp = args("tp").toFloat, ts = args("ts").toInt, som = args("som"), sx = args("sx").toInt, sy = args("sy").toInt, nx = args("nx").toInt, ny = args("ny").toInt, ni = args("ni").toInt, bi = args("bi").toFloat, bf = args("bf").toFloat, iv = args("iv").toFloat, rand = args("rand").toInt, ram = args("ram").toInt))

        case "Coverage.OpticalCalibrationByOTB" =>
          coverageRddList += (UUID -> OTB.otbOpticalCalibration(sc, coverageRddList(args("input")), level = args("level"), milli = args("milli").toBoolean, clamp = args("clamp").toBoolean, acquiMinute = args("acquiMinute").toInt, acquiHour = args("acquiHour").toInt, acquiDay = args("acquiDay").toInt, acquiMonth = args("acquiMonth").toInt, acquiYear = args("acquiYear").toInt, acquiFluxnormcoeff = args("acquiFluxnormcoeff").toFloat, acquiSolardistance = args("acquiSolardistance").toFloat, acquiSunElev = args("acquiSunElev").toFloat, acquiSunAzim = args("acquiSunAzim").toFloat, acquiViewElev = args("acquiViewElev").toFloat, acquiViewAzim = args("acquiViewAzim").toFloat, acquiGainbias = args("acquiGainbias"), acquiSolarilluminations = args("acquiSolarilluminations"), ram = args("ram").toInt, userId, dagId))
        case "Coverage.OpticalAtmosphericByOTB" =>
          coverageRddList += (UUID -> OTB.otbOpticalAtmospheric(sc, coverageRddList(args("input")), acquiGainbias = args("acquiGainbias"), acquiSolarilluminations = args("acquiSolarilluminations"), atmoAerosol = args("atmoAerosol"), atmoOz = args("atmoOz").toFloat, atmoWa = args("atmoWa").toFloat, atmoPressure = args("atmoPressure").toFloat, atmoOpt = args("atmoOpt").toFloat, atmoAeronet = args("atmoAeronet"), atmoRsr = args("atmoRsr"), atmoRadius = args("atmoRadius").toInt, atmoPixsize = args("atmoPixsize").toFloat, level = args("level"), ram = args("ram").toInt, userId, dagId))
        case "Coverage.OrthoRectificationByOTB" =>
          coverageRddList += (UUID -> OTB.otbOrthoRectification(sc, ioIn = coverageRddList(args("ioIn")), map = args("map"), mapUtmZone = args("mapUtmZone").toInt, mapUtmNorthhem = args("mapUtmNorthhem").toBoolean, mapEpsgCode = args("mapEpsgCode").toInt, outputsMode = args("outputsMode"), outputsUlx = args("outputsUlx").toDouble, outputsUly = args("outputsUly").toDouble, outputsSizex = args("outputsSizex").toInt, outputsSizey = args("outputsSizey").toInt, outputsSpacingx = args("outputsSpacingx").toDouble, outputsSpacingy = args("outputsSpacingy").toDouble, outputsLrx = args("outputsLrx").toDouble, outputsLry = args("outputsLry").toDouble, outputsOrtho = args("outputsOrtho"), outputsIsotropic = args("outputsIsotropic").toBoolean, outputsDefault = args("outputsDefault").toDouble, elevDem = args("elevDem"), elevGeoid = args("elevGeoid"), elevDefault = args("elevDefault").toFloat, interpolator = args("interpolator"), interpolatorBcoRadius = args("interpolatorBcoRadius").toInt, optRpc = args("optRpc").toInt, optRam = args("optRam").toInt, optGridspacing = args("optGridspacing").toDouble))
        //        case "Coverage.PansharpeningByOTB" =>
        //          coverageRddList += (UUID -> OTB.otbPansharpening(sc, inp = coverageRddList(args("inp")), inxs = coverageRddList(args("inxs")), method = args("method"), methodRcsRadiusx = args("method.rcs.radiusx").toInt, methodRcsRadiusy = args("method.rcs.radiusy").toInt, methodLmvmRadiusx = args("method.lmvm.radiusx").toInt, methodLmvmRadiusy = args("method.lmvm.radiusy").toInt, methodBayesLambda = args("method.bayes.lambda").toFloat, methodBayesS = args("method.bayes.s").toFloat, ram = args("ram").toInt))
        case "Coverage.BundleToPerfectSensorByOTB" =>
          coverageRddList += (UUID -> OTB.otbBundleToPerfectSensor(sc, inp = coverageRddList(args("inp")), inxs = coverageRddList(args("inxs")), elevDem = args("elevDem"), elevGeoid = args("elevGeoid"), elevDefault = args("elevDefault").toFloat, mode = args("mode"), method = args("method"), methodRcsRadiusx = args("methodRcsRadiusx").toInt, methodRcsRadiusy = args("methodRcsRadiusy").toInt, methodLmvmRadiusx = args("methodLmvmRadiusx").toInt, methodLmvmRadiusy = args("methodLmvmRadiusy").toInt, methodBayesLambda = args("methodBayesLambda").toFloat, methodBayesS = args("methodBayesS").toFloat, lms = args("lms").toFloat, interpolator = args("interpolator"), interpolatorBcoRadius = args("interpolatorBcoRadius").toInt, fv = args("fv").toFloat, ram = args("ram").toInt))


        case "Coverage.SampleExtractionByOTB" =>
          featureRddList += (UUID -> OTB.otbSampleExtraction(sc, in = coverageRddList(args("in")), vec = featureRddList(args("vec")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], outfield = args("outfield").split(",").toList, outfieldPrefixName = args("outfield.prefix.name"), outfieldListNames = args("outfield.list.names").split(",").toList, field = args("field"), layer = args("layer").toInt, ram = args("ram").toInt))
        case "Coverage.SampleSelectionByOTB" =>
          featureRddList += (UUID -> OTB.otbSampleSelection(sc, in = coverageRddList(args("in")), mask = coverageRddList(args("mask")), vec = featureRddList(args("vec")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], instats = args("instats"), outrates = args("outrates"), sampler = args("sampler").split(",").toList, samplerPeriodicJitter = args("samplerPeriodicJitter").toInt, strategy = args("strategy"), strategyByclassIn = args("strategyByclassIn"), strategyConstantNb = args("strategyConstantNb").toInt, strategyPercentP = args("strategyPercentP").toFloat, strategyTotalV = args("strategyTotalV").toInt, field = args("field"), layer = args("layer").toInt, elevDem = args("elevDem"), elevGeoid = args("elevGeoid"), elevDefault = args("elevDefault").toFloat, rand = args("rand").toInt, ram = args("ram").toInt))
        case "Coverage.aspectByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalAspect(sc, coverageRddList(args("input")), band = args("band").toInt, trigAngle = args("trigAngle"), zeroFlat = args("zeroFlat"), computeEdges = args("computeEdges"), zevenbergen = args("zevenbergen"), options = args("options")))
        case "Coverage.contourByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalContour(sc, coverageRddList(args("input")), interval = args("interval").toDouble, ignoreNodata = args("ignoreNodata"), extra = args("extra"), create3D = args("create3D"), nodata = args("nodata"), offset = args("offset").toDouble, band = args("band").toInt, fieldName = args("fieldName"), options = args("options")))
        case "Coverage.contourPolygonByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalContourPolygon(sc, coverageRddList(args("input")), interval = args("interval").toDouble, ignoreNodata = args("ignoreNodata"), extra = args("extra"), create3D = args("create3D"), nodata = args("nodata"), offset = args("offset").toDouble, band = args("band").toInt, fieldNameMax = args("fieldNameMax"), fieldNameMin = args("fieldNameMin"), options = args("options")))
        case "Coverage.fillNodataByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalFillNodata(sc, coverageRddList(args("input")), distance = args("distance").toDouble, iterations = args("iterations").toDouble, extra = args("extra"), maskLayer = args("maskLayer"), noMask = args("noMask"), band = args("band").toInt, options = args("options")))
        case "Coverage.gridAverageByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridAverage(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], minPoints = args("minPoints").toDouble, extra = args("extra"), nodata = args("nodata").toDouble, angle = args("angle").toDouble, zField = args("zField"), dataType = args("dataType"), radius2 = args("radius2").toDouble, radius1 = args("radius1").toDouble, options = args("options")))
        case "Coverage.gridDataMetricsByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridDataMetrics(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], minPoints = args("minPoints").toDouble, extra = args("extra"), metric = args("metric"), nodata = args("nodata").toDouble, angle = args("angle").toDouble, zField = args("zField"), options = args("options"), dataType = args("dataType"), radius2 = args("radius2").toDouble, radius1 = args("radius1").toDouble))
        case "Coverage.gridInverseDistanceByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridInverseDistance(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], extra = args("extra"), power = args("power").toDouble, angle = args("angle").toDouble, radius2 = args("radius2").toDouble, radius1 = args("radius1").toDouble, smoothing = args("smoothing").toDouble, maxPoints = args("maxPoints").toDouble, minPoints = args("minPoints").toDouble, nodata = args("nodata").toDouble, zField = args("zField"), dataType = args("dataType"), options = args("options")))
        case "Coverage.gridInverseDistanceNNRByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridInverseDistanceNNR(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], extra = args("extra"), power = args("power").toDouble, radius = args("radius").toDouble, smoothing = args("smoothing").toDouble, maxPoints = args("maxPoints").toDouble, minPoints = args("minPoints").toDouble, nodata = args("nodata").toDouble, zField = args("zField"), dataType = args("dataType"), options = args("options")))
        case "Coverage.gridLinearByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridLinear(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], radius = args("radius").toDouble, extra = args("extra"), nodata = args("nodata").toDouble, zField = args("zField"), dataType = args("dataType"), options = args("options")))
        case "Coverage.gridNearestNeighborByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalGridNearestNeighbor(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], extra = args("extra"), nodata = args("nodata").toDouble, angle = args("angle").toDouble, radius2 = args("radius2").toDouble, radius1 = args("radius1").toDouble, zField = args("zField"), dataType = args("dataType"), options = args("options")))
        case "Coverage.hillShadeByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalHillShade(sc, coverageRddList(args("input")), combined = args("combined"), computeEdges = args("computeEdges"), extra = args("extra"), band = args("band").toInt, altitude = args("altitude").toDouble, zevenbergenThorne = args("zevenbergenThorne"), zFactor = args("zFactor").toDouble, multidirectional = args("multidirectional"), scale = args("scale").toDouble, azimuth = args("azimuth").toDouble, options = args("options")))
        case "Coverage.nearBlackByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalNearBlack(sc, coverageRddList(args("input")), white = args("white"), extra = args("extra"), near = args("near").toInt, options = args("options")))
        case "Coverage.proximityByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalProximity(sc, coverageRddList(args("input")), extra = args("extra"), nodata = args("nodata").toDouble, values = args("values"), band = args("band").toInt, maxDistance = args("maxDistance").toDouble, replace = args("replace").toDouble, units = args("units"), dataType = args("dataType"), options = args("options")))
        case "Coverage.roughnessByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalRoughness(sc, coverageRddList(args("input")), band = args("band").toInt, computeEdges = args("computeEdges"), options = args("options")))
        case "Coverage.slopeByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalSlope(sc, coverageRddList(args("input")), band = args("band").toInt, computeEdges = args("computeEdges"), asPercent = args("asPercent"), extra = args("extra"), scale = args("scale").toDouble, zevenbergen = args("zevenbergen"), options = args("options")))
        case "Coverage.tpiTopographicPositionIndexByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalTpiTopographicPositionIndex(sc, coverageRddList(args("input")), band = args("band").toInt, computeEdges = args("computeEdges"), options = args("options")))
        case "Coverage.triTerrainRuggednessIndexByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalTriTerrainRuggednessIndex(sc, coverageRddList(args("input")), band = args("band").toInt, computeEdges = args("computeEdges"), options = args("options")))
        case "Coverage.clipRasterByExtentByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalClipRasterByExtent(sc, coverageRddList(args("input")), projwin = args("projwin"), extra = args("extra"), nodata = args("nodata").toDouble, dataType = args("dataType"), options = args("options")))
        case "Coverage.clipRasterByMaskLayerByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalClipRasterByMaskLayer(sc, coverageRddList(args("input")),  featureRddList(args("mask")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], cropToCutLine = args("cropToCutLine"), targetExtent = args("targetExtent"), setResolution = args("setResolution"), extra = args("extra"), targetCrs = args("targetCrs"), keepResolution = args("keepResolution"), alphaBand = args("alphaBand"), options = args("options"), multithreading = args("multithreading"), dataType = args("dataType"), sourceCrs = args("sourceCrs")))
        case "Coverage.polygonizeByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalPolygonize(sc, coverageRddList(args("input")), extra = args("extra"), field = args("field"), band = args("band").toInt, eightConnectedness = args("eightConnectedness")))
        case "Coverage.rasterizeOverByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalRasterizeOver(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], coverageRddList(args("inputRaster")), extra = args("extra"), field = args("field"), add = args("add")))
        case "Coverage.rasterizeOverFixedValueByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalRasterizeOverFixedValue(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], coverageRddList(args("inputRaster")), burn = args("burn").toDouble, extra = args("extra"), add = args("add")))
        case "Coverage.rgbToPctByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalRgbToPct(sc, coverageRddList(args("input")), ncolors = args("ncolors").toDouble))
        case "Coverage.translateByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalTranslate(sc, coverageRddList(args("input")), extra = args("extra"), targetCrs = args("targetCrs"), nodata = args("nodata").toDouble, dataType = args("dataType"), copySubdatasets = args("copySubdatasets"), options = args("options")))
        case "Coverage.warpByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalWarp(sc, coverageRddList(args("input")), sourceCrs = args("sourceCrs"), targetCrs = args("targetCrs"), resampling = args("resampling"), noData = args("noData").toDouble, targetResolution = args("targetResolution").toDouble, options = args("options"), dataType = args("dataType"), targetExtent = args("targetExtent"), targetExtentCrs = args("targetExtentCrs"), multiThreading = args("multiThreading"), extra = args("extra")))
        case "Coverage.assignProjectionByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalAssignProjection(sc, coverageRddList(args("input")), crs = args("crs")))
        case "Feature.dissolveByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalDissolve(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], explodeCollections = args("explodeCollections"), field = args("field"), computeArea = args("computeArea"), keepAttributes = args("keepAttributes"), computeStatistics = args("computeStatistics"), countFeatures = args("countFeatures"), statisticsAttribute = args("statisticsAttribute"), options = args("options"), geometry = args("geometry")))
        case "Feature.clipVectorByExtentByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalClipVectorByExtent(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], extent = args("extent"), options = args("options")))
        case "Feature.clipVectorByPolygonByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalClipVectorByPolygon(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], mask = featureRddList(args("mask")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], options = args("options")))
        case "Feature.offsetCurveByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalOffsetCurve(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], distance = args("distance").toDouble, geometry = args("geometry"), options = args("options")))
        case "Feature.pointsAlongLinesByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalPointsAlongLines(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], distance = args("distance").toDouble, geometry = args("geometry"), options = args("options")))
        case "Feature.bufferVectorsByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalBufferVectors(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], distance = args("distance").toDouble, explodeCollections = args("explodeCollections"), field = args("field"), dissolve = args("dissolve"), geometry = args("geometry"), options = args("options")))
        case "Feature.oneSideBufferByGDAL" =>
          featureRddList += (UUID -> QGIS.gdalOneSideBuffer(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], distance = args("distance").toDouble, explodeCollections = args("explodeCollections"), bufferSide = args("bufferSide"), field = args("field"), dissolve = args("dissolve"), geometry = args("geometry"), options = args("options")))
        case "Feature.convertFromStringByQGIS" =>
          featureRddList += (UUID -> QGIS.convertFromString(sc, inputString = args("inputString")))
        case "Feature.rasterizeByGDAL" =>
          coverageRddList += (UUID -> QGIS.gdalRasterize(sc, featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],field = args("field"),burn = args("burn").toDouble,useZ = args("useZ"),units = args("units"),width = args("width").toDouble,height = args("height").toDouble,extent = args("extent"),nodata = args("nodata").toDouble))
        case "Coverage.calNDVI" =>
          coverageRddList += (UUID -> QGIS.calNDVI(sc, coverageRddList(args("input"))))
        case "Coverage.calLSWI" =>
          coverageRddList += (UUID -> QGIS.calLSWI(sc, coverageRddList(args("input"))))
        case "Coverage.getParaData" =>
          coverageRddList += (UUID -> QGIS.getParaData(sc,args("year"),args("quarter")))
        case "Coverage.calNPP" =>
          coverageRddList += (UUID -> QGIS.calNPP(sc, coverageRddList(args("inputLSWI")),coverageRddList(args("inputNDVI")),coverageRddList(args("paraData"))))

        // SAGA
        case "Coverage.gridStatisticsForPolygonsBySAGA" =>
          stringList += (UUID -> SAGA.sagaGridStatisticsForPolygons(sc, coverageCollectionRddList(args("grids")), featureRddList(args("polygons")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            args("fieldNaming"), args("method"),
            args("useMultipleCores"), args("numberOfCells"), args("minimum"),
            args("maximum"),args("range"),args("sum"),args("mean"),
            args("variance"), args("standardDeviation"), args("gini"),args("percentiles")))
        case "Coverage.histogramMatchingBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaHistogramMatching(sc, coverageRddList(args("grid")), coverageRddList(args("referenceGrid")), args("method").toInt, args("nclasses").toInt, args("maxSamples").toInt))
        case "Coverage.ISODATAClusteringForGridsBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaISODATAClusteringForGrids(sc, coverageCollectionRddList(args("features")), args("normalize"), args("iterations").toInt, args("clusterINI").toInt, args("clusterMAX").toInt, args("samplesMIN").toInt, args("initialize")))
        case "Coverage.simpleFilterBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaSimpleFilter(sc, coverageRddList(args("input")), args("method").toInt, args("kernelType").toInt, args("kernelRadius").toInt))

        case "Coverage.minimumdistanceClassificationBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaMinimumDistanceClassification(sc, coverageRddList(args("grids")), featureRddList(args("training")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],  featureRddList(args("training_samples")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("normalise").toBoolean, args("training_class"), args("training_with").toInt, args("train_buffer").toFloat, args("threshold_dist").toFloat, args("threshold_angle").toFloat,args("threshold_prob").toFloat,args("file_load"), args("relative_prob").toInt,userId,dagId))

        case "Coverage.maximumlikelihoodClassificationBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaMaximumLikelihoodClassification(sc, coverageRddList(args("grids")), featureRddList(args("training")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]],  featureRddList(args("training_samples")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("normalise").toBoolean, args("training_class"), args("training_with").toInt, args("train_buffer").toFloat, args("threshold_dist").toFloat, args("threshold_angle").toFloat,args("threshold_prob").toFloat,args("file_load"), args("relative_prob").toInt,args("method").toInt,userId,dagId))

        case "Coverage.svmClassificationBySAGA" =>
          coverageRddList += (UUID -> SAGA.sagaSVMClassification(sc, coverageRddList(args("grid")), featureRddList(args("ROI")).asInstanceOf[RDD[(String, (Geometry, Map[String, Any]))]], args("scaling").toInt, args("message").toInt, args("model_src").toInt, args("ROI_id"), args("svm_type").toInt, args("kernel_type").toInt, args("degree").toInt, args("gamma").toDouble, args("coef0").toDouble, args("cost").toDouble, args("nu").toDouble, args("eps_svr").toDouble, args("cache_size").toDouble, args("eps").toDouble, args("shrinking").toBoolean, args("probability").toBoolean, args("crossval").toInt))

        //    GRASS
        case "Coverage.neighborsByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_neighbors(sc, input = coverageRddList(args("input")), size = args("size"), method = args("method")))
        case "Coverage.bufferByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_buffer(sc, input = coverageRddList(args("input")), distances = args("distances"), unit = args("unit")))
        case "Coverage.crossByGrass" =>
          isActioned(sc, args("input"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> GrassUtil.r_cross(sc, input = coverageCollectionRddList(args("input"))))
        case "Coverage.patchByGrass" =>
          isActioned(sc, args("input"), OGEClassType.CoverageCollection)
          coverageRddList += (UUID -> GrassUtil.r_patch(sc, input = coverageCollectionRddList(args("input"))))
        case "Coverage.latlongByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_latlong(sc, input = coverageRddList(args("input"))))
        case "Coverage.blendByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_blend(sc, first = coverageRddList(args("first")), second = coverageRddList(args("second")), percent = args("percent")))
        case "Coverage.compositeByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_composite(sc, red = coverageRddList(args("red")), green = coverageRddList(args("green")), blue = coverageRddList(args("blue")), levels = args("levels")))
        case "Coverage.sunmaskByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_sunmask(sc, elevation = coverageRddList(args("elevation")), year = args("year"), month = args("month"), day = args("day"), hour = args("hour"), minute = args("minute"), second = args("second"), timezone = args("timezone")))
        case "Coverage.surfIdwByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_surf_idw(sc, input = coverageRddList(args("input")), npoints = args("npoints")))
        case "Coverage.rescaleByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_rescale(sc, input = coverageRddList(args("input")), to = args("to")))
        case "Coverage.surfAreaByGrass" =>
          stringList += (UUID -> GrassUtil.r_surf_area(sc, map = coverageRddList(args("map")), vscale = args("vscale")))
        case "Coverage.statsByGrass" =>
          stringList += (UUID -> GrassUtil.r_stats(sc, input = coverageRddList(args("input")), flags = args("flags"), separator = args("separator"), null_value = args("null_value"), nsteps = args("nsteps")))
        case "Coverage.coinByGrass" =>
          stringList += (UUID -> GrassUtil.r_coin(sc, first = coverageRddList(args("first")), second = coverageRddList(args("second")), units = args("units")))
        case "Coverage.volumeByGrass" =>
          stringList += (UUID -> GrassUtil.r_volume(sc, input = coverageRddList(args("input")), clump = coverageRddList(args("clump"))))
        case "Coverage.outPNGByGrass" =>
          stringList += (UUID -> GrassUtil.r_out_png(sc, input = coverageRddList(args("input")), compression = args("compression")))
        case "Coverage.resampStatsByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_resamp_stats(sc,coverageRddList(args("input")),args("res"),args("method"),args("quantile")))
        case "Coverage.resampInterpByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_resamp_interp(sc,coverageRddList(args("input")),args("res"),args("method")))
        case "Coverage.resampBsplineByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_resamp_bspline(sc,coverageRddList(args("input")),args("res"),args("method")))
        case "Coverage.resampFilterByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_resamp_filter(sc,coverageRddList(args("input")),args("res"),args("filter"),args("radius")))
        case "Coverage.resampleByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_resample(sc,coverageRddList(args("input")),args("res")))
        case "Coverage.textureByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_texture(sc,coverageRddList(args("input")),args("size"),args("distance"),args("method")))
        case "Coverage.viewshedByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_viewshed(sc,coverageRddList(args("input")),args("coordinates"),args("observer_elevation"),args("target_elevation"),args("max_distance"),args("refraction_coeff")))
        case "Coverage.shadeByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_shade(sc,coverageRddList(args("shade")),coverageRddList(args("color")),args("brighten")))
        case "Coverage.reclassByGrass" =>
          coverageRddList += (UUID -> GrassUtil.reclassByGrass(sc,coverageRddList(args("input")),args("rules")))
        case "Coverage.reportByGrass" =>
          stringList += (UUID -> GrassUtil.reportByGrass(sc,coverageRddList(args("map"))))
        case "Coverage.supportStatsByGrass" =>
          stringList += (UUID -> GrassUtil.supportStatsByGrass(sc,coverageRddList(args("map"))))
        case "Coverage.outGdalByGrass" =>
          coverageRddList += (UUID -> GrassUtil.outGdalByGrass(sc,coverageRddList(args("input")),args("format")))
        case "Coverage.outBinByGrass" =>
          stringList += (UUID -> GrassUtil.outBinByGrass(sc,coverageRddList(args("input")),args("nu_ll"),args("bytes"),args("order")))
        case "Coverage.inGdalByGrass" =>
          coverageRddList += (UUID -> GrassUtil.inGdalByGrass(sc,coverageRddList(args("input")),args("band"),args("location")))
        case "Coverage.growByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_grow(sc,coverageRddList(args("input"))))
        case "Coverage.fillnullByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_fillnulls(sc,coverageRddList(args("input"))))
        case "Coverage.randomByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_random(sc,coverageRddList(args("input")),args("npoints")))
        case "Coverage.univarByGrass" =>
          coverageRddList += (UUID -> GrassUtil.r_univar(sc,coverageRddList(args("input"))))


        // Kernel
        case "Kernel.chebyshev" =>
          kernelRddList += (UUID -> Kernel.chebyshev(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.circle" =>
          kernelRddList += (UUID -> Kernel.circle(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.compass" =>
          kernelRddList += (UUID -> Kernel.compass(args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.diamond" =>
          kernelRddList += (UUID -> Kernel.diamond(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.euclidean" =>
          kernelRddList += (UUID -> Kernel.euclidean(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.fixed" =>
          val kernel = Kernel.fixed(weights = args("weights"))
          kernelRddList += (UUID -> kernel)
        case "Kernel.gaussian" =>
          kernelRddList += (UUID -> Kernel.gaussian(args("radius").toInt, args("sigma").toFloat, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.inverse" =>
          kernelRddList += (UUID -> Kernel.inverse(kernelRddList(args("kernel"))))
        case "Kernel.manhattan" =>
          kernelRddList += (UUID -> Kernel.manhattan(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.octagon" =>
          kernelRddList += (UUID -> Kernel.octagon(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.plus" =>
          kernelRddList += (UUID -> Kernel.plus(args("radius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.square" =>
          val kernel = Kernel.square(radius = args("radius").toInt, normalize = args("normalize").toBoolean, value = args("value").toDouble)
          kernelRddList += (UUID -> kernel)
          print(kernel.tile.asciiDraw())
        case "Kernel.rectangle" =>
          kernelRddList += (UUID -> Kernel.rectangle(args("xRadius").toInt, args("yRadius").toInt, args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.roberts" =>
          kernelRddList += (UUID -> Kernel.roberts(args("normalize").toBoolean, args("magnitude").toFloat))
        case "Kernel.rotate" =>
          kernelRddList += (UUID -> Kernel.rotate(kernelRddList(args("kernel")), args("rotations").toInt))
        case "Kernel.prewitt" =>
          val kernel = Kernel.prewitt(args("normalize").toBoolean, args("magnitude").toFloat)
          kernelRddList += (UUID -> kernel)
          print(kernel.tile.asciiDraw())
        case "Kernel.kirsch" =>
          val kernel = Kernel.kirsch(args("normalize").toBoolean, args("magnitude").toFloat)
          kernelRddList += (UUID -> kernel)
        case "Kernel.sobel" =>
          val kernel = Kernel.sobel(axis = args("axis"))
          kernelRddList += (UUID -> kernel)
        case "Kernel.laplacian4" =>
          val kernel = Kernel.laplacian4()
          kernelRddList += (UUID -> kernel)
        case "Kernel.laplacian8" =>
          val kernel = Kernel.laplacian8()
          kernelRddList += (UUID -> kernel)
        case "Kernel.laplacian8" =>
          val kernel = Kernel.laplacian8()
          kernelRddList += (UUID -> kernel)
          print(kernel.tile.asciiDraw())
        case "Kernel.add" =>
          val kernel = Kernel.add(kernel1 = kernelRddList(args("kernel1")), kernel2 = kernelRddList(args("kernel2")))
          kernelRddList += (UUID -> kernel)

        // Terrain
        case "Coverage.slope" =>
          coverageRddList += (UUID -> Coverage.slope(coverage = coverageRddList(args("coverage")), radius = args("radius").toDouble.toInt, zFactor = args("Z_factor").toDouble))
        case "Coverage.aspect" =>
          coverageRddList += (UUID -> Coverage.aspect(coverage = coverageRddList(args("coverage")), radius = args("radius").toInt))

        // Terrain By CYM
        case "Coverage.terrSlope" =>
          coverageRddList += (UUID -> calculator.Slope(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrAspect" =>
          coverageRddList += (UUID -> calculator.Aspect(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrRuggedness" =>
          coverageRddList += (UUID -> calculator.Ruggedness(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrSlopelength" =>
          coverageRddList += (UUID -> calculator.SlopeLength(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrCurvature" =>
          coverageRddList += (UUID -> calculator.Curvature(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrHillshade" =>
          coverageRddList += (UUID -> calculator.HillShade(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrPitrouter" =>
          coverageRddList += (UUID -> calculator.PitRouter(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrPiteliminator" =>
          coverageRddList += (UUID -> calculator.PitEliminator(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFlowdirection" =>
          coverageRddList += (UUID -> calculator.FlowDirection(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFlowaccumulation" =>
          coverageRddList += (UUID -> calculator.FlowAccumulation(rddImage = coverageRddList(args("coverage")), zFactor = args("z-Factor").toDouble))
        case "Coverage.terrChannelnetwork" =>
          featureRddList += (UUID -> calculator.ChannelNetwork(rddImage = coverageRddList(args("DEM")), flowAccumulationImage = coverageRddList(args("FlowAccumulation")), dirImage = coverageRddList(args("FlowDirection")), zFactor = args("z-Factor").toDouble, threshold = args("threshold").toDouble))
        case "Coverage.terrFilter" =>
          coverageRddList += (UUID -> calculator.Filter(rddImage = coverageRddList(args("coverage")), min = args("min").slice(1, args("min").length - 1).split(',').toList.map(_.toDouble), max = args("max").slice(1, args("max").length - 1).split(',').toList.map(_.toDouble), zFactor = args("z-Factor").toDouble))
        case "Coverage.terrStrahlerOrder" =>
          coverageRddList += (UUID -> calculator.StrahlerOrder(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFlowConnectivity" =>
          coverageRddList += (UUID -> calculator.FlowConnectivity(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFlowLength" =>
          coverageRddList += (UUID -> calculator.FlowLength(rddImage = coverageRddList(args("coverage")), radius = if (args("radius").toInt < 16) 16 else args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFlowWidth" =>
          coverageRddList += (UUID -> calculator.FlowWidth(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble))
        case "Coverage.terrWatershedBasins" =>
          coverageRddList += (UUID -> calculator.WatershedBasins(rddImage = coverageRddList(args("coverage")), flowAccumulationImage = coverageRddList(args("FlowAccumulation")), zFactor = args("z-Factor").toDouble))
        case "Coverage.terrFeatureSelect" =>
          featureRddList += (UUID -> calculator.FeatureSelect(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble, vipValue = args("vipValue").toDouble))
        case "Coverage.terrTIN" =>
          featureRddList += (UUID -> calculator.TIN(rddImage = coverageRddList(args("coverage")), radius = args("radius").toInt, zFactor = args("z-Factor").toDouble, vipValue = args("vipValue").toDouble, geometryType = args("geometryType").toInt))


        case "Coverage.addStyles" =>
          val visParam: VisualizationParam = new VisualizationParam
          visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
          println("isBatch", isBatch)
          if (isBatch == 0) {
            if(workType.equals("main")){
              Coverage.visualizeOnTheFly(sc, coverage = coverageRddList(args("coverage")), visParam = visParam)
            }else if(workType.equals("edu")){
              Coverage.visualizeBatch_edu(sc, coverage = coverageRddList(args("coverage")), batchParam = batchParam, dagId)
            }

          } else {
            // TODO: 增加添加样式的函数
            coverageRddList += (UUID -> Coverage.addStyles(coverageRddList(args("coverage")), visParam = visParam))
          }
        // thirdAlgorithm
        case "Coverage.demRender" =>
          coverageRddList += (UUID -> QGIS.demRender(sc, coverage = coverageRddList(args("coverage"))))
        case "Coverage.surfaceAlbedoLocalNoon" =>
          coverageRddList += (UUID -> QuantRS.surfaceAlbedoLocalNoon(sc, coverageRddList(args("TOAReflectance")), coverageRddList(args("solarZenith")), coverageRddList(args("solarAzimuth")), coverageRddList(args("sensorZenith")), coverageRddList(args("sensorAzimuth")), coverageRddList(args("cloudMask")), args("timeStamp"), args("localnoonCoefs"), args("parameters"), args("bands").toInt))
        case "Coverage.imaginaryConstellations" =>
          coverageRddList += (UUID -> QuantRS.imaginaryConstellations(sc, coverageRddList(args("LAI")), coverageRddList(args("FAPAR")), coverageRddList(args("NDVI")), coverageRddList(args("FVC")), coverageRddList(args("ALBEDO"))))

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
            featureRddList += (UUID -> Feature.geometry(sc, args("coors"), args("crs")))
          else
            featureRddList += (UUID -> Feature.geometry(sc, args("coors")))
        case "Feature.area" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.area(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
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
          stringList += (UUID -> Feature.coordinates(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.reproject" =>
          featureRddList += (UUID -> Feature.reproject(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("tarCrsCode")))
        case "Feature.isUnbounded" =>
          stringList += (UUID -> Feature.isUnbounded(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.getType" =>
          stringList += (UUID -> Feature.getType(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.projection" =>
          stringList += (UUID -> Feature.projection(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.toGeoJSONString" =>
          stringList += (UUID -> Feature.toGeoJSONString(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.length" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.length(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.length(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.geometries" =>
          featureRddList += (UUID -> Feature.geometries(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.dissolve" =>
          if (isOptionalArg(args, "crs") != null)
            featureRddList += (UUID -> Feature.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            featureRddList += (UUID -> Feature.dissolve(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.contains" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.contains(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.containedIn" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.containedIn(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.disjoint" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.disjoint(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.distance" =>
          if (isOptionalArg(args, "crs") != null)
            stringList += (UUID -> Feature.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("crs")))
          else
            stringList += (UUID -> Feature.distance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
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
            stringList += (UUID -> Feature.intersects(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
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
            stringList += (UUID -> Feature.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble, args("crs")))
          else
            stringList += (UUID -> Feature.withDistance(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
              featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("distance").toDouble))
        case "Feature.copyProperties" =>
          val propertyList: List[String] =
            if (args("properties") == "[]") {
              Nil // 表示空列表
            } else {
              args("properties").replace("[", "").replace("]", "").replace("\"", "").split(",").toList
            }
          featureRddList += (UUID -> Feature.copyProperties(featureRddList(args("featureRDD1")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("featureRDD2")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], propertyList))
        case "Feature.get" =>
          stringList += (UUID -> Feature.get(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")).toString())
        case "Feature.propertyNames" =>
          stringList += (UUID -> Feature.propertyNames(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.set" =>
          featureRddList += (UUID -> Feature.set(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property")))
        case "Feature.setGeometry" =>
          featureRddList += (UUID -> Feature.setGeometry(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],
            featureRddList(args("geom")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "Feature.featureCollection" =>
          featureRddList += (UUID -> Feature.featureCollection(sc,args("featureList").stripPrefix("[").stripSuffix("]").split(",").toList.map(t =>{featureRddList(t).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]})))
        case "Feature.addStyles" =>
          Feature.visualize(feature = featureRddList(args("input")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],color = args("color").replace("[", "").replace("]", "").split(',').toList,attribute = args("attribute"))
        //algorithms.SpatialStats
        case "SpatialStats.GWModels.GWRbasic.autoFit" =>
          val re_gwr = GWModels.GWRbasic.autoFit(sc,featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]],args("propertyY"),args("propertiesX"),args("kernel"),args("approach"),args("adaptive").toBoolean)
          featureRddList += (UUID -> re_gwr)
        //          Service.print(re_gwr._2, "Diagnostics", "String")
        case "SpatialStats.GWModels.GWRbasic.fit" =>
          val re_gwr = GWModels.GWRbasic.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean)
          featureRddList += (UUID -> re_gwr)
        case "SpatialStats.GWModels.GWRbasic.auto" =>
          val re_gwr = GWModels.GWRbasic.auto(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("kernel"), args("approach"), args("adaptive").toBoolean,args("varSelTh").toDouble)
          featureRddList += (UUID -> re_gwr)
        case "SpatialStats.BasicStatistics.AverageNearestNeighbor" =>
          stringList += (UUID -> AverageNearestNeighbor.result(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "SpatialStats.BasicStatistics.DescriptiveStatistics" =>
          stringList += (UUID -> DescriptiveStatistics.result(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]]))
        case "SpatialStats.STCorrelations.CorrelationAnalysis.corrMat" =>
          stringList += (UUID -> CorrelationAnalysis.corrMat(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("properties"), args("method")))
        case "SpatialStats.STCorrelations.SpatialAutoCorrelation.globalMoranI" =>
          stringList += (UUID -> SpatialAutoCorrelation.globalMoranI(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("plot").toBoolean, args("test").toBoolean, args("weightstyle")))
        case "SpatialStats.STCorrelations.SpatialAutoCorrelation.localMoranI" =>
          featureRddList += (UUID -> SpatialAutoCorrelation.localMoranI(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("adjust").toBoolean))
        case "SpatialStats.STCorrelations.TemporalAutoCorrelation.ACF" =>
          stringList += (UUID -> TemporalAutoCorrelation.ACF(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("property"), args("timelag").toInt))
        case "SpatialStats.SpatialRegression.SpatialLagModel.fit" =>
          val re_slm = SpatialLagModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
          featureRddList += (UUID -> re_slm)
        case "SpatialStats.SpatialRegression.SpatialErrorModel.fit" =>
          val re_sem = SpatialErrorModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
          featureRddList += (UUID -> re_sem)
        case "SpatialStats.SpatialRegression.SpatialDurbinModel.fit" =>
          val re_sdm = SpatialDurbinModel.fit(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"))
          featureRddList += (UUID -> re_sdm)
        case "SpatialStats.SpatialRegression.LinearRegression.feature" =>
          featureRddList += (UUID -> LinearRegression.LinearReg(sc, featureRddList(args("data")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y"), args("x"), args("Intercept").toBoolean))
        case "SpatialStats.GWModels.GWAverage" =>
          featureRddList += (UUID -> GWModels.GWAverage.cal(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean, args("quantile").toBoolean))
        case "SpatialStats.GWModels.GWCorrelation" =>
          featureRddList += (UUID -> GWModels.GWCorrelation.cal(sc, featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("propertyY"), args("propertiesX"), args("bandwidth").toDouble, args("kernel"), args("adaptive").toBoolean))
        case "SpatialStats.SpatialHeterogeneity.GeoRiskDetector" =>
          stringList += (UUID -> Geodetector.riskDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
        case "SpatialStats.SpatialHeterogeneity.GeoFactorDetector" =>
          stringList += (UUID -> Geodetector.factorDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
        case "SpatialStats.SpatialHeterogeneity.GeoInteractionDetector" =>
          stringList += (UUID -> Geodetector.interactionDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
        case "SpatialStats.SpatialHeterogeneity.GeoEcologicalDetector" =>
          stringList += (UUID -> Geodetector.ecologicalDetector(featureRddList(args("featureRDD")).asInstanceOf[RDD[(String, (Geometry, mutable.Map[String, Any]))]], args("y_title"), args("x_titles")))
        case "algorithms.gmrc.geocorrection.GeoCorrection.geometricCorrection" =>
          coverageRddList += (UUID -> GeoCorrection.geometricCorrection(sc, coverageCollectionRddList(args("coverageCollection"))))
        case "algorithms.gmrc.mosaic.Mosaic.splitMosaic" =>
          coverageRddList += (UUID -> Mosaic.splitMosaic(sc, coverageCollectionRddList(args("coverageCollection"))))
        case "algorithms.gmrc.colorbalance.ColorBalance.colorBalance" =>
          coverageRddList += (UUID -> ColorBalance.colorBalance(sc, coverageRddList(args("coverage"))))
        case "algorithms.gmrc.colorbalanceRef.ColorBalanceWithRef.colorBalanceRef" =>
          coverageRddList += (UUID -> ColorBalanceWithRef.colorBalanceRef(sc, coverageCollectionRddList(args("coverageCollection"))))
        //Cube
        //        case "Service.getCollections" =>
        //          cubeLoad += (UUID -> (isOptionalArg(args, "productIDs"), isOptionalArg(args, "datetime"), isOptionalArg(args, "bbox")))
        //        case "Collections.toCube" =>
        //          cubeRDDList += (UUID -> Cube.load(sc, productList = cubeLoad(args("input"))._1, dateTime = cubeLoad(args("input"))._2, geom = cubeLoad(args("input"))._3, bandList = isOptionalArg(args, "bands")))
        case "Cube.addStyles" => {
          val visParam: VisualizationParam = new VisualizationParam
          visParam.setAllParam(bands = isOptionalArg(args, "bands"), gain = isOptionalArg(args, "gain"), bias = isOptionalArg(args, "bias"), min = isOptionalArg(args, "min"), max = isOptionalArg(args, "max"), gamma = isOptionalArg(args, "gamma"), opacity = isOptionalArg(args, "opacity"), palette = isOptionalArg(args, "palette"), format = isOptionalArg(args, "format"))
          CubeNew.visualizeOnTheFly(sc, cubeRDDList(args("cube")), visParam)
        }
        case "Cube.NDVI" =>
          cubeRDDList += (UUID -> CubeNew.normalizedDifference(sc, cubeRDDList(args("input")), bandName1 = args("bandName1"), platform1 = args("platform1"), bandName2 = args("bandName2"), platform2 = args("platform2")))
        case "Cube.add" =>
          cubeRDDList += (UUID -> CubeNew.add(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
        case "Cube.subtract" =>
          cubeRDDList += (UUID -> CubeNew.subtract(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
        case "Cube.multiply" =>
          cubeRDDList += (UUID -> CubeNew.multiply(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
        case "Cube.divide" =>
          cubeRDDList += (UUID -> CubeNew.divide(cube1 = cubeRDDList(args("cube1")), cube2 = cubeRDDList(args("cube2"))))
        // TrainingDML-AI 新增算子
        case "Dataset.encoding" =>
          stringList += (UUID -> AI.getTrainingDatasetEncoding(args("datasetName")))
        case "AI.ANNClassification" =>
          coverageRddList += (UUID -> ML.ANNClassification(sc,coverage = coverageRddList(args("coverage")),args("sampleFiles").slice(1, args("sampleFiles").length - 1).split(',').toList.map(_.toString)))
        case "AI.SVMClassification" =>
          coverageRddList += (UUID -> ML.SVMClassification(sc,coverage = coverageRddList(args("coverage")),args("sampleFiles").slice(1, args("sampleFiles").length - 1).split(',').toList.map(_.toString)))
        case "Coverage.RandomForestTrainAndRegress" =>
          coverageRddList += (UUID -> RandomForestTrainAndRegress(featuresCoverage = coverageRddList(args("featuresCoverage")),labelCoverage = coverageRddList(args("labelCoverage")),predictCoverage = coverageRddList(args("predictCoverage")),args("checkpointInterval").toInt, args("featureSubsetStrategy"), args("impurity"), args("maxBins").toInt, args("maxDepth").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("numTrees").toInt, args("seed").toLong, args("subsamplingRate").toDouble))
        case "Coverage.histogramBin" =>
          stringList += (UUID -> histogramBin(coverage = coverageRddList(args("coverage")), args("min").toInt, args("max").toInt, args("binSize").toInt, args("bandIndex").toInt).toString())
        case "Coverage.reduceRegion" =>
          doubleList += (UUID -> reduceRegion(coverage = coverageRddList(args("coverage")), args("reducer"), args("bandIndex").toInt))
        case "Coverage.filter" =>
          coverageRddList += (UUID -> Coverage.filter(coverage = coverageRddList(args("coverage")), args("min").toDouble, args("max").toDouble))
        // func已达最大大小，新增算子添加至func1
        case _ =>
          func1(sc, UUID, funcName, args)
      }

    } catch {
      case e: Throwable =>
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
        Trigger.mlmodelRddList.clear()
        Trigger.featureRddList.clear()
        Trigger.cubeRDDList.clear()
        Trigger.cubeLoad.clear()
        Trigger.SheetList.clear()

        throw e
    }
  }

  def func1(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    try {
      val tempNoticeJson = new JSONObject
      println("args:", funcName + args)
      funcName match {
        // MLlib 分类
        case "MLmodel.randomForestClassifierModel" =>
          mlmodelRddList += (UUID -> randomForestClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("checkpointInterval").toInt, args("featureSubsetStrategy"), args("maxBins").toInt, args("maxDepth").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("numTrees").toInt, args("seed").toLong, args("subsamplingRate").toDouble))
        case "MLmodel.logisticRegressionClassifierModel" =>
          mlmodelRddList += (UUID -> logisticRegressionClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("maxIter").toInt, args("regParam").toDouble, args("elasticNetParam").toDouble, args("family"), args("fitIntercept").toBoolean, args("standardization").toBoolean, args("threshold").toDouble, args("tol").toDouble))
        case "MLmodel.decisionTreeClassifierModel" =>
          mlmodelRddList += (UUID -> decisionTreeClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("checkpointInterval").toInt, args("impurity"), args("maxBins").toInt, args("maxDepth").toInt, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("seed").toLong))
        case "MLmodel.gbtClassifierClassifierModel" =>
          mlmodelRddList += (UUID -> gbtClassifierClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("maxIter").toInt, args("featureSubsetStrategy"), args("checkpointInterval").toInt, args("impurity"), args("lossType"), args("maxBins").toInt, args("maxDepth").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("seed").toLong, args("stepSize").toDouble, args("subSamplingRate").toDouble))
        case "MLmodel.multilayerPerceptronClassifierModel" =>
          mlmodelRddList += (UUID -> multilayerPerceptronClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("layers").slice(1, args("layers").length - 1).split(',').map(_.toInt), args("blockSize").toInt, args("seed").toLong, args("maxIter").toInt, args("stepSize").toDouble, args("tol").toDouble))
        case "MLmodel.linearSVCClassifierModel" =>
          mlmodelRddList += (UUID -> linearSVCClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("maxIter").toInt, args("regParam").toDouble))
        case "MLmodel.naiveBayesClassifierModel" =>
          mlmodelRddList += (UUID -> naiveBayesClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("modelType"), args("smoothing").toDouble))
        case "MLmodel.fmClassifierModel" =>
          mlmodelRddList += (UUID -> fmClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("stepSize").toDouble, args("factorSize").toInt, args("fitIntercept").toBoolean, args("fitLinear").toBoolean, args("initStd").toDouble, args("maxIter").toInt, args("minBatchFraction").toDouble, args("regParam").toDouble, args("seed").toLong, args("solver"), args("tol").toDouble))
        case "MLmodel.oneVsRestClassifierModel" =>
          mlmodelRddList += (UUID -> oneVsRestClassifierModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("classifier")))
        case "MLmodel.modelClassify" =>
          coverageRddList += (UUID -> modelClassify(sc, coverage = coverageRddList(args("coverage")), model = mlmodelRddList(args("model"))))
        // 回归
        case "MLmodel.randomForestRegressionModel" =>
          mlmodelRddList += (UUID -> randomForestRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("checkpointInterval").toInt, args("featureSubsetStrategy"), args("impurity"), args("maxBins").toInt, args("maxDepth").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("numTrees").toInt, args("seed").toLong, args("subsamplingRate").toDouble))
        case "MLmodel.linearRegressionModel" =>
          mlmodelRddList += (UUID -> linearRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("maxIter").toInt, args("regParam").toDouble, args("elasticNetParam").toDouble, args("fitIntercept").toBoolean, args("loss"), args("solver"), args("standardization").toBoolean, args("tol").toDouble))
        case "MLmodel.generalizedLinearRegressionModel" =>
          mlmodelRddList += (UUID -> generalizedLinearRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("regParam").toDouble, args("family"), args("link"), args("maxIter").toInt, args("fitIntercept").toBoolean, args("linkPower").toDouble, args("solver"), args("tol").toDouble, args("variancePower").toDouble))
        case "MLmodel.decisionTreeRegressionModel" =>
          mlmodelRddList += (UUID -> decisionTreeRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("checkpointInterval").toInt, args("impurity"), args("maxBins").toInt, args("maxDepth").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("seed").toLong))
        case "MLmodel.gbtRegressionModel" =>
          mlmodelRddList += (UUID -> gbtRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("checkpointInterval").toInt, args("featureSubsetStrategy"), args("impurity"), args("lossType"), args("maxBins").toInt, args("maxDepth").toInt, args("maxIter").toInt, args("minInfoGain").toDouble, args("minInstancesPerNode").toInt, args("minWeightFractionPerNode").toDouble, args("seed").toLong, args("stepSize").toDouble, args("subsamplingRate").toDouble))
        case "MLmodel.isotonicRegressionModel" =>
          mlmodelRddList += (UUID -> isotonicRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("isotonic").toBoolean))
        case "MLmodel.fmRegressionModel" =>
          mlmodelRddList += (UUID -> fmRegressionModel(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), labelCoverage = coverageRddList(args("labelCoverage")), args("factorSize").toInt, args("fitIntercept").toBoolean, args("fitLinear").toBoolean, args("initStd").toDouble, args("maxIter").toInt, args("minBatchFraction").toDouble, args("regParam").toDouble, args("seed").toLong, args("solver"), args("stepSize").toDouble, args("tol").toDouble))
        case "MLmodel.modelRegress" =>
          coverageRddList += (UUID -> modelRegress(sc, coverage = coverageRddList(args("coverage")), model = mlmodelRddList(args("model"))))
        // 聚类
        case "MLmodel.mlKMeans" =>
          coverageRddList += (UUID -> mlKMeans(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), args("k").toInt, args("maxIter").toInt, args("seed").toLong, args("tol").toDouble))
        case "MLmodel.latentDirichletAllocation" =>
          coverageRddList += (UUID -> latentDirichletAllocation(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), args("checkpointInterval").toInt, args("k").toInt, args("maxIter").toInt, args("optimizer"), args("seed").toLong, args("subsamplingRate").toDouble, args("topicConcentration").toDouble))
        case "MLmodel.bisectingKMeans" =>
          coverageRddList += (UUID -> bisectingKMeans(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), args("distanceMeasure"), args("k").toInt, args("maxIter").toInt, args("seed").toLong))
        case "MLmodel.gaussianMixture" =>
          coverageRddList += (UUID -> gaussianMixture(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), args("k").toInt, args("maxIter").toInt, args("seed").toLong, args("tol").toDouble))
        // 精度评估
        case "MLmodel.multiclassClassificationEvaluator" =>
          stringList += (UUID -> multiclassClassificationEvaluator(sc, labelCoverage = coverageRddList(args("labelCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName").slice(1, args("metricName").length - 1).split(',').toList.map(_.toString), args("metricLabel").toDouble).toString)
        case "MLmodel.clusteringEvaluator" =>
          stringList += (UUID -> clusteringEvaluator(sc, featuresCoverage = coverageRddList(args("featuresCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName"), args("distanceMeasure")).toString)
        case "MLmodel.multilabelClassificationEvaluator" =>
          stringList += (UUID -> multilabelClassificationEvaluator(sc, labelCoverage = coverageRddList(args("labelCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName").slice(1, args("metricName").length - 1).split(',').toList.map(_.toString)).toString)
        case "MLmodel.binaryClassificationEvaluator" =>
          stringList += (UUID -> binaryClassificationEvaluator(sc, labelCoverage = coverageRddList(args("labelCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName").slice(1, args("metricName").length - 1).split(',').toList.map(_.toString)).toString)
        case "MLmodel.regressionEvaluator" =>
          stringList += (UUID -> regressionEvaluator(sc, labelCoverage = coverageRddList(args("labelCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName").slice(1, args("metricName").length - 1).split(',').toList.map(_.toString)).toString)
        case "MLmodel.rankingEvaluator" =>
          stringList += (UUID -> rankingEvaluator(sc, labelCoverage = coverageRddList(args("labelCoverage")), predictionCoverage = coverageRddList(args("predictionCoverage")), args("metricName").slice(1, args("metricName").length - 1).split(',').toList.map(_.toString)).toString)

      }
    } catch {
      case e: Throwable =>
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
        Trigger.mlmodelRddList.clear()
        Trigger.featureRddList.clear()
        Trigger.cubeRDDList.clear()
        Trigger.cubeLoad.clear()
        Trigger.SheetList.clear()

        throw e
    }
  }

  def lambda(implicit sc: SparkContext,
             list: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])])
  : Unit = {
    for (i <- list.indices) {
      try {
        func(sc, list(i)._1, list(i)._2, list(i)._3)
      } catch {
        case e: Throwable =>
          //          throw new Exception("Error occur in lambda: " +
          //            "UUID = " + list(i)._1 + "\t" +
          //            "funcName = " + list(i)._2 + "\n" +
          //            "innerErrorType = " + e.getClass + "\n" +
          //            "innerErrorInfo = " + e.getMessage + "\n" +
          //            e.getStackTrace.mkString("StackTrace:(\n", "\n", "\n)"))
          throw new Exception(e)
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
              curDagID: String, userID: String): Unit = {

    // Check if the SparkContext is active
    if(sc.isStopped){
      sendPost(DAG_ROOT_URL + "/deliverUrl","ERROR")
      println("Send to boot!")
      return
    }

    /* sc,workTaskJson,workID,originTaskID */
    workType = "main"
    workTaskJson = curWorkTaskJson
    dagId = curDagID
    userId = userID
    val time1: Long = System.currentTimeMillis()

    val jsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(jsonObject)

    isBatch = jsonObject.getString("isBatch").toInt
    //教育版判断
    try{
      if (jsonObject.getString("dagType").equals("edu")) {
        dagType = "edu"
        outputFile = jsonObject.getString("outputFile")
      }}catch {
      case e:Exception =>
        dagType = ""
        outputFile = ""
    }
    //    if(jsonObject.getString("dagType").equals("edu")){
    //      dagType = "edu"
    //      outputFile = jsonObject.getString("outputFile")
    //    }else{
    //      dagType = ""
    //      outputFile = ""
    //    }

    layerName = jsonObject.getString("layerName")
    try{
      val map: JSONObject = jsonObject.getJSONObject("map")
      level = map.getString("level").toInt
      val spatialRange: Array[Double] = map.getString("spatialRange")
        .substring(1, map.getString("spatialRange").length - 1).split(",").map(_.toDouble)
      println("spatialRange = " + spatialRange.mkString("Array(", ", ", ")"))

      windowExtent = Extent(spatialRange.head, spatialRange(1), spatialRange(2), spatialRange(3))
      println("WindowExtent", windowExtent.xmin, windowExtent.ymin, windowExtent.xmax, windowExtent.ymax)
    }catch {
      case e:Exception=>
        level = 11
        windowExtent = null
    }

    dagMd5 = Others.md5HashPassword(curWorkTaskJson)


    try {
      //      val jedis: Jedis = new JedisUtil().getJedis
      //
      //      // z曲线编码后的索引字符串
      //      //TODO 从redis 找到并剔除这些瓦片中已经算过的，之前缓存在redis中的瓦片编号
      //      // 等价于两层循环
      //      for (y <- yMinOfTile to yMaxOfTile; x <- xMinOfTile to xMaxOfTile
      //           if !jedis.sismember(key, ZCurveUtil.xyToZCurve(Array[Int](x, y), level))
      //        // 排除 redis 已经存在的前端瓦片编码
      //           ) { // redis里存在当前的索引
      //        // Redis 里没有的前端瓦片编码
      //        val zIndexStr: String = ZCurveUtil.xyToZCurve(Array[Int](x, y), level)
      //        zIndexStrArray.append(zIndexStr)
      //        // 将这些新的瓦片编号存到 Redis
      //        //        jedis.sadd(key, zIndexStr)
      //      }
      //      jedis.close()
      //      if (zIndexStrArray.isEmpty) {
      //        //      throw new RuntimeException("窗口范围无明显变化，没有新的瓦片待计算")
      //        println("窗口范围无明显变化，没有新的瓦片待计算")
      //        return
      //      }
    }finally {
      // 处理redis异常情况
      //      if(zIndexStrArray.isEmpty){
      //        zIndexStrArray.clear()
      //        for (y <- yMinOfTile to yMaxOfTile; x <- xMinOfTile to xMaxOfTile) {
      //          val zIndexStr: String = ZCurveUtil.xyToZCurve(Array[Int](x, y), level)
      //          zIndexStrArray.append(zIndexStr)
      //        }
      //      }

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

      //返回正常信息
      PostSender.sendShelvedPost()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        val errorJson = new JSONObject
        errorJson.put("error", e.getCause.getMessage)

        // 回调服务，通过 boot 告知前端：
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("workID", Trigger.dagId)
        outJsonObject.put("json", errorJson)
        //
        println("Error json = " + outJsonObject)
        sendPost(DAG_ROOT_URL + "/deliverUrl",
          outJsonObject.toJSONString)
        println("Send to boot!")
      //         打印至后端控制台


    } finally {
      Trigger.outputInformationList.clear()
      Trigger.optimizedDagMap.clear()
      Trigger.coverageCollectionMetadata.clear()
      Trigger.lazyFunc.clear()
      Trigger.coverageCollectionRddList.clear()
      Trigger.coverageRddList.clear()
      Trigger.zIndexStrArray.clear()
      JsonToArg.dagMap.clear()
      //    // TODO lrx: 以下为未检验
      Trigger.tableRddList.clear()
      Trigger.kernelRddList.clear()
      Trigger.mlmodelRddList.clear()
      Trigger.featureRddList.clear()
      Trigger.cubeRDDList.clear()
      Trigger.cubeLoad.clear()
      Trigger.intList.clear()
      Trigger.doubleList.clear()
      Trigger.stringList.clear()
      PostSender.clearShelvedMessages()
      tempFileList.foreach(tempFile =>{
        if (scala.reflect.io.File(tempFile).exists)
          scala.reflect.io.File(tempFile).delete()
      })

      val time2: Long = System.currentTimeMillis()
      println(time2 - time1)

    }


  }

  // ProcessName:保存结果的文件名称
  def runMain_edu(implicit sc: SparkContext,
                  curWorkTaskJson: String,
                  curDagID: String, userID: String, processName:String): Unit = {

    // Check if the SparkContext is active
    if (sc.isStopped) {
      sendPost(DAG_ROOT_URL + "/deliverUrl", "ERROR")
      println("Send to boot!")
      return
    }

    /* sc,workTaskJson,workID,originTaskID */
    workType = "edu"
    workTaskJson = curWorkTaskJson
    dagId = curDagID
    userId = userID
    ProcessName = processName
    val time1: Long = System.currentTimeMillis()

    val jsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(jsonObject)

    isBatch = jsonObject.getString("isBatch").toInt


    layerName = jsonObject.getString("layerName")
    try {
      val map: JSONObject = jsonObject.getJSONObject("map")
      level = map.getString("level").toInt

      windowExtent = null

    } catch {
      case e: Exception =>
        level = 11
        windowExtent = null
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
      PostSender.shelvePost("extStatus",1.toString)
      PostSender.shelvePost("userId",userID)
      //返回正常信息,发送到教育版地址
      PostSender.sendShelvedPost(GlobalConfig.DagBootConf.EDU_ROOT_URL+ "/deliverUrl")
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        val errorJson = new JSONObject
        errorJson.put("error", e.getCause.getMessage)

        // 回调服务，通过 boot 告知前端：
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("json", errorJson)
        outJsonObject.put("extStatus","0")
        outJsonObject.put("workID",Trigger.dagId)
        //
        println("Error json = " + outJsonObject)
        sendPost(GlobalConfig.DagBootConf.EDU_ROOT_URL + "/deliverUrl",
          outJsonObject.toJSONString)
        println("Send to boot!")
      //         打印至后端控制台


    } finally {
      Trigger.outputInformationList.clear()
      Trigger.optimizedDagMap.clear()
      Trigger.coverageCollectionMetadata.clear()
      Trigger.lazyFunc.clear()
      Trigger.coverageCollectionRddList.clear()
      Trigger.coverageRddList.clear()
      Trigger.zIndexStrArray.clear()
      JsonToArg.dagMap.clear()
      //    // TODO lrx: 以下为未检验
      Trigger.tableRddList.clear()
      Trigger.kernelRddList.clear()
      Trigger.mlmodelRddList.clear()
      Trigger.featureRddList.clear()
      Trigger.cubeRDDList.clear()
      Trigger.cubeLoad.clear()
      Trigger.intList.clear()
      Trigger.doubleList.clear()
      Trigger.stringList.clear()
      CoverageArray.funcNameList.clear()
      CoverageArray.funcArgs.clear()
      PostSender.clearShelvedMessages()
      tempFileList.foreach(tempFile => {
        if (scala.reflect.io.File(tempFile).exists)
          scala.reflect.io.File(tempFile).delete()
      })

      val time2: Long = System.currentTimeMillis()
      println(time2 - time1)

    }
  }


  def runBatch(implicit sc: SparkContext,
               curWorkTaskJson: String,
               curDagId: String, userID: String, crs: String, scale: String, folder: String, fileName: String, format: String): Unit = {
    workTaskJson = curWorkTaskJson
    dagId = curDagId
    userId = userID
    val time1: Long = System.currentTimeMillis()

    val jsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(jsonObject)

    isBatch = jsonObject.getString("isBatch").toInt

    batchParam.setUserId(userID)
    batchParam.setDagId(curDagId)
    batchParam.setCrs(crs)
    batchParam.setScale(scale)
    batchParam.setFolder(folder)
    batchParam.setFileName(fileName)
    batchParam.setFormat(format)

    val resolutionTMS: Double = 156543.033928
    level = Math.floor(Math.log(resolutionTMS / scale.toDouble) / Math.log(2)).toInt + 1


    if (sc.master.contains("local")) {
      //      JsonToArg.jsonAlgorithms = GlobalConfig.Others.jsonAlgorithms
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
      //返回正常信息
      PostSender.sendShelvedPost()
    } catch {
      case e: Throwable =>
        val errorJson = new JSONObject
        errorJson.put("error", e.toString)

        // 回调服务，通过 boot 告知前端：
        val outJsonObject: JSONObject = new JSONObject
        outJsonObject.put("workID", Trigger.dagId)
        outJsonObject.put("json", errorJson)

        println("Error json = " + outJsonObject)
        sendPost(DAG_ROOT_URL + "/deliverUrl",
          outJsonObject.toJSONString)

        // 打印至后端控制台
        e.printStackTrace()
    } finally {
      val tempFilePath = GlobalConfig.Others.tempFilePath
      val filePath = s"${tempFilePath}${dagId}.tiff"

      tempFileList.foreach(file =>{
        if(scala.reflect.io.File(file).exists)
          scala.reflect.io.File(file).delete()
      })

      tempFileList.clear()
    }
  }


  def main(args: Array[String]): Unit = {

    workTaskJson = {
      val fileSource: BufferedSource = Source.fromFile("src/main/scala/whu/edu/cn/testjson/oge_meanfilter.json")
      //      val fileSource: BufferedSource = Source.fromFile("src/main/scala/whu/edu/cn/testjson/coveragearray.json")
      //      val fileSource: BufferedSource = Source.fromFile("/mnt/storage/data/thirdTest.json")
      val line: String = fileSource.mkString
      fileSource.close()
      line
    } // 任务要用的 JSON,应当由命令行参数获取

    dagId = Random.nextInt().toString
    dagId = "12345678"
    userId = "96787d4b-9b13-4f1c-af39-9f4f1ea75299"
    // 点击整个run的唯一标识，来自boot

    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    //    runBatch(sc,workTaskJson,dagId,"zy","EPSG:4326","100","","","tif")
    runMain(sc, workTaskJson, dagId, userId)

    println("Finish")
    sc.stop()
  }
}
