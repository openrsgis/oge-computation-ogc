package whu.edu.cn.geocube.application.gdc

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{Formats, NoTypeHints}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey, WorkflowCollectionParam}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GDCTrigger {
  var optimizedDagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]
  var cropExtent: String = null

  // TODO lrx: 以下为未检验

  var tableRddList: mutable.Map[String, String] = mutable.Map.empty[String, String]
  var kernelRddList: mutable.Map[String, geotrellis.raster.mapalgebra.focal.Kernel] = mutable.Map.empty[String, geotrellis.raster.mapalgebra.focal.Kernel]
  var featureRddList: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

  //  var cubeRDDList: mutable.Map[String, mutable.Map[String, Any]] = mutable.Map.empty[String, mutable.Map[String, Any]]
  var cubeLoad: mutable.Map[String, (String, String, String)] = mutable.Map.empty[String, (String, String, String)]

  var cubeRddList: mutable.Map[String, (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty[String, (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey])]

  var level: Int = _
  var layerName: String = _
  var windowExtent: Extent = _
  var isBatch: Int = _
  // 此次计算工作的任务json
  var workTaskJson: String = _
  // 输出文件夹
  var outputDir: String = "E:\\LaoK\\data2\\GDC_API\\"
  // 查询参数
  var collectionQueryParams: QueryParams = new QueryParams()
  var scaleSize: Array[java.lang.Integer] = null
  var scaleAxes: Array[java.lang.Double] = null
  var scaleFactor: java.lang.Double = null
  var imageFormat: String = "tif"
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


  def action(): Unit = {

  }

  /**
   * match the function to execute
   *
   * @param sc       the spark context
   * @param UUID     the UUID
   * @param funcName the function name
   * @param args     the args of t
   */
  def func(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    funcName match {
      case "loadCube" => {
        cropExtent = isOptionalArg(args, "extent")
        cubeRddList += (UUID -> RasterCubeFun.loadCube(sc, cubeName = isOptionalArg(args, "cubeName"), extent = isOptionalArg(args, "extent"),
          startTime = isOptionalArg(args, "startTime"), endTime = isOptionalArg(args, "endTime"), queryParams = collectionQueryParams,
          scaleSize = scaleSize, scaleAxes = scaleAxes, scaleFactor = scaleFactor))
      }
      case "normalize" => {
        //        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        //        var dimensionMembers: Array[String] = null
        //        if (dimensionMembersStr != null) {
        //          dimensionMembers = dimensionMembersStr
        //            .stripPrefix("[").stripSuffix("]") // 去掉开头和结尾的方括号
        //            .split(",") // 使用逗号分割字符串
        //            .map(_.trim.stripPrefix("\"").stripSuffix("\"")) // 去掉双引号和首尾空格
        //        }
        //        cubeRddList += (UUID -> RasterCubeFun.normalize(cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
        //          dimensionMembers = dimensionMembers))
        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        cubeRddList += (UUID -> RasterCubeFun.calculateAlongDimensionWithString(data = cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          dimensionMembersStr = dimensionMembersStr, method = "normalize"))
      }
      case "add" => {
        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        cubeRddList += (UUID -> RasterCubeFun.calculateAlongDimensionWithString(data = cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          dimensionMembersStr = dimensionMembersStr, method = "add"))
      }
      case "divide" => {
        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        cubeRddList += (UUID -> RasterCubeFun.calculateAlongDimensionWithString(data = cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          dimensionMembersStr = dimensionMembersStr, method = "divide"))
      }
      case "subtract" => {
        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        cubeRddList += (UUID -> RasterCubeFun.calculateAlongDimensionWithString(data = cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          dimensionMembersStr = dimensionMembersStr, method = "subtract"))
      }
      case "aggregateAlongDimension" => {
        cubeRddList += (UUID -> RasterCubeFun.aggregateAlongDimension(cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          method = isOptionalArg(args, "method")))
      }
      case "aggregate" => {
        cubeRddList += (UUID -> RasterCubeFun.aggregateAlongDimension(cubeRddList(args("data")), dimensionName = isOptionalArg(args, "dimensionName"),
          method = isOptionalArg(args, "method")))
      }
      case "exportFile" => {
        val cropExtentArray = new ArrayBuffer[Double]()
        if (cropExtent != null) {
          cropExtentArray.append(cropExtent.split(",")(0).toDouble, cropExtent.split(",")(1).toDouble,
            cropExtent.split(",")(2).toDouble, cropExtent.split(",")(3).toDouble)
          RasterCubeFun.exportFile(cubeRddList(args("data")), extent = cropExtentArray, queryParams = collectionQueryParams, outputDir = outputDir, imageFormat = imageFormat)
        } else if (collectionQueryParams != null && collectionQueryParams.extentCoordinates.nonEmpty) {
          RasterCubeFun.exportFile(cubeRddList(args("data")), extent = collectionQueryParams.extentCoordinates, queryParams = collectionQueryParams, outputDir = outputDir, imageFormat = imageFormat)
        } else {
          RasterCubeFun.exportFile(cubeRddList(args("data")), outputDir = outputDir, queryParams = collectionQueryParams, imageFormat = imageFormat)
        }
        cubeRddList(args("data"))._1.unpersist()
      }
      case _ =>
    }
  }

  /**
   * lambda for meta data change with the functions
   *
   * @param list the rdd list
   */
  def lambdaMeta(list: mutable.ArrayBuffer[Tuple3[String, String, mutable.Map[String, String]]]): JSONObject = {
    var metaObject = new JSONObject()
    metaObject.put("bands", new JSONArray())
    metaObject.put("dimensions", new JSONArray())
    for (i <- list.indices) {
      metaObject = funcMeta(list(i)._1, list(i)._2, list(i)._3, metaObject)
    }
    metaObject
  }

  /**
   *
   * @param UUID     the UUID
   * @param funcName the function name
   * @param args     the args
   */
  def funcMeta(UUID: String, funcName: String, args: mutable.Map[String, String], metaObject: JSONObject): JSONObject = {

    funcName match {
      case "loadCube" => {
        metaObject.put("collection", isOptionalArg(args, "cubeName"))
        metaObject.put("extent", isOptionalArg(args, "extent"))
        metaObject.put("startTime", isOptionalArg(args, "startTime"))
        metaObject.put("endTime", isOptionalArg(args, "endTime"))
      }
      case "normalize" => {
        val dimensionMembersStr = isOptionalArg(args, "dimensionMembers")
        var dimensionMembers: Array[String] = null
        val bandArray = metaObject.getJSONArray("bands")
        val bandObj = new JSONObject();
        if (dimensionMembersStr != null) {
          dimensionMembers = dimensionMembersStr
            .stripPrefix("[").stripSuffix("]") // 去掉开头和结尾的方括号
            .split(",") // 使用逗号分割字符串
            .map(_.trim.stripPrefix("\"").stripSuffix("\"")) // 去掉双引号和首尾空格
          bandObj.put("description", "(" + dimensionMembers(0) + "-" + dimensionMembers(1) + ")/" + "(" + dimensionMembers(0) + "+" + dimensionMembers(1) + ")")
        }
        bandObj.put("name", "normalize")
        bandArray.add(bandObj)
        metaObject.put("bands", bandArray)
      }
      case "aggregateAlongDimension" | "aggregate" => {
        val dimensionName = isOptionalArg(args, "dimensionName")
        val method = isOptionalArg(args, "method")
        if (dimensionName != null && (dimensionName.contains("band") || dimensionName.contains("measurement"))) {
          val bandArray = metaObject.getJSONArray("bands")
          val bandObj = new JSONObject();
          bandObj.put("name", "method")
          bandObj.put("description", "aggregate along with " + dimensionName + "with method " + method)
          bandArray.add(bandObj)
          metaObject.put("bands", bandArray)
        } else if (dimensionName.equals("time") || dimensionName.equals("phenomenonTime")) {
          metaObject.put("startTime", "None")
          metaObject.put("endTime", "None")
        } else {
          val dimensionArray = metaObject.getJSONArray("dimensions")
          val dimensionObj = new JSONObject();
          dimensionObj.equals("name", dimensionName)
          dimensionObj.put("coordinates", method)
          dimensionArray.add(dimensionObj)
          metaObject.put("dimensions", dimensionArray)
        }
      }
      case _ =>
    }
    metaObject
  }

  def lambda(implicit sc: SparkContext, list: mutable.ArrayBuffer[Tuple3[String, String, mutable.Map[String, String]]]): Unit = {
    for (i <- list.indices) {
      func(sc, list(i)._1, list(i)._2, list(i)._3)
    }
  }

  def optimizedDAG(list: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    val duplicates: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = removeDuplicates(list)
    val checked: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = checkMap(duplicates)
    checked
  }

  def checkMap(collection: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = {
    var result: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = collection
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

  def updateDagMap(curWorkTaskJson: String, curDagId: String): Unit = {
    workTaskJson = curWorkTaskJson
    dagId = curDagId
    val jsonObject: JSONObject = JSON.parseObject(workTaskJson)
    println(jsonObject)
    println("***********************************************************")
    JsonToGDCArg.trans(jsonObject, "0")
    println("JsonToArg.dagMap.size = " + JsonToGDCArg.dagMap)
    JsonToGDCArg.dagMap.foreach(DAGList => {
      println("************优化前的DAG*************")
      println(DAGList._1)
      DAGList._2.foreach(println(_))
      println("************优化后的DAG*************")
      var optimizedDAGList: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = optimizedDAG(DAGList._2)
      val exportNode = optimizedDAGList(0)._1 + "0"
      val exportParams: mutable.Map[String, String] = mutable.Map()
      exportParams.put("data", "0")
      exportParams.put("imageFormat", "tif")
      optimizedDAGList.append((exportNode, "exportFile", exportParams))
      optimizedDagMap += (DAGList._1 -> optimizedDAGList)
      optimizedDAGList.foreach(println(_))
    })
  }

  def runMetaAnalysis(curWorkTaskJson: String, curDagId: String): JSONObject = {
    updateDagMap(curWorkTaskJson, curDagId)
    lambdaMeta(optimizedDagMap("0"))
  }

  def runMain(implicit sc: SparkContext,
              curWorkTaskJson: String,
              curDagId: String): Unit = {
    updateDagMap(curWorkTaskJson, curDagId)
    lambda(sc, optimizedDagMap("0"))
  }

  /**
   * Synchronous execution workflow and support OGC API - Processes: Part3
   *
   * @param workflowJson    the workflow dag json
   * @param queryParams     the query params
   * @param scaleSize       the scale size
   * @param scaleAxes       the scale axes
   * @param scaleFactor     the scale factor
   * @param outputDirectory the output directory
   * @param imageFormat     the image format as the output
   */
  //  def runWorkflow(workflowJson: String, queryParams: QueryParams, scaleSize: Array[java.lang.Integer],
  //                  scaleAxes: Array[java.lang.Double] = null, scaleFactor: java.lang.Double = null,
  //                  outputDirectory: String, imageFormat: String = "tif"): Unit = {
  //    val conf = new SparkConf()
  //      .setMaster("spark://192.168.0.203:7077")
  //      .setAppName("run workflow collection")
  //      .set("spark.driver.allowMultipleContexts", "true")
  //      .set("spark.jars", "/home/geocube/kernel/geocube-core/tb19/geocube-core.jar,/home/geocube/mylib/geocube/netcdfAll-5.5.3.jar")
  //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
  //      .set("spark.driver.memory", SparkCustomConf.driverMemory)
  //      .set("spark.executor.memory", SparkCustomConf.executorMemory)
  //      .set("spark.cores.max", SparkCustomConf.coresMax)
  //      .set("spark.executor.cores", SparkCustomConf.executorCores)
  //      .set("spark.kryoserializer.buffer.max", SparkCustomConf.bufferMax)
  //      .set("spark.rpc.message.maxSize", SparkCustomConf.messageMaxSize)
  //      .set("spark.driver.maxResultSize", SparkCustomConf.maxResultSize)
  //    val sc = new SparkContext(conf)
  //    workTaskJson = workflowJson
  //    outputDir = outputDirectory
  //    dagId = Random.nextInt().toString
  //    collectionQueryParams = queryParams
  //    this.scaleSize = scaleSize
  //    this.scaleFactor = scaleFactor
  //    this.scaleAxes = scaleAxes
  //    this.imageFormat = imageFormat
  //    runMain(sc, workTaskJson, dagId)
  //    sc.stop()
  //  }


  def main(args: Array[String]): Unit = {
    //        workTaskJson = {
    //          val fileSource: BufferedSource = Source.fromFile("src/main/resource/workflow5.json")
    //          val line: String = fileSource.mkString
    //          fileSource.close()
    //          line
    //        } // 任务要用的 JSON,应当由命令行参数获取
    //
    //        dagId = Random.nextInt().toString
    //    //    val a: JSONObject = runMetaAnalysis(workTaskJson, dagId)
    //    //    val b = 1
    //        // 点击整个run的唯一标识，来自boot
    //
    //        val conf: SparkConf = new SparkConf()
    //          .setMaster("local[8]")
    //          .setAppName("query")
    //        val sc = new SparkContext(conf)
    //        runMain(sc, workTaskJson, dagId)
    //
    //        //    Thread.sleep(1000000)
    //
    //        sc.stop()

    //////////////////////////////////////deploy need///////////////////////////////////
    workTaskJson = args(0)

    println("workTaskJson" + workTaskJson)
    outputDir = args(1)
    // 是否是collection方式的处理 "true"则是collection "false"则不是collection
    val isCollection = args(2)
    dagId = Random.nextInt().toString
    if (isCollection.equals("false") || isCollection == null) {
      val conf = new SparkConf()
        .setAppName("Processes workflow")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      val sc = new SparkContext(conf)
      runMain(sc, workTaskJson, dagId)
    } else if (isCollection.equals("true")) {
      val workflowCollectionParamStr = args(3)
      implicit val formats: Formats = Serialization.formats(NoTypeHints)
      val workflowCollectionParam: WorkflowCollectionParam = parse(workflowCollectionParamStr).extract[WorkflowCollectionParam]
      val conf = new SparkConf()
        .setAppName("Processes workflow")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      val sc = new SparkContext(conf)
      collectionQueryParams = workflowCollectionParam.queryParams
      collectionQueryParams.updatePolygon()
      workflowCollectionParam.scaleFactor match {
        case Some(value) =>
          this.scaleFactor = java.lang.Double.valueOf(value)
        case None =>
      }
      if (workflowCollectionParam.getScaleSize != null && (!workflowCollectionParam.getScaleSize.isEmpty)) {
        scaleSize = workflowCollectionParam.getScaleSize.map(java.lang.Integer.valueOf)
      }
      if (workflowCollectionParam.getScaleAxes != null && (!workflowCollectionParam.getScaleAxes.isEmpty)) {
        scaleAxes = workflowCollectionParam.getScaleAxes.map(java.lang.Double.valueOf)
      }
      this.imageFormat = workflowCollectionParam.imageFormat
      runMain(sc, workTaskJson, dagId)
    }
    /////////////////////////////////////////////////////////////////////////
  }

  def runWorkflow(implicit sc: SparkContext, workTaskJson: String, outputDir: String,
                  isCollection: String, workflowCollectionParamStr: String): Unit = {
    this.workTaskJson = workTaskJson

    println("workTaskJson" + workTaskJson)
    this.outputDir = outputDir
    // 是否是collection方式的处理 "true"则是collection "false"则不是collection
    this.dagId = Random.nextInt().toString
    if (isCollection.equals("false") || isCollection == null) {
      runMain(sc, workTaskJson, dagId)
    } else if (isCollection.equals("true")) {
      implicit val formats: Formats = Serialization.formats(NoTypeHints)
      val workflowCollectionParam: WorkflowCollectionParam = parse(workflowCollectionParamStr).extract[WorkflowCollectionParam]
      this.collectionQueryParams = workflowCollectionParam.queryParams
      this.collectionQueryParams.updatePolygon()
      workflowCollectionParam.scaleFactor match {
        case Some(value) =>
          this.scaleFactor = java.lang.Double.valueOf(value)
        case None =>
      }
      if (workflowCollectionParam.getScaleSize != null && (!workflowCollectionParam.getScaleSize.isEmpty)) {
        this.scaleSize = workflowCollectionParam.getScaleSize.map(java.lang.Integer.valueOf)
      }
      if (workflowCollectionParam.getScaleAxes != null && (!workflowCollectionParam.getScaleAxes.isEmpty)) {
        this.scaleAxes = workflowCollectionParam.getScaleAxes.map(java.lang.Double.valueOf)
      }
      this.imageFormat = workflowCollectionParam.imageFormat
      runMain(sc, workTaskJson, dagId)
    }
  }

}
