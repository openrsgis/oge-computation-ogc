package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.Others.tempFilePath
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.oge.Coverage.loadTxtFromUpload
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.CoverageUtil.removeZeroFromCoverage
import whu.edu.cn.util.PostSender.{sendShelvedPost, shelvePost}
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, makeFeatureRDDFromShp, saveFeatureRDDToShp, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import scala.collection.immutable.Map
import scala.collection.mutable.Map
import scala.collection.{immutable, mutable}
import java.io.File

object SAGA {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
  }

  val algorithmData = GlobalConfig.SAGAConf.SAGA_DATA
  val algorithmDockerData = GlobalConfig.SAGAConf.SAGA_DOCKERDATA
//  val algorithmCode = GlobalConfig.SAGAConf.SAGA_ALGORITHMCODE
  val host = GlobalConfig.SAGAConf.SAGA_HOST
  val userName = GlobalConfig.SAGAConf.SAGA_USERNAME
  val password = GlobalConfig.SAGAConf.SAGA_PASSWORD
  val port = GlobalConfig.SAGAConf.SAGA_PORT

  def sagaGridStatisticsForPolygons(implicit sc: SparkContext,
                                    grids: immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                    polygons: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                    fieldNaming: String = "1",
                                    method: String = "0",
                                    useMultipleCores: String = "False",
                                    numberOfCells: String = "True",
                                    minimum: String = "True",
                                    maximum: String = "True",
                                    range: String = "True",
                                    sum: String = "True",
                                    mean: String = "True",
                                    variance: String = "True",
                                    standardDeviation: String = "True",
                                    gini: String = "False",
                                    percentiles: String
                                   ): String = {
    // 枚举类型参数
    val fieldNamingInput: String = mutable.Map(
      "0" -> "0",
      "1" -> "1",
    ).getOrElse(fieldNaming, "1")

    val methodInput: String = mutable.Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3"
    ).getOrElse(method, "0")

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    // 输入的栅格影像集合
    var tiffDockerPathList: List[String] = List()
    for (grid <- grids) {
      // 影像落地为tif
      val tiffPath = algorithmData + "sagaGridStatisticsGrids" + grid._1 + "_" + time + ".tif"
      val tiffDockerPath = algorithmDockerData + "sagaGridStatisticsGrids" + grid._1 + "_" + time + ".tif"
      saveRasterRDDToTif(grid._2, tiffPath)
      tiffDockerPathList = tiffDockerPathList :+ tiffDockerPath
    }
    val tiffDockerPathCollection = tiffDockerPathList.mkString(";")
    //输入矢量文件路径
    val polygonsPath = algorithmData + "sagaGridStatisticsPolygons_" + time + ".shp"
    //输出结果文件路径
    val writePath = algorithmData + "sagaGridStatistics_" + time + "_out.shp"

    saveFeatureRDDToShp(polygons, polygonsPath)
    // docker路径
    // docker矢量文件路径
    val dockerPolygonsPath = algorithmDockerData + "sagaGridStatisticsPolygons_" + time + ".shp"
    // docker输出结果文件路径
    val writeDockerPath = algorithmDockerData + "sagaGridStatistics_" + time + "_out.shp"
    try {
      versouSshUtil(host, userName, password, port)

      val st =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd shapes_grid 2 -GRIDS "$tiffDockerPathCollection" -POLYGONS "$dockerPolygonsPath" -NAMING $fieldNamingInput -METHOD $methodInput -PARALLELIZED $useMultipleCores -RESULT "$writeDockerPath"  -COUNT $numberOfCells -MIN $minimum -MAX $maximum -RANGE $range -SUM $sum -MEAN $mean -VAR $variance -STDDEV $standardDeviation -GINI $gini -QUANTILES "$percentiles" """.stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val feature = makeFeatureRDDFromShp(sc, writePath)
    val list: mutable.ListBuffer[JSONObject] = new mutable.ListBuffer[JSONObject]
    feature.collect().foreach(f => {
      val map = f._2._2
      val mapJson = new JSONObject();
      map.foreach(m => {
        mapJson.put(m._1, m._2)
      })
      list.append(mapJson)
    })
    println(list.toArray)
    val resultJson = new JSONObject();
    resultJson.put("info",list.toArray)
//    shelvePost("info", list.toArray)
//    sendShelvedPost()
    resultJson.toString
  }

  def sagaHistogramMatching(implicit sc: SparkContext,
                            grid: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            referenceGrid: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            method: Int = 1,
                            nclasses: Int = 100,
                            maxSamples: Int = 1000000):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val methodInput: Int = mutable.Map(
      0 -> 0,
      1 -> 1,
    ).getOrElse(method, 1)

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath1 = algorithmData + "sagaHistogramMatchingGrid_" + time + ".tif"
    val outputTiffPath2 = algorithmData + "sagaHistogramMatchingReference_" + time + ".tif"
    val writePath = algorithmData + "sagaHistogramMatching_" + time + "_out.tif"
    saveRasterRDDToTif(grid, outputTiffPath1)
    saveRasterRDDToTif(referenceGrid, outputTiffPath2)
    // docker路径
    val dockerTiffPath1 = algorithmDockerData + "sagaHistogramMatchingGrid_" + time + ".tif"
    val dockerTiffPath2 = algorithmDockerData + "sagaHistogramMatchingReference_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "sagaHistogramMatching_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)

      val st =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd grid_calculus 21 -GRID "$dockerTiffPath1" -REFERENCE "$dockerTiffPath2" -MATCHED "$writeDockerPath" -METHOD $methodInput -NCLASSES $nclasses -MAXSAMPLES $maxSamples""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }
  def sagaISODATAClusteringForGrids(implicit sc: SparkContext,
                                    features: immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                                    normalize: String = "0",
                                    iterations: Int = 20,
                                    clusterINI: Int = 5,
                                    clusterMAX: Int = 16,
                                    samplesMIN: Int = 5,
                                    initialize: String = "0"
  ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val initializeInput: String = mutable.Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2"
    ).getOrElse(initialize, "0")
    val time = System.currentTimeMillis()

    // 输入的影像集合
    var tiffDockerPathList: List[String] = List()
    for (feature <- features) {
      // 影像落地为tif
      val tiffPath = algorithmData + "sagaISODATAClusteringForGrids" + feature._1 + "_" + time + ".tif"
      val tiffDockerPath = algorithmDockerData + "sagaISODATAClusteringForGrids" + feature._1 + "_" + time + ".tif"
      saveRasterRDDToTif(feature._2, tiffPath)
      tiffDockerPathList = tiffDockerPathList :+ tiffDockerPath
    }

    val tiffDockerPathCollection = tiffDockerPathList.mkString(";")
    val writePath = algorithmData + "sagaISODATAClusteringForGrids_" + time + "_out.tif"
//    val tiffDockerPathCollection = "/tmp/saga/sagaISODATAClusteringForGridsdata1_1717593290374.tif;/tmp/saga/sagaISODATAClusteringForGridsdata2_1717593290374.tif"
//    val writePath = "/mnt/storage/SAGA/sagaData/sagaISODATAClusteringForGridsdata1_1717593290374.tif"

    // docker路径
    val dockerDbfPath = algorithmDockerData + "sagaISODATAClusteringForGrids_" + time + ".dbf"
    val writeDockerPath = algorithmDockerData + "sagaISODATAClusteringForGrids_" + time + "_out.tif"
//    val dockerDbfPath = "/tmp/saga/output.dbf"
//    val writeDockerPath = "/tmp/saga/sagaISODATAClusteringForGridsdata1_1717593290374.tif"
    try {
      versouSshUtil(host, userName, password, port)

      val st2 =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd imagery_isocluster 0 -FEATURES "$tiffDockerPathCollection" -CLUSTER "$writeDockerPath" -STATISTICS "$dockerDbfPath" -NORMALIZE $normalize -ITERATIONS $iterations -CLUSTER_INI $clusterINI -CLUSTER_MAX $clusterMAX -SAMPLES_MIN $samplesMIN -INITIALIZE "$initializeInput"""".stripMargin

      println(s"st = $st2")
      runCmd(st2, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }
  def sagaSimpleFilter(implicit sc: SparkContext,
                       input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                       method: Int = 0,
                       kernelType: Int = 1,
                       kernelRadius: Int = 2):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val methodInput: Int = mutable.Map(
      0 -> 0,
      1 -> 1,
      2 -> 2
    ).getOrElse(method, 0)
    val kernelTypeInput: Int = mutable.Map(
      0 -> 0,
      1 -> 1
    ).getOrElse(kernelType, 1)

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath = algorithmData + "sagaSimpleFilter_" + time + ".tif"
    val writePath = algorithmData + "sagaSimpleFilter_" + time + "_out.tif"
    saveRasterRDDToTif(input, outputTiffPath)
    // docker路径
    val dockerTiffPath = algorithmDockerData + "sagaSimpleFilter_" + time + ".tif"
    val writeDockerPath = algorithmDockerData + "sagaSimpleFilter_" + time + "_out.tif"
    try {
      versouSshUtil(host, userName, password, port)

      val st =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd grid_filter 0 -INPUT "$dockerTiffPath" -RESULT "$writeDockerPath" -METHOD $methodInput -KERNEL_TYPE $kernelTypeInput -KERNEL_RADIUS $kernelRadius""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }
  def sagaMinimumDistanceClassification(implicit sc: SparkContext,
                       grids: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                       training: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                       training_samples: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                       normalise: Boolean = false,
                       training_class:String,
                       training_with:Int=0,
                       train_buffer:Float=1.0f,
                       threshold_dist:Float=0.0f,
                       threshold_angle:Float=0.0f,
                       threshold_prob:Float=0.0f,
                       file_load:String,
                       relative_prob:Int=1,
                       userId:String,
                       dagId:String):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath = algorithmData + "sagaMinimumDistanceClassification_" + dagId + time + ".tif"
    val writePathSdat = algorithmData + "sagaMinimumDistanceClassification_" + dagId + time + "_out.sdat"
    val writePath = algorithmData + "sagaMinimumDistanceClassification_" + dagId + time + "_out.tif"
    saveRasterRDDToTif(grids, outputTiffPath)
    // docker路径
    val dockerTiffPath = algorithmDockerData + "sagaMinimumDistanceClassification_" + dagId + time + ".tif"
    val writeDockerPath = algorithmDockerData + "sagaMinimumDistanceClassification_" + dagId + time + "_out.sdat"

    //输入矢量文件路径
    val trainingPath = algorithmData + "sagaMinimumDistanceClassificationtraining_" + dagId + time + ".shp"
    //输出结果文件路径
//    val traingwritePath = algorithmData + "sagaMinimumDistanceClassificationtraining_" + dagId + time + "_out.shp"

    saveFeatureRDDToShp(training, trainingPath)
    // docker路径
    // docker矢量文件路径
    val dockertrainingPath = algorithmDockerData + "sagaMinimumDistanceClassificationtraining_" + dagId + time + ".shp"
    // docker输出结果文件路径
    // val writetrainingDockerPath = algorithmDockerData + "sagaMinimumDistanceClassificationtraining_" + dagId + time + "_out.shp"

    //输入矢量文件路径
    val training_samplesPath = algorithmData + "sagaMinimumDistanceClassificationtraining_samples_" + dagId + time + ".shp"
    //输出结果文件路径
    // val traing_sampleswritePath = algorithmData + "sagaMinimumDistanceClassificationtraining_samples_" + dagId + time + "_out.shp"

    saveFeatureRDDToShp(training_samples, training_samplesPath)
    // docker路径
    // docker矢量文件路径
    val dockertraining_samplesPath = algorithmDockerData + "sagaMinimumDistanceClassificationtraining_samples_" + dagId + time + ".shp"
    // docker输出结果文件路径
    //    val writetraining_samplesDockerPath = algorithmDockerData + "sagaMinimumDistanceClassificationtraining_samples_" + dagId + time + "_out.shp"

    //加载预训练模型
    print("读取txt")
    val file_loadPath=loadTxtFromUpload(file_load,userId,dagId,"saga")
    print("读取txt成功")
    val save_loadDockerPath = algorithmDockerData + "sagaMinimumDistanceClassificationsavaload_" + dagId + time + ".txt"
    //保存文件
    val  class_lut_writeDockerPath = algorithmDockerData + "sagaMinDisClassification_class_lut" + dagId + time + "_out.dbf"
    val  quality_writeDockerPath = algorithmDockerData + "sagaMinDisClassification_quality" + dagId + time + "_out.sdat"



    try {
      versouSshUtil(host, userName, password, port)

      val st1 =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd imagery_classification 0 -GRIDS "$dockerTiffPath" -NORMALISE $normalise -CLASSES "$writeDockerPath" -CLASSES_LUT "$class_lut_writeDockerPath" -QUALITY "$quality_writeDockerPath" -TRAIN_WITH $training_with -TRAINING "$dockertrainingPath" -TRAINING_CLASS "$training_class" -TRAIN_SAMPLES "$dockertraining_samplesPath" -FILE_LOAD "$file_loadPath" -FILE_SAVE "$save_loadDockerPath" -TRAIN_BUFFER $train_buffer -THRESHOLD_DIST $threshold_dist -THRESHOLD_ANGLE $threshold_angle -THRESHOLD_PROB $threshold_prob -RELATIVE_PROB  $relative_prob"""

      val st2 = s"conda activate cv && python /root/svm/sdattotif.py --imagePath $writePathSdat --outputPath $writePath"

      println(s"st = $st1")
      runCmd(st1, "UTF-8")
      println(s"st = $st2")
      versouSshUtil(host, userName, password, port)
      runCmd(st2, "UTF-8")
      println("Success")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    print(writePath)
    makeChangedRasterRDDFromTif(sc, writePath)

  }
  def sagaMaximumLikelihoodClassification(implicit sc: SparkContext,
                                        grids: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                        training: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                        training_samples: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                                        normalise: Boolean = false,
                                        training_class:String,
                                        training_with:Int=0,
                                        train_buffer:Float=1.0f,
                                        threshold_dist:Float=0.0f,
                                        threshold_angle:Float=0.0f,
                                        threshold_prob:Float=0.0f,
                                        file_load:String,
                                        relative_prob:Int=1,
                                        method:Int=4,
                                        userId:String,
                                        dagId:String):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath = algorithmData + "sagaMaximumLikelihoodClassification_" + dagId + time + ".tif"
    val writePathSdat = algorithmData + "sagaMaximumLikelihoodClassification_" + dagId + time + "_out.sdat"
    val writePath = algorithmData + "sagaMaximumLikelihoodClassification_" + dagId + time + "_out.tif"
    saveRasterRDDToTif(grids, outputTiffPath)
    // docker路径
    val dockerTiffPath = algorithmDockerData + "sagaMaximumLikelihoodClassification_" + dagId + time + ".tif"
    val writeDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassification_" + dagId + time + "_out.sdat"

    //输入矢量文件路径
    val trainingPath = algorithmData + "sagaMaximumLikelihoodClassificationtraining_" + dagId + time + ".shp"
    //输出结果文件路径
    //    val traingwritePath = algorithmData + "sagaMaximumLikelihoodClassificationtraining_" + dagId + time + "_out.shp"

    saveFeatureRDDToShp(training, trainingPath)
    // docker路径
    // docker矢量文件路径
    val dockertrainingPath = algorithmDockerData + "sagaMaximumLikelihoodClassificationtraining_" + dagId + time + ".shp"
    // docker输出结果文件路径
    // val writetrainingDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassificationtraining_" + dagId + time + "_out.shp"

    //输入矢量文件路径
    val training_samplesPath = algorithmData + "sagaMaximumLikelihoodClassificationtraining_samples_" + dagId + time + ".shp"
    //输出结果文件路径
    // val traing_sampleswritePath = algorithmData + "ssagaMaximumLikelihoodClassificationtraining_samples_" + dagId + time + "_out.shp"

    saveFeatureRDDToShp(training_samples, training_samplesPath)
    // docker路径
    // docker矢量文件路径
    val dockertraining_samplesPath = algorithmDockerData + "sagaMaximumLikelihoodClassificationtraining_samples_" + dagId + time + ".shp"
    // docker输出结果文件路径
    //    val writetraining_samplesDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassificationtraining_samples_" + dagId + time + "_out.shp"

    //加载预训练模型
    print("读取txt")
    val file_loadPath=loadTxtFromUpload(file_load,userId,dagId,"saga")
    print("读取txt成功")
    val save_loadDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassificationsavaload_" + dagId + time + ".txt"
    //保存文件
    val  class_lut_writeDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassification_class_lut" + dagId + time + "_out.dbf"
    val  quality_writeDockerPath = algorithmDockerData + "sagaMaximumLikelihoodClassification_quality" + dagId + time + "_out.sdat"



    try {
      versouSshUtil(host, userName, password, port)

      val st1 =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd imagery_classification 0 -GRIDS "$dockerTiffPath" -NORMALISE $normalise -CLASSES "$writeDockerPath" -CLASSES_LUT "$class_lut_writeDockerPath" -QUALITY "$quality_writeDockerPath" -TRAIN_WITH $training_with -TRAINING "$dockertrainingPath" -TRAINING_CLASS "$training_class" -TRAIN_SAMPLES "$dockertraining_samplesPath" -FILE_LOAD "$file_loadPath" -FILE_SAVE "$save_loadDockerPath" -TRAIN_BUFFER $train_buffer -THRESHOLD_DIST $threshold_dist -THRESHOLD_ANGLE $threshold_angle -THRESHOLD_PROB $threshold_prob -RELATIVE_PROB  $relative_prob -METHOD $method"""

      val st2 = s"conda activate cv && python /root/svm/sdattotif.py --imagePath $writePathSdat --outputPath $writePath"

      println(s"st = $st1")
      runCmd(st1, "UTF-8")
      println(s"st = $st2")
      versouSshUtil(host, userName, password, port)
      runCmd(st2, "UTF-8")
      println("Success")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    print(writePath)
    makeChangedRasterRDDFromTif(sc, writePath)

  }

  def sagaSVMClassification(implicit sc: SparkContext,
                            grid: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            ROI: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                            scaling: Int = 2,
                            message: Int = 0,
                            model_src: Int = 0,
                            ROI_id: String,
                            svm_type: Int = 0,
                            kernel_type: Int = 2,
                            degree: Int = 3,
                            gamma: Double = 0.000000,
                            coef0: Double = 0.000000,
                            cost: Double = 1.000000,
                            nu: Double = 0.500000,
                            eps_svr: Double = 0.100000,
                            cache_size: Double = 100.000000,
                            eps: Double = 0.001000,
                            shrinking: Boolean = false,
                            probability: Boolean = false,
                            crossval: Int = 0):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time = System.currentTimeMillis()
    // 服务器上挂载的路径
    val outputTiffPath = algorithmData + "sagaSVMClassification_" + time + ".tif"
    val writePath = algorithmData + "sagaSVMClassification_" + time + "_svm_result.tif"
    val model_load = new File(algorithmData + "sagaSVMClassification_" + time + "_svm_remodel.txt")
    val model_save = new File(algorithmData + "sagaSVMClassification_" + time + "_svm_smodel.txt")
    model_load.createNewFile()
    model_save.createNewFile()
    saveRasterRDDToTif(grid, outputTiffPath)

    // docker路径
    val dockerTiffPath = algorithmDockerData + "sagaSVMClassification_" + time + ".tif"
    val classes = algorithmDockerData + "sagaSVMClassification_" + time + "_svm_result.sdat"
    val outputPath = algorithmData + "sagaSVMClassification_" + time + "_svm_result.sdat"
    val classes_lut = algorithmDockerData + "sagaSVMClassification_" + time + "_svm_table.dbf"
    val docker_load = algorithmDockerData + "sagaSVMClassification_" + time + "_svm_remodel.txt"
    val docker_save = algorithmDockerData + "sagaSVMClassification_" + time + "_svm_smodel.txt"

    //输入矢量文件路径
    val trainingPath = algorithmData + "sagaSVMClassification_" + time + ".shp"
    //输出结果文件路径
    //    val traingwritePath = algorithmData + "sagaMinimumDistanceClassificationtraining_" + dagId + time + "_out.shp"

    saveFeatureRDDToShp(ROI, trainingPath)

    val dockertraining_samplePath = algorithmDockerData + "sagaSVMClassification_" + time + ".shp"


    try {
      versouSshUtil(host, userName, password, port)

      val st1 =
        raw"""docker start strange_pare;docker exec strange_pare saga_cmd imagery_svm 0 -GRIDS $dockerTiffPath -CLASSES $classes -CLASSES_LUT $classes_lut -SCALING $scaling -MESSAGE $message -MODEL_SRC $model_src -MODEL_LOAD $docker_load -ROI $dockertraining_samplePath -ROI_ID $ROI_id -MODEL_SAVE $docker_save -SVM_TYPE $svm_type -KERNEL_TYPE $kernel_type -DEGREE $degree -GAMMA $gamma -COEF0 $coef0 -COST $cost -NU $nu -EPS_SVR $eps_svr -CACHE_SIZE $cache_size -EPS $eps -SHRINKING $shrinking -PROBABILITY $probability -CROSSVAL $crossval""".stripMargin
      val st2 = s"conda activate cv && python /root/svm/sdattotif.py --imagePath $outputPath --outputPath $writePath"

      runCmd(st1, "UTF-8")
      println(s"st = $st1")
      println(s"st = $st2")
      versouSshUtil(host, userName, password, port)
      runCmd(st2, "UTF-8")
      println("Success")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val svm_result = makeChangedRasterRDDFromTif(sc, writePath)
    // 解决黑边值为255影响渲染的问题
    removeZeroFromCoverage(Coverage.addNum(svm_result, 1))

  }



}


