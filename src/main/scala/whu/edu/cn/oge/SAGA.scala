package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.PostSender.{sendShelvedPost, shelvePost}
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, makeFeatureRDDFromShp, saveFeatureRDDToShp, saveRasterRDDToTif}
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import scala.collection.immutable.Map
import scala.collection.mutable.Map
import scala.collection.{immutable, mutable}

object SAGA {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)

//    val feature  = makeFeatureRDDFromShp(sc,"D:\\whu_master\\temp\\temp.shp")
    val feature  = makeFeatureRDDFromShp(sc,"/Users/churcy/Desktop/影像文件/temp/temp.shp")
    val list:mutable.ListBuffer[JSONObject] = new mutable.ListBuffer[JSONObject]
    feature.collect().foreach(f =>{
//      result +=f._2
//      println(f._2._2.get())
      val map = f._2._2
      val mapJson = new JSONObject();
      map.foreach(m =>{
        mapJson.put(m._1,m._2)
      })
      list.append(mapJson)
    })
    println(list.toArray)
    val resultJson = new JSONObject();
    resultJson.put("info",list.toArray)
    //    shelvePost("info", list.toArray)
    //    sendShelvedPost()
    println(resultJson.toString)
//    // test
//    val grid = makeChangedRasterRDDFromTif(sc, "/D:/mnt/storage/SAGA/sagaData/sagaISODATAClusteringForGridsdata2_1717593290374.tif")
//    val reference = makeChangedRasterRDDFromTif(sc, "/D:/mnt/storage/SAGA/sagaData/sagaISODATAClusteringForGridsdata1_1717593290374.tif")
//
//
//    val inputMap: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map()
//    val updatedMap1 = inputMap + ("data1" -> grid)
//    val updatedMap2 = updatedMap1 + ("data2" -> reference)
//
//    val resultRDD =  sagaISODATAClusteringForGrids(sc, updatedMap2)
//    saveRasterRDDToTif(resultRDD, "/C:/Users/BBL/Desktop/algorithm/saga_algorithms/result.tif")
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
        raw"""docker start 8bb3a634bcd6;docker exec strange_pare saga_cmd shapes_grid 2 -GRIDS "$tiffDockerPathCollection" -POLYGONS "$dockerPolygonsPath" -NAMING $fieldNamingInput -METHOD $methodInput -PARALLELIZED $useMultipleCores -RESULT "$writeDockerPath"  -COUNT $numberOfCells -MIN $minimum -MAX $maximum -RANGE $range -SUM $sum -MEAN $mean -VAR $variance -STDDEV $standardDeviation -GINI $gini -QUANTILES "$percentiles" """.stripMargin

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
        raw"""docker start 8bb3a634bcd6;docker exec strange_pare saga_cmd grid_calculus 21 -GRID "$dockerTiffPath1" -REFERENCE "$dockerTiffPath2" -MATCHED "$writeDockerPath" -METHOD $methodInput -NCLASSES $nclasses -MAXSAMPLES $maxSamples""".stripMargin

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
        raw"""docker start 8bb3a634bcd6;docker exec strange_pare saga_cmd imagery_isocluster 0 -FEATURES "$tiffDockerPathCollection" -CLUSTER "$writeDockerPath" -STATISTICS "$dockerDbfPath" -NORMALIZE $normalize -ITERATIONS $iterations -CLUSTER_INI $clusterINI -CLUSTER_MAX $clusterMAX -SAMPLES_MIN $samplesMIN -INITIALIZE "$initializeInput"""".stripMargin

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
        raw"""docker start 8bb3a634bcd6;docker exec strange_pare saga_cmd grid_filter 0 -INPUT "$dockerTiffPath" -RESULT "$writeDockerPath" -METHOD $methodInput -KERNEL_TYPE $kernelTypeInput -KERNEL_RADIUS $kernelRadius""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    makeChangedRasterRDDFromTif(sc, writePath)

  }





}
