package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, Raster, TileLayout}
import geotrellis.spark._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import io.minio.{MinioClient, UploadObjectArgs}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import spire.math.Polynomial.x
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.CLIENT_NAME
import whu.edu.cn.config.GlobalConfig.Others.tempFilePath
import whu.edu.cn.entity.{BatchParam, CoverageMetadata, RawTile, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.oge.Coverage.{addStyles, makeTIFF, reproject, resolutionTMSArray}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.{dagId, userId}
import whu.edu.cn.util.CoverageCollectionUtil.makeCoverageCollectionRDD
import whu.edu.cn.util.{COGUtil, ClientUtil, PostSender}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverageCollection

import java.io.File
import scala.+:
import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm}
import sys.process._

object CoverageArray {
  var CoverageList: mutable.Map[Int, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = mutable.Map.empty[Int, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
  var funcNameList: ListBuffer[String] = ListBuffer[String]()
  var funcArgs: ListBuffer[List[Any]] = ListBuffer[List[Any]]()

  def getLoadParams(productName: String, sensorName: String = null, measurementName: ArrayBuffer[String] = ArrayBuffer.empty[String], startTime: String = null, endTime: String = null, extent: Extent = null, crs: CRS = null, level: Int = 0, cloudCoverMin: Float = 0, cloudCoverMax: Float = 100): Array[(String, String)] = {
    val union = extent
    val metaList: ListBuffer[CoverageMetadata] = queryCoverageCollection(productName, sensorName, measurementName, startTime, endTime, union, crs,cloudCoverMin, cloudCoverMax)
    metaList.groupBy(x => x.getCoverageID).keys.toArray.map(y => (y, productName))

  }

  //  def addCoverage()

  private class CoverageArray {
    def load(implicit sc: SparkContext, coverageId: String, productKey: String, level: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.load(sc, coverageId, productKey, level)
    }

    def add(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
            coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.add(coverage1, coverage2)
    }

    def subtract(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                 coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.subtract(coverage1, coverage2)
    }

    def divide(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.divide(coverage1, coverage2)
    }

    def multiply(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                 coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.multiply(coverage1, coverage2)
    }

    def addNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               i: Any*): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.addNum(coverage, i)
    }

    def subtractNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.subtractNum(coverage, i)
    }

    def divideNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.divideNum(coverage, i)
    }

    def multiplyNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                    i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.multiplyNum(coverage, i)
    }

    def normalizedDifference(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bandNames: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.normalizedDifference(coverage, bandNames)
    }

    def toInt8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toInt8(coverage)
    }

    def toUint8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toUint8(coverage)
    }

    def toInt16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toInt16(coverage)
    }

    def toUint16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toUint16(coverage)
    }

    def toInt32(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toInt32(coverage)
    }

    def toFloat(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toFloat(coverage)
    }

    def toDouble(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.toDouble(coverage)
    }

    def gdalClipRasterByMaskLayer(implicit sc: SparkContext,
                                  coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                                  mask: RDD[(String, (Geometry, Map[String, Any]))],
                                  cropToCutLine: String = "True",
                                  targetExtent: String = "",
                                  setResolution: String = "False",
                                  extra: String = "",
                                  targetCrs: String = "",
                                  keepResolution: String = "False",
                                  alphaBand: String = "False",
                                  options: String = "",
                                  multithreading: String = "False",
                                  dataType: String = "0",
                                  sourceCrs: String = "")
    : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      QGIS.gdalClipRasterByMaskLayer(sc, coverage, mask, cropToCutLine, targetExtent, setResolution, extra, targetCrs, keepResolution, alphaBand, options, multithreading, dataType, sourceCrs)
    }

    def addStyles(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      Coverage.addStyles(coverage, visParam)
    }

    def visualizeOnTheFly(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam, index: Int): Unit = {
      val dagId = Trigger.dagId + index

      // 教育版额外的判断,保存结果,调用回调接口
      if(Trigger.dagType.equals("edu")){
        makeTIFF(coverage,dagId)
        val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)

        val saveFilePath = s"$tempFilePath$dagId.tif"

        val path = s"$userId/result/${Trigger.outputFile}"
        //上传
        clientUtil.Upload(path, saveFilePath)
      }


      val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = addStyles(if (Trigger.coverageReadFromUploadFile) {
        reproject(coverage, CRS.fromEpsgCode(3857), resolutionTMSArray(Trigger.level))
      } else {
        coverage
      }, visParam)

      val tmsCrs: CRS = CRS.fromEpsgCode(3857)
      val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
      val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
      val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
      val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      })

      var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

      //     TODO lrx:后面要不要考虑直接从MinIO读出来的数据进行上下采样？
      //     大于0是上采样
      if (COGUtil.tileDifference > 0) {
        // 首先对其进行上采样
        // 上采样必须考虑范围缩小，不然非常占用内存
        val levelUp: Int = COGUtil.tileDifference
        val layoutOrigin: LayoutDefinition = coverageTMS.metadata.layout
        val extentOrigin: Extent = coverageTMS.metadata.layout.extent
        val extentIntersect: Extent = extentOrigin.intersection(COGUtil.extent).orNull
        val layoutCols: Int = math.max(math.ceil((extentIntersect.xmax - extentIntersect.xmin) / 256.0 / layoutOrigin.cellSize.width * (1 << levelUp)).toInt, 1)
        val layoutRows: Int = math.max(math.ceil((extentIntersect.ymax - extentIntersect.ymin) / 256.0 / layoutOrigin.cellSize.height * (1 << levelUp)).toInt, 1)
        val extentNew: Extent = Extent(extentIntersect.xmin, extentIntersect.ymin, extentIntersect.xmin + layoutCols * 256.0 * layoutOrigin.cellSize.width / (1 << levelUp), extentIntersect.ymin + layoutRows * 256.0 * layoutOrigin.cellSize.height / (1 << levelUp))

        val tileLayout: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
        val layoutNew: LayoutDefinition = LayoutDefinition(extentNew, tileLayout)
        coverageTMS = coverageTMS.reproject(coverageTMS.metadata.crs, layoutNew)._2
      }

      val (zoom, reprojected) = coverageTMS.reproject(tmsCrs, layoutScheme)

      val on_the_fly_path = GlobalConfig.Others.ontheFlyStorage + dagId
      val file = new File(on_the_fly_path)
      if (file.exists() && file.isDirectory) {
        println("Delete existed on_the_fly_path")
        val command = s"rm -rf $on_the_fly_path"
        println(command)
        //调用系统命令
        command.!!
      }


      val outputPath: String = GlobalConfig.Others.ontheFlyStorage
      // Create the attributes store that will tell us information about our catalog.
      val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
      // Create the writer that we will use to store the tiles in the local catalog.
      val writer: FileLayerWriter = FileLayerWriter(attributeStore)


      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
        if (Trigger.level - z <= 2 && Trigger.level - z >= 0) {
          val layerId: LayerId = LayerId(dagId, z)
          println(layerId)
          // If the layer exists already, delete it out before writing
          if (attributeStore.layerExists(layerId)) {
            //        new FileLayerManager(attributeStore).delete(layerId)
            try {
              writer.overwrite(layerId, rdd)
            } catch {
              case e: Exception =>
                e.printStackTrace()
            }
          }
          else {
            try{
              writer.write(layerId, rdd, ZCurveKeyIndexMethod)
            } catch {
              case e:Exception =>
                println(e)
                println("Continue writing Layers!")
            }

          }
        }
      }

      // 回调服务
      val jsonObject: JSONObject = new JSONObject
      val rasterJsonObject: JSONObject = new JSONObject
      if (visParam.getFormat == "png") {
        rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + dagId + "/{z}/{x}/{y}.png")
      }
      else {
        rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + dagId + "/{z}/{x}/{y}.jpg")
      }

      PostSender.shelvePost("raster",rasterJsonObject)

    }

    def visualizeBatch(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), batchParam: BatchParam, dagId: String): Unit = {
      val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      }).collect()

      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
      val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
      // 先转到EPSG:3857，将单位转为米缩放后再转回指定坐标系
      var reprojectTile: Raster[MultibandTile] = stitchedTile.reproject(coverage._2.crs, CRS.fromName("EPSG:3857"))
      val resample: Raster[MultibandTile] = reprojectTile.resample(math.max((reprojectTile.cellSize.width * reprojectTile.cols / batchParam.getScale).toInt, 1), math.max((reprojectTile.cellSize.height * reprojectTile.rows / batchParam.getScale).toInt, 1))
      reprojectTile = resample.reproject(CRS.fromName("EPSG:3857"), batchParam.getCrs)


      // 上传文件
      val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.tiff"
      GeoTiff(reprojectTile, batchParam.getCrs).write(saveFilePath)


      val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
      val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
      val obj: JSONObject = new JSONObject
      obj.put("path",path.toString)
      PostSender.shelvePost("info",obj)
      //    client.putObject(PutObjectArgs.builder().bucket("oge-user").`object`(batchParam.getFileName + "." + batchParam.getFormat).stream(inputStream,inputStream.available(),-1).build)
      clientUtil.Upload(path, saveFilePath)

      //    client.putObject(PutObjectArgs)
      //    minIOUtil.releaseMinioClient(client)

    }
  }


  def process(funcName: String, args: List[Any], number: Int): Unit = {
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(new CoverageArray)
    val methodX = ru.typeOf[CoverageArray].decl(ru.TermName(funcName.split('.')(1))).asMethod
    val mm = im.reflectMethod(methodX)
    try {
      args.length match {
        case 0 =>
          val result = mm(CoverageList(number))
          result match {
            case value: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) => CoverageList += ((number + 1) -> value)
            case _ => println("不知道啥类型")
          }
        case 1 =>
          val result = mm(CoverageList(number), args.head)
          result match {
            case value: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) => CoverageList += ((number + 1) -> value)
            case _ => println("不知道啥类型")
          }
        case 2 =>
          println(args.head)
          println(args(1))
          println(CoverageList(number))
          val result = mm(CoverageList(number), args.head, args(1))
          result match {
            case value: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) => CoverageList += ((number + 1) -> value)
            case _ => println("不知道啥类型")
          }
        case 3 =>
          val result = mm(CoverageList(number), args.head, args(1), args(2))
          result match {
            case value: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) => CoverageList += ((number + 1) -> value)
            case _ => println("不知道啥类型")
          }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //  def main(args: Array[String]): Unit = {
  //    val conf: SparkConf = new SparkConf().setAppName("New Coverage").setMaster("local[*]")
  //    val sc = new SparkContext(conf)
  //
  //    val metadata = Service.getCoverageCollection(productName = "GF1_L1_PMS1_EO", dateTime = "[2000-01-01 00:00:00,2023-12-31 00:00:00]", extent = "[114.3, 30, 114.5, 31]")
  //    val coverageArray1: Array[(String, String)] = CoverageArray.getLoadParams(productName = metadata.productName, sensorName = metadata.sensorName, measurementName = metadata.measurementName, startTime = metadata.startTime.toString, endTime = metadata.endTime.toString, extent = metadata.extent, crs = metadata.crs, level = Trigger.level, cloudCoverMin = metadata.getCloudCoverMin(), cloudCoverMax = metadata.getCloudCoverMax())
  //    val coverageArray = coverageArray1.slice(0, 5)
  //    coverageArray.foreach(t => println(t._1))
  //
  //    //    coverageArray.foreach(t => TriggerEdu.makeTIFF(t, "/D:/TMS/coverageArray/" + System.currentTimeMillis() + ".tif"))
  //    funcNameList += "CoverageArray.toDouble"
  //    funcNameList += "CoverageArray.normalizedDifference"
  //    funcNameList += "CoverageArray.visualizeBatch"
  //    funcArgs += List()
  //    funcArgs += List(List("MSS1_band2", "MSS1_band4"))
  //
  //    coverageArray.zipWithIndex.foreach {case (element, index) =>
  //      val coverage = Coverage.load(sc, element._1, element._2, 8)
  //      val firstCoverage: (Int, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])) = (0 -> coverage)
  //      CoverageList += firstCoverage
  //      println(CoverageList.toString())
  ////      val visParam: VisualizationParam = new VisualizationParam
  //      val batchParam = new BatchParam
  //      batchParam.setCrs("EPSG:4326")
  //      batchParam.setScale("100")
  //      funcArgs += List(batchParam, Trigger.dagId + index)
  //      println(funcArgs.toString())
  //      for (i <- funcNameList.indices) {
  //        println(i)
  //        println(funcNameList(i))
  //        println(funcArgs(i).toString)
  //        process(funcNameList(i), funcArgs(i), i)
  //      }
  //      //      TriggerEdu.makeTIFF(CoverageList(funcNameList.length), "/D:/Intermediate_results/TMS/" + System.currentTimeMillis() + ".tif")
  //      funcArgs.remove(funcArgs.length - 1)
  //      CoverageList.clear()
  //    }

  //  }
}
