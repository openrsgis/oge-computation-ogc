package whu.edu.cn.oge.extension

import com.alibaba.fastjson.{JSONArray, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig.DockerSwarmConf
import whu.edu.cn.entity.{SpaceTimeBandKey, ThirdOperationDataType}
import whu.edu.cn.oge.extension.convert.Converter
import whu.edu.cn.oge.extension.convert.Converter.{coverage2TifConverter, feature2FileConverter, file2FeatureConverter, tif2CoverageConverter}
import whu.edu.cn.oge.extension.docker.DockerExecutor
import whu.edu.cn.trigger.Trigger

import scala.collection.mutable

object ThirdSource {

  def execute(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]) = {
    // 查数据库,检查算子是否存在,将输入输出数据元信息存本地
    // 检查算子输入是否合法
    // （参数转换后），通过docker swarm调用算子
    // 检查算子输出是否合法
    // （输出参数转换后）返回结果


    val dockerExecutor = DockerExecutor.getExecutor()

    // 获取算子配置信息
    val config = dockerExecutor.getConfig(funcName)
    val inputParamsArray = config.getJSONArray("inputs")
    val outParamsArray = config.getJSONArray("outputs")

    // 构造入参
    val inputParameters = makeInputParam(inputParamsArray, args, funcName, sc)
    val outputParameters = makeOutputParam(outParamsArray, funcName)

    inputParameters ++= outputParameters

    val command = dockerExecutor.makeCommand(config, inputParameters)

    // 执行命令
    dockerExecutor.execute(command)

    // 文件转换为RDD
    makeResult(outParamsArray, outputParameters, sc, UUID)

  }

  /**
   * 构造入参
   *
   * @param paramsObject
   * @param args
   * @param sc
   * @return
   */
  private def makeInputParam
  (paramsArray: JSONArray, args: mutable.Map[String, String], funcName: String, sc: SparkContext)
  : mutable.Map[String, Any] = {

    var parameters: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

    for (i <- 0 until paramsArray.size) {
      val param: JSONObject = paramsArray.getJSONObject(i)
      val name: String = param.getString("name")
      val format: String = param.getString("format")

      if (Trigger.coverageRddList.contains(args(name))) {
        // 栅格落盘
        val fileName = makeFileName(format, funcName, name)
        Converter.convert(Trigger.coverageRddList(args(name)), ThirdOperationDataType.TIF, sc, fileName)
        parameters += (name, fileName)

      } else if (Trigger.featureRddList.contains(args(name))) {
        // 矢量落盘
        val value = Trigger.featureRddList(args(name))
        value match {
          case rdd: RDD[(String, (Geometry, mutable.Map[String, Any]))] => {
            val fileName = makeFileName(format, funcName, name)
            Converter.convert(rdd, ThirdOperationDataType.SHP, sc, fileName)
            parameters += (name, fileName)
          }
          case _ => {
            throw new IllegalArgumentException("不支持的数据类型")
          }
        }

      } else if (Trigger.stringList.contains(args(name))) {
        parameters += (name, Trigger.stringList(args(name)))
      } else if (Trigger.doubleList.contains(args(name))) {
        parameters += (name, Trigger.stringList(args(name)))
      } else if (Trigger.intList.contains(args(name))) {
        parameters += (name, Trigger.stringList(args(name)))
      } else {
        parameters += (name, args(name))
      }
    }

    parameters
  }

  /**
   * 构造出参
   * 当前版本只支持输出文件路径
   *
   * @param paramsArray
   * @return
   */
  private def makeOutputParam(paramsArray: JSONArray, funcName: String): mutable.Map[String, String] = {

    var parameters: mutable.Map[String, String] = mutable.Map.empty[String, String]

    for (i <- 0 until paramsArray.size) {
      val param: JSONObject = paramsArray.getJSONObject(i)
      val name: String = param.getString("name")
      val format: String = param.getString("format")
      parameters += (name, makeFileName(format, funcName, name))
    }
    parameters
  }

  /**
   * 输出文件转回RDD
   *
   * @param paramsArray
   * @param outFiles
   * @param sc
   * @param UUID
   * @return
   */
  private def makeResult(paramsArray: JSONArray, outFiles: mutable.Map[String, String], sc: SparkContext, UUID: String): Any = {

    for (i <- 0 until paramsArray.size) {
      val param: JSONObject = paramsArray.getJSONObject(i)
      val name: String = param.getString("name")
      val format: String = param.getString("format")

      format match {
        case ThirdOperationDataType.TIF => {

          Trigger.coverageRddList += (
            UUID ->
              Converter.convert[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])](
                outFiles(name),
                ThirdOperationDataType.TIF,
                sc
              )
            )

        }
        case ThirdOperationDataType.SHP | ThirdOperationDataType.GEOJSON => {
          Trigger.featureRddList += (
            UUID ->
              Converter.convert[String, RDD[(String, (Geometry, mutable.Map[String, Any]))]](
                outFiles(name),
                ThirdOperationDataType.SHP,
                sc
              )
            )
        }
        case _ => {
          throw new IllegalArgumentException("不支持的文件类型")
        }
      }

    }

  }

  /**
   * 构造落盘文件名称
   *
   * @param format
   * @param funcName
   * @param paramName
   * @return
   */
  private def makeFileName(format: String, funcName: String, paramName: String): String = {

    val formatStr = format match {
      case ThirdOperationDataType.TIF => {
        "tif"
      }
      case ThirdOperationDataType.SHP => {
        "shp"
      }
      case ThirdOperationDataType.GEOJSON => {
        "geojson"
      }
      case _ => {
        throw new IllegalArgumentException("输出文件类型不支持")
      }
    }

    s"${DockerSwarmConf.MOUNT_TARGET}/${funcName}-${paramName}-" + System.currentTimeMillis() + s".${formatStr}"
  }

}
