package whu.edu.cn.oge.extension

import com.alibaba.fastjson.{JSONArray, JSONObject}
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig.DockerSwarmConf
import whu.edu.cn.entity.ThirdOperationDataType.ThirdOperationDataType
import whu.edu.cn.entity.{SpaceTimeBandKey, ThirdOperationDataType}
import whu.edu.cn.oge.extension.convert.Converter
import whu.edu.cn.oge.extension.convert.Converter._
import whu.edu.cn.oge.extension.docker.DockerExecutor
import whu.edu.cn.trigger.Trigger

import scala.collection.mutable

object ThirdSource {

  /**
   * 执行第三方算子
   *
   * @param sc       spark上下文
   * @param UUID     算子UUID
   * @param funcName 算子名称
   * @param args     参数值
   */
  def execute(implicit sc: SparkContext, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {

    val dockerExecutor = DockerExecutor.getExecutor

    // 获取算子配置信息
    val config = dockerExecutor.getConfig(funcName)
    val inputParams = config.getJSONArray("args")
    val outParam = config.getJSONObject("output")

    // 构造命令
    val inputs = makeInputParam(inputParams, args, funcName, sc)
    val output = makeOutputParam(outParam, funcName)

    inputs += (output._1 -> output._2)

    val command = dockerExecutor.makeCommand(config, inputs)

    // 执行命令
    try {
      println(s"command: ${command._2}")
      dockerExecutor.execute(command._2)
    } catch {
      case e: Exception => {
        println(s"docker command exception: ${e}")
        dockerExecutor.clean(command._1)
      }
    } finally {
      dockerExecutor.clean(command._1)
    }

    // 文件转换为RDD
    try {
      makeResult(outParam, output._2, sc, UUID)
    } catch {
      case e: Exception => {
        println(s"make result exception: ${e}")
      }
    }

  }

  /**
   * 构造入参
   *
   * @param paramsArray 入参列表
   * @param args        参数
   * @param funcName    算子名称
   * @param sc          spark 上下文
   * @return
   */
  private def makeInputParam
  (paramsArray: JSONArray, args: mutable.Map[String, String], funcName: String, sc: SparkContext)
  : mutable.Map[String, String] = {

    var parameters: mutable.Map[String, String] = mutable.Map.empty[String, String]

    for (i <- 0 until paramsArray.size) {
      val param: JSONObject = paramsArray.getJSONObject(i)
      val name: String = param.getString("name")
      val format: String = param.getString("format")

      if (Trigger.coverageRddList.contains(args(name))) {
        // 栅格落盘
        val fileName = makeFileName(format, funcName, name)
        Converter.convert(Trigger.coverageRddList(args(name)), ThirdOperationDataType.TIF, sc, fileName)
        parameters += (name -> fileName)

      } else if (Trigger.featureRddList.contains(args(name))) {
        // 矢量落盘
        val value = Trigger.featureRddList(args(name))
        value match {
          case rdd: RDD[(String, (Geometry, mutable.Map[String, Any]))] =>
            val fileName = makeFileName(format, funcName, name)
            Converter.convert(rdd, ThirdOperationDataType.SHP, sc, fileName)
            parameters += (name -> fileName)
          case _ =>
            throw new IllegalArgumentException("不支持的数据类型")

        }

      } else if (Trigger.stringList.contains(args(name))) {
        parameters += (name -> Trigger.stringList(args(name)))
      } else if (Trigger.doubleList.contains(args(name))) {
        parameters += (name -> Trigger.stringList(args(name)))
      } else if (Trigger.intList.contains(args(name))) {
        parameters += (name -> Trigger.stringList(args(name)))
      } else {
        parameters += (name -> args(name))
      }
    }

    parameters
  }

  /**
   * 构造出参
   * 当前版本只支持输出文件路径
   *
   * @param param    出参
   * @param funcName 算子名称
   * @return
   */
  private def makeOutputParam(param: JSONObject, funcName: String): (String, String) = {
    val name: String = param.getString("name")
    val format: String = param.getString("format")
    (name, makeFileName(format, funcName, name))
  }

  /**
   * 输出文件转回RDD
   *
   * @param param   出参
   * @param outFile 输出文件
   * @param sc          spark上下文
   * @param UUID        参数UUID
   * @return
   */
  private def makeResult(param: JSONObject, outFile: String, sc: SparkContext, UUID: String): Any = {

    val format: ThirdOperationDataType = ThirdOperationDataType.withNameInsensitive(param.getString("format"))

    format match {
      case ThirdOperationDataType.TIF =>

        Trigger.coverageRddList += (
          UUID ->
            Converter.convert[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])](
              outFile,
              format,
              sc
            )
          )
      case ThirdOperationDataType.SHP | ThirdOperationDataType.GEOJSON =>
        Trigger.featureRddList += (
          UUID ->
            Converter.convert[String, RDD[(String, (Geometry, mutable.Map[String, Any]))]](
              outFile,
              format,
              sc
            )
          )
      case ThirdOperationDataType.STRING =>
        Trigger.stringList += (
          UUID ->
            Converter.convert[String, String](
              outFile,
              format,
              sc
            )
          )
      case ThirdOperationDataType.INT =>
        Trigger.intList += (
          UUID ->
            Converter.convert[String, Int](
              outFile,
              format,
              sc
            )
          )
      case ThirdOperationDataType.DOUBLE =>
        Trigger.doubleList += (
          UUID ->
            Converter.convert[String, Double](
              outFile,
              format,
              sc
            )
          )
      case _ =>
        throw new IllegalArgumentException("不支持的文件类型")
    }

  }

  /**
   * 构造落盘文件名称
   *
   * @param format    落盘文件格式
   * @param funcName  算子名称
   * @param paramName 参数名称
   * @return
   */
  private def makeFileName(format: String, funcName: String, paramName: String): String = {
    val dataType: ThirdOperationDataType = ThirdOperationDataType.withName(format)
    val formatStr = dataType match {
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
