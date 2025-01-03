package whu.edu.cn.oge.extension.docker

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable

/**
 * 第三方算子容器
 */
trait Docker {

  /**
   * 获取算子配置信息
   *
   * @param functionName
   * @return
   */
  def getConfig(functionName: String): JSONObject = {

    //todo 从接口拿算子配置信息

    val config =
      """{
        "name": "Source.ISO",
        "image": "saga-gis",
        "version": "v2.0",
        "script": "saga_cmd grid_calculus 21",

        "inputs": [
          {
            "name": "GRID",
            "type": "Coverage",
            "format": "TIF",
            "description": "",
            "default": "None",
            "optional": "False"
          },
          {
            "name": "REFERENCE",
            "type": "Coverage",
            "format": "TIF",
            "description": "",
            "default": "None",
            "optional": "False"
          }
        ],
        "outputs": [
          {
            "name": "MATCHED",
            "type": "Coverage",
            "format": "TIF",
            "description": ""
          }
        ]
      }"""

    // 解析算子配置
    val configObject = try {
      JSON.parseObject(config)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to parse JSON content", e)
    }

    // 提取算子信息
    if (!configObject.containsKey("image")) {
      throw new RuntimeException(s"Missing 'image' key in 'operator' section of JSON")
    }

    if (!configObject.containsKey("version")) {
      throw new RuntimeException(s"Missing 'version' key in 'operator' section of JSON")
    }

    if (!configObject.containsKey("script")) {
      throw new RuntimeException(s"Missing 'script' key in 'operator' section of JSON")
    }

    if (!configObject.containsKey("inputs")) {
      throw new RuntimeException(s"Missing 'parameters' key in 'operator' section of JSON")
    }

    if (!configObject.containsKey("outputs")) {
      throw new RuntimeException(s"Missing 'parameters' key in 'operator' section of JSON")
    }

    configObject

  }

  /**
   * 是否可用
   *
   * @return
   */
  def available(): Boolean

  /**
   * 构造命令
   *
   * @param configObject
   * @param parameters
   * @return
   */
  def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, Any]): String

  /**
   * 执行第三方算子
   *
   * @param command
   * @return
   */
  def execute(command: String): Unit

}
