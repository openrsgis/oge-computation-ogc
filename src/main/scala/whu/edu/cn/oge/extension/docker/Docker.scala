package whu.edu.cn.oge.extension.docker

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.jsonparser.JsonToArg

import scala.collection.mutable

/**
 * 第三方算子容器
 */
trait Docker {

  /**
   * 获取算子配置信息
   *
   * @param functionName 算子名称
   * @return
   */
  def getConfig(functionName: String): JSONObject = {

    val configObject: JSONObject = JsonToArg.getAlgorithmJson(functionName)

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

    if (!configObject.containsKey("args")) {
      throw new RuntimeException(s"Missing 'args' key in 'operator' section of JSON")
    }

    if (!configObject.containsKey("output")) {
      throw new RuntimeException(s"Missing 'output' key in 'operator' section of JSON")
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
   * 构造docker 命令
   *
   * @param configObject 算子配置JSON对象
   * @param parameters   参数信息
   * @return (服务名称， 执行命令)
   */
  def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, String]): (String, String)

  /**
   * 执行第三方算子
   *
   * @param command 命令
   * @return
   */
  def execute(command: String): Unit

  /**
   * 清理Service
   *
   * @param name 服务名称
   */
  def clean(name: String): Unit

}
