package whu.edu.cn.oge.extension.docker

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable

/**
 * 第三方算子容器
 * K8S 方式
 */
object K8SDocker extends Docker {
  /**
   * 执行第三方算子
   *
   * @param jsonFilePath
   * @return
   */
  override def execute(jsonFilePath: String): Unit = ???

  /**
   * 是否可用
   *
   * @return
   */
  override def available(): Boolean = {
    false
  }

  override def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, Any]): String = ???
}
