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
   * @param command 命令
   * @return
   */
  override def execute(command: String): Unit = ???

  /**
   * 是否可用
   *
   * @return
   */
  override def available(): Boolean = {
    false
  }

  /**
   * 构造docker 命令
   *
   * @param configObject 算子配置JSON对象
   * @param parameters   参数信息
   * @return (服务名称， 执行命令)
   */
  override def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, String]): (String, String) = ???

  /**
   * 清理Service
   *
   * @param name 服务名称
   */
  override def clean(name: String): Unit = ???
}
