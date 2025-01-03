package whu.edu.cn.oge.extension.docker

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.util.SSHClient

import scala.collection.mutable

/**
 * 第三方算子容器
 * swarm 方式
 */
object SwarmDocker extends Docker {

  val mountType = GlobalConfig.DockerSwarmConf.MOUNT_TYPE
  val mountSource = GlobalConfig.DockerSwarmConf.MOUNT_SOURCE
  val mountTarget = GlobalConfig.DockerSwarmConf.MOUNT_TARGET
  val constraint = GlobalConfig.DockerSwarmConf.CONSTRAINT
  val mode = GlobalConfig.DockerSwarmConf.MODE
  val masterHost = GlobalConfig.DockerSwarmConf.MASTER_HOST
  val registryPort = GlobalConfig.DockerSwarmConf.REGISTRY_PORT

  val username: String = GlobalConfig.ThirdApplication.THIRD_USERNAME
  val host: String = GlobalConfig.ThirdApplication.THIRD_HOST
  val password: String = GlobalConfig.ThirdApplication.THIRD_PASSWORD
  val port: Int = GlobalConfig.ThirdApplication.THIRD_PORT

  /**
   * 执行第三方算子
   *
   * @param jsonFilePath
   * @return
   */
  override def execute(dockerCommand: String): Unit = {

    val client = new SSHClient()
    client.versouSshUtil(host, username, password, port)
    client.runCmd(dockerCommand, "UTF-8")
  }

  /**
   * 是否可用
   *
   * @return
   */
  override def available(): Boolean = {
    true
  }

  /**
   *
   * @param config
   * @param parameters
   */
  def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, Any]): String = {

    // 提取算子信息
    val name = configObject.getString("name")
    val image = configObject.getString("image")
    val version = configObject.getString("version")
    val script = configObject.getString("script")
    val paramsObject = configObject.getJSONObject("parameters")

    parameters.foreach { case (key, value) =>
      paramsObject.put(key, value)
    }

    // 构建参数字符串
    val parameterString = paramsObject.entrySet().toArray.map { entry =>
      val param = entry.asInstanceOf[java.util.Map.Entry[String, Any]]
      s"-${param.getKey} ${param.getValue}"
    }.mkString(" ")

    // 构建镜像名
    val imageName = s"$image:$version"

    // 完整Docker命令
    val time = System.currentTimeMillis()
    val dockerCommand =
      s"""docker service create --name ${name}_$time --mount type=$mountType,source=$mountSource,target=$mountTarget
         |--constraint $constraint --mode $mode
         |$masterHost:$registryPort/$imageName sh -c "$script $parameterString"""".stripMargin

    dockerCommand
  }
}
