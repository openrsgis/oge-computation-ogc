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

  val mountType: String = GlobalConfig.DockerSwarmConf.MOUNT_TYPE
  val mountSource: String = GlobalConfig.DockerSwarmConf.MOUNT_SOURCE
  val mountTarget: String = GlobalConfig.DockerSwarmConf.MOUNT_TARGET
  val constraint: String = GlobalConfig.DockerSwarmConf.CONSTRAINT
  val mode: String = GlobalConfig.DockerSwarmConf.MODE

  val username: String = GlobalConfig.ThirdApplication.THIRD_USERNAME
  val host: String = GlobalConfig.ThirdApplication.THIRD_HOST
  val password: String = GlobalConfig.ThirdApplication.THIRD_PASSWORD
  val port: Int = GlobalConfig.ThirdApplication.THIRD_PORT

  /**
   * 执行第三方算子
   *
   * @param command 命令
   * @return
   */
  override def execute(command: String): Unit = {

    val client = new SSHClient()
    client.versouSshUtil(host, username, password, port)
    client.runCmd(command, "UTF-8")
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
   * 构造docker 命令
   *
   * @param configObject 算子配置JSON对象
   * @param parameters   参数信息
   * @return (服务名称， 执行命令)
   */
  def makeCommand(configObject: JSONObject, parameters: mutable.Map[String, String]): (String, String) = {

    // 提取算子信息
    val name = configObject.getString("name")
    val image = configObject.getString("image")
    val version = configObject.getString("version")
    val script = configObject.getString("script")
    val separators = if (configObject.containsKey("separators")) configObject.getString("separators") else "-"

    // 构建参数字符串
    val parameterString = parameters.map { entry =>
      s"${separators}${entry._1} ${entry._2}"
    }.mkString(" ")

    // 构建镜像名
    val imageName = s"$image:$version"

    // 完整Docker命令
    val time = System.currentTimeMillis()
    val serviceName = s"${name}_$time".replace(".", "")
    val dockerCommand =
      s"docker service create --name ${serviceName} --mount type=$mountType,source=$mountSource,target=$mountTarget " +
        s"--constraint $constraint --mode $mode ${GlobalConfig.DockerSwarmConf.HUB_SERVER}/${imageName} " +
        s"sh -c '${script} ${parameterString} 2>&1 | tee /mnt/storage/${serviceName}.log'"

    (serviceName, dockerCommand)
  }


  /**
   * 清理Service
   *
   * @param name 服务名称
   */
  override def clean(name: String): Unit = {

    execute(s"docker rm ${name}")
  }
}
