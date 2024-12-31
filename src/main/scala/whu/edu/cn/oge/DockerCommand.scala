package whu.edu.cn.oge
import scala.io.Source
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import whu.edu.cn.config.GlobalConfig
import java.nio.file.{Paths, Files}
import java.io.{PrintWriter, File}

object DockerCommand {

  val mountType = GlobalConfig.DockerSwarmConf.MOUNT_TYPE
  val mountSource = GlobalConfig.DockerSwarmConf.MOUNT_SOURCE
  val mountTarget = GlobalConfig.DockerSwarmConf.MOUNT_TARGET
  val constraint = GlobalConfig.DockerSwarmConf.CONSTRAINT
  val mode = GlobalConfig.DockerSwarmConf.MODE
  val masterHost = GlobalConfig.DockerSwarmConf.MASTER_HOST
  val registryPort = GlobalConfig.DockerSwarmConf.REGISTRY_PORT

    def updateJsonParameters(jsonFilePath: String, parameters: Map[String, Any]):Unit = {

      // 解析文件路径
      val absolutePath = Paths.get(jsonFilePath).toAbsolutePath.toString
      val fileName = jsonFilePath.split("/").last
      val file = new File(absolutePath)

      if(!file.exists() || !file.isFile) {
        throw new  IllegalArgumentException(s"algorithm json file not found: $fileName")
      }

      // 读取json配置文件
      val jsonString = try {
        Source.fromFile(file).mkString
      }catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to read JSON file: $fileName",e)
      }
      // 解析json配置文件
      val jsonObject = try {
        JSON.parseObject(jsonString)
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to parse JSON content from file: $fileName", e)
      }
      // 更新参数值
      val operator = jsonObject.getJSONObject("operator")
      if (operator == null) {
        throw new RuntimeException(s"Missing 'operator' key in JSON file: $fileName")
      }
      val paramsObject = operator.getJSONObject("parameters")
      if (paramsObject == null){
        throw new RuntimeException(s"Missing 'parameters' key in 'operator' section of JSON file: $fileName")
      }
      parameters.foreach{ case(key, value) =>
        paramsObject.put(key, value)
      }

      // 更新json
      try {
        val writer = new  PrintWriter(file)
        try writer.write(jsonObject.toJSONString) finally writer.close()
      } catch {
        case e: Exception =>
          throw new   RuntimeException(s"Failed to write updated JSON to file: $fileName", e)
      }
      println(s"Successfully updated JSON file")

    }


  def buildDockerCommand(jsonFilePath: String, sep: String): String = {
// 读取json配置文件
    val absolutePath = Paths.get(jsonFilePath).toAbsolutePath.toString
    val fileName = jsonFilePath.split("/").last
    val file = new File(absolutePath)
    if (!file.exists() || !file.isFile) {
      throw new IllegalArgumentException(s"JSON file $fileName not found")
    }

    val jsonString = try {
      Source.fromFile(file).mkString
    } catch {
      case e:Exception =>
        throw new RuntimeException(s"Failed to read JSON file: $fileName", e)
    }
    val jsonObject = try {
      JSON.    parseObject(jsonString)
    } catch {
      case e:Exception =>
        throw new RuntimeException(s"Failed to parse JSON file: $fileName", e)
    }

  // 提取算子信息
    val operator = jsonObject.getJSONObject("operator")
    val name = operator.getString("name")
    val image = operator.getString("image")
    val version = operator.getString("version")
    val script = operator.getString("script")
    val parameters = operator.getJSONObject("parameters")

    // 构建参数字符串
    val parameterString = parameters.entrySet().toArray.map{ entry =>
      val param = entry.asInstanceOf[java.util.Map.Entry[String, Any]]
      s"$sep${param.getKey} ${param.getValue}"
    }.mkString(" ")
    // 构建镜像名
    val imageName = s"$image:$version"

    // 完整Docker命令
    val time = System.currentTimeMillis()
    val dockerCommand =
      s"""docker service create --name ${name}_$time --mount type=$mountType,source=$mountSource,target=$mountTarget --constraint $constraint --mode $mode $masterHost:$registryPort/$imageName sh -c "$script $parameterString"""".stripMargin

dockerCommand

  }
}
