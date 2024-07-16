package whu.edu.cn.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.jcraft.jsch.{ChannelShell, JSch, Session}
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.jsonparser.JsonToArg.thirdJson
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import java.io._
import java.util
import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.language.postfixOps

/**
 * @author yqx
 * @since 2024/5/30
 */
object BashUtil {

  val algorithmInfos: JSONObject = {
    val source: BufferedSource = Source.fromFile(thirdJson)
    val line: String = source.mkString
    JSON.parseObject(line)
  }

  private val username: String = GlobalConfig.ThirdApplication.THIRD_USERNAME
  private val host: String = GlobalConfig.ThirdApplication.THIRD_HOST
  private val password: String = GlobalConfig.ThirdApplication.THIRD_PASSWORD
  private val port : Int = GlobalConfig.ThirdApplication.THIRD_PORT

  /**
   *
   * @param functionName 函数执行的类+方法名(在third-algorithm-infos.json文件里配置的)
   * @param args 函数参数
   * @param argumentSeparator 调用命令的参数分隔符
   * @param inputFiles 第三方算子的输入文件路径
   */
  def execute(fileName: String, args: mutable.Map[String, Any], argumentSeparator: String, time: String): Unit = {
    //     编写调用算子的sh命令
    val outputPath = " --output ./clip_" + time + "_out.tif"
    print("555555555555555555555555555555555555555555555555555555555555555555555555555")
    val baseContent: String =  "docker run --rm -v /mnt/storage/dem:/home/dell/cppGDAL -w " + "/home/dell/cppGDAL" + " " + "gdaltorch:v1" + " " + "python DoShading.py" + outputPath
    val command = baseContent
    println(command)

    try {
      print("3333333333333333333333333333333333333333333333333333333333333333333333333333")
      versouSshUtil(host, username, password, port)

      println(s"st = $command")
      runCmd(command, "UTF-8")

    } catch {
      case e: Exception =>
        throw new Exception(e)
    }
  }

  /**
   *
   * @param functionName 函数执行的类+方法
   * @param args 函数参数
   * @param inputFiles 第三方算子的输入文件路径
   */
//  def execute(functionName: String, args: mutable.Map[String, Any], inputFiles: Array[String]): Unit = {
//
//    val algorithmInfo: JSONObject = algorithmInfos.getJSONObject(functionName)
//    //     编写调用算子的sh命令
//    val baseContent: String =  "docker run --rm -v /mnt/dem:/home/dell/cppGDAL -w " + "/home/dell/cppGDAL" + " " + "gdaltorch:v1" + " " + "python DoShading.py"
//    val argsContent = args.values.mkString(" ") + " " + inputFiles.mkString(" ")
//    val command = baseContent + " " + argsContent
//    println(command)
//    // 远程执行sh命令
//    shellProcess(command)
//  }

  /**
   *
   * @param command 远程调用的命令
   */
  def shellProcess(command: String): Unit = {
    var session: Session = null
    var shell: ChannelShell = null

    try {
      val jsch = new JSch()
      session = jsch.getSession(username, host, port)
      session.setPassword(password)

      // Avoid asking for key confirmation
      session.setConfig("StrictHostKeyChecking", "no")
      session.connect()

      shell = session.openChannel("shell").asInstanceOf[ChannelShell]
      val input: InputStream = shell.getInputStream
      val output: OutputStream = shell.getOutputStream
      val printWriter: PrintWriter = new PrintWriter(output)
      shell.setPty(true)

      shell.connect()

      println("开始执行")
      printWriter.println(command)
      printWriter.println("exit")
      printWriter.flush()
      output.flush()
      val in: BufferedReader = new BufferedReader(new InputStreamReader(input))
      var msg: String = null
      val lis: util.ArrayList[String] = new util.ArrayList();
      msg = in.readLine()
      while (msg != null) {
        lis.add(msg.trim())
        msg = in.readLine()
      }
      in.close()
      session.disconnect()
      println(lis)

    } catch {
      case e: Exception =>
        println(e)
    } finally {
      println("结束执行")
      if (shell != null && shell.isConnected) {
        shell.disconnect()
      }
      if (session != null && session.isConnected) {
        session.disconnect()
      }
    }
  }

//  def main(args: Array[String]): Unit = {
//    val args: mutable.Map[String, Any] = mutable.Map.empty[String, Any]
//    val fileNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
//    fileNames += "/usr/local/data/clip.tiff"
//    execute("Coverage.demRender", args, "--", fileNames.toArray)
//  }
}

