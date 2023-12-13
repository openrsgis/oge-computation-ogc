package whu.edu.cn.util

import whu.edu.cn.config.GlobalConfig.PostgreSqlConf.{POSTGRESQL_DRIVER, POSTGRESQL_MAX_RETRIES, POSTGRESQL_PWD, POSTGRESQL_RETRY_DELAY, POSTGRESQL_URL, POSTGRESQL_USER}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

/**
 * A config class for postresql connection.
 * */
@deprecated("forDece: 该类已被弃用，" +
  "如有相关需求请移步 PostgresqlUtilDev")
class PostgresqlUtil(sql: String) {
  Class.forName(POSTGRESQL_DRIVER)
  private val connection: Connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD)
  private val statement: PreparedStatement = connection.prepareStatement(sql)
  println("正在创建已被弃用的类！！")

  def getConnection: Connection = {
    var retries = 0
    var connection: Connection = null

    while (retries < POSTGRESQL_MAX_RETRIES && connection == null) {
      try {
        connection = this.connection
      } catch {
        case _: Exception =>
          retries += 1
          println(s"连接失败，重试第 $retries 次...")
          Thread.sleep(POSTGRESQL_RETRY_DELAY)
      }
    }

    if (connection == null) {
      throw new RuntimeException("无法建立数据库连接")
    }

    connection
  }

  @deprecated("forDece: 该类已被弃用，" +
    "如有相关需求请移步 PostgresqlUtilDev")
  def getStatement: PreparedStatement = statement

  @deprecated("forDece: 该类已被弃用，" +
    "如有相关需求请移步 PostgresqlUtilDev")
  def close(): Unit = {
    try {
      this.connection.close()
      this.statement.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

@deprecated("forDece: 该对象已被弃用，" +
  "如有相关需求请移步 PostgresqlUtilDev")
object PostgresqlUtil {
  //val url = "jdbc:postgresql://125.220.153.26:5432/geocube"
  //val url = "jdbc:postgresql://125.220.153.26:5432/whugeocube"
  //  val url = "jdbc:postgresql://125.220.153.26:5432/multigeocube"
  //  val url = "jdbc:postgresql://172.20.20.9:25432/multigeocube"
  //  val driver = "org.postgresql.Driver"
  //  val user = "geocube"
  ////  val password = "ypfamily608"
  //  val password = "ypfamilysouthgis"
  var url = POSTGRESQL_URL
  var driver = POSTGRESQL_DRIVER
  var user = POSTGRESQL_USER
  //  val password = "ypfamily608"
  var password = POSTGRESQL_PWD

  def get(): Unit = {
    //    val prop = new Properties()
    //    val inputStream = PostgresqlUtil.getClass.getClassLoader.getResourceAsStream("app.properties")
    //    prop.load(inputStream);

    //    this.url=prop.get("url").toString
    //    this.driver=prop.get("driver").toString
    //    this.user=prop.get("user").toString
    //    this.password=prop.get("password").toString

    this.url = POSTGRESQL_URL
    this.driver = POSTGRESQL_DRIVER
    this.user = POSTGRESQL_USER
    this.password = POSTGRESQL_PWD
  }
}

