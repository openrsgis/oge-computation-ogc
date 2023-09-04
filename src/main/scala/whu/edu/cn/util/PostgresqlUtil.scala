package whu.edu.cn.util

import whu.edu.cn.util.GlobalConstantUtil.{POSTGRESQL_MAX_RETRIES, POSTGRESQL_PWD, POSTGRESQL_RETRY_DELAY, POSTGRESQL_URL, POSTGRESQL_USER}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * A config class for postresql connection.
 * */
class PostgresqlUtil(sql: String) {
  private val connection: Connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD)
  private val statement: PreparedStatement = connection.prepareStatement(sql)

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

  def getStatement: PreparedStatement = statement

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
