package whu.edu.cn.util

import whu.edu.cn.util.GlobalConstantUtil.{POSTGRESQL_PWD, POSTGRESQL_URL, POSTGRESQL_USER}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * A config class for postresql connection.
 * */
class PostgresqlUtil(sql: String) {
  private lazy val connection: Connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD);
  private val statement: PreparedStatement = connection.prepareStatement(sql)

  def getConnection: Connection = connection

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
