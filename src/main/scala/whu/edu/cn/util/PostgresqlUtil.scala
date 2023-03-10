package whu.edu.cn.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * A config class for postresql connection.
 * */
class PostgresqlUtil (sql: String){
  Class.forName(PostgresqlUtil.driver)
  private val connection: Connection = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password);
  private val statement: PreparedStatement = connection.prepareStatement(sql)

  def getConnection():Connection = connection

  def getStatement(): PreparedStatement = statement

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

object PostgresqlUtil{
  val url = "jdbc:postgresql://125.220.153.28:31340/oge"
  val driver = "org.postgresql.Driver"
  val user = "oge"
  val password = "ypfamily608"
}
