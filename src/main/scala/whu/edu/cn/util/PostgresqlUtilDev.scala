package whu.edu.cn.util

import whu.edu.cn.util.GlobalConstantUtil._

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A config class for postresql connection.
 * */
object PostgresqlUtilDev {


  /**
   * 简单的数据库查询语句
   *
   * @param resultNames 结果集字段名
   * @param tableName   表名
   * @param rangeLimit  为空则不做范围过滤
   * @param connection  为空则内部创建连接
   * @param aliases     查询结果集的别名
   * @param jointLimit  连接查询: (表名, 限制条件)
   * @return java.sql.ResultSet
   * @author forDecember
   */
  def simpleSelect(resultNames: Array[String],
                   tableName: String,
                   rangeLimit: Array[(String, String, String)] = null,
                   connection: Connection = null,
                   aliases: Array[String] = null,
                   jointLimit: Array[(String, String)] = null)
  : ResultSet = {
    assert(resultNames.length > 0)
    assert(resultNames(0).nonEmpty)
    assert(tableName.nonEmpty)

    val conn: Connection = Option(connection)
      .orElse(Some(getConnection)).get
    val range: Array[(String, String, String)] = Option(rangeLimit)
      .orElse(Some(Array[(String, String, String)]())).get

    val sql = new mutable.StringBuilder()

    sql ++= "SELECT "
    resultNames.zipWithIndex.foreach {
      case (name, i) =>
        if (i == 0) sql ++= name
        else sql ++= ", " + name
    }

    // 别名(可选)
    if (aliases != null) {
      assert(aliases.nonEmpty)
      sql ++= " AS"
      aliases.foreach(alias => sql ++= " " + alias)
    }

    sql ++= " FROM " + tableName

    // 连接查询(可选)
    if (jointLimit != null) {
      assert(jointLimit.nonEmpty)
      jointLimit.foreach {
        case (table, limit) =>
          sql ++= " JOIN " + table + " ON " + limit
      }
    }

    sql ++= " WHERE"

    val limitList = new ArrayBuffer[String]()
    range.zipWithIndex.foreach {
      case ((key, operator, value), i) =>
        if (i == 0) {
          sql ++= key + " " + operator + " ?"
          limitList.append(value)
        } else {
          sql ++= " AND " + key + " " + operator + " ?"
          limitList.append(value)
        }
    }
    println(sql)
    // Configure to be Read Only
    val statement: PreparedStatement = conn.prepareStatement(
      sql.toString(),
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
    )
    limitList.zipWithIndex.foreach {
      case (value, i) => statement.setString(i, value)
    }
    try {
      statement.executeQuery()
    } finally {
      statement.close()
      conn.close()
    }
  }


  /**
   * 获取数据库连接
   *
   * @param
   * @return java.sql.Connection
   * @author forDecember
   */
  def getConnection: Connection = {
    var retries = 0
    var connection: Connection = null

    while (retries < POSTGRESQL_MAX_RETRIES && connection == null) {
      try {
        connection = DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD)
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

  //  def getStatement: PreparedStatement = statement
  //
  //  def close(): Unit = {
  //    try {
  //      this.connection.close()
  //      this.statement.close()
  //    } catch {
  //      case e: Exception =>
  //        e.printStackTrace()
  //    }
  //  }
}
