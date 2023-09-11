package whu.edu.cn.geocube.conf

import java.util.Properties

import whu.edu.cn.util.PostgresqlUtil

/**
 * @author czp
 * @date 2022/3/18 17:55
 */
object Address {
  var ip=""
  def get: Unit ={
    val prop = new Properties()
    val inputStream = PostgresqlUtil.getClass.getClassLoader.getResourceAsStream("app.properties")
    prop.load(inputStream);

    this.ip=prop.get("ip").toString
  }
}
