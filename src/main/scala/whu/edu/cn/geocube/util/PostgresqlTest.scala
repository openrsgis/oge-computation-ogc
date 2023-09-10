package whu.edu.cn.geocube.util

/**
 * @author czp
 * @date 2022/3/18 11:54
 */
object PostgresqlTest {
  def main(args: Array[String]): Unit = {
    val postgresqlService = new PostgresqlService

    //get maxProductId and maxProductKey
    var productKey = postgresqlService.getMaxRasterProductId
    print(productKey)
  }
}
