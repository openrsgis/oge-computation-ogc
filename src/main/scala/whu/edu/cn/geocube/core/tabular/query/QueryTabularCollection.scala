package whu.edu.cn.geocube.core.tabular.query

import java.text.SimpleDateFormat
import java.util.Date

import whu.edu.cn.geocube.core.entity.QueryParams

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object QueryTabularCollection {
  val prefixPath = "/home/geocube/data/tabular/"

  def getTabularCollection(queryParams: QueryParams): Array[Map[String, String]] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- The inquiry request of tabular collection is being processed...")
    val queryBegin = System.currentTimeMillis()
    val tabularProductName = queryParams.getTabularProductName
    val path = prefixPath + tabularProductName + ".txt"
    val results = new ArrayBuffer[Map[String, String]]()

    val file=Source.fromFile(path)
    val attributeArr = file.getLines().next().split(" ")
    for(line <- file.getLines) {
      val valueArr = line.split(" ")
      var result: Map[String, String] = Map()
      (0 until attributeArr.length).foreach{ i =>
        result += (attributeArr(i)->valueArr(i))
      }
      results.append(result)
    }
    file.close

    val queryEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost of querying the tabular collection: " + (queryEnd - queryBegin) + " ms")

    results.toArray
  }
}
