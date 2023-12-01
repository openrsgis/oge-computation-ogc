package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.BosClientUtil_scala
import whu.edu.cn.util.HttpRequestUtil.sendPost

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

object Sheet{
  case class CsvData(header: List[String], data: List[List[String]])
  def loadCSVFromUpload(implicit sc: SparkContext, csvId: String, userID: String, dagId: String): CsvData = {
    var path: String = new String()
    if (csvId.endsWith(".csv")) {
      path = s"${userID}/$csvId"
    } else {
      path = s"$userID/$csvId.csv"
    }

    //向百度云发送下载数据的请求，将数据下载到临时文件夹
    val client = BosClientUtil_scala.getClient2
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}.csv"
    val tempfile = new File(filePath)
    val getObjectRequest = new GetObjectRequest("oge-user", path)
    tempfile.createNewFile()
    val bosObject = client.getObject(getObjectRequest, tempfile)
    println(filePath)

    val csvdata = readCsv(filePath)

    csvdata
  }

  def readCsv(filePath: String): CsvData = {
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.getLines().toList finally source.close()

    val data = lines.map(line => line.split(",").toList)
    val header = data.head

    CsvData(header, data.tail) // 返回包含表头和去掉表头后的数据部分的 CsvData
  }

  def writeCsv(filePath: String, csvData: CsvData): Unit = {
    val writer = new BufferedWriter(new FileWriter(filePath))

    try {
      // 写入表头
      writer.write(csvData.header.mkString(","))
      writer.newLine()

      // 写入数据
      csvData.data.foreach { row =>
        val csvLine = row.mkString(",")
        writer.write(csvLine)
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  //根据数据的行列号来进行查询（除去表头后的行列号）
  def getcellValue(sheet: CsvData, row: Int, col: Int): String = {
    // 检查行和列的有效性
    if (row >= 1 && row <= sheet.data.length && col >= 1 && col <= sheet.data(row).length) {
      Some(sheet.data(row-1)(col-1)).get
    } else {
      "None"
    }
  }

  //对csv进行切片，sliceRows为True时进行行切片，反之进行列切片
  //start和end为切片的起始行/列，如果是行的话是去除表头后的行号
  def slice(csvData: CsvData, sliceRows: Boolean, start: Int, end: Int): CsvData = {
    val slicedData = if (sliceRows) {
      csvData.data.slice(start-1, end)
    } else {
      csvData.data.map(row => row.slice(start-1, end))
    }
    val sliceHeader = if(sliceRows){
      csvData.header
    }else{
      csvData.header.slice(start-1,end)
    }
    // 添加表头到结果中
    CsvData(sliceHeader,slicedData)
  }

  def filterByHeader(csvData: CsvData, condition: String, value: String): CsvData = {

    // 找到要筛选的列的索引
    val columnIndex = csvData.header.indexOf(condition)

    if (columnIndex != -1) {
      // 根据条件和值进行筛选
      CsvData(csvData.header,csvData.data.filter(row => row(columnIndex) == value))
    } else {
      // 如果找不到匹配的列，返回空列表或抛出异常，根据实际需求
      println("no data find in this case")
      CsvData(List.empty,List.empty)
    }
  }

//  def printSheet(csvData: CsvData)

  def printSheet(res: CsvData, name: String): Unit = {
    val j = new JSONObject()
    val res_format = formatCsvData(res)
    j.put("name", name)
    j.put("value", res_format)
//    j.put("type", valueType)
    Trigger.outputInformationList.append(j)


    val jsonObject: JSONObject = new JSONObject

    jsonObject.put("info", Trigger.outputInformationList.toArray)

    val outJsonObject: JSONObject = new JSONObject
    outJsonObject.put("workID", Trigger.dagId)
    outJsonObject.put("json", jsonObject)
    println(outJsonObject)
    sendPost(DAG_ROOT_URL + "/deliverUrl", outJsonObject.toJSONString)
  }

  def formatCsvData(csvData: CsvData): String = {
    val headerRow = csvData.header.mkString(" | ")
    val separatorRow = "-" * headerRow.length

    val dataRows = csvData.data.map(row => row.mkString(" | "))

    val formattedTable = (headerRow +: separatorRow +: dataRows).mkString("\n")

    s"Table:\n$formattedTable"
  }

  def main(args: Array[String]): Unit = {
    val filePath = "C:\\Users\\17510\\Desktop\\test1.csv"

    // 读取CSV文件
    val csvData = readCsv(filePath)

    // 写入CSV文件
//    writeCsv("C:\\Users\\17510\\Desktop\\test2.csv", CsvData(header = csvData.header, data = csvData.data))

    // 获取第二行第三列的值
    val value = getcellValue(csvData, row = 2, col = 3)

//    value match {
//      case Some(v) => println(s"Value at row 2, column 3: $v")
//      case None => println("Invalid row or column index.")
//    }

    // 切片部分行
//    val slicedRows = slice(csvData, sliceRows = true, start = 1, end = 2)
//    slicedRows.foreach(row => println(row.mkString(", ")))

    val filteredData = filterByHeader(csvData, condition = "age", value = "25")
    writeCsv("C:\\Users\\17510\\Desktop\\test2.csv", filteredData)
    println(filteredData)

    val formattedTable = formatCsvData(filteredData)
    println(formattedTable)
  }


}
