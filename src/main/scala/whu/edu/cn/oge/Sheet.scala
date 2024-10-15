package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_BUCKET_NAME
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.objectStorage.ObjectStorageFactory
import whu.edu.cn.oge.Feature.geometry
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostSender

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import scala.collection.mutable.Map

object Sheet{
  case class CsvData(header: List[String], data: List[List[String]])

  def loadCSVFromUpload(implicit sc: SparkContext, csvId: String, userID: String, dagId: String): CsvData = {
    var path: String = new String()
    if (csvId.endsWith(".csv")) {
      path = s"${userID}/$csvId"
    } else {
      path = s"$userID/$csvId.csv"
    }

    val platform = GlobalConfig.Others.platform
    var endpoint = ""
    if(platform =="bmr"){
      endpoint = GlobalConfig.BosConf.BOS_ENDPOINT
    }else if(platform =="cc"){
      endpoint = GlobalConfig.MinioConf.MINIO_ENDPOINT
    }else{
      endpoint = ""
    }

    //向百度云发送下载数据的请求，将数据下载到临时文件夹
    val client = ObjectStorageFactory.getClient(platform,endpoint)
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}.csv"
    val bosObject = client.DownloadObject(path, filePath, MINIO_BUCKET_NAME)
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
      val ans = Some(sheet.data(row-1)(col-1)).get
      println("cellValue is"+ans.toString)
      ans
    } else {
      println("None value")
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

    PostSender.shelvePost("info",Trigger.outputInformationList.toArray)
  }

  def formatCsvData(csvData: CsvData): String = {
    val headerRow = csvData.header.mkString(" | ")
    val separatorRow = "-" * headerRow.length

    val dataRows = csvData.data.map(row => row.mkString(" | "))

    val formattedTable = (headerRow +: separatorRow +: dataRows).mkString("\n")

    s"Table:\n$formattedTable"
  }

  def sheetToPoint(implicit sc: SparkContext,csvData: CsvData,lat_col:String,lon_col:String): RDD[(String, (Geometry, Map[String, Any]))] = {
    val keys = csvData.header
    val data = csvData.data
    val propertyKeys = keys.filterNot(Set(lat_col, lon_col).contains)

    val containsLatAndLon = keys.contains(lat_col) && keys.contains(lon_col)
    if(containsLatAndLon){
      // 创建一个 JSONArray 用于存放 features
      val features = new JSONArray()

      // 遍历数据列表
      data.foreach { row =>
        // 将 keys 和当前行的数据组合成一个 Map
        val coordinates = keys.zip(row).toMap
        // 创建一个 JSONObject 用于表示 feature
        val feature = new JSONObject()
        // 创建一个 JSONObject 用于表示 geometry
        val geometry = new JSONObject()
        // 向 geometry 中添加 coordinates 和 type 键值对
        val coordinatesList = new JSONArray()
        coordinatesList.add(coordinates(lon_col).toDouble)
        coordinatesList.add(coordinates(lat_col).toDouble)

        //      val coordinatesList = Seq(coordinates("lon").toDouble, coordinates("lat").toDouble)
        geometry.put("coordinates", coordinatesList)
        geometry.put("type", "Point")

        // 向 feature 中添加 geometry 和 type 键值对
        feature.put("geometry", geometry)
        feature.put("type", "Feature")

        // 创建一个 JSONObject 用于表示 properties
        val properties = new JSONObject()
        // 遍历所有的属性键，添加到 properties 中
        propertyKeys.foreach { propertyKey =>
          properties.put(propertyKey, coordinates.getOrElse(propertyKey, ""))
        }

        feature.put("properties", properties)
        features.add(feature)
      }

      // 创建一个 JSONObject 用于表示整个 GeoJSON
      val geoJson = new JSONObject()
      // 向 geoJson 中添加 features 和 type 键值对
      geoJson.put("features", features)
      geoJson.put("type", "FeatureCollection")

      // 将整个 geoJson 转换为 JSON 字符串并返回
      val geoJson_string = geoJson.toJSONString
      println(geoJson_string)
      geometry(sc, geoJson_string, "EPSG:4326")
    }else{
      throw new IllegalArgumentException("请确保表格中有表示“lat”和“lon”的字段！")
    }

  }

  def pointToSheet(feature: RDD[(String, (Geometry, Map[String, Any]))]): CsvData = {
    //先将feature转为geojson
    val jsonString = pointToGeojson(feature)
    // 将 JSON 字符串解析为 JSONObject
    val jsonObject: JSONObject = JSON.parseObject(jsonString)

    // 获取 features 数组
    val featuresArray: JSONArray = jsonObject.getJSONArray("features")

    // 提取 coordinates、type 和 properties 字段
    //读取csv数据的数据体部分
    val extractedData: List[List[String]] = featuresArray.toArray.map { feature =>
      val geometry = JSON.parseObject(feature.toString).getJSONObject("geometry")
      val coordinates = geometry.getJSONArray("coordinates").toArray.map(_.toString)
      val properties = JSON.parseObject(feature.toString).getJSONObject("properties")
      // 获取属性的键集合
      val keys_tmp:List[String] = properties.keySet().toArray.map(_.toString).toList
      // 提取属性值，如果属性不存在则用空字符串表示
      val propertyValues = keys_tmp.map(key => Option(properties.getString(key)).getOrElse(""))
      // 组合经纬度和属性值
      coordinates.toList ++ propertyValues
    }.toList

    //从第一个点要素中读取csv数据的表头部分
    //不要遗漏了经纬度属性
    val first_elepro = featuresArray.getJSONObject(0).getString("properties")
    val fieldNames: List[String] = List("lon","lat")++JSON.parseObject(first_elepro).keySet().toArray.map(_.toString).toList

    CsvData(fieldNames,extractedData)
  }

  def pointToGeojson(feature: RDD[(String, (Geometry, Map[String, Any]))]): String = {
    val jsonArray = new JSONArray()

    feature.collect().foreach { case (_, (geometry, properties)) =>
      // 提取几何对象的坐标和其他属性
      val coordinates: Array[Double] = Array(geometry.getCoordinate.x, geometry.getCoordinate.y)
      // 创建 JSON 对象
      val featureJson = new JSONObject()
      featureJson.put("type", "Feature")
      featureJson.put("geometry", new JSONObject().fluentPut("type", "Point").fluentPut("coordinates", coordinates))

      // 处理 properties 中的键值对
      val propertiesJson = new JSONObject()
      properties.foreach { case (key, value) =>
        // 将属性值根据类型进行处理
        value match {
          case boolValue: Boolean => propertiesJson.put(key, boolValue)
          case _ => propertiesJson.put(key, value.toString)
        }
      }
      featureJson.put("properties", propertiesJson)
      jsonArray.add(featureJson)
    }
    new JSONObject().fluentPut("type", "FeatureCollection").fluentPut("features", jsonArray).toString
  }

  def main(args: Array[String]): Unit = {
    val filePath = "C:\\Users\\17510\\Desktop\\ICESat_test.csv"
    // 读取CSV文件
    val csvData = readCsv(filePath)

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    // 获取第二行第三列的值
    //    val value = getcellValue(csvData, row = 2, col = 3)
    //
    //    val filteredData = filterByHeader(csvData, condition = "sig", value = "3")
    //    writeCsv("C:\\Users\\17510\\Desktop\\test2.csv", filteredData)
    val geometry_rdd = sheetToPoint(sc,csvData,"lat","lon")
    //    val geojson = "{\"features\":[{\"geometry\":{\"coordinates\":[114.2,39.25400195],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"sig\":\"1\",\"h\":\"850.4276848\"}},{\"geometry\":{\"coordinates\":[104.2,39.25400228],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"sig\":\"3\",\"h\":\"858.6771965\"}},{\"geometry\":{\"coordinates\":[94.2,39.2540023],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"sig\":\"3\",\"h\":\"859.198925\"}}],\"type\":\"FeatureCollection\"}"
    //    val sheet = GeoJsonToSheet(geojson)
    val sheet1 = pointToSheet(geometry_rdd)
    val formattedTable = formatCsvData(sheet1)
    //    writeCsv("C:\\Users\\17510\\Desktop\\test2.csv", sheet1)

    println(formattedTable)
  }

}