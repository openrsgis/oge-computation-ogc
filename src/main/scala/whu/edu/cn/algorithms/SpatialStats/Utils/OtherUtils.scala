package whu.edu.cn.algorithms.SpatialStats.Utils

import au.com.bytecode.opencsv._
import breeze.plot.Figure
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString, MultiPolygon, Point}
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.GlobalConstantUtil.DAG_ROOT_URL
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}
import whu.edu.cn.util.ShapeFileUtil._

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.io.StringReader
import scala.collection.mutable


object OtherUtils {

  /**
   * 读入csv
   *
   * @param sc      SparkContext
   * @param csvPath csvPath
   * @return        RDD
   */
  def readcsv2(implicit sc: SparkContext, csvPath: String, firstline: Boolean = true): RDD[mutable.Map[String, Any]] = {
    val data = sc.textFile(csvPath)
    val csvdata = data.map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })
    val preRDD=ArrayBuffer.empty[mutable.Map[String, Any]]
    val names = csvdata.take(1).flatten
    if(firstline) {
      val no1line = csvdata.collect().drop(1)
      no1line.foreach(t => {
        val mapdata = mutable.Map.empty[String, Any]
        for (i <- names.indices) {
          mapdata += (names(i) -> t(i))
        }
        preRDD += mapdata
      })
    }
    else{
      val nameidx=names.zipWithIndex.map(t=>"x"+t._2.toString)
      csvdata.collect().foreach(t => {
        val mapdata = mutable.Map.empty[String, Any]
        for (i <- nameidx.indices) {
          mapdata += (nameidx(i) -> t(i))
        }
        preRDD += mapdata
      })
    }
    //    preRDD.foreach(println)
    sc.makeRDD(preRDD)
  }

  /**
   * 读入csv
   *
   * @param sc      SparkContext
   * @param csvPath csvPath
   * @return RDD
   */
  def readcsv(implicit sc: SparkContext, csvPath: String): RDD[Array[(String, Int)]] = {
    val data = sc.textFile(csvPath)
    val csvdata = data.map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })
    csvdata.map(t => t.zipWithIndex)
  }

  /**
   * 获取RDD矢量的数据类型（points，lines，multipolygons etc）
   *
   * @param geomRDD Input RDD
   * @return        type String
   */
  def getGeometryType(geomRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): String = {
    geomRDD.map(t => t._2._1).first().getGeometryType
  }

  /**
   * 输入要添加的属性数据和RDD，输出RDD
   *
   * @param sc           SparkContext
   * @param shpRDD       源RDD
   * @param writeArray   要写入的属性数据，Array形式
   * @param propertyName 属性名，String类型，需要少于10个字符
   * @return RDD
   */
  def writeRDD(implicit sc: SparkContext, shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], writeArray: Array[Double], propertyName: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    if (propertyName.length >= 10) {
      throw new IllegalArgumentException("the length of property name must not longer than 10!!")
    }
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += (propertyName -> writeArray(t._2))
    })
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  /**
   * 输出shp
   *
   * @param outputshpRDD  需要输出的RDD
   * @param outputshpPath 输出路径
   */
  def writeshpfile(outputshpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], outputshpPath: String): Unit = {
    val geom = getGeometryType(outputshpRDD)
    geom match {
      case "MultiPolygon" => createShp(outputshpPath, "utf-8", classOf[MultiPolygon], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case "Point" => createShp(outputshpPath, "utf-8", classOf[Point], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case "LineString" => createShp(outputshpPath, "utf-8", classOf[LineString], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case _ => throw new IllegalArgumentException("can not modified geometry type, please retry")
    }
    println(s"shpfile written successfully in $outputshpPath")
  }

  /**
   * 通过列名称header获取属性值
   *
   * @param csvRDD    输入RDD
   * @param property  header名称，String
   * @return          属性列，Array[String]
   */
  def attributeSelectHead(csvRDD: RDD[Array[(String, Int)]], property: String): Array[String] = {
    val head = csvRDD.collect()(0)
    val csvArr = csvRDD.collect().drop(1)
    var resultArr = new Array[String](csvArr.length)
    var idx: Int = -1
    head.map(t => {
      if (t._1 == property) idx = t._2
    })
    //    println(idx)
    if (idx == -1) {
      throw new IllegalArgumentException("property name didn't exist!!")
    } else {
      val df = csvArr.map(t => t.filter(i => i._2 == idx).map(t => t._1))
      resultArr = df.flatten
    }
    //    resultArr.foreach(println)
    resultArr
  }

  /**
   * 通过列数获取属性值
   *
   * @param csvRDD  输入RDD
   * @param number  输入列数
   * @return        属性列，Array[String]
   */
  def attributeSelectNum(csvRDD: RDD[Array[(String, Int)]], number: Int): Array[String] = {
    val head = csvRDD.collect()(0)
    val csvArr = csvRDD.collect().drop(1)
    var resultArr = new Array[String](csvArr.length)
    val idx: Int = number - 1 //从1开始算
    //    println(idx)
    if (idx >= head.length || idx < 0) {
      throw new IllegalArgumentException("property number didn't exist!!")
    } else {
      val df = csvArr.map(t => t.filter(i => i._2 == idx).map(t => t._1))
      resultArr = df.flatten
    }
    //    resultArr.foreach(println)
    resultArr
  }

  def readtimeExample(implicit sc: SparkContext, csvpath: String, timeproperty: String, timepattern: String = "yyyy/MM/dd"): Unit = {
    val csvdata = readcsv(sc, csvpath)
    val timep = attributeSelectHead(csvdata, timeproperty)
    val date = timep.map(t => {
      val date = new SimpleDateFormat(timepattern).parse(t)
      date
    })
    date.foreach(println)
    println((date(300).getTime - date(0).getTime) / 1000 / 60 / 60 / 24)
  }

  def showPng(fileName: String, fig: Figure): Unit = {
    val time = System.currentTimeMillis()

    fig.saveas(s"$fileName+$time.png", 300)
    val outputPath = s"$fileName+$time.png"

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
      val st = s"scp $outputPath root@10.101.240.20:/home/oge/tomcat/apache-tomcat-8.5.57/webapps/oge_vector/vector_${time}.json"
      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val storageURL = "http://10.101.240.20:8080/oge_vector/vector_" + time + ".json"
    val geoJson = new JSONObject
    geoJson.put(Trigger.layerName, storageURL)
    val jsonObject = new JSONObject
    jsonObject.put("vector", geoJson)
    val outJsonObject: JSONObject = new JSONObject
    outJsonObject.put("workID", Trigger.dagId)
    outJsonObject.put("json", jsonObject)
    sendPost(DAG_ROOT_URL + "/deliverUrl", outJsonObject.toJSONString)
    println(outJsonObject.toJSONString)
  }

}
