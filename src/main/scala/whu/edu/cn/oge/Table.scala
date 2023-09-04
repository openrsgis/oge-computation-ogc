package whu.edu.cn.oge

import com.alibaba.fastjson.{JSON, JSONObject}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import whu.edu.cn.util.HttpRequestUtil.sendPost

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Table {
  def getDownloadUrl(url: String, fileName: String): Unit ={
    val writeFile = new File(fileName)
    val writerOutput = new BufferedWriter(new FileWriter(writeFile))
    val outputString = "{\"table\":[{\"url\":" + "\"" + url + "\"}], \"vector\":[], \"raster\":[]}"
    writerOutput.write(outputString)
    writerOutput.close()
  }
  //水文算子接口
  def hargreaves(inputTemperature: String, inputStation: String, startTime: String, endTime: String, timeStep: Long): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/"+ inputTemperature +".csv")
    body.put("inputTemperature", bodyChildren)
    val bodyChildren2: JSONObject = new JSONObject()
    bodyChildren2.put("href", "http://125.220.153.22:8027/test_data/" + inputStation + ".geojson")
    body.put("inputStation", bodyChildren2)
    body.put("startTime", startTime)
    body.put("endTime", endTime)
    body.put("timeStep", timeStep)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/hargreaves", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def topModel(inputPrecipEvapFile: String, inputTopoIndex: String, startTime: String, endTime: String, timeStep: Long, rate: Double,
               recession: Int, tMax: Int, iterception: Int, waterShedArea: Int): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/" + "TI_raster" + ".txt")
    body.put("inputTopoIndex", bodyChildren)
    val bodyChildren2: JSONObject = new JSONObject()
    bodyChildren2.put("href", "http://125.220.153.22:8027/test_data/" + "prec_pet" + ".csv")
    body.put("inputPrecipEvapFile", bodyChildren2)
    body.put("rate", rate)
    body.put("recession", recession)
    body.put("tMax", tMax)
    body.put("iterception", iterception)
    body.put("waterShedArea", waterShedArea)
    body.put("startTime", startTime)
    body.put("endTime", endTime)
    body.put("timeStep", timeStep)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/topmodel", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def SWMM5(input: String): String = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/" + "Example2-Post" + ".inp")
    body.put("inputInp", bodyChildren)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/hydrology/swmm5", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  //定量遥感产品生产算子
  def virtualConstellation(): Unit = {
    val body: JSONObject = new JSONObject()
    val bodyChildren: JSONObject = new JSONObject()
    bodyChildren.put("href", "http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801.tif")
    bodyChildren.put("attachment", Array("http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_MTD_TL.xml",
      "http://125.220.153.22:8027/test_data/Sentinel2_GEE/GEE_S2A_MSIL1C_20201022T031801_N0209_R118_T49RCN_20201022T052801_MTD_MSIL1C.xml"))
    body.put("input", bodyChildren)
    body.put("bands", "B02,B03,B04,B08")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ard/virtualConstellation", param)
    println(s)
  }

  //湖南省定量遥感案例
  def calCrop(year: String, quarter: String, feature: String = null, sort: String, input: String = null): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", year)
    body.put("Quarter", quarter)
    body.put("Sort", sort)
    body.put("Region", "changsha")
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/crop", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def calVegIndex(quarter: String, year: String): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", year)
    body.put("Quarter", quarter)
    val param: String = body.toJSONString
    val outputString = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVegIndex", param)
    println(outputString)
    val jsonObject = JSON.parseObject(outputString)
    if (jsonObject.getString("status").equals("failed")) {
      throw new RuntimeException("web API failed!")
    }
    jsonObject.getJSONObject("result").getJSONObject("Output").getString("href")
  }

  def calVegCoverage(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    body.put("Quarter", "2")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVegCoverage", param)
    println(s)
  }

  def calNPP(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    body.put("Quarter", "1")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calNPP", param)
    println(s)
  }

  def calVEI(): Unit = {
    val body: JSONObject = new JSONObject()
    body.put("Year", "2018")
    val param: String = body.toJSONString
    val s = sendPost("http://125.220.153.22:18027/oge-model-service/api/model/service/ht/calVEI", param)
    println(s)
  }
}
