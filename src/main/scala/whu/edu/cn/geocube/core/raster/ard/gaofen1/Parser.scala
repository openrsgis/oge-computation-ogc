package whu.edu.cn.geocube.core.raster.ard.gaofen1

import java.io.FileInputStream
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Extract metadata in Gaofen-1 xml file.
 *
 * @param _xmlPath
 */
case class Parser(_xmlPath: String) {
  val xmlPath: String = _xmlPath
  val gainParams = new ArrayBuffer[Float]()
  val offsetParams = new ArrayBuffer[Float]()
  val esun = new ArrayBuffer[Float]()
  val date = new ArrayBuffer[Int]()
  var solarAzimuth = 0.0f
  var solarZenith = 0.0f
  var platform = ""
  var sensor = ""
  var receiveTime = ""
  var produceTime = ""
  var widthInPixels = ""
  var heightInPixels = ""
  var topLeftLatitude = ""
  var topLeftLongitude = ""
  var topRightLatitude = ""
  var topRightLongitude = ""
  var bottomRightLatitude = ""
  var bottomRightLongitude = ""
  var bottomLeftLatitude = ""
  var bottomLeftLongitude = ""
  var resolution = ""

  /**
   * Collect Radiometric Rescaling Parameters.
   *
   * @param platform
   * @param sensor
   * @param year
   * @return Gain and offset
   */
  def getGainOffset(platform: String, sensor: String, year: String):Array[Float] = {
    val objectMapper=new ObjectMapper()
    val node = objectMapper.readTree(new FileInputStream("/home/geocube/data/conf/Gaofen_CalibrationParameters.json"))
    //val node = objectMapper.readTree(new FileInputStream("conf/Gaofen_CalibrationParameters.json"))
//    val fileName = this.getClass().getClassLoader().getResource("Gaofen_CalibrationParameters.json").getPath()
//    val node = objectMapper.readTree(new FileInputStream(fileName))

    val gainOffsetBuffer = new ArrayBuffer[Float]()
    if(node != null && node.has(platform)) {
      val platformNode = node.get(platform)
      if(platformNode.has(sensor)){
        val sensorNode = platformNode.get(sensor)
        if(sensorNode.has(year)){
          val yearNode = sensorNode.get(year)
          val gainNode = yearNode.get("gain").asInstanceOf[ArrayNode]
          val gainElems = gainNode.elements()
          while(gainElems.hasNext) gainParams.append(gainElems.next().toString.toFloat)
          val offsetNode = yearNode.get("offset").asInstanceOf[ArrayNode]
          val offsetElems = offsetNode.elements()
          while(offsetElems.hasNext)offsetParams.append(offsetElems.next().toString.toFloat)
        }
      }
    }
    gainParams.foreach(gainOffsetBuffer.append(_))
    offsetParams.foreach(gainOffsetBuffer.append(_))
    if(gainOffsetBuffer.length < 8)
      throw new RuntimeException("There is no gain_offset parameters for " + platform + "/" + sensor + "/" + year)
    gainOffsetBuffer.toArray
  }

  /**
   * Get ESUN parameters with input platform.
   *
   * @param platform
   * @param sensor
   *
   * @return Esun parameters
   */
  def getEsun(platform: String, sensor: String):Array[Float] = {
    val objectMapper=new ObjectMapper()
    val node = objectMapper.readTree(new FileInputStream("/home/geocube/data/conf/Gaofen_Esun.json"))
    //val node = objectMapper.readTree(new FileInputStream("conf/Gaofen_Esun.json"))
//    val fileName = this.getClass().getClassLoader().getResource("Gaofen_Esun.json").getPath()
//    val node = objectMapper.readTree(new FileInputStream(fileName))

    //val esunBuffer = new ArrayBuffer[Float]()
    if(node != null && node.has(platform)) {
      val platformNode = node.get(platform)
      if(platformNode.has(sensor)){
        val sensorNode = platformNode.get(sensor)
        val esunElems = sensorNode.elements()
        while(esunElems.hasNext) esun.append(esunElems.next().toString.toFloat)
      }
    }
    if(esun.length < 4)
      throw new RuntimeException("There is no esun parameters for " + platform + "/" + sensor)
    esun.toArray
  }

  /**
   * Collect ReceiveTime, SolarAzimuth, SolarZenith, Gain and offset and ESUN.
   */
  def collectMetadata(): Unit = {
    val xml = XML.load(new FileInputStream(xmlPath))
    if(xml.isEmpty)
      throw new RuntimeException("Invalid xml path!")

    val receiveTimeNode = xml \ "ReceiveTime"
    if(receiveTimeNode.isEmpty)
      throw new RuntimeException("No ReceiveTime Node exists!")
    receiveTime =receiveTimeNode.text
    val receiveData = receiveTime.split(" ")(0).split("-")
    receiveData.foreach(x => date.append(x.toInt))

    val solarAzimuthNode = xml \ "SolarAzimuth"
    if(solarAzimuthNode.isEmpty)
      throw new RuntimeException("No SolarAzimuth Node exists!")
    solarAzimuth = solarAzimuthNode.text.toFloat

    val solarZenithNode = xml \ "SolarZenith"
    if(solarZenithNode.isEmpty)
      throw new RuntimeException("No SolarZenith Node exists!")
    solarZenith = solarZenithNode.text.toFloat

    platform = (xml \ "SatelliteID").text
    sensor = (xml \ "SensorID").text
    produceTime = (xml \ "ProduceTime").text
    widthInPixels = (xml \ "WidthInPixels").text
    heightInPixels = (xml \ "HeightInPixels").text
    resolution = (xml \ "ImageGSD").text
    topLeftLatitude = (xml \ "TopLeftLatitude").text
    topLeftLongitude = (xml \ "TopLeftLongitude").text
    topRightLatitude = (xml \ "TopRightLatitude").text
    topRightLongitude = (xml \ "TopRightLongitude").text
    bottomRightLatitude = (xml \ "BottomRightLatitude").text
    bottomRightLongitude = (xml \ "BottomRightLongitude").text
    bottomLeftLatitude = (xml \ "BottomLeftLatitude").text
    bottomLeftLongitude = (xml \ "BottomLeftLongitude").text

    val year = receiveData(0)
    getGainOffset(platform, sensor, year)
    getEsun(platform, sensor)
  }


}


