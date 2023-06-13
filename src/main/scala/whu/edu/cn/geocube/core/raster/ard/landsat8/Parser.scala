package whu.edu.cn.geocube.core.raster.ard.landsat8

import java.io.FileInputStream

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Extract metadata in Gaofen-1 xml file.
 *
 * @param _mltPath
 * @param _nSubdataset band num
 */
case class Parser(_mltPath: String, _nSubdataset: Int) {
  val mltPath: String = _mltPath
  val nSubdataset: Int = _nSubdataset
  val multParams = new ArrayBuffer[Float]()
  val addParams = new ArrayBuffer[Float]()
  val maxDN = new ArrayBuffer[Float]()
  val minDN = new ArrayBuffer[Float]()
  val date = new ArrayBuffer[Int]()
  var sunAzimuth = 0.0f
  var sunElevation = 0.0f
  var earthSunDistance = 0.0f
  val esun = new ArrayBuffer[Float]()
  var satellite = ""
  var sceneCenterTime = ""
  var defaultLines = ""
  var defaultSamples = ""
  var defaultULX = ""
  var defaultULY = ""
  var geomULLati = ""
  var geomULLong = ""
  var geomURLati = ""
  var geomURLong = ""
  var geomLRLati = ""
  var geomLRLong = ""
  var geomLLLati = ""
  var geomLLLong = ""

  /**
   * Collect Radiometric Rescaling Parameters.
   *
   * @return Gains and bias
   */
  def getRadiometricRescalingParams(): (Array[Float], Array[Float]) = {
    Source.fromFile(mltPath).getLines.foreach{ line =>
      if (line.contains("RADIANCE_MULT_BAND"))
        multParams.append(line.split("=")(1).toFloat)
      if (line.contains("RADIANCE_ADD_BAND"))
        addParams.append(line.split("=")(1).toFloat)
    }
    (multParams.toArray, addParams.toArray)
  }

  /**
   * Collect min and max DN value.
   *
   * @return Min and max DN value
   */
  def getQuantizeCalibratedPixel(): (Array[Float], Array[Float]) = {
    Source.fromFile(mltPath).getLines.foreach{ line =>
      if (line.contains("QUANTIZE_CAL_MAX_BAND"))
        maxDN.append(line.split("=")(1).toFloat)
      if (line.contains("QUANTIZE_CAL_MIN_BAND"))
        minDN.append(line.split("=")(1).toFloat)
    }
    (minDN.toArray, maxDN.toArray)
  }

  /**
   * Get ESUN parameters with input platform.
   *
   * @param platform
   * @return Esun parameters
   */
  def getEsun(platform: String):Array[Float] = {
    val objectMapper=new ObjectMapper()
    val node = objectMapper.readTree(new FileInputStream("/home/geocube/data/conf/Landsat_Esun.json"))
    //val node = objectMapper.readTree(new FileInputStream("conf/Landsat_Esun.json"))
    if(node != null && node.has(platform)) {
      val platformNode = node.get(platform)
      if(platformNode.has("value")){
        val sensorNode = platformNode.get("value")
        val esunParams = sensorNode.elements()
        while(esunParams.hasNext) esun.append(esunParams.next().toString.toFloat)
      }
    }
    esun.toArray
  }

  /**
   * Collect DATE_ACQUIRED, SUN_ELEVATION, EARTH_SUN_DISTANCE and ESUN.
   * @param platform
   */
  def collectMetadata(platform: String): Unit = {
    Source.fromFile(mltPath).getLines.foreach{ line =>
      if (line.contains("DATE_ACQUIRED")) {
        val str = line.split("=")(1).trim.split("-")
        str.foreach(x => date.append(x.toInt))
      }
      if (line.contains("SUN_AZIMUTH"))
        sunAzimuth = line.split("=")(1).toFloat
      if(line.contains("SUN_ELEVATION"))
        sunElevation = line.split("=")(1).toFloat
      if(line.contains("EARTH_SUN_DISTANCE"))
        earthSunDistance = line.split("=")(1).toFloat
      if(line.contains("SPACECRAFT_ID"))
        satellite = line.split("=")(1).trim.replace("\"", "")
      if(line.contains("SCENE_CENTER_TIME"))
        sceneCenterTime = line.split("=")(1).trim.replace("\"", "")
      if(line.contains("REFLECTIVE_LINES"))
        defaultLines = line.split("=")(1).trim
      if(line.contains("REFLECTIVE_SAMPLES"))
        defaultSamples = line.split("=")(1).trim
      if(line.contains("CORNER_UL_PROJECTION_X_PRODUCT"))
        defaultULX = line.split("=")(1).trim
      if(line.contains("CORNER_UL_PROJECTION_Y_PRODUCT"))
        defaultULY = line.split("=")(1).trim
      if(line.contains("CORNER_UL_LAT_PRODUCT"))
        geomULLati = line.split("=")(1).trim
      if(line.contains("CORNER_UL_LON_PRODUCT"))
        geomULLong = line.split("=")(1).trim
      if(line.contains("CORNER_UR_LAT_PRODUCT"))
        geomURLati = line.split("=")(1).trim
      if(line.contains("CORNER_UR_LON_PRODUCT"))
        geomURLong = line.split("=")(1).trim
      if(line.contains("CORNER_LR_LAT_PRODUCT"))
        geomLRLati = line.split("=")(1).trim
      if(line.contains("CORNER_LR_LON_PRODUCT"))
        geomLRLong = line.split("=")(1).trim
      if(line.contains("CORNER_LL_LAT_PRODUCT"))
        geomLLLati = line.split("=")(1).trim
      if(line.contains("CORNER_LL_LAT_PRODUCT"))
        geomLLLong = line.split("=")(1).trim
    }
    getEsun(platform)
  }


}

