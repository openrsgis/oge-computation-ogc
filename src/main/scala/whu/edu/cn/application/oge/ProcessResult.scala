package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{FastMapHistogram, Tile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.application.oge.Trigger.argOrNot
import whu.edu.cn.application.oge.WebAPI.tiff2RDD
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.ogc.entity.process.{CoverageMediaType, FeatureMediaType}
import whu.edu.cn.ogc.ogcAPIUtil.OgcAPI

object ProcessResult {
  def getCoverage(implicit sc: SparkContext, processUrl:String, outputName:String, processResult:JSONObject): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) ={
    val ogcAPI = new OgcAPI;
    val process =  ogcAPI.getProcess(processUrl);
    // 注意 这里应该查询output的输出类型是什么
    val coverageTypeList = ogcAPI.getTypeOfCoverageOutput(process, outputName);
    if(coverageTypeList.size()!=0){
      val coverageType = CoverageMediaType.sort(coverageTypeList)
      var outputValue:String = ""
      // 如果包含了key的话进行判断
      if(processResult.containsKey(outputName)){
        val value = processResult.get(outputName)
        value match {
          case _: String =>
            outputValue = processResult.getString(outputName)
          case _: JSONObject =>
            val valueObj:JSONObject = processResult.getJSONObject(outputName)
            if(valueObj.containsKey("href")){
              outputValue = valueObj.getString("href")
            }else if(valueObj.containsKey("value")){
              outputValue = valueObj.getString("value")
            }
        }
      }else{
        //如果不包含这个key,一般只有输出是一个的时候出现
        if(processResult.containsKey("href")){
          outputValue = processResult.getString("href")
        }else if(processResult.containsKey("value")){
          outputValue = processResult.getString("value")
        }
      }
      tiff2RDD(sc, outputValue, coverageType)
    }
    null
  }
  def getFeatureCollection( processUrl:String, outputName:String, processResult:JSONObject): JSONObject ={
    val ogcAPI = new OgcAPI;
    val process =  ogcAPI.getProcess(processUrl);
    // 注意 这里应该查询output的输出类型是什么
    val featureTypeList = ogcAPI.getTypeOfFeatureOutput(process, outputName);
    var geojson: JSONObject = new JSONObject()
    if(featureTypeList.size()!=0){
      val featureType = FeatureMediaType.sort(featureTypeList)
      val featureTypeEnum = FeatureMediaType.valueOf(featureType)
      if(processResult.containsKey(outputName)){
        val value = processResult.get(outputName)
        featureTypeEnum match {
          case FeatureMediaType.GML =>
            value match {
              case _: String =>
              // TODO GML 转Geojson
              case _: JSONObject =>
                val GMLStr = processResult.getJSONObject(outputName).getString("href")
              // TODO GML 转Geojson
            }
          case  FeatureMediaType.GEOJSON =>
            if(processResult.getJSONObject(outputName).containsKey("value")){
              geojson = processResult.getJSONObject(outputName).getJSONObject("value")
            }else{
              geojson = processResult.getJSONObject(outputName)
            }
        }
      }else {
        if(featureType.equals(FeatureMediaType.GEOJSON.getType)){
          if(processResult.containsKey("value")){
            geojson = processResult.getJSONObject("value")
          }else{
            geojson = processResult
          }
        }
      }
    }
    geojson
  }
}
