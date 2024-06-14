package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.entity.cube.CubeTileKey
import whu.edu.cn.entity.{CoverageCollectionMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.entity.RasterTileLayerMetadata
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostSender

object Service {

  def getCoverageCollection(productName: String, dateTime: String = null, extent: String = null, cloudCoverMin: Float = 0, cloudCoverMax: Float = 100): CoverageCollectionMetadata = {
    val coverageCollectionMetadata: CoverageCollectionMetadata = new CoverageCollectionMetadata()
    coverageCollectionMetadata.setProductName(productName)
    if (extent != null) {
      coverageCollectionMetadata.setExtent(extent)
    }
    if (dateTime != null) {
      val timeArray: Array[String] = dateTime.replace("[", "").replace("]", "").split(",")
      coverageCollectionMetadata.setStartTime(timeArray.head)
      coverageCollectionMetadata.setEndTime(timeArray(1))
    }
    coverageCollectionMetadata.setCloudCoverMin(cloudCoverMin)
    coverageCollectionMetadata.setCloudCoverMax(cloudCoverMax)
    coverageCollectionMetadata
  }

  // TODO lrx: 这里也搞一个惰性函数
  def getCoverage(implicit sc: SparkContext, coverageId: String,productKey: String="3", level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val time1: Long = System.currentTimeMillis()
    val coverage = Coverage.load(sc, coverageId, productKey,level)
    println("Loading data Time: "+ (System.currentTimeMillis()-time1))
    coverage
  }

  def getFeature(implicit sc: SparkContext, featureId: String,dataTime:String= null,crs:String="EPSG:4326")={
    Feature.load(sc,featureId,dataTime = dataTime,crs)
  }


  def getCube(implicit sc: SparkContext, CubeId: String, ProductString: String, BandString: String, TimeString: String, ExtentString: String, Tms: String, Resolution: String = "100"): Array[(RDD[(CubeTileKey, Tile)], TileLayerMetadata[SpatialKey])] = {
    CubeNew.loadCubeByImage(sc, cubeId = CubeId, productString = ProductString, bandString = BandString, timeString = TimeString, extentString = ExtentString, tms = Tms, resolution = Resolution.toDouble)
  }

  def print(res:String,name:String,valueType:String):Unit={
    val j = new JSONObject()
    j.put("name",name)
    j.put("value",res)
    j.put("type",valueType)
    Trigger.outputInformationList.append(j)


    val jsonObject: JSONObject = new JSONObject

    jsonObject.put("info",Trigger.outputInformationList.toArray)

    val outJsonObject: JSONObject = new JSONObject
    outJsonObject.put("workID", Trigger.dagId)
    outJsonObject.put("json", jsonObject)
    println(outJsonObject)
    PostSender.shelvePost("info",Trigger.outputInformationList.toArray)
  }


}
