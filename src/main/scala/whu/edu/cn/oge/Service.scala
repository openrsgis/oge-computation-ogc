package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{CoverageCollectionMetadata, SpaceTimeBandKey}

object Service {

  def getCoverageCollection(productName: String, dateTime: String = null, extent: String = null): CoverageCollectionMetadata = {
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

}
