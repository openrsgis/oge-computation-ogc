package whu.edu.cn.application.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.application.oge.HttpRequest.writeTIFF
import whu.edu.cn.application.oge.utils.TiffUtil.tiff2RDD
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.ogc.entity.coverage.Coverage
import whu.edu.cn.ogc.entity.process.{CoverageMediaType, Link}
import whu.edu.cn.ogc.ogcAPIUtil.OgcAPI

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, List => JList}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
object Service {

  /**
   * 这里只请求了f=tif的影像
   * @param sc sc Spark Context
   * @param baseUrl OGC API - Coverages 的url
   * @param productId 产品Id
   * @param coverageID coverageId
   * @param subset Coverage的子集subset
   * @param properties 属性名称
   * @param geom 空间查询范围 这里是视口范围
   * @param mapLevel 前端层级
   * @return 返回的瓦片RDD
   */
  def getCoverage(implicit sc: SparkContext, baseUrl: String, productId: String = null, coverageID: String = null,
                  subset: String = null, properties: String = null, geom: String = null,
                  mapLevel:Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])={
    // TODO存储路径
    val ogcAPI = new OgcAPI()
    val coverage = ogcAPI.getCoverage(baseUrl, coverageID)
    var time = coverage.getTimeStamp
    if(time == null){
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // 定义要输出的日期时间格式
      val now = new Date() // 获取当前时间
      val formattedDate = dateFormat.format(now) // 将当前时间格式化为指定格式的字符串
      time = formattedDate
    }
    val linkList:JList[Link] = coverage.getCoverageLinks
    for (link <- linkList.asScala) {
      if(link.getHref.contains("f=tif")){
        val rdd = tiff2RDD(sc, link.getHref, CoverageMediaType.GEOTIFF.getType, geom, mapLevel, productId, time)
        return rdd
      }
    }
    null
  }

  /**
   * 请求CoverageCollection
   * @param sc sc Spark Context
   * @param baseUrl  OGC API - Coverages 基础Url http://125.220.153.26:8080/
   * @param productID 产品Id
   * @param bbox bounding box 空间范围
   * @param datetime 时间范围
   * @param bboxCrs bounding box的坐标系
   * @return 返回(RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
   */
  def getCoverageCollection(implicit sc: SparkContext, baseUrl:String, productID:String=null, bbox:String=null, datetime:String=null,
                            bboxCrs:String=null):(RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])={
    val ogcAPI = new OgcAPI()
    var bboxList: JList[java.lang.Float] = new util.ArrayList[java.lang.Float]()
    bbox match {
      case null => bboxList = null
      case _ => {
        val bboxElements = bbox.replaceAll("\\[|\\]|\\s", "").split(",")
        for (elem <- bboxElements) {
          bboxList.add(elem.toFloat)
        }
      }
    }
    var datetimeList: JList[String] = new util.ArrayList[String]()
    datetime match {
      case null => datetimeList = null
      case _ => {
        val datetimeElements = datetime.replaceAll("\\[|\\]|\\s", "").split(",")
        for (elem <- datetimeElements) {
          datetimeList.add(elem)
        }
      }
    }
    val coverageCollection = ogcAPI.getCoverageCollection(baseUrl, productID, bboxList, bboxCrs, datetimeList)
    val coverageList:JList[Coverage] = coverageCollection.getCoverageCollection
    //TODO 如果是不同张影像 如何转换为RDD？
    null
  }
}
