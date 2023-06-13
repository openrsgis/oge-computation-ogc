package whu.edu.cn.util

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.Tile
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{RawTile, SpaceTimeBandKey}
import whu.edu.cn.util.CoverageUtil.makeCoverageRDD

import scala.collection.mutable

object CoverageCollectionUtil {
  def checkMapping(coverage: String, algorithm: (String, String, mutable.Map[String, String])): (String, String, mutable.Map[String, String]) = {
    for (tuple <- algorithm._3) {
      if (tuple._2.contains("MAPPING")) {
        algorithm._3.remove(tuple._1)
        algorithm._3.put(tuple._1, coverage)
      }
    }
    algorithm
  }

  // TODO: lrx: 函数的RDD大写，变量的Rdd小写，为了开源全局改名，提升代码质量
  // 不能用嵌套RDD，因为makeCoverageRDD需要MinIOClient，这个类不能序列化
  def makeCoverageCollectionRDD(rawTileRdd: Map[String, RDD[RawTile]]): Map[String, (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])] = {
    rawTileRdd.map(t=>{
        (t._1, makeCoverageRDD(t._2))
    })
  }

}
