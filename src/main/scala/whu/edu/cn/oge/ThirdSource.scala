package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable

class ThirdSource {
  // 参数元信息
  class Param(val name: String, val dataType: String, val optional: Boolean, val defaultValue: String) {
    // 参数转换
    def inputConverter(): Unit = {}

    def outputConverter(): Unit = {}
  }

  class CoverageParam(name: String, dataType: String, optional: Boolean, defaultValue: String)
    extends Param(name, dataType, optional, defaultValue) {
    def inputConverter(input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = null

    def outputConverter(input: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = null
  }

  var inputParams: mutable.ListBuffer[Param] = mutable.ListBuffer.empty[Param]

  def execute(functionName:String,input:Any*) = {
    // 查数据库,检查算子是否存在,将输入输出数据元信息存本地
    // 检查算子输入是否合法
    // （参数转换后），通过docker swarm调用算子
    // 检查算子输出是否合法
    // （输出参数转换后）返回结果
  }


}
