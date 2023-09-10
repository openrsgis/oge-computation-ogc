package whu.edu.cn.geocube.core.entity

import geotrellis.raster.Tile

import scala.beans.BeanProperty

/**
 * A wrap class for raster tile.
 *
 */
case class RasterTile(_id: String = ""){
  @BeanProperty
  var id: String = _id
  @BeanProperty
  var rowNum: String = ""
  @BeanProperty
  var colNum: String = ""
  @BeanProperty
  var leftBottomLat: String = ""
  @BeanProperty
  var leftBottomLong: String = ""
  @BeanProperty
  var rightUpperLat: String = ""
  @BeanProperty
  var rightUpperLong: String = ""
  @BeanProperty
  var CRS: String = ""
  @BeanProperty
  var productMeta: GcProduct = GcProduct()
  /*@BeanProperty
  var extentMeta: GcExtent = GcExtent()
  @BeanProperty
  var tileQualityMeta: GcTileQuality = GcTileQuality()*/
  @BeanProperty
  var measurementMeta: GcMeasurement = GcMeasurement()
  @BeanProperty
  var data:Tile = null

}
