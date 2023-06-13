package whu.edu.cn.geocube.core.entity

import geotrellis.layer.Bounds
import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import whu.edu.cn.geocube.core.vector.grid.GridConf

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

case class TabularGridLayerMetadata[K] (_gridConf: GridConf,
                                       _extent: Extent = null,
                                       _bounds: Bounds[K] = null,
                                       _crs: CRS = CRS.fromEpsgCode(4326),
                                       _productName: String = "",
                                        _productNames: ArrayBuffer[String] = new ArrayBuffer[String]()){
  @BeanProperty
  var gridConf = _gridConf
  @BeanProperty
  var extent = _extent
  @BeanProperty
  var bounds = _bounds
  @BeanProperty
  var crs = _crs
  @BeanProperty
  var productName = _productName
  @BeanProperty
  var productNames: ArrayBuffer[String] = _productNames

}
