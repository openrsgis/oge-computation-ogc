package whu.edu.cn.geocube.core.entity

import geotrellis.layer.TileLayerMetadata

import scala.collection.mutable.ArrayBuffer

/**
 * Extend Geotrellis TileLayerMetadata class to
 * contain product and measurement dimension info.
 *
 */
case class RasterTileLayerMetadata[K](_tileLayerMetadata: TileLayerMetadata[K],
                                      _productName: String = "",
                                      _productNames: ArrayBuffer[String] = new ArrayBuffer[String](),
                                      _measurementNames: ArrayBuffer[String] = new ArrayBuffer[String] ) {
  var tileLayerMetadata: TileLayerMetadata[K] = _tileLayerMetadata
  var productName: String = _productName
  var productNames: ArrayBuffer[String] = _productNames
  var measurementNames: ArrayBuffer[String] = _measurementNames

  def getTileLayerMetadata: TileLayerMetadata[K] = this.tileLayerMetadata

  def getProductName: String = this.productName

  def getProductNames: ArrayBuffer[String] = this.measurementNames

  def getMeasurementNames: ArrayBuffer[String] = this.productNames

  def setTileLayerMetadata(tileLayerMetadata: TileLayerMetadata[K]) =
    this.tileLayerMetadata = tileLayerMetadata

  def setProductName(rasterProductName: String) =
    this.productName = rasterProductName

  def setProductNames(rasterProductNames: ArrayBuffer[String]) =
    this.productNames = rasterProductNames

  def setProductNames(rasterProductNames: Array[String]) =
    this.productNames = rasterProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]

  def setMeasurementNames(measurementNames: ArrayBuffer[String]) =
    this.measurementNames = measurementNames

  def setMeasurementNames(measurementNames: Array[String]) =
    this.measurementNames = measurementNames.toBuffer.asInstanceOf[ArrayBuffer[String]]

}
