package whu.edu.cn.geocube.application.conjoint

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.Tile
import geotrellis.spark.TileLayerRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.geocube.core.cube.vector.GeoObjectRDD
import whu.edu.cn.geocube.application.timeseries.WaterChangeDetection._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.cube.tabular.{TabularRecord, TabularRecordRDD}
import whu.edu.cn.geocube.core.cube.vector.{GeoObject, GeoObjectRDD}

import scala.collection.mutable.ArrayBuffer

/**
 * This class provides overlap function for raster and vector data
 */
object Overlap {

  def overlappedTabularRecords(tileLayerRdd: TileLayerRDD[SpatialKey],
                               gridLayerTabularRecordRdd: RDD[(SpatialKey, Iterable[TabularRecord])]): TabularRecordRDD = {
    val ld = tileLayerRdd.metadata.layout

    val joinedRdd:RDD[(SpatialKey, (Tile, Iterable[TabularRecord]))] =
      tileLayerRdd.join(gridLayerTabularRecordRdd)

    val overlappedTabularRecordRdd:RDD[TabularRecord] = joinedRdd.flatMap(x=>{
      val affectedTabularRecords: ArrayBuffer[TabularRecord] = new ArrayBuffer[TabularRecord]()
      val spatialKey = x._1
      val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
      val tile = x._2._1
      val tabularRecordList = x._2._2

      val tileCols = ld.tileCols
      val tileRows = ld.tileRows
      val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
      val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
      val tabularIterator = tabularRecordList.iterator

      while (tabularIterator.hasNext) {
        val tabularRecord: TabularRecord = tabularIterator.next()
        val attributs = tabularRecord.getAttributes
        val x = attributs.get("longitude").get.toDouble
        val y = attributs.get("latitude").get.toDouble
        val pixelX: Int = math.floor((x - tileExtent.xmin) / pixelWidth).toInt
        val pixelY: Int = tileRows - 1 - math.floor((y - tileExtent.ymin) / pixelHeight).toInt

        /*if(tile.getDouble(pixelX, pixelY) == 255.0)
          affectedGeoObjects.append(geoObject)*/
        var flag = true
        (-2 until 3).foreach{i =>
          (-2 until 3).foreach{j =>
            if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
              if (tile.getDouble(pixelX + i, pixelY + j) == 255.0 && flag) {
                affectedTabularRecords.append(tabularRecord)
                flag = false
              }
            }
          }
        }
      }
      affectedTabularRecords
    })
    new TabularRecordRDD(overlappedTabularRecordRdd)
  }

  /**
   * Find vectors that overlap with the raster.
   *
   * @param tileLayerRdd Raster tiles RDD
   * @param gridLayerGeoObjectRdd GeoObjects RDD
   *
   * @return A GeoObjectRDD that overlap with input raster tiles.
   */
  def overlappedGeoObjects(tileLayerRdd: TileLayerRDD[SpatialKey],
                           gridLayerGeoObjectRdd: RDD[(SpatialKey, Iterable[GeoObject])]): GeoObjectRDD = {
    val ld = tileLayerRdd.metadata.layout

    val joinedRdd:RDD[(SpatialKey, (Tile, Iterable[GeoObject]))] =
      tileLayerRdd.join(gridLayerGeoObjectRdd)

    val overlappedGeoObjectRdd:RDD[GeoObject] = joinedRdd.flatMap(x=>{
      val affectedGeoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
      val spatialKey = x._1
      val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
      val tile = x._2._1
      val geoObjectList = x._2._2

      val tileCols = ld.tileCols
      val tileRows = ld.tileRows
      val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
      val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
      val geomIterator = geoObjectList.iterator

      while (geomIterator.hasNext) {
        val geoObject: GeoObject = geomIterator.next()
        val feature: SimpleFeature = geoObject.feature
        val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val pixelX: Int = math.floor((geometry.getCoordinate.x - tileExtent.xmin) / pixelWidth).toInt
        val pixelY: Int = tileRows - 1 - math.floor((geometry.getCoordinate.y - tileExtent.ymin) / pixelHeight).toInt
        /*if(tile.getDouble(pixelX, pixelY) == 255.0)
          affectedGeoObjects.append(geoObject)*/
        var flag = true
        (-2 until 3).foreach{i =>
          (-2 until 3).foreach{j =>
            if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
              if (tile.getDouble(pixelX + i, pixelY + j) == 255.0 && flag) {
                affectedGeoObjects.append(geoObject)
                flag = false
              }
            }
          }
        }
      }
      affectedGeoObjects

    })
    new GeoObjectRDD(overlappedGeoObjectRdd)
  }


  def overlappedGeoObjects(rasterRdd: RasterRDD,
                           gridLayerGeoObjectRdd: RDD[(SpatialKey, Iterable[GeoObject])]): GeoObjectRDD = {
    val ld = rasterRdd.meta.tileLayerMetadata.layout

    val joinedRdd:RDD[(SpatialKey, (Tile, Iterable[GeoObject]))] = {
      rasterRdd.rddPrev.map(t=>(t._1.spaceTimeKey.spatialKey,t._2)).join(gridLayerGeoObjectRdd)
    }

    val overlappedGeoObjectRdd:RDD[GeoObject] = joinedRdd.flatMap(x=>{
      val affectedGeoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
      val spatialKey = x._1
      val tileExtent = ld.mapTransform.keyToExtent(spatialKey)
      val tile = x._2._1
      val geoObjectList = x._2._2

      val tileCols = ld.tileCols
      val tileRows = ld.tileRows
      val pixelWidth: Double = ld.extent.width / ld.layoutCols / tileCols
      val pixelHeight: Double = ld.extent.height / ld.layoutRows / tileRows
      val geomIterator = geoObjectList.iterator

      while (geomIterator.hasNext) {
        val geoObject: GeoObject = geomIterator.next()
        val feature: SimpleFeature = geoObject.feature
        val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val pixelX: Int = math.floor((geometry.getCoordinate.x - tileExtent.xmin) / pixelWidth).toInt
        val pixelY: Int = tileRows - 1 - math.floor((geometry.getCoordinate.y - tileExtent.ymin) / pixelHeight).toInt
        /*if(tile.getDouble(pixelX, pixelY) == 255.0)
          affectedGeoObjects.append(geoObject)*/
        var flag = true
        (-2 until 3).foreach{i =>
          (-2 until 3).foreach{j =>
            if(!((pixelX + i) < 0 || (pixelX + i) >= tileCols || (pixelY + j) < 0 || (pixelY + j) >= tileRows)){
              if (tile.getDouble(pixelX + i, pixelY + j) == 255.0 && flag) {
                affectedGeoObjects.append(geoObject)
                flag = false
              }
            }
          }
        }
      }
      affectedGeoObjects
    })
    new GeoObjectRDD(overlappedGeoObjectRdd)
  }
}

