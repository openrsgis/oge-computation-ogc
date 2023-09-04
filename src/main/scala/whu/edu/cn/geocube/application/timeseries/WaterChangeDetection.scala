package whu.edu.cn.geocube.application.timeseries

import geotrellis.layer._
import geotrellis.raster._
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, RasterTileLayerMetadata}

import java.text.SimpleDateFormat

/**
 * Detect water change between two instants
 */
object WaterChangeDetection {
  /**
   * Detect water change between two instants.
   *
   * Using RasterRDD as input, which contains ndwi tiles of two instants.
   *
   * @param ndwiRasterRdd a RasterRdd of ndwi tiles
   * @return a rdd of water change detection result
   */
  def waterChangeDetectionUsingNDWIProduct(ndwiRasterRdd: RasterRDD, certainTimes: Array[String]): RasterRDD = {
    val ndwiTileLayerRddWithMeta: (RDD[(SpaceTimeKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
      (ndwiRasterRdd.map(x => (x._1.spaceTimeKey, x._2)), ndwiRasterRdd.rasterTileLayerMetadata)

    val ndwiRdd: RDD[(SpaceTimeKey, Tile)] = ndwiTileLayerRddWithMeta._1
    val ndwiMeta: RasterTileLayerMetadata[SpaceTimeKey] = ndwiTileLayerRddWithMeta._2

    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
    //and generate change tile between two instants
    val changedRdd: RDD[(SpaceTimeBandKey, Tile)] = ndwiRdd
      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
      .map { x =>
        val list = x._2.toList
        var instant1: Long = -1
        var instant2: Long = -1
        var listI: Int = -1
        var listJ: Int = -1
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        for (i <- list.indices) {
          if (list(i)._1.instant == fm.parse(certainTimes(0)).getTime) {
            instant1 = list(i)._1.instant
            listI = i
          }
          if (list(i)._1.instant == fm.parse(certainTimes(1)).getTime) {
            instant2 = list(i)._1.instant
            listJ = i
          }
        }
        if (instant1 == -1 || instant2 == -1) {
          null
        }
        else {
          val previous: (SpaceTimeKey, Tile) = list(listI)
          val rear: (SpaceTimeKey, Tile) = list(listJ)
          val changedTile = rear._2 - previous._2
          (SpaceTimeBandKey(new SpaceTimeKey(x._1.col, x._1.row, System.currentTimeMillis()), "WaterChangeDetection"), changedTile)
        }
      }.filter(t => t != null)
    val srcLayout = ndwiMeta.tileLayerMetadata.layout
    val srcExtent = ndwiMeta.tileLayerMetadata.extent
    val srcCrs = ndwiMeta.tileLayerMetadata.crs
    val srcBounds = ndwiMeta.tileLayerMetadata.bounds
    val changedMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)

    val changedMeta: RasterTileLayerMetadata[SpaceTimeKey] = ndwiMeta
    changedMeta.setTileLayerMetadata(changedMetaData)

    new RasterRDD(changedRdd, changedMeta)
  }

//  def waterChangeDetectionUsingNDWIProduct(ndwiRasterRdd: RasterRDD): RasterRDD = {
//    val ndwiTileLayerRddWithMeta: (RDD[(SpaceTimeKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
//      (ndwiRasterRdd.map(x => (x._1.spaceTimeKey, x._2)), ndwiRasterRdd.rasterTileLayerMetadata)
//
//    val ndwiRdd: RDD[(SpaceTimeKey, Tile)] = ndwiTileLayerRddWithMeta._1
//    val ndwiMeta: RasterTileLayerMetadata[SpaceTimeKey] = ndwiTileLayerRddWithMeta._2
//
//    //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])],
//    //and generate change tile between two instants
//    val changedRdd: RDD[(SpaceTimeBandKey, Tile)] = ndwiRdd
//      .groupBy(_._1.spatialKey) //group by SpatialKey to get a time-series RDD, i.e. RDD[(SpatialKey, Iterable[(SpaceTimeKey,Tile)])]
//      .map { x =>
//        val list = x._2.toList
//        if (list.length != 2) {
//          throw new RuntimeException("Band num is not 2!")
//        }
//        val instant1 = list(0)._1.instant
//        val instant2 = list(1)._1.instant
//        if (instant1 < instant2) {
//          val previous: (SpaceTimeKey, Tile) = list(0)
//          val rear: (SpaceTimeKey, Tile) = list(1)
//          val changedTile = rear._2 - previous._2
//          (SpaceTimeBandKey(new SpaceTimeKey(x._1.col, x._1.row, System.currentTimeMillis()), "WaterChangeDetection"), changedTile)
//        } else {
//          val previous: (SpaceTimeKey, Tile) = list(1)
//          val rear: (SpaceTimeKey, Tile) = list(0)
//          val changedTile = rear._2 - previous._2
//          (SpaceTimeBandKey(new SpaceTimeKey(x._1.col, x._1.row, System.currentTimeMillis()), "WaterChangeDetection"), changedTile)
//        }
//      }
//
//    val srcLayout = ndwiMeta.tileLayerMetadata.layout
//    val srcExtent = ndwiMeta.tileLayerMetadata.extent
//    val srcCrs = ndwiMeta.tileLayerMetadata.crs
//    val srcBounds = ndwiMeta.tileLayerMetadata.bounds
//    val changedMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)
//
//    val changedMeta: RasterTileLayerMetadata[SpaceTimeKey] = ndwiMeta
//    changedMeta.setTileLayerMetadata(changedMetaData)
//
//    new RasterRDD(changedRdd, changedMeta)
//  }

}
