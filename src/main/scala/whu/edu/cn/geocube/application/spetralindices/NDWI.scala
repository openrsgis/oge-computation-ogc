package whu.edu.cn.geocube.application.spetralindices

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, RasterTileLayerMetadata}
import whu.edu.cn.geocube.core.cube.raster.RasterRDD

/**
 * Generate NDWI product.
 *
 * NDWI = (Green – NIR) / (Green + NIR).
 */
object NDWI {
  /**
   * This NDWI function is used in OGE.
   *
   * Use RasterRDD as input.
   *
   * @param rasterRdd a rdd of queried tiles
   * @param threshold NDWI threshold
   * @return results info containing thematic ndwi product path, time, product type and water body area.
   */
  def ndwi(rasterRdd: RasterRDD): RasterRDD = {
    val rasterTileRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterRdd, rasterRdd.rasterTileLayerMetadata)
    val rasterTileRddExceptBandRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (rasterTileRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), rasterTileRddWithMeta._2)
    val rasterTileRddExceptBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = rasterTileRddExceptBandRddWithMeta._1
    val rasterTileRddExceptBandMeta = rasterTileRddExceptBandRddWithMeta._2

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndwi tile.
    val ndwiRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTileRddExceptBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
        if (greenBandTile.isEmpty || nirBandTile.isEmpty)
          throw new RuntimeException("There is no Green band or Nir band")
        val ndwiTile: Tile = ndwiTileComputation(greenBandTile.get, nirBandTile.get)
        (SpaceTimeBandKey(spaceTimeKey, "NDWI"), ndwiTile)
      }

    val srcLayout = rasterTileRddExceptBandMeta.tileLayerMetadata.layout
    val srcExtent = rasterTileRddExceptBandMeta.tileLayerMetadata.extent
    val srcCrs = rasterTileRddExceptBandMeta.tileLayerMetadata.crs
    val srcBounds = rasterTileRddExceptBandMeta.tileLayerMetadata.bounds
    val ndwiMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)

    val ndwiMeta: RasterTileLayerMetadata[SpaceTimeKey] = rasterTileRddExceptBandMeta
    ndwiMeta.setTileLayerMetadata(ndwiMetaData)

    new RasterRDD(ndwiRdd, ndwiMeta)
  }

  def ndwiRadar(rasterRdd: RasterRDD): RasterRDD = {
//    val rasterTileRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterRdd, rasterRdd.rasterTileLayerMetadata)
//    val rasterTileRddExceptBandRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
//      (rasterTileRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), rasterTileRddWithMeta._2)
//    val rasterTileRddExceptBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = rasterTileRddExceptBandRddWithMeta._1
//    val rasterTileRddExceptBandMeta = rasterTileRddExceptBandRddWithMeta._2
//
//    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
//    //and generate ndwi tile.
//    val ndwiRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTileRddExceptBandRdd
//      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
//      .map { x => //generate ndwi tile
//        val spaceTimeKey = x._1
//        val bandTileMap = x._2.toMap
//        val (greenBandTile, nirBandTile) = (bandTileMap.get("Green"), bandTileMap.get("Near-Infrared"))
//        if (greenBandTile.isEmpty || nirBandTile.isEmpty)
//          throw new RuntimeException("There is no Green band or Nir band")
//        val ndwiTile: Tile = ndwiTileComputation(greenBandTile.get, nirBandTile.get)
//        (SpaceTimeBandKey(spaceTimeKey, "NDWI"), ndwiTile)
//      }
//
//    val srcLayout = rasterTileRddExceptBandMeta.tileLayerMetadata.layout
//    val srcExtent = rasterTileRddExceptBandMeta.tileLayerMetadata.extent
//    val srcCrs = rasterTileRddExceptBandMeta.tileLayerMetadata.crs
//    val srcBounds = rasterTileRddExceptBandMeta.tileLayerMetadata.bounds
//    val ndwiMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)
//
//    val ndwiMeta: RasterTileLayerMetadata[SpaceTimeKey] = rasterTileRddExceptBandMeta
//    ndwiMeta.setTileLayerMetadata(ndwiMetaData)
//
//    new RasterRDD(ndwiRdd, ndwiMeta)
    rasterRdd
  }

  /**
   * Generate a ndwi tile of ShortConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param nirBandTile   Nir band Tile
   * @param threshold
   * @return a ndwi tile of ShortConstantNoDataCellType
   */
  def ndwiShortTileComputation(greenBandTile: Tile, nirBandTile: Tile, threshold: Double): Tile = {
    //calculate ndwi tile
    val ndwiTile = Divide(
      Subtract(greenBandTile, nirBandTile),
      Add(greenBandTile, nirBandTile))

    ndwiTile.mapDouble(pixel => {
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    }).convert(ShortConstantNoDataCellType) //convert ndwi tile with constant Float.NaN to Short.NaN
  }

  /**
   * Generate a ndwi tile of DoubleConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param nirBandTile   Nir band Tile
   * @param threshold
   * @return a ndwi tile of DoubleConstantNoDataCellType
   */
  def ndwiTileComputation(greenBandTile: Tile, nirBandTile: Tile): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleGreenBandTile = DoubleArrayTile(greenBandTile.toArrayDouble(), greenBandTile.cols, greenBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate ndwi tile
    val ndwiTile = Divide(
      Subtract(doubleGreenBandTile, doubleNirBandTile),
      Add(doubleGreenBandTile, doubleNirBandTile))
    ndwiTile
  }
}

