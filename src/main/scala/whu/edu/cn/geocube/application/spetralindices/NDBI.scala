package whu.edu.cn.geocube.application.spetralindices

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, RasterTileLayerMetadata}
import org.apache.spark.rdd.RDD

/**
 * Generate NDBI product.
 *
 * NDBI = (SWIR1 – NIR) / (SWIR1 + NIR)
 */
object NDBI {
  /**
   * This NDBI function is used in Jupyter Notebook, e.g. cloud free ndbi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param rasterRdd a rdd of queried tiles
   * @param threshold NDBI threshold
   * @return results Info containing thematic ndbi product path, time and product type
   */
  def ndbi(rasterRdd: RasterRDD): RasterRDD = {
    val rasterTileRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterRdd, rasterRdd.rasterTileLayerMetadata)
    val rasterTileRddExceptBandRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (rasterTileRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), rasterTileRddWithMeta._2)
    val rasterTileRddExceptBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = rasterTileRddExceptBandRddWithMeta._1
    val rasterTileRddExceptBandMeta = rasterTileRddExceptBandRddWithMeta._2

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndbi tile.
    val ndbiRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTileRddExceptBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndbi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (swir1BandTile, nirBandTile) = (bandTileMap.get("SWIR 1"), bandTileMap.get("Near-Infrared"))
        if (swir1BandTile.isEmpty || nirBandTile.isEmpty)
          throw new RuntimeException("There is no SWIR 1 band or Near-Infrared band")
        val ndbiTile: Tile = ndbiTileComputation(swir1BandTile.get, nirBandTile.get)
        (SpaceTimeBandKey(spaceTimeKey, "NDBI"), ndbiTile)
      }

    val srcLayout = rasterTileRddExceptBandMeta.tileLayerMetadata.layout
    val srcExtent = rasterTileRddExceptBandMeta.tileLayerMetadata.extent
    val srcCrs = rasterTileRddExceptBandMeta.tileLayerMetadata.crs
    val srcBounds = rasterTileRddExceptBandMeta.tileLayerMetadata.bounds
    val ndbiMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)

    val ndbiMeta: RasterTileLayerMetadata[SpaceTimeKey] = rasterTileRddExceptBandMeta
    ndbiMeta.setTileLayerMetadata(ndbiMetaData)

    new RasterRDD(ndbiRdd, ndbiMeta)
  }

  /**
   * Generate a ndbi tile of DoubleConstantNoDataCellType.
   *
   * @param swir1BandTile Swir1 band Tile
   * @param nirBandTile   Nir band Tile
   * @param threshold
   * @return a ndbi tile of DoubleConstantNoDataCellType
   */
  def ndbiTileComputation(swir1BandTile: Tile, nirBandTile: Tile): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleSwir1BandTile = DoubleArrayTile(swir1BandTile.toArrayDouble(), swir1BandTile.cols, swir1BandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate ndbi tile
    val ndbiTile = Divide(
      Subtract(doubleSwir1BandTile, doubleNirBandTile),
      Add(doubleSwir1BandTile, doubleNirBandTile))

    ndbiTile
  }
}

