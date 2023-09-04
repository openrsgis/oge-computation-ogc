package whu.edu.cn.geocube.application.spetralindices

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, RasterTileLayerMetadata}

/**
 * Generate NDVI product.
 *
 * NDVI = (NIR – Red) / (NIR + Red)
 */
object NDVI {
  /**
   * This NDVI function is used in Jupyter Notebook, e.g. cloud free ndvi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param rasterRdd a rdd of queried tiles
   * @param threshold NDVI threshold
   * @return results Info containing thematic ndvi product path, time, product type and vegetation-cover area.
   */
  def ndvi(rasterRdd: RasterRDD): RasterRDD = {
    val rasterTileRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterRdd, rasterRdd.rasterTileLayerMetadata)
    val rasterTileRddExceptBandRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (rasterTileRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), rasterTileRddWithMeta._2)
    val rasterTileRddExceptBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = rasterTileRddExceptBandRddWithMeta._1
    val rasterTileRddExceptBandMeta = rasterTileRddExceptBandRddWithMeta._2

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate ndvi tile.
    val ndviRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTileRddExceptBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate ndvi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile.isEmpty || nirBandTile.isEmpty)
          throw new RuntimeException("There is no Red band or Nir band")
        val ndviTile: Tile = ndviTileComputation(redBandTile.get, nirBandTile.get)
        (SpaceTimeBandKey(spaceTimeKey, "NDVI"), ndviTile)
      }

    val srcLayout = rasterTileRddExceptBandMeta.tileLayerMetadata.layout
    val srcExtent = rasterTileRddExceptBandMeta.tileLayerMetadata.extent
    val srcCrs = rasterTileRddExceptBandMeta.tileLayerMetadata.crs
    val srcBounds = rasterTileRddExceptBandMeta.tileLayerMetadata.bounds
    val ndviMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)

    val ndviMeta: RasterTileLayerMetadata[SpaceTimeKey] = rasterTileRddExceptBandMeta
    ndviMeta.setTileLayerMetadata(ndviMetaData)

    new RasterRDD(ndviRdd, ndviMeta)
  }

  /**
   * Generate a ndvi tile of DoubleConstantNoDataCellType.
   *
   * @param redBandTile Red band Tile
   * @param nirBandTile Nir band Tile
   * @param threshold
   * @return a ndvi tile of DoubleConstantNoDataCellType
   */
  def ndviTileComputation(redBandTile: Tile, nirBandTile: Tile): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleRedBandTile = DoubleArrayTile(redBandTile.toArrayDouble(), redBandTile.cols, redBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate ndvi tile
    val ndviTile = Divide(
      Subtract(doubleNirBandTile, doubleRedBandTile),
      Add(doubleNirBandTile, doubleRedBandTile))

    ndviTile
  }
}