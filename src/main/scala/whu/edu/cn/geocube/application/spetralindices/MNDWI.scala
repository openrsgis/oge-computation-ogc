package whu.edu.cn.geocube.application.spetralindices

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.cube.raster.RasterRDD
import whu.edu.cn.geocube.core.entity.{SpaceTimeBandKey, RasterTileLayerMetadata}

/**
 * Generate MNDWI product.
 *
 * mNDWI = NDSI = (Green – SWIR1) / (Green + SWIR1)
 */
object MNDWI {
  /**
   * This MNDWI function is used in Jupyter Notebook, e.g. cloud free mndwi.
   *
   * Use (RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param rasterRdd a rdd of queried tiles
   * @param threshold MNDWI threshold
   * @return results Info containing thematic mndwi product path, time, product type and water body area.
   */
  def mndwi(rasterRdd: RasterRDD, threshold: Double): RasterRDD = {
    val rasterTileRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = (rasterRdd, rasterRdd.rasterTileLayerMetadata)
    val rasterTileRddExceptBandRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (rasterTileRddWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2))), rasterTileRddWithMeta._2)
    val rasterTileRddExceptBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = rasterTileRddExceptBandRddWithMeta._1
    val rasterTileRddExceptBandMeta = rasterTileRddExceptBandRddWithMeta._2

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate mndwi tile.
    val mndwiRdd: RDD[(SpaceTimeBandKey, Tile)] = rasterTileRddExceptBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate mndwi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (greenBandTile, swir1BandTile) = (bandTileMap.get("Green"), bandTileMap.get("SWIR 1"))
        if (greenBandTile.isEmpty || swir1BandTile.isEmpty)
          throw new RuntimeException("There is no Green band or SWIR 1 band")
        val mndwiTile: Tile = mndwiTileComputation(greenBandTile.get, swir1BandTile.get, threshold)
        (SpaceTimeBandKey(spaceTimeKey, "MNDWI"), mndwiTile)
      }

    val srcLayout = rasterTileRddExceptBandMeta.tileLayerMetadata.layout
    val srcExtent = rasterTileRddExceptBandMeta.tileLayerMetadata.extent
    val srcCrs = rasterTileRddExceptBandMeta.tileLayerMetadata.crs
    val srcBounds = rasterTileRddExceptBandMeta.tileLayerMetadata.bounds
    val mndwiMetaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, srcBounds)

    val mndwiMeta: RasterTileLayerMetadata[SpaceTimeKey] = rasterTileRddExceptBandMeta
    mndwiMeta.setTileLayerMetadata(mndwiMetaData)

    new RasterRDD(mndwiRdd, mndwiMeta)
  }

  /**
   * Generate a mndwi tile of DoubleConstantNoDataCellType.
   *
   * @param greenBandTile Green band Tile
   * @param swir1BandTile Swir1 band Tile
   * @param threshold
   * @return a mndwi tile of DoubleConstantNoDataCellType
   */
  def mndwiTileComputation(greenBandTile: Tile, swir1BandTile: Tile, threshold: Double): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleGreenBandTile = DoubleArrayTile(greenBandTile.toArrayDouble(), greenBandTile.cols, greenBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleSwir1BandTile = DoubleArrayTile(swir1BandTile.toArrayDouble(), swir1BandTile.cols, swir1BandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate mndwi tile
    val mndwiTile = Divide(
      Subtract(doubleGreenBandTile, doubleSwir1BandTile),
      Add(doubleGreenBandTile, doubleSwir1BandTile))

    mndwiTile.mapDouble(pixel => {
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    })
  }

}
