package whu.edu.cn.geocube.core.io

import geotrellis.layer._
import geotrellis.proj4._
import geotrellis.raster.resample._
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._
import org.locationtech.jts.geom._
import whu.edu.cn.geocube.core.cube.vector.{GeoObject, GeoObjectRDD, SpatialRDD}
import whu.edu.cn.geocube.core.vector.grid.GridConf
import whu.edu.cn.geocube.core.vector.ingest.Ingestor._
import whu.edu.cn.geocube.core.cube.vector.{GeoObjectRDD, SpatialRDD}
import whu.edu.cn.geocube.core.vector.grid
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.{groupGeom2SKgrid, groupGeom2ZCgrid}

/**
 * Only for development.
 * */
@deprecated(message = "Only for development!")
object Input{

  /**
   * Generate raster tiles with spatial, temporal and band information.
   *
   * @param sc A SparkContext
   * @param inputPath Separate band raster file path
   * @param instant Temporal information matching the input band raster
   * @param ld A predefined layout
   *
   * @return A spatial temporal TileLayerArray with band identification
   */
  def getSpaceTimeMultibandGeotiffTileArray(implicit sc: SparkContext,
                                            inputPath: Array[String],
                                            instant:Array[Long],
                                            ld: LayoutDefinition):(Array[(SpaceTimeKey, (String, Tile))], TileLayerMetadata[SpaceTimeKey]) = {
    val greenRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(0), ld, instant(0))
    //val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "Green", x._2))} //for NDWI
    val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "Red", x._2))} //for NDVI
    val nirRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(1), ld, instant(1))
    val nirBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = nirRasterTime1Rdd.map{ x => (x._1, ( "Nir", x._2 ))}

    val greenRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(2), ld, instant(2))
    //val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Green", x._2 ))}  //for NDWI
    val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Red", x._2 ))} //for NDVI
    val nirRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(3), ld, instant(3))
    val nirBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = nirRasterTime2Rdd.map{ x => (x._1, ( "Nir", x._2 ))}


    val unionRdd: RDD[(SpaceTimeKey, (String, Tile))] = greenBandTime1Rdd
      .union(nirBandTime1Rdd)
      .union(greenBandTime2Rdd)
      .union(nirBandTime2Rdd)

    (unionRdd.collect(), greenRasterTime1Rdd.metadata)
  }

  /**
   * Generate raster tiles with spatial, temporal, band and mask information.
   *
   * @param sc A SparkContext
   * @param inputPath Separate band raster file path
   * @param instant Temporal information matching the input band raster
   * @param ld A predefined layout
   *
   * @return A spatial temporal TileLayerRdd with band identification
   */
  def getSpaceTimeMultibandGeotiffTileRddWithMask(implicit sc: SparkContext,
                                                  inputPath: Array[String],
                                                  instant:Array[Long],
                                                  ld: LayoutDefinition):(RDD[(SpaceTimeKey, (String, Tile))], TileLayerMetadata[SpaceTimeKey]) = {

    assert(inputPath.length == instant.length)

    val greenRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(0), ld, instant(0))
    val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "blue", x._2))}
    val maskRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(1), ld, instant(1))
    val maskBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime1Rdd.map{ x => (x._1, ( "green", x._2 ))}
    val greenRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(2), ld, instant(2))
    val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Mask", x._2 ))}

    val maskRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(3), ld, instant(3))
    val maskBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime2Rdd.map{ x => (x._1, ( "blue", x._2 ))}
    val greenRasterTime3Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(4), ld, instant(4))
    val greenBandTime3Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime3Rdd.map{ x => (x._1, ( "green", x._2 ))}
    val maskRasterTime3Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(5), ld, instant(5))
    val maskBandTime3Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime3Rdd.map{ x => (x._1, ( "Mask", x._2 ))}

    /*val greenRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(0), ld, instant(0))
    val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "Green", x._2))} //for NDWI
    val maskRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(1), ld, instant(1))
    val maskBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime1Rdd.map{ x => (x._1, ( "Mask", x._2 ))}

    val greenRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(2), ld, instant(2))
    val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Green", x._2 ))}  //for NDWI
    val maskRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(3), ld, instant(3))
    val maskBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime2Rdd.map{ x => (x._1, ( "Mask", x._2 ))}

    val greenRasterTime3Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(4), ld, instant(4))
    val greenBandTime3Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime3Rdd.map{ x => (x._1, ( "Green", x._2 ))}  //for NDWI
    val maskRasterTime3Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(5), ld, instant(5))
    val maskBandTime3Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime3Rdd.map{ x => (x._1, ( "Mask", x._2 ))}*/

    /*val greenRasterTime4Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(6), ld, instant(6))
    val greenBandTime4Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime4Rdd.map{ x => (x._1, ( "Green", x._2 ))}  //for NDWI
    val maskRasterTime4Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(7), ld, instant(7))
    val maskBandTime4Rdd: RDD[(SpaceTimeKey, (String, Tile))] = maskRasterTime4Rdd.map{ x => (x._1, ( "Mask", x._2 ))}*/

    def unionBounds(bounds1: KeyBounds[SpaceTimeKey], bounds2: KeyBounds[SpaceTimeKey]): KeyBounds[SpaceTimeKey] = {
      val (minCol1, minRow1) = (bounds1._1.col, bounds1._1.row)
      val (maxCol1, maxRow1) = (bounds1._2.col, bounds1._2.row)

      val (minCol2, minRow2) = (bounds2._1.col, bounds2._1.row)
      val (maxCol2, maxRow2) = (bounds2._2.col, bounds2._2.row)

      val (minCol, minRow) = (Math.min(minCol1, minCol2), Math.min(minRow1, minRow2))
      val (maxCol, maxRow) = (Math.max(maxCol1, maxCol2), Math.max(maxRow1, maxRow2))

      val minInstant = Math.min(bounds1._1.instant, bounds2._1.instant)
      val maxInstant = Math.max(bounds1._1.instant, bounds2._1.instant)

      KeyBounds[SpaceTimeKey](SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
    }
    val boundsTime = Array(greenRasterTime1Rdd.metadata.bounds.get,
      greenRasterTime2Rdd.metadata.bounds.get,
      greenRasterTime3Rdd.metadata.bounds.get/*,
      greenRasterTime4Rdd.metadata.bounds.get*/)


    //val unionbounds = unionBounds(unionBounds(unionBounds(boundsTime(0), boundsTime(1)),boundsTime(2)), boundsTime(3))
    val unionbounds = unionBounds(unionBounds(boundsTime(0), boundsTime(1)),boundsTime(2))
    val unionExtent = greenRasterTime1Rdd.metadata.extent
      .union(greenRasterTime2Rdd.metadata.extent)
      .union(greenRasterTime3Rdd.metadata.extent)
      //.union(greenRasterTime4Rdd.metadata.extent)

    val unionRdd: RDD[(SpaceTimeKey, (String, Tile))] = greenBandTime1Rdd
      .union(maskBandTime1Rdd)
      .union(greenBandTime2Rdd)
      .union(maskBandTime2Rdd)
      .union(greenBandTime3Rdd)
      .union(maskBandTime3Rdd)
      /*.union(greenBandTime4Rdd)
      .union(maskBandTime4Rdd)*/

    val unionMetadata = TileLayerMetadata[SpaceTimeKey](
      greenRasterTime1Rdd.metadata.cellType,
      greenRasterTime1Rdd.metadata.layout,
      unionExtent.getEnvelopeInternal,
      greenRasterTime1Rdd.metadata.crs,
      unionbounds)

    println(unionMetadata.toString)

    //unionRdd.collect().foreach { x => println(x._1, x._2._1)}
    (unionRdd, unionMetadata)
  }

  /**
   * Generate raster tiles with spatial, temporal and band information.
   *
   * @param sc A SparkContext
   * @param inputPath Separate band raster file path
   * @param instant Temporal information matching the input band raster
   * @param ld A predefined layout
   *
   * @return A spatial temporal TileLayerRdd with band identification
   */
  def getSpaceTimeMultibandGeotiffTileRdd(implicit sc: SparkContext,
                                          inputPath: Array[String],
                                          instant:Array[Long],
                                          ld: LayoutDefinition):(RDD[(SpaceTimeKey, (String, Tile))], TileLayerMetadata[SpaceTimeKey]) = {
    val greenRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(0), ld, instant(0))
    val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "Green", x._2))} //for NDWI
    //val greenBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime1Rdd.map{ x => (x._1, ( "Red", x._2))} //for NDVI
    val nirRasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(1), ld, instant(1))
    val nirBandTime1Rdd: RDD[(SpaceTimeKey, (String, Tile))] = nirRasterTime1Rdd.map{ x => (x._1, ( "Nir", x._2 ))}

    val greenRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(2), ld, instant(2))
    val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Green", x._2 ))}  //for NDWI
    //val greenBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = greenRasterTime2Rdd.map{ x => (x._1, ( "Red", x._2 ))} //for NDVI
    val nirRasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(3), ld, instant(3))
    val nirBandTime2Rdd: RDD[(SpaceTimeKey, (String, Tile))] = nirRasterTime2Rdd.map{ x => (x._1, ( "Nir", x._2 ))}

    def unionBounds(bounds1: KeyBounds[SpaceTimeKey], bounds2: KeyBounds[SpaceTimeKey]): KeyBounds[SpaceTimeKey] = {
      val (minCol1, minRow1) = (bounds1._1.col, bounds1._1.row)
      val (maxCol1, maxRow1) = (bounds1._2.col, bounds1._2.row)

      val (minCol2, minRow2) = (bounds2._1.col, bounds2._1.row)
      val (maxCol2, maxRow2) = (bounds2._2.col, bounds2._2.row)

      val (minCol, minRow) = (Math.min(minCol1, minCol2), Math.min(minRow1, minRow2))
      val (maxCol, maxRow) = (Math.max(maxCol1, maxCol2), Math.max(maxRow1, maxRow2))

      val minInstant = Math.min(bounds1._1.instant, bounds2._1.instant)
      val maxInstant = Math.max(bounds1._1.instant, bounds2._1.instant)

      KeyBounds[SpaceTimeKey](SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
    }
    val boundsTime1 = greenRasterTime1Rdd.metadata.bounds.get
    val boundsTime2 = greenRasterTime2Rdd.metadata.bounds.get
    val unionbounds = unionBounds(boundsTime1, boundsTime2)
    val unionExtent = greenRasterTime1Rdd.metadata.extent.union(greenRasterTime2Rdd.metadata.extent)
    println(boundsTime1.toString)
    println(boundsTime2.toString)
    println(unionbounds.toString)
    println(unionExtent.toString)

    val unionRdd: RDD[(SpaceTimeKey, (String, Tile))] = greenBandTime1Rdd
      .union(nirBandTime1Rdd)
      .union(greenBandTime2Rdd)
      .union(nirBandTime2Rdd)

    val unionMetadata = TileLayerMetadata[SpaceTimeKey](
      greenRasterTime1Rdd.metadata.cellType,
      greenRasterTime1Rdd.metadata.layout,
      unionExtent.getEnvelopeInternal,
      greenRasterTime1Rdd.metadata.crs,
      unionbounds)

    println(unionMetadata.toString)

    //unionRdd.collect().foreach { x => println(x._1, x._2._1)}
    (unionRdd, unionMetadata)
  }

  /**
   * Generate raster tiles with spatial and multiple temporal information.
   *
   * @param sc A SparkContext
   * @param inputPath Separate band raster file path
   * @param instant Temporal information matching the input band raster
   * @param ld A predefined layout
   *
   * @return A spatial temporal TileLayerRdd with band identification
   */
  def getMultiSpaceTimeGeotiffTileRdd(implicit sc: SparkContext,
                                      inputPath: Array[String],
                                      instant:Array[Long],
                                      ld: LayoutDefinition):(RDD[(SpaceTimeKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val rasterTime1Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(0), ld, instant(0))
    val rasterTime2Rdd: TileLayerRDD[SpaceTimeKey] = getSpaceTimeGeotiffTileRdd(sc, inputPath(1), ld, instant(1))

    def unionBounds(bounds1: KeyBounds[SpaceTimeKey], bounds2: KeyBounds[SpaceTimeKey]): KeyBounds[SpaceTimeKey] = {
      val (minCol1, minRow1) = (bounds1._1.col, bounds1._1.row)
      val (maxCol1, maxRow1) = (bounds1._2.col, bounds1._2.row)

      val (minCol2, minRow2) = (bounds2._1.col, bounds2._1.row)
      val (maxCol2, maxRow2) = (bounds2._2.col, bounds2._2.row)

      val (minCol, minRow) = (Math.min(minCol1, minCol2), Math.min(minRow1, minRow2))
      val (maxCol, maxRow) = (Math.max(maxCol1, maxCol2), Math.max(maxRow1, maxRow2))

      val minInstant = Math.min(bounds1._1.instant, bounds2._1.instant)
      val maxInstant = Math.max(bounds1._1.instant, bounds2._1.instant)

      KeyBounds[SpaceTimeKey](SpaceTimeKey(minCol, minRow, minInstant), SpaceTimeKey(maxCol, maxRow, maxInstant))
    }
    val boundsTime1 = rasterTime1Rdd.metadata.bounds.get
    val boundsTime2 = rasterTime2Rdd.metadata.bounds.get
    val unionbounds = unionBounds(boundsTime1, boundsTime2)
    val unionExtent = rasterTime1Rdd.metadata.extent.union(rasterTime2Rdd.metadata.extent)
    println(boundsTime1.toString)
    println(boundsTime2.toString)
    println(unionbounds.toString)
    println(unionExtent.toString)

    val unionRdd: RDD[(SpaceTimeKey, Tile)] = rasterTime1Rdd
      .union(rasterTime2Rdd)

    val unionMetadata = TileLayerMetadata[SpaceTimeKey](
      rasterTime1Rdd.metadata.cellType,
      rasterTime1Rdd.metadata.layout,
      unionExtent.getEnvelopeInternal,
      rasterTime1Rdd.metadata.crs,
      unionbounds)

    println(unionMetadata.toString)

    //unionRdd.collect().foreach { x => println(x._1, x._2._1)}
    (unionRdd, unionMetadata)
  }


  /**
   * Generate raster tiles with spatial and temporal information.
   *
   * @param sc A SparkContext
   * @param inputPath Single band raster file path
   * @param ld A predefined layout
   * @param instant Temporal information
   *
   * @return A spatial temporal TileLayerRdd
   */
  def getSpaceTimeGeotiffTileRdd(implicit sc: SparkContext,
                                 inputPath: String,
                                 ld: LayoutDefinition,
                                 instant:Long):TileLayerRDD[SpaceTimeKey] = {
    val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(inputPath)

    val localLayoutScheme = FloatingLayoutScheme(512)

    val (_:Int, srcMetadata: TileLayerMetadata[SpatialKey]) =
      rdd.collectMetadata[SpatialKey](localLayoutScheme)
    println(srcMetadata.toString)

    val tilerOptions =
      Tiler.Options(
        resampleMethod =  Bilinear,
        partitioner = new HashPartitioner(18) //partitioner = new HashPartitioner(rdd.partitions.length)
      )
    val tiledRdd = rdd.tileToLayout[SpatialKey](srcMetadata,tilerOptions)

    val (zoom, reprojectedRdd):(Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiledRdd, srcMetadata)
        .reproject(LatLng, ld, Bilinear)
    val reprojectedMetadata = reprojectedRdd.metadata
    println(reprojectedMetadata.toString)

    val reprojectedSpaceTimeRdd: RDD[(SpaceTimeKey, Tile)] = reprojectedRdd.map(x =>
      (SpaceTimeKey(x._1.col, x._1.row, instant), x._2)
    )
    val spatialKeyBounds = reprojectedMetadata.bounds.get
    val keyBounds = KeyBounds(SpaceTimeKey(spatialKeyBounds._1.col, spatialKeyBounds._1.row, instant),
      SpaceTimeKey(spatialKeyBounds._2.col, spatialKeyBounds._2.row, instant))

    val reprojectedSpaceTimeMetadata = TileLayerMetadata(
      reprojectedMetadata.cellType,
      reprojectedMetadata.layout,
      reprojectedMetadata.extent,
      reprojectedMetadata.crs,
      keyBounds)

    ContextRDD(reprojectedSpaceTimeRdd, reprojectedSpaceTimeMetadata)
  }

  /**
   * Generate raster tiles with spatial information.
   *
   * @param sc A SparkContext
   * @param inputPath Single band raster file path
   * @param ld A predefined layout
   *
   * @return A spatial TileLayerRdd
   */
  def getGeotiffTileRdd(implicit sc: SparkContext,
                        inputPath: String,
                        ld: LayoutDefinition):TileLayerRDD[SpatialKey] = {
    val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(inputPath)

    val localLayoutScheme = FloatingLayoutScheme(512)

    val (_:Int, metadata: TileLayerMetadata[SpatialKey]) =
      rdd.collectMetadata[SpatialKey](localLayoutScheme)
    println(metadata.toString)

    val tilerOptions =
      Tiler.Options(
        resampleMethod =  Bilinear,
        partitioner = new HashPartitioner(18) //partitioner = new HashPartitioner(rdd.partitions.length)
      )
    val tiledRdd = rdd.tileToLayout[SpatialKey](metadata,tilerOptions)

    val (zoom, reprojectedRdd):(Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiledRdd, metadata)
        .reproject(LatLng, ld, Bilinear)
    println(reprojectedRdd.metadata.toString)

    ContextRDD(reprojectedRdd, reprojectedRdd.metadata)

  }

  /**
   * For GaoFen test, generate multibands raster tiles  using custom layout definition.
   *
   * @param sc A SparkContext
   * @param inputPath Multi bands raster file path
   * @param ld A predefined layout
   *
   * @return A spatial MultibandTileLayerRdd
   */
  def getMultibandGeotiffTileRdd(implicit sc: SparkContext,
                                 inputPath: String,
                                 ld: LayoutDefinition):MultibandTileLayerRDD[SpatialKey] = {
    val rdd: RDD[(ProjectedExtent, MultibandTile)] = sc.hadoopMultibandGeoTiffRDD(inputPath)

    val localLayoutScheme = FloatingLayoutScheme(512)

    val (_:Int, srcMetadata: TileLayerMetadata[SpatialKey]) =
      rdd.collectMetadata[SpatialKey](localLayoutScheme)
    println(srcMetadata.toString)

    val tilerOptions =
      Tiler.Options(
        resampleMethod =  Bilinear,
        partitioner = new HashPartitioner(rdd.partitions.length)
      )
    val tiledRdd = rdd.tileToLayout[SpatialKey](srcMetadata,tilerOptions)

    val (zoom, reprojectedRdd):(Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiledRdd, srcMetadata)
        .reproject(LatLng, ld, Bilinear)

    ContextRDD(reprojectedRdd, reprojectedRdd.metadata)
  }

  /**
   * Group geometries by grid code with a custom layout definition.
   *
   * @param sc A SparkContext
   * @param inputPath vector file path
   * @param ld A predefined layout
   *
   * @return grid attached with geometry list
   */
  def getGeometryGridCodeRdd(implicit sc: SparkContext,
                             inputPath: String,
                             ld: LayoutDefinition):RDD[(Long, Iterable[(String, Geometry)])] = {
    val rdd:SpatialRDD = SpatialRDD.createSpatialRDDFromLocalWKT(sc, inputPath,8)

    //Using Hilbert
    /*val indexFactory = new IndexFactory(-180.0, 180.0, -90.0, 90.0, 1.0, 1.0, 360*180)
    val gridWithGeomIdList = ClipToGrid(sc, rdd, indexFactory)*/

    //Using custom grid
    /*val extent = Extent(-180, -90, 180, 90)
    val gridWithGeomIdList = rdd.flatMap(x=>geom2XY(x, 360, 180, extent)).groupByKey()*/

    //using ZOrder in geotrellis
    val gridConf: GridConf = new GridConf(ld.layoutCols, ld.layoutRows, ld.extent)
    val gridWithGeomIdList = rdd.flatMap(x=>grid.GridTransformer.groupGeom2ZCgrid(x, gridConf)).groupByKey()
    gridWithGeomIdList
  }

  /**
   * Group features by grid code with a custom layout definition.
   *
   * @param sc A SparkContext
   * @param inputPath vector file path
   * @param ld A predefined layout
   *
   * @return grid code attached with geometry list
   */
  def getGeoObjectGridCodeRdd(implicit sc: SparkContext,
                              inputPath: String,
                              ld: LayoutDefinition):RDD[(Long, Iterable[GeoObject])] = {
    //val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromShp(sc, inputPath,8)
    val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromFeatureJson(sc, inputPath,8)

    //using ZOrder in geotrellis
    val gridConf: GridConf = new GridConf(ld.layoutCols, ld.layoutRows, ld.extent)
    val gridWithGeomIdList:RDD[(Long, Iterable[GeoObject])] =
      rdd.flatMap(x=>groupGeom2ZCgrid(x, gridConf)).groupByKey()
    gridWithGeomIdList
  }

  /**
   * Group geometries by spatial key with a custom layout definition.
   *
   * @param sc A SparkContext
   * @param inputPath vector file path
   * @param ld A predefined layout
   *
   * @return grid attached with geometry list
   */
  def getGeometryGridSKRdd(implicit sc: SparkContext,
                           inputPath: String,
                           ld: LayoutDefinition):RDD[(SpatialKey, Iterable[(String, Geometry)])] = {
    val rdd:SpatialRDD = SpatialRDD.createSpatialRDDFromLocalWKT(sc, inputPath,8)

    //using SpatialKey in geotrellis
    val gridConf: GridConf = new GridConf(ld.layoutCols, ld.layoutRows, ld.extent)
    val gridWithGeomIdList = rdd.flatMap(x=>grid.GridTransformer.groupGeom2SKgrid(x, gridConf)).groupByKey()
    gridWithGeomIdList
  }

  /**
   * Group features by spatial key with a custom layout definition.
   *
   * @param sc A SparkContext
   * @param inputPath vector file path
   * @param ld A predefined layout
   *
   * @return spatial key attached with geometry list
   */
  def getGeoObjectGridSKRdd(implicit sc: SparkContext,
                            inputPath: String,
                            ld: LayoutDefinition):RDD[(SpatialKey, Iterable[GeoObject])] = {
    //val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromShp(sc, inputPath,8)
    val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromFeatureJson(sc, inputPath,8)

    //using Spatial Key in geotrellis
    val gridConf: GridConf = new GridConf(ld.layoutCols, ld.layoutRows, ld.extent)
    val gridWithGeomIdList:RDD[(SpatialKey, Iterable[GeoObject])] =
      rdd.flatMap(x=>groupGeom2SKgrid(x, gridConf)).groupByKey()
    gridWithGeomIdList
  }

}



