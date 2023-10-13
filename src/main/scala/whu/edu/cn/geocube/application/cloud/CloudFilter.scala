package whu.edu.cn.geocube.application.cloud

import cern.colt.matrix.impl.DenseDoubleMatrix2D
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{FloatArrayTile, Raster, Tile}
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.spark._

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig

import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.QueryRasterTiles
import whu.edu.cn.geocube.view.Info

/**
 * Cloud filter for landsat-8 ARD product.
 */
object CloudFilter {
  /**
   * Cloud filter for landsat-8 ARD product,
   * and return a rdd of cloud filtered product
   * for further analysis.
   *
   * Can be used in Jupyter Notebook, and web service && web platform (under development).
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta
   * @param temporalWindow time range to generate a cloud-free product
   * @return a rdd of cloud filtered result
   */
  def apply(implicit sc: SparkContext,
            tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
            temporalWindow: Array[Long] = null): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Cloud free task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Cloud free task is running ...")
    val bandNameIndexMap: Map[String, Int] = tileLayerArrayWithMeta._1.map(_._1.measurementName).toSet.-("Quality Assessment").zipWithIndex.toMap

    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    var noCloudRdd: RDD[Array[(SpaceTimeBandKey, Tile)]] = sc.emptyRDD[Array[(SpaceTimeBandKey, Tile)]]

    val actualTemporalWindow =
      if (temporalWindow == null) Array(srcMetadata.bounds.get._1.instant - 1, srcMetadata.bounds.get._2.instant + 1)
      else temporalWindow

    for (i <- 0 until actualTemporalWindow.length - 1) {
      val avgInstant = (actualTemporalWindow(i) + actualTemporalWindow(i + 1)) / 2
      val windowSpatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = spatialTemporalBandRdd.filter(x => x._1.instant > actualTemporalWindow(i) && x._1.instant < actualTemporalWindow(i + 1))

      //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      val multibandsRdd: RDD[(SpaceTimeKey, Iterable[(String, Tile)])] = windowSpatialTemporalBandRdd
        .groupByKey()

      //group by SpatialKey to get a time-band series RDD, i.e. Iterable[Iterable[(band, Tile)]])]
      val timeSeriesMultibandsRdd: RDD[(SpatialKey, Iterable[Iterable[(String, Tile)]])] = multibandsRdd
        .map(x => (x._1.spatialKey, x._2))
        .groupByKey()

      val multibandTileRdd: RDD[Array[(SpaceTimeBandKey, Tile)]] = timeSeriesMultibandsRdd.map { x =>
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val spatialKey = x._1
        val timeSeriesMultibandsTiles = x._2.toList

        val bandDimensions = timeSeriesMultibandsTiles(0).toList.length - 1
        val timeDimensions = timeSeriesMultibandsTiles.length

        val dstMultibandsTile = new Array[FloatArrayTile](bandDimensions)
        for (i <- 0 until bandDimensions) {
          val dstArray = Array.fill(cols * rows)(Float.NaN)
          dstMultibandsTile(i) = FloatArrayTile(dstArray, cols, rows)
        }

        val spaceTimeBandTile: ArrayBuffer[(SpaceTimeBandKey, Tile)] = new ArrayBuffer[(SpaceTimeBandKey, Tile)]()

        //filter cloud
        for (i <- 0 until cols; j <- 0 until rows) {
          var instantIndex = 0
          val matrix = new DenseDoubleMatrix2D(bandDimensions, timeDimensions)
          val matrixArray = Array.ofDim[Double](bandDimensions, timeDimensions)
          for (x <- 0 until bandDimensions; y <- 0 until timeDimensions)
            matrixArray(x)(y) = Double.NaN
          matrix.assign(matrixArray)

          for (instantMutilbandsTile <- timeSeriesMultibandsTiles) {
            val bandTileMap: Map[String, Tile] = instantMutilbandsTile.toMap
            val maskTile = bandTileMap.get("Quality Assessment").get
            val keys = bandTileMap.keySet.-("Quality Assessment")
            var bandIndex = 0
            for (key <- keys) {
              val tile = bandTileMap.get(key).get
              val maskTilePixelValue = maskTile.get(i, j)
              if (!GlobalConfig.GcConf.cloudValueLs8.contains(maskTilePixelValue) && !GlobalConfig.GcConf.cloudShadowValueLs8.contains(maskTilePixelValue))
                matrix.setQuick(bandNameIndexMap.get(key).get, instantIndex, tile.getDouble(i, j))
              bandIndex += 1
            }
            instantIndex += 1
          }
          val multibandsPixel = Geomedian.nangeomedian(matrix, 1)
          assert(multibandsPixel.size() == bandDimensions)

          for (bandName <- bandNameIndexMap.keySet)
            dstMultibandsTile(bandNameIndexMap.get(bandName).get).setDouble(i, j, multibandsPixel.getQuick(bandNameIndexMap.get(bandName).get))
        }
        for (bandName <- bandNameIndexMap.keySet) {
          spaceTimeBandTile.append((SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, avgInstant), bandName), dstMultibandsTile(bandNameIndexMap.get(bandName).get)))
        }
        spaceTimeBandTile.toArray
      }
      noCloudRdd = noCloudRdd ++ multibandTileRdd
    }

    //output
    (noCloudRdd.flatMap(x => x), RasterTileLayerMetadata(srcMetadata))
  }


  /**
   * Cloud filter for landsat-8 ARD product,
   * and return cloud-filtered product info
   * for generating cloud-filtered product.
   *
   * Thematic cloud-filtered product is under development.
   *
   * Used in Jupyter Notebook.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta
   * @param temporalWindow time range to generate a cloud-free product
   * @return result info
   */
  def cloudFilter(implicit sc: SparkContext,
                  tileLayerArrayWithMeta: (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                  temporalWindow: Array[Long] = null): Array[Info] = {
    val bandNameIndexMap: Map[String, Int] = tileLayerArrayWithMeta._1.map(_._1.measurementName).toSet.-("Quality Assessment").zipWithIndex.toMap
    for (key <- bandNameIndexMap.keySet) println(key, bandNameIndexMap.get(key))

    val tileLayerRddWithMeta: (RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x => (x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val spatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata
    val results = new ArrayBuffer[Info]()

    var noCloudRdd: RDD[Array[(SpaceTimeBandKey, Tile)]] = sc.emptyRDD[Array[(SpaceTimeBandKey, Tile)]]
    val actualTemporalWindow =
      if (temporalWindow == null) Array(srcMetadata.bounds.get._1.instant - 1, srcMetadata.bounds.get._2.instant + 1)
      else temporalWindow

    for (i <- 0 until actualTemporalWindow.length - 1) {
      val avgInstant = (actualTemporalWindow(i) + actualTemporalWindow(i + 1)) / 2
      val windowSpatialTemporalBandRdd: RDD[(SpaceTimeKey, (String, Tile))] = spatialTemporalBandRdd.filter(x => x._1.instant > actualTemporalWindow(i) && x._1.instant < actualTemporalWindow(i + 1))

      //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      val multibandsRdd: RDD[(SpaceTimeKey, Iterable[(String, Tile)])] = windowSpatialTemporalBandRdd
        .groupByKey()

      //group by SpatialKey to get a time-band series RDD, i.e. Iterable[Iterable[(band, Tile)]])]
      val timeSeriesMultibandsRdd: RDD[(SpatialKey, Iterable[Iterable[(String, Tile)]])] = multibandsRdd
        .map(x => (x._1.spatialKey, x._2))
        .groupByKey()

      val multibandTileRdd: RDD[Array[(SpaceTimeBandKey, Tile)]] = timeSeriesMultibandsRdd.map { x =>
        val (cols, rows) = (srcMetadata.tileCols, srcMetadata.tileRows)
        val spatialKey = x._1
        val timeSeriesMultibandsTiles = x._2.toList

        val bandDimensions = timeSeriesMultibandsTiles(0).toList.length - 1
        val timeDimensions = timeSeriesMultibandsTiles.length

        val dstMultibandsTile = new Array[FloatArrayTile](bandDimensions)
        for (i <- 0 until bandDimensions) {
          val dstArray = Array.fill(cols * rows)(Float.NaN)
          dstMultibandsTile(i) = FloatArrayTile(dstArray, cols, rows)
        }

        val spaceTimeBandTile: ArrayBuffer[(SpaceTimeBandKey, Tile)] = new ArrayBuffer[(SpaceTimeBandKey, Tile)]()

        //filter cloud
        for (i <- 0 until cols; j <- 0 until rows) {
          var instantIndex = 0
          val matrix = new DenseDoubleMatrix2D(bandDimensions, timeDimensions)
          val matrixArray = Array.ofDim[Double](bandDimensions, timeDimensions)
          for (x <- 0 until bandDimensions; y <- 0 until timeDimensions)
            matrixArray(x)(y) = Double.NaN
          matrix.assign(matrixArray)

          for (instantMutilbandsTile <- timeSeriesMultibandsTiles) {
            val bandTileMap: Map[String, Tile] = instantMutilbandsTile.toMap
            val maskTile = bandTileMap.get("Quality Assessment").get
            val keys = bandTileMap.keySet.-("Quality Assessment")
            var bandIndex = 0
            for (key <- keys) {
              val tile = bandTileMap.get(key).get
              val maskTilePixelValue = maskTile.get(i, j)
              if (!GlobalConfig.GcConf.cloudValueLs8.contains(maskTilePixelValue) && !GlobalConfig.GcConf.cloudShadowValueLs8.contains(maskTilePixelValue))
                matrix.setQuick(bandNameIndexMap.get(key).get, instantIndex, tile.getDouble(i, j))
              bandIndex += 1
            }
            instantIndex += 1
          }
          val multibandsPixel = Geomedian.nangeomedian(matrix, 1)
          assert(multibandsPixel.size() == bandDimensions)

          for (bandName <- bandNameIndexMap.keySet)
            dstMultibandsTile(bandNameIndexMap.get(bandName).get).setDouble(i, j, multibandsPixel.getQuick(bandNameIndexMap.get(bandName).get))
        }
        for (bandName <- bandNameIndexMap.keySet) {
          spaceTimeBandTile.append((SpaceTimeBandKey(SpaceTimeKey(spatialKey.col, spatialKey.row, avgInstant), bandName), dstMultibandsTile(bandNameIndexMap.get(bandName).get)))
        }
        spaceTimeBandTile.toArray
      }
      noCloudRdd = noCloudRdd ++ multibandTileRdd
    }

    //metadata
    val spatialKeyBounds = srcMetadata.bounds.get.toSpatial
    val cloudSpatialMetadata = TileLayerMetadata(
      srcMetadata.cellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      spatialKeyBounds)

    //stitch and generate cloud-filtered png
    val noCloudTileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(noCloudRdd.flatMap(x => x).map(x => (x._1.spaceTimeKey.spatialKey, x._2)), cloudSpatialMetadata)
    val stitched = noCloudTileLayerRdd.stitch()

    val uuid = UUID.randomUUID
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    stitched.tile.renderPng(colorRamp).write(GlobalConfig.GcConf.localHtmlRoot + uuid + "_noCloud.png")

    results += Info(GlobalConfig.GcConf.localHtmlRoot + uuid + "_noCloud.png", 0, "Cloud/Cloud Shadow Free")
    results.toArray
  }

  /**
   * API test.
   */
  def main(args: Array[String]): Unit = {
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO"))
    queryParams.setExtent(113.01494046724021,30.073457222586285,113.9181165740333,30.9597805438586)
    //queryParams.setExtent(112.6594200000,29.2322300000,115.0695900000,31.3623400000)
    queryParams.setTime("2018-07-01 00:00:00.000", "2018-10-01 00:00:00.000")
    queryParams.setMeasurements(Array("Green", "Near-Infrared", "Quality Assessment"))
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = QueryRasterTiles.getRasterTileArray(queryParams)
    val conf = new SparkConf()
      .setAppName("Cloud/Cloudshadow free")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    val cloudFiltered:(RDD[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = CloudFilter(sc, tileLayerArrayWithMeta)
    val resultsRdd = cloudFiltered.map(x=>(x._1.spaceTimeKey.spatialKey, x._2))
    val spatialKeyBounds = cloudFiltered._2.tileLayerMetadata.bounds.get.toSpatial
    val spatialMetadata = TileLayerMetadata(
      cloudFiltered._2.tileLayerMetadata.cellType,
      cloudFiltered._2.tileLayerMetadata.layout,
      cloudFiltered._2.tileLayerMetadata.extent,
      cloudFiltered._2.tileLayerMetadata.crs,
      spatialKeyBounds)
    val stitchedRdd:TileLayerRDD[SpatialKey] = ContextRDD(resultsRdd, spatialMetadata)

    val stitch: Raster[Tile] = stitchedRdd.stitch()
    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    stitch.tile.renderPng(colorRamp).write("/home/geocube/environment_test/geocube_core_jar/cloud_filter.png")
  }

}


