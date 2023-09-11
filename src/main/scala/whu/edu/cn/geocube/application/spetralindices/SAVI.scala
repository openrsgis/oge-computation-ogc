package whu.edu.cn.geocube.application.spetralindices

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local.{Add, Divide, Subtract}
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.{DoubleArrayTile, DoubleConstantNoDataCellType, Raster, Tile}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.sys.process.ProcessLogger
import sys.process._
import whu.edu.cn.geocube.core.entity.{RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.entity.SpaceTimeBandKey
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.view.Info

object SAVI {
  /**
   * This SAVI function is used in Jupyter Notebook.
   *
   * Use (Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]) as input.
   *
   * @param sc a SparkContext
   * @param tileLayerArrayWithMeta an array of queried tiles
   * @param threshold SAVI threshold
   *
   * @return results info containing thematic savi product path, time and product type.
   */
  def savi(implicit sc:SparkContext,
           tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey,Tile)],RasterTileLayerMetadata[SpaceTimeKey]),
           threshold: Double): Array[Info] = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- savi task is submitted")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- savi task is running ...")
    val analysisBegin = System.currentTimeMillis()

    //transform tile array to tile rdd
    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) =
      (sc.parallelize(tileLayerArrayWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2)))), tileLayerArrayWithMeta._2)
    val latiRad = (tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymin + tileLayerArrayWithMeta._2.tileLayerMetadata.extent.ymax) / 2 * Math.PI / 180

    val spatialTemporalBandRdd:RDD[(SpaceTimeKey, (String, Tile))] = tileLayerRddWithMeta._1
    val srcMetadata = tileLayerRddWithMeta._2.tileLayerMetadata

    //group by SpaceTimeKey to get a band-series RDD, i.e., RDD[(SpaceTimeKey, Iterable((bandname, Tile)))],
    //and generate savi tile.
    val saviRdd: RDD[(SpaceTimeKey, Tile)] = spatialTemporalBandRdd
      .groupByKey() //group by SpaceTimeKey to get a band-series RDD, i.e. RDD[(SpaceTimeKey, Iterable((band, Tile)))]
      .map { x => //generate savi tile
        val spaceTimeKey = x._1
        val bandTileMap = x._2.toMap
        val (redBandTile, nirBandTile) = (bandTileMap.get("Red"), bandTileMap.get("Near-Infrared"))
        if (redBandTile == None || nirBandTile == None)
          throw new RuntimeException("There is no Red band or Nir band")
        val savi: Tile = saviTile(redBandTile.get, nirBandTile.get, threshold)
        (spaceTimeKey, savi)
      }

    //group by TemporalKey to get a extent-series RDD, i.e. RDD[(time, Iterable[(SpaceTimeKey,Tile)])]
    val temporalGroupRdd:RDD[(Long, Iterable[(SpaceTimeKey,Tile)])] = saviRdd.groupBy(_._1.instant)

    val results:RDD[Info] = temporalGroupRdd.map{x =>
      //stitch extent-series tiles of each time instant to pngs
      val metadata = srcMetadata
      val layout = metadata.layout
      val crs = metadata.crs
      val instant = x._1
      val tileLayerArray: Array[(SpatialKey, Tile)] = x._2.toArray.map(ele=>(ele._1.spatialKey, ele._2))
      val stitched: Raster[Tile] = TileUtil.stitch(tileLayerArray, layout)

      var accum = 0.0
      tileLayerArray.foreach { x =>
        val tile = x._2
        tile.foreachDouble { x =>
          if (x == 255.0) accum += x
        }
      }

      val colorRamp = ColorRamp(
        0xB96230FF,
        0xDB9842FF,
        0xDFAC6CFF,
        0xE3C193FF,
        0xE6D6BEFF,
        0xE4E7C4FF,
        0xD9E2B2FF,
        0xBBCA7AFF,
        0x9EBD4DFF,
        0x569543FF
      )
      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
      val uuid = UUID.randomUUID
      stitched.tile.renderPng(colorRamp).write(outputDir + uuid + "_savi_" + instant + ".png")

      val outputTiffPath = outputDir + uuid + "_savi_" + instant + ".TIF"
      GeoTiff(stitched, crs).write(outputTiffPath)
      val outputThematicPngPath = outputDir + uuid + "_savi_thematic" + instant + ".png"
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      Seq("/home/geocube/qgis/run.sh", "-t", "NDVI", "-r", s"$outputTiffPath", "-o", s"$outputThematicPngPath") ! ProcessLogger(stdout append _, stderr append _)
      new Info(outputThematicPngPath, instant, "SAVI Analysis", accum / 255 / 1024 / 1024 * 110.947 * 110.947 * Math.cos(latiRad))
    }
    val ret = results.collect()
    val analysisEnd = System.currentTimeMillis()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " --- Time cost: " + (analysisEnd - analysisBegin) + " ms")
    ret
  }

  /**
   * Generate a savi tile of DoubleConstantNoDataCellType.
   *
   * @param redBandTile Red band Tile
   * @param nirBandTile Nir band Tile
   * @param threshold
   *
   * @return savi Tile
   */
  def saviTile(redBandTile: Tile, nirBandTile: Tile, threshold: Double): Tile = {
    //convert stored tile with constant Float.NaN to Double.NaN
    val doubleRedBandTile = DoubleArrayTile(redBandTile.toArrayDouble(), redBandTile.cols, redBandTile.rows)
      .convert(DoubleConstantNoDataCellType)
    val doubleNirBandTile = DoubleArrayTile(nirBandTile.toArrayDouble(), nirBandTile.cols, nirBandTile.rows)
      .convert(DoubleConstantNoDataCellType)

    //calculate savi tile
    val saviTile = Divide(
      Subtract( doubleNirBandTile, doubleRedBandTile ),
      Add( doubleNirBandTile, doubleRedBandTile).localAdd(0.5)).localMultiply(1.5)

    saviTile.mapDouble(pixel=>{
      if (pixel > threshold) 255.0
      else if (pixel >= -1) 0.0
      else Double.NaN
    })
  }

}
