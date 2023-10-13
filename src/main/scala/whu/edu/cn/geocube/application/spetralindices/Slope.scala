package whu.edu.cn.geocube.application.spetralindices

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{stitch, _}
import geotrellis.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles

import java.io.{File, FileOutputStream}
import java.util.UUID
import scala.sys.process._


object Slope {

  def slope(tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]), outputDir: String): Unit ={
    println(tileLayerRddWithMeta)
    val tranTileLayerRddWithMeta:(RDD[(SpaceTimeKey, (String, Tile))], RasterTileLayerMetadata[SpaceTimeKey]) = (tileLayerRddWithMeta._1.map(x=>(x._1.spaceTimeKey, (x._1.measurementName, x._2))), tileLayerRddWithMeta._2)

    val bandTileLayerRddWithMeta:(RDD[(String,(SpaceTimeKey,Tile))], RasterTileLayerMetadata[SpaceTimeKey]) = (tileLayerRddWithMeta._1.map(x=>(x._1.measurementName, (x._1.spaceTimeKey, x._2))), tileLayerRddWithMeta._2)

    val srcMetadata = tranTileLayerRddWithMeta._2.tileLayerMetadata
    val spatialMetadata = TileLayerMetadata(
      srcMetadata.cellType,
      srcMetadata.layout,
      srcMetadata.extent,
      srcMetadata.crs,
      srcMetadata.bounds.get.toSpatial)
    val bandAndSpatialTemporalRdd:RDD[(String, (SpaceTimeKey, Tile))] = bandTileLayerRddWithMeta._1

    val tileLayerRdd: TileLayerRDD[SpatialKey] = ContextRDD(bandAndSpatialTemporalRdd.values.map({x=>(x._1.spatialKey,x._2)}), spatialMetadata)
    val stitched: Raster[Tile] = tileLayerRdd.stitch()
    val srcExtent = stitched.extent

    val slopeTile = stitched.tile.slope(spatialMetadata.cellSize, 1/(111*math.cos(srcExtent.center.getY)), None)

    val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()
    //      val outputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"//"/home/geocube/environment_test/geocube_core_jar/"
    val uuid = UUID.randomUUID
    slopeTile.renderPng(colorRamp).write(executorOutputDir + uuid + "_slope.png")
    val SoutputTiffPath = executorOutputDir + uuid + "_slope.TIF"
    val slopeRaster: Raster[Tile] =Raster(slopeTile,srcExtent)
    GeoTiff(slopeRaster, spatialMetadata.crs).write(SoutputTiffPath)


    val outputMetaPath = executorOutputDir + "slope_job_res_meta.json"
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("pngPath", (executorOutputDir + uuid + "_slope.png").replace(localDataRoot, httpDataRoot))
    node.put("TiffPath", SoutputTiffPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", srcExtent.toString())
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)

    val scpPngCommand = "scp " + executorOutputDir + uuid + "_slope.png" + " geocube@gisweb1:" + outputDir
    scpPngCommand!
    val scpTiffCommand = "scp " + SoutputTiffPath + " geocube@gisweb1:" + outputDir
    scpTiffCommand!
    val scpMetaCommand1 = "scp " + outputMetaPath + " geocube@gisweb1:" + outputDir
    scpMetaCommand1.!


  }


  def main(args: Array[String]): Unit = {
    //parse the web request params

    val cubeId = args(0)
    val rasterProductNames = args(1).split(",")
    val extent = args(2).split(",").map(_.toDouble)
    val startTime = args(3) + " 00:00:00.000"
    val endTime = args(4) + " 00:00:00.000"
    val outputDir = args(5)


    val conf = new SparkConf()
      .setAppName("slope analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and acces
    val queryParams = new QueryParams
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(Array("DEM"))
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
    println("tileLayerRddWithMeta: " + tileLayerRddWithMeta)
    val timeBegin = System.currentTimeMillis()
    slope(tileLayerRddWithMeta,outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))

  }
}

