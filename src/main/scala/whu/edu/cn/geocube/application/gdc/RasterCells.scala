package whu.edu.cn.geocube.application.gdc

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{Tile, TileLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles.getRasterTileRDD
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.geocube.util.PostgresqlService

import sys.process._

object RasterCells {
  def getCells(sc: SparkContext, cubeId: String, productName: String, bbox: String, startTime: String, endTime: String, measurementsStr: String, outputDir: String): Unit = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for(i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")

    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    val extent = bbox.split(",").map(_.toDouble)
    val measurements = measurementsStr.split(",")
    val queryParams: QueryParams = new QueryParams()
    queryParams.setCubeId(cubeId)
    queryParams.setRasterProductName(productName)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setTime(startTime, endTime)
    queryParams.setMeasurements(measurements)
    val rasterTileLayerRdd:(RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = getRasterTileRDD(sc, queryParams)

    val SRE = new PostgresqlService().getSizeResAndExtentByCubeId(cubeId, PostgresqlUtil.url,PostgresqlUtil.user,PostgresqlUtil.password).toList.head

    val metadata = rasterTileLayerRdd._2.tileLayerMetadata
    rasterTileLayerRdd._1.foreach{x =>
      val spaceTimeBandKey = x._1
      val measurementName = spaceTimeBandKey.measurementName

      val tileSize = x._2.cols
      val extent = geotrellis.vector.Extent(SRE._2._1, SRE._2._2, SRE._2._3, SRE._2._4)
      val tl = TileLayout(((SRE._2._3 - SRE._2._1 )/ SRE._1._1).toInt, ((SRE._2._4 - SRE._2._2 )/ SRE._1._1).toInt, tileSize, tileSize)
      val ld = LayoutDefinition(extent, tl)
      val spatialKey = spaceTimeBandKey.spaceTimeKey.spatialKey
      val tileSpatialExtent = ld.mapTransform.keyToExtent(spatialKey)

      val instant = spaceTimeBandKey.spaceTimeKey.instant
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new Date(instant)
      val tileTime = sdf.format(date)

      val executorSessionDir = sessionDir.toString
      val executorSessionFile = new File(executorSessionDir)
      if (!executorSessionFile.exists) executorSessionFile.mkdir
      val executorOutputDir = outputDir
      val executorOutputFile = new File(executorOutputDir)
      if (!executorOutputFile.exists()) executorOutputFile.mkdir()

      val outputMetaPath = executorOutputDir + cubeId + "_" + productName + "_" + measurementName + "_" + spatialKey.col + "_"  + spatialKey.row + "_" + tileTime.substring(0,10) + ".txt"
      val writer = new PrintWriter(new File(outputMetaPath))
      writer.println(measurementName)
      writer.println(tileSpatialExtent.xmin + "," + tileSpatialExtent.ymin + "," + tileSpatialExtent.xmax + "," + tileSpatialExtent.ymax)
      writer.println(tileTime)
      writer.close()
      val scpMetaCommand = "scp " + outputMetaPath + " geocube@gisweb1:" + executorOutputDir
      scpMetaCommand.!

      val outputImagePath = executorOutputDir + cubeId + "_" + productName + "_" + measurementName + "_" + spatialKey.col + "_"  + spatialKey.row + "_" + tileTime.substring(0,10) + ".TIF"
      GeoTiff(x._2, tileSpatialExtent, metadata.crs).write(outputImagePath)
      val scpImageCommand = "scp " + outputImagePath + " geocube@gisweb1:" + executorOutputDir
      scpImageCommand.!
      val colorRamp = ColorRamp(RGB(0, 0, 0), RGB(255, 255, 255))
        .stops(100)
        .setAlphaGradient(0xFF, 0xAA)
      val outputPngPath = executorOutputDir + cubeId + "_" + productName + "_" + measurementName + "_" + spatialKey.col + "_"  + spatialKey.row + "_" + tileTime.substring(0,10) + ".png"
      x._2.renderPng(colorRamp).write(outputPngPath)
      val scpPngCommand = "scp " + outputPngPath + " geocube@gisweb1:" + executorOutputDir
      scpPngCommand.!
    }

  }

  def main(args: Array[String]): Unit = {
    //parse the web request params
    val cubeId = args(0)
    val rasterProductName = args(1)
    val bbox = args(2)
    val startTime = args(3)
    val endTime = args(4)
    val measurementStr = args(5)
    val outputDir = args(6)

    println("cubeId: " + cubeId)
    println("rasterProductName: " + rasterProductName)
    println("bbox: " + bbox)
    println("time: " + (startTime, endTime))
    println("measurements:" + measurementStr)


    val conf = new SparkConf()
      .setAppName("DAPA cube")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    //query and access
    val timeBegin = System.currentTimeMillis()
    getCells(sc, cubeId, rasterProductName, bbox, startTime, endTime, measurementStr, outputDir)
    val timeEnd = System.currentTimeMillis()
    println("time cost: " + (timeEnd - timeBegin))
  }

}
