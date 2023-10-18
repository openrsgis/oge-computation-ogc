package whu.edu.cn.geocube.application.timeseries

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.proj4.CRS
import geotrellis.raster.{ByteCellType, ColorMap, Raster, Tile, TileLayout}
import geotrellis.layer._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.render.GreaterThanOrEqualTo

import java.io.{File, FileOutputStream, PrintWriter}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import whu.edu.cn.config.GlobalConfig.GcConf.{httpDataRoot, localDataRoot}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import sys.process._
import whu.edu.cn.geocube.application.timeseries.WOfS.wofsSpatialBatch
import whu.edu.cn.geocube.util.TileUtil
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.query.DistributedQueryRasterTiles

/**
 * Batch execution of WOfS analysis called by the main function in WOfS class.
 *
 * Since there are some bugs when calling wofsSpatialBatch() in WOfS class,
 * this class has eliminated these bugs.
 */
object WOfSBatchScript {
  def wofsBatchExecutor(batches: Int,
                        queryParams: QueryParams,
                        outputDir: String): Unit = {
    val outputDirArray = outputDir.split("/")
    val sessionDir = new StringBuffer()
    for (i <- 0 until outputDirArray.length - 1)
      sessionDir.append(outputDirArray(i) + "/")
    val executorSessionDir = sessionDir.toString
    val executorSessionFile = new File(executorSessionDir)
    if (!executorSessionFile.exists) executorSessionFile.mkdir
    val executorOutputDir = outputDir
    val executorOutputFile = new File(executorOutputDir)
    if (!executorOutputFile.exists()) executorOutputFile.mkdir()

    //extract raster product names
    val rasterProductNamesArray = queryParams.getRasterProductNames
    val rasterProductNames = new StringBuilder
    rasterProductNamesArray.foreach(x => rasterProductNames.append(x + ","))
    rasterProductNames.deleteCharAt(rasterProductNames.lastIndexOf(","))
    println("rasterProductNames: " + rasterProductNames.toString())

    //extract time range
    val startTime = queryParams.getStartTime
    val endTime = queryParams.getEndTime
    println("time: " + (startTime, endTime))

    //measurement(Green, Near-Infrared) and resolution(4000) fixed

    //extract extent
    val envelope = queryParams.getPolygon.getEnvelopeInternal
    val extentArray: Array[String] = Array(envelope.getMinX.toString, envelope.getMinY.toString, envelope.getMaxX.toString, envelope.getMaxY.toString)
    val extentStr = new StringBuilder
    extentArray.foreach(x => extentStr.append(x + ","))
    extentStr.deleteCharAt(extentStr.lastIndexOf(","))

    //extract grid codes
    val gridCodes: Array[String] = queryParams.getGridCodes.toArray
    println("all gridCodes: " + gridCodes.foreach(x => print(x + " ")))

    val batchInterval = gridCodes.length / batches
    if (batchInterval == gridCodes.length) {
      throw new RuntimeException("Grid codes length is " + gridCodes.length + ", batches is " + batches + ", batch interval is " + batchInterval + ", which is too large, please increase the batches!")
    } else if (batchInterval <= 1) {
      throw new RuntimeException("Grid codes length is " + gridCodes.length + ", batches is " + batches + ", batch interval is " + batchInterval + ", which is too small, please reduce the batches!")
    } else {
      (0 until batches).foreach { i =>
        println("**************batch " + i + "*****************")
        println("**************batch " + i + "*****************")
        println("**************batch " + i + "*****************")
        println("**************batch " + i + "*****************")
        println("**************batch " + i + "*****************")
        println("**************batch " + i + "*****************")
        //extract grid code for each batch
        val batchGridCodeArray: ArrayBuffer[String] = new ArrayBuffer[String]()
        ((i * batchInterval) until (i + 1) * batchInterval).foreach(j => batchGridCodeArray.append(gridCodes(j)))
        if (i == (batches - 1) && gridCodes.length % batches != 0) {
          ((i + 1) * batchInterval until gridCodes.length).foreach(j => batchGridCodeArray.append(gridCodes(j)))
        }
        val batchGridCodes = new StringBuilder
        batchGridCodeArray.foreach(x => batchGridCodes.append(x + ","))
        batchGridCodes.deleteCharAt(batchGridCodes.lastIndexOf(","))

        //call main function of the cuurrent class
        val callScript = "/home/geocube/spark/bin/spark-submit " +
          "--master spark://125.220.153.26:7077 --class whu.edu.cn.application.timeseries.WOfSBatchScript " +
          "--driver-memory 24G --executor-memory 8G --total-executor-cores 16 --executor-cores 1 " +
          "/home/geocube/environment_test/geocube_core_jar/geocube.jar " +
          rasterProductNames + " " + startTime + " " + endTime + " " + extentStr + " " + batchGridCodes + " " + executorOutputDir + " " + i

        callScript.!
      }
    }

    val dir = new File(executorOutputDir)

    val metaPaths: Array[String] = dir.list().filter(x => x.endsWith("txt"))
    var queryTime: Long = 0
    var analysisTime: Long = 0
    var tileCount: Long = 0
    metaPaths.foreach { x =>
      println(executorOutputDir + x)
      val metaFile = Source.fromFile(executorOutputDir + x)
      val lines = metaFile.getLines()
      val queryLine = lines.next()
      queryTime += queryLine.split(":")(1).toLong
      val analysisLine = lines.next()
      analysisTime += analysisLine.split(":")(1).toLong
      val tileCountLine = lines.next()
      tileCount += tileCountLine.split(":")(1).toLong
      metaFile.close
    }

    val tilePaths: Array[String] = dir.list().filter(x => x.endsWith("TIF"))
    val spatialTiles = ArrayBuffer[(SpatialKey, Tile)]()
    tilePaths.foreach { x =>
      println(executorOutputDir + x)
      val col_row = x.replace(".TIF", "").split("_")
      val (col, row) = (col_row(0).toInt, col_row(1).toInt)
      val spatialKey = SpatialKey(col, row)
      val tile = GeoTiffReader.readSingleband(executorOutputDir + x).tile
      spatialTiles.append((spatialKey, tile))
    }

    val stitchBegin = System.currentTimeMillis()

    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tileSize: Int = spatialTiles(0)._2.rows
    val tl = TileLayout(360, 180, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)
    val stitched: Raster[Tile] = TileUtil.stitch(spatialTiles.toArray, ld)
    val extentRet = stitched.extent
    println("<--------wofs: " + extentRet + "-------->")

    val map: Map[Double, Int] =
      Map(
        0.0 -> 0xFFFFFFFF,
        1.1 -> 0xF6EDB1FF,
        2.2 -> 0xF7DA22FF,
        3.3 -> 0xECBE1DFF,
        4.4 -> 0xE77124FF,
        5.5 -> 0xD54927FF,
        6.6 -> 0xCF3A27FF,
        7.7 -> 0xA33936FF,
        8.8 -> 0x7F182AFF,
        9.9 -> 0x68101AFF
      )

    val colorMap =
      ColorMap(
        map,
        ColorMap.Options(
          classBoundaryType = GreaterThanOrEqualTo,
          noDataColor = 0x00000000, // transparent
          fallbackColor = 0x00000000, // transparent
          strict = true
        )
      )
    val outputPath = executorOutputDir + "WOfS.png"
    stitched.tile.renderPng(colorMap).write(outputPath)

    val outputMetaPath = executorOutputDir + "WOfS.json"
    val objectMapper = new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("path", outputPath.replace(localDataRoot, httpDataRoot))
    node.put("meta", outputMetaPath.replace(localDataRoot, httpDataRoot))
    node.put("extent", extentRet.xmin + "," + extentRet.ymin + "," + extentRet.xmax + "," + extentRet.ymax)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputMetaPath), node)

    val stitchEnd = System.currentTimeMillis()
    analysisTime += (stitchEnd - stitchBegin)

    println("Query time of " + tileCount + " raster tiles: " + queryTime + "ms")
    println("Analysis time: " + analysisTime + "ms")
  }

  def callWOfSBatchScript(queryParams: QueryParams,
                          executorOutputDir: String,
                          batchCount: Int): Unit = {
    val conf = new SparkConf()
      .setAppName("WOfS analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rpc.message.maxSize", "1024")
      .set("spark.driver.maxResultSize", "8g")
    val sc = new SparkContext(conf)
    try {
      //data access
      val batchQueryBegin = System.currentTimeMillis()
      val tileLayerRddWithMeta: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) =
        DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams)
      if (tileLayerRddWithMeta == null)
        return
      val batchQueryEnd = System.currentTimeMillis()
      val queryTilesCount = tileLayerRddWithMeta._1.count()

      //data processing
      val batchAnalysisBegin = System.currentTimeMillis()
      val batchWOfS: Array[(SpatialKey, Tile)] = wofsSpatialBatch(tileLayerRddWithMeta)
      val batchAnalysisEnd = System.currentTimeMillis()

      batchWOfS.foreach { x =>
        val (col, row) = (x._1.col, x._1.row)
        val metaDataPath = executorOutputDir + col + "_" + row + ".txt"
        val metaWriter = new PrintWriter(new File(metaDataPath))
        metaWriter.println("QueryTimeCost:" + (batchQueryEnd - batchQueryBegin))
        metaWriter.println("AnalysisTimeCost:" + (batchAnalysisEnd - batchAnalysisBegin))
        metaWriter.println("TilesCount:" + queryTilesCount)
        metaWriter.close()

        val tileDataPath = executorOutputDir + col + "_" + row + ".TIF"
        val tile = x._2.localMultiply(10).convert(ByteCellType)
        val crs: CRS = CRS.fromEpsgCode(4326)
        val globalExtent = geotrellis.vector.Extent(-180, -90, 180, 90)
        val tileSize: Int = queryParams.getLevel.toInt
        val tl = TileLayout(360, 180, tileSize, tileSize)
        val ld = LayoutDefinition(globalExtent, tl)
        val extent = x._1.extent(ld)
        GeoTiff(Raster(tile, extent), crs).write(tileDataPath)
      }
      println("batch " + batchCount + " success")
    } finally {
      sc.stop()
    }

  }

  def main(args: Array[String]): Unit = {
    val rasterProductNames = args(0).split(",")
    val startTime = args(1) + " 00:00:00.000"
    val endTime = args(2) + " 00:00:00.000"
    val extent = args(3).split(",").map(_.toDouble)
    val gridCodes = args(4).split(",")
    val executorOutputDir = args(5)
    val batchCount = args(6).toInt

    println("rasterProductNames: " + rasterProductNames.foreach(x => print(x + " ")))
    println("time: " + (startTime, endTime))
    println("gridCodes of the current batch: " + gridCodes.foreach(x => print(x + " ")))

    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setTime(startTime, endTime)
    queryParams.setExtent(extent(0), extent(1), extent(2), extent(3))
    queryParams.setGridCodes(gridCodes.toBuffer.asInstanceOf[ArrayBuffer[String]])
    queryParams.setMeasurements(Array("Green", "Near-Infrared")) //该条件为NDWI分析固定条件
    queryParams.setLevel("4000")

    callWOfSBatchScript(queryParams, executorOutputDir, batchCount)
  }

}
