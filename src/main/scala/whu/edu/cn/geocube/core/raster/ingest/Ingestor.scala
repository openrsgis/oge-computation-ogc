package whu.edu.cn.geocube.core.raster.ingest

import geotrellis.layer._
import geotrellis.raster.resample._
import geotrellis.raster.{Tile, TileLayout}
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.raster.render.{ColorMap, ColorRamp}
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.rdd._
import whu.edu.cn.geocube.util.{HbaseUtil, TileUtil}
import scala.io.Source

/**
 * In the GeoCube, raster data is segmented into tiles physically
 * based on a global grid tessellation.
 *
 * The metadata of tile is stored in PostgreSQL based on a fact
 * constellation schema, while tile data is stored in HBase.
 * This class is used to generate tile and ingest them to HBase.
 * Dimensional metadata of these tiles are created and ingested
 * to PostgreSQL in another project, i.e. GeoCube-boot.
 *
 * Now support Landsat-8 and Gaofen-1 data segmentation and ingestion.
 *
 **/
object Ingestor extends java.io.Serializable {

  /**
   * Called by Spring-boot project
   */
  def main(args: Array[String]): Unit = {
    //spark context
    val conf = new SparkConf()
      .setAppName("Raster data segmentation Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "256m")
      .set("spark.rpc.message.maxSize", "512")
    val sc = new SparkContext(conf)

    //use java multi-threads tech, each thread processes 1 band
    val jobExecutor = Executors.newFixedThreadPool(args(2).toInt)

    //tiling and ingest
    try {
      val tilingType = args(1)
      if (tilingType.equals("custom")) { // tile for analysis
        ingestGlobalCustomTiles(sc, jobExecutor, args(0),args(3),
          args(4).toInt,args(5).toInt,args(6).toInt,args(7).toInt,args(8).toInt,args(9).toInt)
      } else if (tilingType.equals("TMS")) { //TMS tile for visualization
        ingestOsGeoTiles(sc, jobExecutor, args(0))
      }
    } finally {
      var flag = true
      while (flag) {
        if (jobExecutor.isTerminated)
          flag = false
        Thread.sleep(200)
      }
      sc.stop()
    }
  }

  /**
   * Generate custom/analysis tiles based on the custom layout definition
   * and ingest them into HBase.
   *
   * @param sc spark context
   * @param jobExecutor job executor of java multi threads
   * @param configPath the path of config file
   */
  def ingestGlobalCustomTiles(implicit sc: SparkContext, jobExecutor: ExecutorService, configPath: String,hbaseTableName:String,
                              gridDimX:Int, gridDimY:Int, minX:Int, minY:Int, maxX:Int, maxY:Int): Unit = {
    //read parameters in config file
    val source = Source.fromFile(configPath, "UTF-8")
    val lines = source.getLines().toArray
    val filePaths = lines(0).split("\t")
    val fileReGrid_TileDataId: Map[String, String] = lines(1).split("\t").map { x =>
      val key_value = x.split("->")
      (key_value(0), key_value(1))
    }.toMap
    val gcTileMetaDataHashMap: Map[String, String] = lines(2).split("\t").map { x =>
      val key_value = x.split("->")
      (key_value(0), key_value(1))
    }.toMap
    val tileSize = lines(3).toInt
    source.close()

    //tiling and ingest
    ingestGlobalCustomTiles(sc, jobExecutor, filePaths, fileReGrid_TileDataId, gcTileMetaDataHashMap,
      tileSize,hbaseTableName,gridDimX, gridDimY, minX, minY, maxX, maxY)
  }

  /**
   * Generate custom/analysis tiles based on the custom layout definition
   * and ingest them into HBase.
   *
   * @param sc spark context
   * @param jobExecutor job executor of java multi threads
   * @param filePaths files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   * @param tileSize tile size
   */
  def ingestGlobalCustomTiles(implicit sc: SparkContext,
                              jobExecutor: ExecutorService,
                              filePaths: Array[String],
                              fileReGrid_TileDataId: Map[String, String],
                              gcTileMetaDataHashMap: Map[String, String],
                              tileSize: Int,
                              hbaseTableName:String,
                              gridDimX:Int, gridDimY:Int, minX:Int, minY:Int, maxX:Int, maxY:Int): Unit = {
    //use java multi-threads tech, each thread processes 1 band
    val arr = filePaths
    for (path <- arr) {
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          val filePath = path
          //read as RDD[(ProjectedExtent, Tile)]
          val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(filePath)

          //set local layout scheme and collect rdd metadata based on the scheme
          val localLayoutScheme = FloatingLayoutScheme(512)
          val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
            rdd.collectMetadata[SpatialKey](localLayoutScheme)

          //set local tiling configuration
          val tilerOptions =
            Tiler.Options(
              resampleMethod = Bilinear,
              partitioner = new HashPartitioner(108)
            )

          //local tiling
          val tiledRdd = rdd.tileToLayout[SpatialKey](metadata, tilerOptions)

          //set global tiling configuration
          val size = tileSize
          val gridDX=gridDimX
          val gridDY=gridDimY
          val minXcoor=minX
          val minYcoor=minY
          val maxXcoor=maxX
          val maxYcoor=maxY
          //          val extent = Extent(-180, -90, 180, 90) //global extent under LatLng
          //          val tl = TileLayout(360, 180, size, size) //tile:1°×1°, 1024×1024
          val extent = Extent(minXcoor, minYcoor, maxXcoor, maxYcoor) //local extent under LatLng
          val tl = TileLayout(gridDX, gridDY, size, size)
          val ld = LayoutDefinition(extent, tl)

          //global tiling in distributed way using the result rdd of local tiling as input
          val (zoom, reprojectedRdd): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
            TileLayerRDD(tiledRdd, metadata)
              .reproject(LatLng, ld, Bilinear)

          //ingest to HBase
          val fileReGrid_TileDataIdMy = fileReGrid_TileDataId
          val gcTileMetaDataHashMapMy = gcTileMetaDataHashMap
          val tableName=hbaseTableName
          val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
          reprojectedRdd.foreach(layer => {
            val key = layer._1
            val tile = layer._2
            val tileArrByte = tile.toBytes()
            val zorderIndex = keyIndex.toIndex(key)
            var resolutionKey = size.toString

            val fileReGrid = filePath + "_" + resolutionKey + "_" + zorderIndex
            val rowKey = TileUtil.strConvert(fileReGrid_TileDataIdMy.get(fileReGrid))
            if (!rowKey.isEmpty) {
              val tileMetaData = TileUtil.strConvert(gcTileMetaDataHashMapMy.get(rowKey))
              HbaseUtil.insertData(tableName, rowKey, "rasterData", "tile", tileArrByte)
              HbaseUtil.insertData(tableName, rowKey, "rasterData", "metaData", tileMetaData.getBytes())
            }
          })
        }
      })
    }
    jobExecutor.shutdown()
  }

  /**
   * Generate OsGeo TMS pyramid tiles of png formatand ingest
   * them into HBase. Level 0 of OsGeo TMS contains 2 tiles.
   *
   * @param sc spark context
   * @param jobExecutor job executor of java multi threads
   * @param configPath the path of config file
   */
  def ingestOsGeoTiles(implicit sc: SparkContext, jobExecutor: ExecutorService, configPath: String): Unit = {
    import scala.io.Source
    //read parameters in config file
    val source = Source.fromFile(configPath, "UTF-8")
    val lines = source.getLines().toArray
    val filePaths = lines(0).split("\t")
    val fileReGrid_TileDataId: Map[String, String] = lines(1).split("\t").map { x =>
      val key_value = x.split("->")
      (key_value(0), key_value(1))
    }.toMap
    val gcTileMetaDataHashMap: Map[String, String] = lines(2).split("\t").map { x =>
      val key_value = x.split("->")
      (key_value(0), key_value(1))
    }.toMap
    val tileSize = lines(3).toInt
    source.close()

    //tiling and ingest
    ingestOsGeoTiles(sc, jobExecutor, filePaths, fileReGrid_TileDataId, gcTileMetaDataHashMap)
  }

  /**
   * Generate OsGeo TMS pyramid tiles of png formatand ingest
   * them into HBase. Level 0 of OsGeo TMS contains 2 tiles.
   *
   * @param sc spark context
   * @param jobExecutor job executor of java multi threads
   * @param filePaths files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   */
  def ingestOsGeoTiles(implicit sc: SparkContext,
                       jobExecutor: ExecutorService,
                       filePaths: Array[String],
                       fileReGrid_TileDataId: Map[String, String],
                       gcTileMetaDataHashMap: Map[String, String]): Unit = {
    //use java multi-threads tech, each thread processes 1 band
    val arr = filePaths
    for (path <- arr) {
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          val filePath = path
          //read as RDD[(ProjectedExtent, Tile)]
          val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(filePath)

          //set local layout scheme and collect rdd metadata based on the scheme
          val localLayoutScheme = FloatingLayoutScheme(512)
          val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
            rdd.collectMetadata[SpatialKey](localLayoutScheme)

          //set local tiling configuration
          val tilerOptions =
            Tiler.Options(
              resampleMethod = Bilinear,
              partitioner = new HashPartitioner(108) //new HashPartitioner(rdd.partitions.length)
            )

          //local tiling
          val tiledRdd = rdd.tileToLayout[SpatialKey](metadata, tilerOptions)

          val extent = Extent(-180, -90, 180, 90)
          //OsGeo TMS level range
          for (zoo <- 0 to 9) {
            //set tiling configuration of the level
            val colNum = Math.pow(2, zoo + 1).toInt
            val rowNum = Math.pow(2, zoo).toInt
            val tl = TileLayout(colNum, rowNum, 256, 256)
            val ld = LayoutDefinition(extent, tl)

            //global tiling in distributed way using the result rdd of local tiling as input
            val (zoom, reprojectedRdd): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
              TileLayerRDD(tiledRdd, metadata)
                .reproject(LatLng, ld, Bilinear)
            val cacheReprojectedRdd = reprojectedRdd.cache()

            //set color
            val histogram = cacheReprojectedRdd.stitch().tile.histogramDouble()
            val colorMap = ColorMap.fromQuantileBreaks(histogram, ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))

            //ingest to HBase
            val fileReGrid_TileDataIdMy = fileReGrid_TileDataId
            val gcTileMetaDataHashMapMy = gcTileMetaDataHashMap
            val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
            cacheReprojectedRdd.foreach(layer => {
              val key = layer._1
              val tile = layer._2
              val tileArrByte = tile.renderPng(colorMap).bytes
              val col = key.col
              val osGeoRow = (Math.pow(2, zoo) - key.row - 1).toInt
              val osGeoSpatialKey = SpatialKey(col, osGeoRow)
              val zorderIndex = keyIndex.toIndex(osGeoSpatialKey)
              val resolutionKey = zoo.toString
              val fileReGrid = filePath + "_" + resolutionKey + "_" + zorderIndex
              val rowKey = TileUtil.strConvert(fileReGrid_TileDataIdMy.get(fileReGrid))
              if (!rowKey.isEmpty()) {
                val tileMetaData = TileUtil.strConvert(gcTileMetaDataHashMapMy.get(rowKey))
                HbaseUtil.insertData("hbase_raster", rowKey, "tile", "rasterData", tileArrByte)
                HbaseUtil.insertData("hbase_raster", rowKey, "rasterData", "metaData", tileMetaData.getBytes())
              }
            })
          }
        }
      })
    }
    jobExecutor.shutdown()
  }

  /**
   * Create ingestion config file.
   *
   * @param configPath the path of a parameter file
   * @param filePaths files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   * @param tileSize tile size
   */
  def createGlobalCustomTilingConfig(configPath: String,
                                     filePaths: Array[String],
                                     fileReGrid_TileDataId: Map[String, String],
                                     gcTileMetaDataHashMap: Map[String, String],
                                     tileSize: Int): Unit = {
    import java.io.PrintWriter
    val out = new PrintWriter(configPath)
    val sb = new mutable.StringBuilder()
    filePaths.foreach { filePath =>
      sb.append(filePath + ",")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()

    fileReGrid_TileDataId.foreach { map =>
      val key = map._1
      val value = map._2
      sb.append(key + "->" + value + ",")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()

    gcTileMetaDataHashMap.foreach { map =>
      val key = map._1
      val value = map._2
      sb.append(key + "->" + value + ",")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()

    out.println(tileSize)
    out.close()
  }

}
