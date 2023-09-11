package whu.edu.cn.geocube.core.raster.ingest

import geotrellis.layer._
import geotrellis.raster.resample._
import geotrellis.raster.{IntUserDefinedNoDataCellType, MultibandTile, Tile, TileLayout}
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.store.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.raster.render.{ColorMap, ColorRamp}
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import java.util.concurrent.{ExecutorService, Executors}
import org.apache.spark._
import org.apache.spark.rdd._
import whu.edu.cn.geocube.util.{HbaseUtil, TileUtil}

import scala.io.StdIn
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.geocube.conf.Address
import whu.edu.cn.geocube.core.vector.grid.GridConf

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
 * */
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
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    //    val serConf = new SerializableConfiguration(conf) // it would ser fine
    //use java multi-threads tech, each thread processes 1 band
    val jobExecutor = Executors.newFixedThreadPool(args(2).toInt)

    //tiling and ingest
    try {
      val tilingType = args(1)
      if (tilingType.equals("custom")) { // tile for analysis
        ingestGlobalCustomTiles(sc, jobExecutor, args(0), args(3),
          args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt, args(8).toInt, args(9).toInt)
      } else if (tilingType.equals("TMS")) { //TMS tile for visualization
        ingestOsGeoTiles(sc, jobExecutor, args(0), args(3))
      }
      //      globalDcTiling(sc, jobExecutor,args(0),args(1))
    } finally {
      var flag = true
      while (flag) {
        if (jobExecutor.isTerminated)
          flag = false
        Thread.sleep(200)
      }
      sc.stop()
    }

    ////    val inputFile =  "file:///input\\data.txt"
    //    val inputFile =  args(0)
    //    val textFile = sc.textFile(inputFile)
    //    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //    wordCount.foreach(println)

  }

  /**
   * Generate custom/analysis tiles based on the custom layout definition
   * and ingest them into HBase.
   *
   * @param sc          spark context
   * @param jobExecutor job executor of java multi threads
   * @param configPath  the path of config file
   */
  def ingestGlobalCustomTiles(implicit sc: SparkContext, jobExecutor: ExecutorService, configPath: String, hbaseTableName: String,
                              gridDimX: Int, gridDimY: Int, minX: Int, minY: Int, maxX: Int, maxY: Int): Unit = {
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
    val resKey = lines(3).toInt
    val tileSize = lines(4).toInt
    source.close()

    //tiling and ingest
    ingestGlobalCustomTiles(sc, jobExecutor, filePaths, fileReGrid_TileDataId, gcTileMetaDataHashMap,
      resKey, tileSize, hbaseTableName, gridDimX, gridDimY, minX, minY, maxX, maxY)
  }

  /**
   * Generate custom/analysis tiles based on the custom layout definition
   * and ingest them into HBase.
   *
   * @param sc                    spark context
   * @param jobExecutor           job executor of java multi threads
   * @param filePaths             files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   * @param resKey                tile size
   * @param tileSize              tile size
   */
  def ingestGlobalCustomTiles(implicit sc: SparkContext,
                              jobExecutor: ExecutorService,
                              filePaths: Array[String],
                              fileReGrid_TileDataId: Map[String, String],
                              gcTileMetaDataHashMap: Map[String, String],
                              resKey: Int,
                              tileSize: Int,
                              hbaseTableName: String,
                              gridDimX: Int, gridDimY: Int, minX: Int, minY: Int, maxX: Int, maxY: Int): Unit = {
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
          val gridDX = gridDimX
          val gridDY = gridDimY
          val minXcoor = minX
          val minYcoor = minY
          val maxXcoor = maxX
          val maxYcoor = maxY
          //          val extent = Extent(-180, -90, 180, 90) //global extent under LatLng
          //          val tl = TileLayout(360, 180, size, size) //tile:1°×1°, 1024×1024
          val extent = Extent(minXcoor, minYcoor, maxXcoor, maxYcoor) //local extent under LatLng
          val tl = TileLayout(gridDX, gridDY, size, size)
          val ld = LayoutDefinition(extent, tl)
          println(extent)
          println(tl)
          //global tiling in distributed way using the result rdd of local tiling as input
          val (zoom, reprojectedRdd): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
            TileLayerRDD(tiledRdd, metadata)
              .reproject(LatLng, ld, Bilinear)

          //ingest to HBase
          val fileReGrid_TileDataIdMy = fileReGrid_TileDataId
          val gcTileMetaDataHashMapMy = gcTileMetaDataHashMap
          val tableName = hbaseTableName
          val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
          var resolutionKey = resKey.toString
          reprojectedRdd.foreach(layer => {
            val key = layer._1
            val tile = layer._2
            val tileArrByte = tile.toBytes()
            val zorderIndex = keyIndex.toIndex(key)
            //            var resolutionKey = resKey.toString

            val fileReGrid = filePath + "_" + resolutionKey + "_" + zorderIndex
            val rowKey = TileUtil.strConvert(fileReGrid_TileDataIdMy.get(fileReGrid))
            print("rowKey is:" + rowKey)
            if (!rowKey.isEmpty) {
              println("insert rowKey!")
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
   * Generate OsGeo TMS pyramid tiles of png format and ingest
   * them into HBase. Level 0 of OsGeo TMS contains 2 tiles.
   *
   * @param sc          spark context
   * @param jobExecutor job executor of java multi threads
   * @param configPath  the path of config file
   */
  def ingestOsGeoTiles(implicit sc: SparkContext, jobExecutor: ExecutorService, configPath: String, hbaseTableName: String): Unit = {
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
    val resKey = lines(3).toInt
    val tileSize = lines(4).toInt
    source.close()

    //tiling and ingest
    ingestOsGeoTiles(sc, jobExecutor, filePaths, fileReGrid_TileDataId, gcTileMetaDataHashMap, hbaseTableName)
  }

  /**
   * Generate OsGeo TMS pyramid tiles of png formatand ingest
   * them into HBase. Level 0 of OsGeo TMS contains 2 tiles.
   *
   * @param sc                    spark context
   * @param jobExecutor           job executor of java multi threads
   * @param filePaths             files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   */
  def ingestOsGeoTiles(implicit sc: SparkContext,
                       jobExecutor: ExecutorService,
                       filePaths: Array[String],
                       fileReGrid_TileDataId: Map[String, String],
                       gcTileMetaDataHashMap: Map[String, String],
                       hbaseTableName: String): Unit = {
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
            val tableName = hbaseTableName
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
                HbaseUtil.insertData(tableName, rowKey, "rasterData", "tile", tileArrByte)
                HbaseUtil.insertData(tableName, rowKey, "rasterData", "metaData", tileMetaData.getBytes())
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
   * @param configPath            the path of a parameter file
   * @param filePaths             files need to be ingested
   * @param fileReGrid_TileDataId tile with its id
   * @param gcTileMetaDataHashMap tile id with its metadata
   * @param tileSize              tile size
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

  def writeGlobalDcTilingHbaseParams(paramsPath: String,
                                     filePaths: Array[String],
                                     fileReGrid_TileDataId: Map[String, String],
                                     gcTileMetaDataHashMap: Map[String, String],
                                     resKey: Int,
                                     tileSize: Int): Unit = {
    import java.io.PrintWriter
    val out = new PrintWriter(paramsPath)
    val sb = new mutable.StringBuilder()
    filePaths.foreach { filePath =>
      sb.append(filePath + "\t")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()

    fileReGrid_TileDataId.foreach { map =>
      val key = map._1
      val value = map._2
      sb.append(key + "->" + value + "\t")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()

    gcTileMetaDataHashMap.foreach { map =>
      val key = map._1
      val value = map._2
      sb.append(key + "->" + value + "\t")
    }
    sb.deleteCharAt(sb.length - 1)
    println(sb)
    out.println(sb.toString())
    sb.clear()
    out.println(resKey)
    out.println(tileSize)
    out.close()
  }

  def GFTile(filePath: String,
             bandGrid_TileDataId: Map[String, String],
             gcTileMetaDataHashMap: Map[String, String],
             resKey: Int,
             tileSize: Int,
             hbaseTableName: String,
             gridDimX: Int, gridDimY: Int, minX: Int, minY: Int, maxX: Int, maxY: Int): Unit = {
    Address.get
    val ip = Address.ip
    val conf = new SparkConf()
      //      .setMaster("local[*]")
      .setMaster("spark://" + ip + ":7077")
      .setAppName("GF Tile Divide Using Spark")
      .set("spark.driver.memory", "32g")
      .set("spark.executor.memory", "32g")
      .setJars(Array("/home/geocube/environment_test/geocube_ogc/geocube-core/geocube-core.jar")) //需要加上去的
      //      .set("spark.cores.max", "16")
      //      .set("spark.executor.cores", "1")
      .set("spark.kryoserializer.buffer.max", "256m")
      .set("spark.rpc.message.maxSize", "512")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")


    val sc = new SparkContext(conf)
    println("bandGrid_TileDataId.size:" + bandGrid_TileDataId.size)

    try {
      //      globalGFPyramidTiling(sc)
      //      globalWebMercatorTMSPyramidTilingGF(sc)
      globalDcTilingGFHbase(sc, filePath, bandGrid_TileDataId, gcTileMetaDataHashMap, resKey, tileSize, hbaseTableName, gridDimX, gridDimY, minX, minY, maxX, maxY)
      println("Hit enter to exit")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def globalDcTilingGFHbase(sc: SparkContext, filePath: String, bandGrid_TileDataId: Map[String, String], gcTileMetaDataHashMap: Map[String, String],
                            resKey: Int,
                            tileSize: Int,
                            hbaseTableName: String,
                            gridDimX: Int, gridDimY: Int, minX: Int, minY: Int, maxX: Int, maxY: Int): Unit = {
    val rdd: RDD[(ProjectedExtent, MultibandTile)] = sc.hadoopMultibandGeoTiffRDD(filePath)
    val localLayoutScheme = FloatingLayoutScheme(512)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      rdd.collectMetadata[SpatialKey](localLayoutScheme)


    println("crs:" + metadata.crs)
    println("extent:" + metadata.extent)
    println("bounds:" + metadata.bounds.toString)
    println("cellType:" + metadata.cellType.toString())
    println("layout:" + metadata.layout.toString())

    val tilerOptions =
      Tiler.Options(
        resampleMethod = Bilinear,
        partitioner = new HashPartitioner(rdd.partitions.length)
      )
    val tiledRdd = rdd.tileToLayout[SpatialKey](metadata, tilerOptions).cache()

    val size = tileSize
    val gridDX = gridDimX
    val gridDY = gridDimY
    val minXcoor = minX
    val minYcoor = minY
    val maxXcoor = maxX
    val maxYcoor = maxY
    //          val extent = Extent(-180, -90, 180, 90) //global extent under LatLng
    //          val tl = TileLayout(360, 180, size, size) //tile:1°×1°, 1024×1024
    val extent = Extent(minXcoor, minYcoor, maxXcoor, maxYcoor) //local extent under LatLng
    val tl = TileLayout(gridDX, gridDY, size, size)
    val ld = LayoutDefinition(extent, tl)
    println(extent)
    print(ld)
    val (zoom, reprojectedRdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiledRdd, metadata)
        .reproject(LatLng, ld, Bilinear)
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    val bandGrid_TileDataIdMy = bandGrid_TileDataId
    val gcTileMetaDataHashMapMy = gcTileMetaDataHashMap
    val tableName = hbaseTableName
    var resolutionKey = resKey.toString
    print("resolutionKey is " + resolutionKey)
    reprojectedRdd.foreach(layer => {
      val key = layer._1
      val tile = layer._2
      println("tile.bandCount" + tile.bandCount)
      for (i <- 0 until tile.bandCount) {
        //保存到hbase中
        val tileArrByte = tile.band(i).toBytes()
        val zorderIndex = keyIndex.toIndex(SpatialKey(key.col, key.row))
        val bandGrid = i.toString + "_" + resolutionKey + "_" + zorderIndex
        val rowKey = TileUtil.strConvert(bandGrid_TileDataIdMy.get(bandGrid))
        print("rowKey is:" + rowKey)
        if (!rowKey.isEmpty) {
          println("insert rowKey!")
          val tileMetaData = TileUtil.strConvert(gcTileMetaDataHashMapMy.get(rowKey))
          HbaseUtil.insertData(tableName, rowKey, "rasterData", "tile", tileArrByte)
          HbaseUtil.insertData(tableName, rowKey, "rasterData", "metaData", tileMetaData.getBytes())
        }


      }


    })
    HbaseUtil.close()
  }

  /** *
   * 根据切片规则获得影像覆盖瓦片编号
   *
   * @param wkt
   * @param _gridDimX
   * @param _gridDimY
   * @param minX
   * @param minY
   * @param maxX
   * @param maxY
   * @param pixNumX
   * @param pixNumY
   * @return
   */
  def getTilesCode(wkt: String, _gridDimX: Int, _gridDimY: Int,
                   minX: Double, minY: Double, maxX: Double, maxY: Double, pixNumX: Int, pixNumY: Int): Array[String] = {
    var gridsArray: ArrayBuffer[String] = new ArrayBuffer[String]()
    val gridConf: GridConf = new GridConf(_gridDimX, _gridDimY, Extent(minX, minY, maxX, maxY))

    val geom = new WKTReader().read(wkt)
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val mbr = (geom.getEnvelopeInternal.getMinX, geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)

    val tl = TileLayout(gridDimX.toInt, gridDimY.toInt, pixNumX, pixNumY) //tile:1°×1°, 4000×4000
    val ld = LayoutDefinition(extent, tl)
    //    ld.reproject()
    //遍历网格获得相交的网格

    for (i <- _xmin until _xmax; j <- _ymin until _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        //        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        val index = keyIndex.toIndex(SpatialKey(i, j))
        gridsArray.append(index.toString())
      }
    }
    gridsArray.toArray
  }

  def getGeoTrellisTilesCode(wkt: String, _gridDimX: Int, _gridDimY: Int,
                             minX: Double, minY: Double, maxX: Double, maxY: Double, pixNumX: Int, pixNumY: Int): Array[String] = {
    var gridsArray: ArrayBuffer[String] = new ArrayBuffer[String]()
    val gridConf: GridConf = new GridConf(_gridDimX, _gridDimY, Extent(minX, minY, maxX, maxY))

    val geom = new WKTReader().read(wkt)
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    //通过wkt获取最小外接矩形
    val mbr = (geom.getEnvelopeInternal.getMinX, geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)

    println("_xmin:" + _xmin)
    println("_xmax:" + _xmax)
    println("_ymin:" + _ymin)
    println("_ymax:" + _ymax)

    val tl = TileLayout(gridDimX.toInt, gridDimY.toInt, pixNumX, pixNumY) //tile:1°×1°, 4000×4000
    val ld = LayoutDefinition(extent, tl)
    //    ld.reproject()
    //遍历网格获得相交的网格

    for (i <- _xmin until _xmax; j <- _ymin until _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      //      val gridExtent = ld.mapTransform.keyToExtent(i, j)
      if (gridExtent.intersects(geom)) {
        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        //        val index = keyIndex.toIndex(SpatialKey(i, j))
        gridsArray.append(index.toString())
      }
    }
    gridsArray.toArray
  }

  def getAllTilesCode(_gridDimX: Int, _gridDimY: Int,
                      minX: Double, minY: Double, maxX: Double, maxY: Double, pixNumX: Int, pixNumY: Int): Array[(String, (Int, Int, Extent))] = {
    var gridsArray: ArrayBuffer[(String, (Int, Int, Extent))] = new ArrayBuffer[(String, (Int, Int, Extent))]()

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    //遍历网格获得相交的网格
    val extent = Extent(minX, minY, maxX, maxY)
    //为什么要瓦片像素
    val tl = TileLayout(_gridDimX, _gridDimY, pixNumX, pixNumY) //tile:1°×1°, 4000×4000
    val ld = LayoutDefinition(extent, tl)
    for (i <- 0 until _gridDimX; j <- 0 until _gridDimY) {
      //左上角?
      val geotrellis_j = _gridDimY - 1 - j
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      val index = keyIndex.toIndex(SpatialKey(i, j))
      val grid = (index.toString(), (i, j, gridExtent))
      gridsArray.append(grid)
    }
    gridsArray.toArray
  }

  def getAllGeoTrellisTilesCode(_gridDimX: Int, _gridDimY: Int,
                                minX: Double, minY: Double, maxX: Double, maxY: Double, pixNumX: Int, pixNumY: Int): Array[(String, (Int, Int, Extent))] = {
    var gridsArray: ArrayBuffer[(String, (Int, Int, Extent))] = new ArrayBuffer[(String, (Int, Int, Extent))]()

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    //遍历网格获得相交的网格
    val extent = Extent(minX, minY, maxX, maxY)
    val tl = TileLayout(_gridDimX, _gridDimY, pixNumX, pixNumY) //tile:1°×1°, 4000×4000
    val ld = LayoutDefinition(extent, tl)
    for (i <- 0 until _gridDimX; j <- 0 until _gridDimY) {
      val gridExtent = ld.mapTransform.keyToExtent(i, j)
      val index = keyIndex.toIndex(SpatialKey(i, j))
      val grid = (index.toString(), (i, j, gridExtent))
      gridsArray.append(grid)
    }
    gridsArray.toArray
  }


  /**
   * Generate Datacube tiles concurently using landsat/Gaofen images based on the custom layout definition,
   * and render and save these tiles as jpg format.
   *
   * @param sc A SparkContext
   */
  def globalDcTiling(implicit sc: SparkContext, jobExecutor: ExecutorService, inputFile: String, outputDir: String): Unit = {
    val arr = Array((inputFile, outputDir))
    for (path <- arr) {
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(path._1)


          val localLayoutScheme = FloatingLayoutScheme(512)

          val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
            rdd.collectMetadata[SpatialKey](localLayoutScheme)

          val tilerOptions =
            Tiler.Options(
              resampleMethod = Bilinear,
              partitioner = new HashPartitioner(18)
            )
          val tiledRdd = rdd.tileToLayout[SpatialKey](metadata, tilerOptions)
            .convert(IntUserDefinedNoDataCellType(0))
          val extent = Extent(-180, -90, 180, 90) //global extent under LatLng
          val tl = TileLayout(360, 180, 256, 256) //tile:1°×1°, 1024×1024
          val ld = LayoutDefinition(extent, tl)

          val crs = metadata.crs
          val (zoom, reprojectedRdd): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
            TileLayerRDD(tiledRdd, metadata)
              .reproject(LatLng, ld, Bilinear)
          val cacheReprojectedRdd = reprojectedRdd.cache()
          val histogram = cacheReprojectedRdd.stitch().tile.histogramDouble()
          val colorMap = ColorMap.fromQuantileBreaks(histogram, ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))

          //          val colorRamp = ColorRamp(RGB(0,0,0), RGB(255,255,255))
          //            .stops(100)
          //            .setAlphaGradient(0xFF, 0xAA)

          val outputPath = path._2

          //渲染为jpg
          val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
          val tableName = "hbase_raster_test"
          //          reprojectedRdd.foreach(layer => {
          cacheReprojectedRdd.foreach(layer => {
            val key = layer._1
            val tile = layer._2
            val zorderIndex = keyIndex.toIndex(key)
            //            val dcTilesDir = new File(outputPath + "/DcTiles")
            //            if (!dcTilesDir.exists()) {
            //              dcTilesDir.mkdirs()
            //            }
            //            //            val layerPath = outputPath + "/DcTiles" + "/" + zorderIndex+ "_" + key.row + "_" + key.col + ".jpg"
            //            //            tile.renderJpg(colorRamp).write(layerPath)  //renderJpg可调用渲染方法，colorRamp为非必须参数
            //            val layerPath = outputPath + "/" + zorderIndex+ "_" + key.row + "_" + key.col + ".png"
            ////            print("layerPath"+layerPath)
            //            tile.renderPng(colorMap).write(layerPath)  //renderJpg可调用渲染方法，colorRamp为非必须参数
            val rowKey = key.row + "_" + key.col
            val tileArrByte = tile.toBytes()
            HbaseUtil.insertData(tableName, rowKey, "rasterData", "tile", tileArrByte)
            //            HbaseUtil.insertData(tableName,rowKey,"rasterData","metaData",tileMetaData.getBytes())
            //            val pngFile:Png= tile.renderPng()
            //            HadoopUtil.putHDFS(tile.toBytes(),layerPath)
            //            val tileextent=tile
            //
            //            GeoTiff(tile, tileextent, crs)
            //              .write(layerPath, serConf.value)
          })
          //          println("outputPath"+outputPath)
        }
      })
    }
    jobExecutor.shutdown()
  }


  def reprojectExtent(filePath: String): Extent = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Ls7 Tile Divide Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    val rdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(filePath)
    val localLayoutScheme = FloatingLayoutScheme(512)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      rdd.collectMetadata[SpatialKey](localLayoutScheme)
    val srcCrs = metadata.crs

    val srcExtent = metadata.extent
    println("srcExtent.xmin" + srcExtent.xmin)
    println("srcExtent.xmax" + srcExtent.xmax)
    println("srcExtent.ymin" + srcExtent.ymin)
    println("srcExtent.ymax" + srcExtent.ymax)
    val newExtent = srcExtent.reproject(srcCrs, LatLng)
    sc.stop()
    newExtent

  }

  def reprojectExtent(srcExtent: Extent, epsgCode: Int): Extent = {
    val srcCrs = CRS.fromEpsgCode(epsgCode)
    //    println("srcExtent.xmin"+srcExtent.xmin)
    //    println("srcExtent.xmax"+srcExtent.xmax)
    //    println("srcExtent.ymin"+srcExtent.ymin)
    //    println("srcExtent.ymax"+srcExtent.ymax)
    val newExtent = srcExtent.reproject(srcCrs, LatLng)
    newExtent

  }

  def strConvert[T](v: Option[T]): String = {
    if (v.isDefined)
      v.get.toString
    else
      ""
  }


}