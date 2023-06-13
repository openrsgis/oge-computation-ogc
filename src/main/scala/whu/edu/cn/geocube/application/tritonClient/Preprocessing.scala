package whu.edu.cn.geocube.application.tritonClient

import geotrellis.layer._
import geotrellis.spark._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.ResampleMethods.Bilinear
import geotrellis.raster.{CellType, Tile, TileLayout}
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file._
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.store.file.FileAttributeStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GDT_Byte
import whu.edu.cn.geocube.application.tritonClient.examples.GF2Example
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.util.TileSerializerCoverageUtil.deserializeTileData

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.ResultSet
import scala.collection.JavaConverters.asScalaBuffer
import scala.collection.immutable.Range
import scala.collection.mutable.ListBuffer


object Preprocessing {

  def queryOPT(): (String, String) = {
    var metaData: (String, String) = (null, null)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select path, crs from oge_image where image_id = 505118"
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())
        while (extentResults.next()) {
          val path = extentResults.getString("path")
          val srcID = extentResults.getString("crs")
          metaData = (path, srcID)
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
    metaData
  }

  def querySAR(): (String, String) = {
    var metaData: (String, String) = (null, null)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select path, crs from oge_image where image_id = 505117"
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())
        while (extentResults.next()) {
          val path = extentResults.getString("path")
          val srcID = extentResults.getString("crs")
          metaData = (path, srcID)
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
    metaData
  }



  def queryGF2(): (String, String) = {
    var metaData: (String, String) = (null, null)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select path, crs from oge_image where image_id = 1"

        println(sql)
        val extentResults = statement.executeQuery(sql.toString())


        while (extentResults.next()) {
          val path = extentResults.getString("path")
          val srcID = extentResults.getString("crs")
          metaData = (path, srcID)
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")
    metaData
  }

//  def load(implicit sc: SparkContext, path: String, crs: String, geom: String): Long = {
//    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
//      t.toDouble
//    }).to[ListBuffer]
//    val query_extent = new Array[Double](4)
//    query_extent(0) = geomReplace(0)
//    query_extent(1) = geomReplace(1)
//    query_extent(2) = geomReplace(2)
//    query_extent(3) = geomReplace(3)
//    println("crs = " + crs)
//    val tilesMetaData = Tiffheader_parse_DL.tileQuery(-1, path, null, crs, null, query_extent)
//    println(tilesMetaData.size())
//    val tile_srch: ListBuffer[(Array[Float], Int, Int)] = new ListBuffer[(Array[Float], Int, Int)]
//    val tile_srch_origin: ListBuffer[(Array[Byte], Int, Int)] = new ListBuffer[(Array[Byte], Int, Int)]
//    for (i <- Range(0, tilesMetaData.size(), 3)) {
//      val tile1 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i))
//      val tile2 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i + 1))
//      val tile3 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i + 2))
//      println("tile1.getTilebuf.length=" + tile1.getTilebuf.length)
//      println("tile1.getTilebuf.size=" + tile1.getTilebuf.size)
//      val tileResult = GF2Example.processOneTile(GF2Example.byteToFloat(tile1.getTilebuf, tile2.getTilebuf, tile3.getTilebuf))
//      tile_srch += Tuple3(tileResult, tile1.getRow, tile1.getCol)
//      println("tileResult.size=" + tileResult.size)
//      tile_srch_origin += Tuple3(tile1.getTilebuf, tile1.getRow, tile1.getCol)
//      tile_srch_origin += Tuple3(tile2.getTilebuf, tile2.getRow, tile2.getCol)
//      tile_srch_origin += Tuple3(tile3.getTilebuf, tile3.getRow, tile3.getCol)
//    }
//    writePNG(tile_srch)
//    //    writePNGOrigin(tile_srch_origin)
//  }

  def writePNGOrigin(tile_srch: ListBuffer[(Array[Byte], Int, Int)]): Unit = {
    val cols = (tile_srch.last._3 - tile_srch.head._3 + 1) * 512
    val rows = (tile_srch.last._2 - tile_srch.head._2 + 1) * 512

    val dstPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogedemooutput/DLOrigin.png"
    gdal.AllRegister()
    val dr = gdal.GetDriverByName("PNG")
    val dr1 = gdal.GetDriverByName("MEM")
    val dm = dr1.Create(dstPath, cols, rows, 3, GDT_Byte)

    for (i <- Range(0, tile_srch.size, 3)) {
      for (j <- tile_srch.head._2 to tile_srch.last._2) {
        if (tile_srch(i)._2 == j) {
          val yoff = (tile_srch(i)._3 - tile_srch.head._3) * 512
          val xoff = (tile_srch(i)._2 - tile_srch.head._2) * 512
          dm.GetRasterBand(1).WriteRaster(yoff, xoff, 512, 512, 512, 512, GDT_Byte, tile_srch(i)._1)
          dm.GetRasterBand(2).WriteRaster(yoff, xoff, 512, 512, 512, 512, GDT_Byte, tile_srch(i + 1)._1)
          dm.GetRasterBand(3).WriteRaster(yoff, xoff, 512, 512, 512, 512, GDT_Byte, tile_srch(i + 2)._1)
        }
      }
    }
    dr.CreateCopy(dstPath, dm)
  }


  def writePNG(tile_srch: ListBuffer[(Array[Float], Int, Int)]): Long = {
    val cols = (tile_srch.last._3 - tile_srch.head._3 + 1) * 512
    val rows = (tile_srch.last._2 - tile_srch.head._2 + 1) * 512
    val time = System.currentTimeMillis()
    val dstPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogeoutput/DL_" + time + ".png"
    gdal.AllRegister()
    val dr = gdal.GetDriverByName("PNG")
    val dr1 = gdal.GetDriverByName("MEM")
    val dm = dr1.Create(dstPath, cols, rows, 1, GDT_Byte)

    for (i <- tile_srch.indices) {
      for (j <- tile_srch.head._2 to tile_srch.last._2) {
        if (tile_srch(i)._2 == j) {
          val yoff = (tile_srch(i)._3 - tile_srch.head._3) * 512
          val xoff = (tile_srch(i)._2 - tile_srch.head._2) * 512

          val band = new Array[Byte](512 * 512)
          for (k <- 0 until 512 * 512) {
            if (tile_srch(i)._1(k) > tile_srch(i)._1(k + 512 * 512)) band(k) = 0
            else band(k) = -1
          }

          dm.GetRasterBand(1).WriteRaster(yoff, xoff, 512, 512, 512, 512, GDT_Byte, band)
        }
      }
    }
    dr.CreateCopy(dstPath, dm)
    time
  }


//  def loadOnTheFly(implicit sc: SparkContext, level: Int, path: String, crs: String, geom: String, fileName: String): Unit = {
//    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
//      t.toDouble
//    }).to[ListBuffer]
//    val query_extent = new Array[Double](4)
//    query_extent(0) = geomReplace(0)
//    query_extent(1) = geomReplace(1)
//    query_extent(2) = geomReplace(2)
//    query_extent(3) = geomReplace(3)
//    println("crs = " + crs)
//    val tilesMetaData = Tiffheader_parse_DL.tileQuery(level, path, null, crs, null, query_extent)
//    println(tilesMetaData.size())
//    val tilesRdd = sc.makeRDD(asScalaBuffer(tilesMetaData))
//      .groupBy(t => (t.getRow, t.getCol))
//    val tile_srch = tilesRdd.map(t => {
//      val tileArray = t._2.toArray
//      if (tileArray.length == 3) {
//        val tile1 = Tiffheader_parse_DL.getTileBuf(tileArray(0))
//        val tile2 = Tiffheader_parse_DL.getTileBuf(tileArray(1))
//        val tile3 = Tiffheader_parse_DL.getTileBuf(tileArray(2))
//        val tileResult = GF2Example.processOneTile(GF2Example.byteToFloat(tile1.getTilebuf, tile2.getTilebuf, tile3.getTilebuf))
//        (tileResult, tile1.getRow, tile1.getCol, tile1.getP_bottom_left, tile1.getP_upper_right)
//      }
//      else {
//        null
//      }
//    })
//    writePNGOnTheFly(sc, tile_srch, fileName)
//  }

  def writePNGOnTheFly(implicit sc: SparkContext, tile_srch: RDD[(Array[Float], Int, Int, Array[Double], Array[Double])], fileName: String): Unit = {
    val time = System.currentTimeMillis()
    val tileMetaB = tile_srch.map(t => (t._2, t._3, t._4(0), t._4(1), t._5(0), t._5(1)))
    val tileMetaMinB = tileMetaB.reduce((a, b) => {
      (Math.min(a._1, b._1), Math.min(a._2, b._2), Math.min(a._3, b._3), Math.min(a._4, b._4), Math.min(a._5, b._5), Math.min(a._6, b._6))
    })
    val tileMetaMin = (tileMetaMinB._1, tileMetaMinB._2, Math.min(tileMetaMinB._3, tileMetaMinB._5), Math.min(tileMetaMinB._4, tileMetaMinB._6))
    val tileMetaMaxB = tileMetaB.reduce((a, b) => {
      (Math.max(a._1, b._1), Math.max(a._2, b._2), Math.max(a._3, b._3), Math.max(a._4, b._4), Math.max(a._5, b._5), Math.max(a._6, b._6))
    })
    val tileMetaMax = (tileMetaMaxB._1, tileMetaMaxB._2, Math.max(tileMetaMaxB._3, tileMetaMaxB._5), Math.max(tileMetaMaxB._4, tileMetaMaxB._6))
    val tiled = tile_srch.map(t => {
      val band = new Array[Byte](512 * 512)
      for (k <- 0 until 512 * 512) {
        if (t._1(k) > t._1(k + 512 * 512)) band(k) = 0
        else band(k) = -1
      }
      val tile = deserializeTileData("", band, 256, "uint8")
      val k = SpatialKey(t._3 - tileMetaMin._2, t._2 - tileMetaMin._1)
      (k, tile)
    })

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val extent = geotrellis.vector.Extent(tileMetaMin._4, tileMetaMin._3, tileMetaMax._4, tileMetaMax._3)
    val tl = TileLayout(tileMetaMax._2 - tileMetaMin._2 + 1, tileMetaMax._1 - tileMetaMin._1 + 1, 512, 512)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName("uint8")
    val crs = CRS.fromEpsgCode(4490)
    val bounds = Bounds(SpatialKey(0, 0), SpatialKey(tileMetaMax._2 - tileMetaMin._2, tileMetaMax._1 - tileMetaMin._1))
    val rasterMetaData = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiled, rasterMetaData)
        .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

    val appID = sc.applicationId
    val outputPath = "/home/geocube/oge/on-the-fly"
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)
    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)
    val layerIDAll = appID + "-layer-" + time + "_" + "origin" + "-" + "0" + "-" + "255"
    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      if (z == zoom) {
        val layerId = LayerId(layerIDAll, z)
        // If the layer exists already, delete it out before writing
        if (attributeStore.layerExists(layerId)) {
          new FileLayerManager(attributeStore).delete(layerId)
        }
        writer.write(layerId, rdd, ZCurveKeyIndexMethod)
      }
    }
    val writeFile = new File(fileName)
    val writerOutput = new BufferedWriter(new FileWriter(writeFile))
    val outputString = "{\"vectorCube\":[],\"rasterCube\":[],\"table\":[], \"vector\":[], \"raster\":[{\"url\":\"http://oge.whu.edu.cn/api/oge-tms/" + layerIDAll + "/{z}/{x}/{y}\"}]}"
    writerOutput.write(outputString)
    writerOutput.close()
  }
}