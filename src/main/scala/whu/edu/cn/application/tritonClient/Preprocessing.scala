package whu.edu.cn.application.tritonClient

import org.gdal.gdal.{Dataset, Driver, gdal}
import org.gdal.gdalconst.gdalconstConstants.GDT_Byte
import whu.edu.cn.application.oge.Tiffheader_parse
import whu.edu.cn.application.tritonClient.examples.GF2Example
import whu.edu.cn.util.PostgresqlUtil

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.ResultSet
import java.util
import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Range

object Preprocessing {
  def queryGF2(): (String, String) = {
    var metaData: (String, String) = (null, null)
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection()
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

  def load(path: String, crs: String, geom: String): Long = {
    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
      t.toDouble
    }).to[ListBuffer]
    val query_extent = new Array[Double](4)
    query_extent(0) = geomReplace(0)
    query_extent(1) = geomReplace(1)
    query_extent(2) = geomReplace(2)
    query_extent(3) = geomReplace(3)
    println("crs = " + crs)
    val tilesMetaData = Tiffheader_parse_DL.tileQuery(path, null, crs, null, query_extent)
    println(tilesMetaData.size())
    val tile_srch: ListBuffer[(Array[Float], Int, Int)] = new ListBuffer[(Array[Float], Int, Int)]
    val tile_srch_origin: ListBuffer[(Array[Byte], Int, Int)] = new ListBuffer[(Array[Byte], Int, Int)]
    for (i <- Range(0, tilesMetaData.size(), 3)) {
      val tile1 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i))
      val tile2 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i + 1))
      val tile3 = Tiffheader_parse_DL.getTileBuf(tilesMetaData.get(i + 2))
      println("tile1.getTilebuf.length="+tile1.getTilebuf.length)
      println("tile1.getTilebuf.size=" + tile1.getTilebuf.size)
      val tileResult = GF2Example.processOneTile(GF2Example.byteToFloat(tile1.getTilebuf, tile2.getTilebuf, tile3.getTilebuf))
      tile_srch += Tuple3(tileResult, tile1.getRow, tile1.getCol)
      println("tileResult.size=" + tileResult.size)
      tile_srch_origin+=Tuple3(tile1.getTilebuf, tile1.getRow,tile1.getCol)
      tile_srch_origin+=Tuple3(tile2.getTilebuf, tile2.getRow,tile2.getCol)
      tile_srch_origin+=Tuple3(tile3.getTilebuf, tile3.getRow,tile3.getCol)
    }
    writePNG(tile_srch)
//    writePNGOrigin(tile_srch_origin)
  }

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
    val dstPath = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogedemooutput/DL_" + time +".png"
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
}