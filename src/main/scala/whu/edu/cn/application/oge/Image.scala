package whu.edu.cn.application.oge

import whu.edu.cn.core.entity
import whu.edu.cn.core.entity.SpaceTimeBandKey
import Tiffheader_parse._
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.util.TileSerializerImage.deserializeTileData
import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, CellType, ColorMap, ColorRamp, Raster, Tile, TileLayout, UByteCellType, render}
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render.{Exact, Png, RGBA}
import geotrellis.vector.Extent
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.geotools.geometry.jts.JTS
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.locationtech.jts.geom.Coordinate
import whu.edu.cn.application.tritonClient.Preprocessing

import scala.math._
import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

object Image {
  def load(implicit sc: SparkContext, productName: String = null, sensorName: String = null, measurementName: String = null, dateTime: String = null, geom: String = null, crs: String = null, method: String = null): ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile]) = {
    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
      t.toDouble
    }).to[ListBuffer]
    val dateTimeArray = if(dateTime!=null) dateTime.replace("[", "").replace("]", "").split(",") else null
    val StartTime = if(dateTimeArray.length==2) dateTimeArray(0) else null
    val EndTime = if(dateTimeArray.length==2) dateTimeArray(1) else null
    var startTime = ""
    if (StartTime == null || StartTime == "") startTime = "2000-01-01 00:00:00"
    else startTime = StartTime
    var endTime = ""
    if (EndTime == null || EndTime == "") endTime = new Timestamp(System.currentTimeMillis).toString
    else endTime = EndTime

    var geom_str = ""
    if (geomReplace != null && geomReplace.length == 4) {
      val minx = geomReplace(0)
      val miny = geomReplace(1)
      val maxx = geomReplace(2)
      val maxy = geomReplace(3)
      geom_str = "POLYGON((" + minx + " " + miny + ", " + minx + " " + maxy + ", " + maxx + " " + maxy + ", " + maxx + " " + miny + "," + minx + " " + miny + "))"
    }
    val query_extent = new Array[Double](4)
    query_extent(0) = geomReplace(0)
    query_extent(1) = geomReplace(1)
    query_extent(2) = geomReplace(2)
    query_extent(3) = geomReplace(3)
    val metaData = query(productName, sensorName, measurementName, startTime, endTime, geom_str, crs)
    println("metadata.length = " + metaData.length)

    val imagePathRdd = sc.parallelize(metaData, metaData.length)
    val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = imagePathRdd.map(t => {
      val tiles = tileQuery(t._1, t._2, t._3, t._4, t._5, productName, query_extent)
      if (tiles.size() > 0) {
        asScalaBuffer(tiles)
      }
      else {
        mutable.Buffer.empty[RawTile]
      }
    }).persist()
    val tileNum = tileRDDNoData.map(t => t.length).reduce((x, y) => {
      x + y
    })
    println("tileNum = " + tileNum)
    val tileRDDFlat: RDD[RawTile] = tileRDDNoData.flatMap(t => t)
    val tileRDDReP = tileRDDFlat.repartition(tileNum).persist()
    (noReSlice(sc, tileRDDReP), tileRDDReP)
  }

  def noReSlice(implicit sc: SparkContext, tileRDDReP: RDD[RawTile]): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val extents = tileRDDReP.map(t => {
      (t.getP_bottom_leftX, t.getP_bottom_leftY, t.getP_upper_rightX, t.getP_upper_rightY)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant = tileRDDReP.map(t => {
      (t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime, t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), min(a._3, b._3), max(a._4, b._4), max(a._5, b._5), max(a._6, b._6))
    })
    val extent = geotrellis.vector.Extent(extents._1, extents._2, extents._3, extents._4)
    val firstTile = tileRDDReP.take(1)(0)
    val tl = TileLayout(Math.ceil((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.ceil((extents._4 - extents._2) / firstTile.getResolution / 256).toInt, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDType)
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(colRowInstant._1, colRowInstant._2, colRowInstant._3), SpaceTimeKey(colRowInstant._4, colRowInstant._5, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val tileRDD = tileRDDReP.map(t => {
      val tile = getTileBuf(t)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val phenomenonTime = sdf.parse(tile.getTime).getTime
      val measurement = tile.getMeasurement
      val rowNum = tile.getRow
      val colNum = tile.getCol
      val Tile = deserializeTileData("", tile.getTilebuf, 256, tile.getDType)
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum, rowNum, phenomenonTime), measurement)
      val v = Tile
      (k, v)
    })
    (tileRDD, tileLayerMetadata)
  }

  def mosaic(implicit sc: SparkContext, tileRDDReP: RDD[RawTile], method: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val extents = tileRDDReP.map(t => {
      (t.getP_bottom_leftX, t.getP_bottom_leftY, t.getP_upper_rightX, t.getP_upper_rightY)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), max(a._3, b._3), max(a._4, b._4))
    })
    val colRowInstant = tileRDDReP.map(t => {
      (t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime, t.getCol, t.getRow, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t.getTime).getTime)
    }).reduce((a, b) => {
      (min(a._1, b._1), min(a._2, b._2), min(a._3, b._3), max(a._4, b._4), max(a._5, b._5), max(a._6, b._6))
    })
    val extent = geotrellis.vector.Extent(extents._1, extents._2, extents._3, extents._4)
    println(extent)
    val firstTile = tileRDDReP.take(1)(0)
    val tl = TileLayout(Math.ceil((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.ceil((extents._4 - extents._2) / firstTile.getResolution / 256.0).toInt, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDType)
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(colRowInstant._1, colRowInstant._2, colRowInstant._3), SpaceTimeKey(colRowInstant._4, colRowInstant._5, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val rawtileRDD = tileRDDReP.map(t => {
      val tile = getTileBuf(t)
      t.setTile(deserializeTileData("", tile.getTilebuf, 256, tile.getDType))
      t
    })
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rawtileArray = rawtileRDD.collect()
    val RowSum = ld.tileLayout.layoutRows
    val ColSum = ld.tileLayout.layoutCols
    println("RowSum = " + RowSum)
    println("ColSum = " + ColSum)
    val tileBox = new ListBuffer[((Extent, SpaceTimeKey), List[RawTile])]
    for (i <- 0 until ColSum) {
      for (j <- 0 until RowSum) {
        val rawTiles = new ListBuffer[RawTile]
        val tileExtent = new Extent(extents._1 + i * 256 * tileLayerMetadata.cellSize.width, extents._4 - (j + 1) * 256 * tileLayerMetadata.cellSize.height, extents._1 + (i + 1) * 256 * tileLayerMetadata.cellSize.width, extents._4 - j * 256 * tileLayerMetadata.cellSize.height)
        for (rawtile <- rawtileArray) {
          if (Extent(rawtile.getP_bottom_leftX, rawtile.getP_bottom_leftY, rawtile.getP_upper_rightX, rawtile.getP_upper_rightY).intersects(tileExtent))
            rawTiles.append(rawtile)
        }
        val now = "1000-01-01 00:00:00"
        val date = sdf.parse(now).getTime
        tileBox.append(((tileExtent, SpaceTimeKey(i, j, date)), rawTiles.toList))
      }
    }
    val TileRDDUnComputed = sc.parallelize(tileBox, tileBox.length)
    method match {
      case "min" => {
        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
          val mutableArrayTile = ByteArrayTile(Array.fill[Byte](256 * 256)(Byte.MinValue), 256, 256, ByteConstantNoDataCellType).mutable
          if (t._2.length > 0) {
            val sizeWidth = tileLayerMetadata.cellSize.width
            val sizeHeight = tileLayerMetadata.cellSize.height
            val measurementName = t._2(0).getMeasurement
            for (rawtile <- t._2) {
              for (i <- 0 to 255) {
                for (j <- 0 to 255) {
                  val y = Math.ceil((t._1._1.ymax - (rawtile.getP_upper_rightY - sizeHeight * i)) / sizeHeight).toInt
                  val x = Math.ceil((rawtile.getP_bottom_leftX + sizeWidth * j - t._1._1.xmin) / sizeWidth).toInt
                  val value = rawtile.getTile.get(j, i).toByte
                  if (x >= 0 && x < 256 && y >= 0 && y < 256 && value > (-128) && value < 128 &&
                    (mutableArrayTile.get(x, y) == Int.MinValue || mutableArrayTile.get(x, y) == 0 || (value != 0 && value < mutableArrayTile.get(x, y))))
                    mutableArrayTile.set(x, y, value)
                }
              }
            }
            (entity.SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
          }
          else {
            (entity.SpaceTimeBandKey(t._1._2, firstTile.getMeasurement), mutableArrayTile)
          }
        })
        (TileRDDComputed, tileLayerMetadata)
      }
      case "max" => {
        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
          val mutableArrayTile = ByteArrayTile(Array.fill[Byte](256 * 256)(Byte.MinValue), 256, 256, ByteConstantNoDataCellType).mutable
          if (t._2.length > 0) {
            val sizeWidth = tileLayerMetadata.cellSize.width
            val sizeHeight = tileLayerMetadata.cellSize.height
            val measurementName = t._2(0).getMeasurement
            for (rawtile <- t._2) {
              for (i <- 0 to 255) {
                for (j <- 0 to 255) {
                  val y = Math.ceil((t._1._1.ymax - (rawtile.getP_upper_rightY - sizeHeight * i)) / sizeHeight).toInt
                  val x = Math.ceil((rawtile.getP_bottom_leftX + sizeWidth * j - t._1._1.xmin) / sizeWidth).toInt
                  val value = rawtile.getTile.get(j, i).toByte
                  if (x >= 0 && x < 256 && y >= 0 && y < 256 && value > (-128) && value < 128 &&
                    (mutableArrayTile.get(x, y) == Int.MinValue || mutableArrayTile.get(x, y) == 0 || (value != 0 && value > mutableArrayTile.get(x, y))))
                    mutableArrayTile.set(x, y, value)
                }
              }
            }
            (entity.SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
          }
          else {
            (entity.SpaceTimeBandKey(t._1._2, firstTile.getMeasurement), mutableArrayTile)
          }
        })
        (TileRDDComputed, tileLayerMetadata)
      }
      case _ => {
        val TileRDDComputed: RDD[(SpaceTimeBandKey, Tile)] = TileRDDUnComputed.map(t => {
          val assignTimes = Array.fill[Byte](256, 256)(0)
          val mutableArrayTile = ByteArrayTile(Array.fill[Byte](256 * 256)(Byte.MinValue), 256, 256, ByteConstantNoDataCellType).mutable
          if (t._2.length > 0) {
            val sizeWidth = tileLayerMetadata.cellSize.width
            val sizeHeight = tileLayerMetadata.cellSize.height
            val measurementName = t._2(0).getMeasurement
            for (rawtile <- t._2) {
              for (i <- 0 to 255) {
                for (j <- 0 to 255) {
                  val y = Math.ceil((t._1._1.ymax - (rawtile.getP_upper_rightY - sizeHeight * i)) / sizeHeight).toInt
                  val x = Math.ceil((rawtile.getP_bottom_leftX + sizeWidth * j - t._1._1.xmin) / sizeWidth).toInt
                  val value = rawtile.getTile.get(j, i).toByte
                  if (x >= 0 && x < 256 && y >= 0 && y < 256 && value > (-128) && value < 128 && value != 0) {
                    mutableArrayTile.set(x, y, (1 / (assignTimes(y)(x) + 1) * value + assignTimes(y)(x) / (assignTimes(y)(x) + 1) * mutableArrayTile.get(x, y).toByte))
                    assignTimes(y)(x) = (assignTimes(y)(x) + 1).toByte
                  }
                }
              }
            }
            (entity.SpaceTimeBandKey(t._1._2, measurementName), mutableArrayTile)
          }
          else {
            (entity.SpaceTimeBandKey(t._1._2, firstTile.getMeasurement), mutableArrayTile)
          }
        })
        (TileRDDComputed, tileLayerMetadata)
      }
    }
  }


  def query(productName: String = null, sensorName: String = null, measurementName: String = null, startTime: String = null, endTime: String = null, geom: String = null, crs: String = null): (ListBuffer[(String, String, String, String, String)]) = {
    val metaData = new ListBuffer[(String, String, String, String, String)]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection()
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new StringBuilder
        sql ++= "select oge_image.*, oge_data_resource_product.name, oge_data_resource_product.dtype"
        if (measurementName != "" && measurementName != null) {
          sql ++= ", oge_product_measurement.band_num"
        }
        sql ++= " from oge_image "
        if (productName != "" && productName != null) {
          sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        }
        if (sensorName != "" && sensorName != null) {
          sql ++= " join oge_sensor on oge_data_resource_product.sensor_Key=oge_sensor.sensor_Key "
        }
        if (measurementName != "" && measurementName != null) {
          sql ++= "join oge_product_measurement on oge_product_measurement.product_key=oge_data_resource_product.id join oge_measurement on oge_product_measurement.measurement_key=oge_measurement.measurement_key "
        }
        var t = "where"
        if (productName != "" && productName != null) {
          sql ++= t
          sql ++= " name="
          sql ++= "\'"
          sql ++= productName
          sql ++= "\'"
          t = "AND"

        }
        if (sensorName != "" && sensorName != null) {
          sql ++= t
          sql ++= " sensor_name="
          sql ++= "\'"
          sql ++= sensorName
          sql ++= "\'"
          t = "AND"
        }
        if (measurementName != "" && measurementName != null) {
          sql ++= t
          sql ++= " measurement_name="
          sql ++= "\'"
          sql ++= measurementName
          sql ++= "\'"
          t = "AND"
        }
        if (startTime != null) {
          sql ++= t
          sql ++= " phenomenon_time>="
          sql ++= "\'"
          sql ++= startTime
          sql ++= "\'"
          t = "AND"
        }
        if (endTime != null) {
          sql ++= t
          sql ++= "\'"
          sql ++= endTime
          sql ++= "\'"
          sql ++= ">=phenomenon_time"
          t = " AND"
        }
        if (crs != null && crs != "") {
          sql ++= t
          sql ++= " crs="
          sql ++= "\'"
          sql ++= crs
          sql ++= "\'"
          t = "AND"
        }
        if (geom != null && geom != "") {
          sql ++= t
          sql ++= " ST_Intersects(geom,'SRID=4326;"
          sql ++= geom
          sql ++= "')"
        }
        println(sql)
        val extentResults = statement.executeQuery(sql.toString())


        while (extentResults.next()) {
          val path = if (measurementName != "" && measurementName != null) {
            extentResults.getString("path") + "/" + extentResults.getString("image_identification") + "_" + extentResults.getString("band_num") + ".TIF"
          }
          else {
            extentResults.getString("path") + "/" + extentResults.getString("image_identification") + ".TIF"
          }
          println("path = " + path)
          val time = extentResults.getString("phenomenon_time")
          val srcID = extentResults.getString("crs")
          val dtype = extentResults.getString("dtype")
          val measurement = if (measurementName != "" && measurementName != null) extentResults.getString("band_num") else null
          metaData.append((path, time, srcID, measurement, dtype))
        }
      }
      finally {
        conn.close
      }
    } else throw new RuntimeException("connection failed")

    metaData
  }


  /**
   * if both image1 and image2 has only 1 band, add operation is applied between the 2 bands
   * if not, add the first value to the second for each matched pair of bands in image1 and image2
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def add(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    println("add bandNum1 = " + bandNum1)
    println("add bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val addRDD = image1NoBand.join(image2NoBand)
      (addRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Add"), Add(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Add(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, subtarct the second tile from the first tile for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def subtract(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    println("subtract bandNum1 = " + bandNum1)
    println("subtract bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val subtractRDD = image1NoBand.join(image2NoBand)
      (subtractRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Subtract"), Subtract(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Subtract(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, divide operation is applied between the 2 bands
   * if not, divide the first tile by the second tile for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def divide(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
             image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    println("divide bandNum1 = " + bandNum1)
    println("divide bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val divideRDD = image1NoBand.join(image2NoBand)
      (divideRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Divide"), Divide(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Divide(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * if both image1 and image2 has only 1 band, multiply operation is applied between the 2 bands
   * if not, multipliy the first tile by the second for each matched pair of bands in image1 and image2.
   *
   * @param image1 first image rdd
   * @param image2 second image rdd
   * @return
   */
  def multiply(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    println("multiply bandNum1 = " + bandNum1)
    println("multiply bandNum2 = " + bandNum2)
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val multiplyRDD = image1NoBand.join(image2NoBand)
      (multiplyRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Multiply"), Multiply(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Multiply(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * image Binarization
   *
   * @param image     image rdd for operation
   * @param threshold threshold
   * @return
   */
  def binarization(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), threshold: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      val tilemap = t._2.map(pixel => {
        if (pixel > threshold) {
          255
        }
        else {
          0
        }
      })
      (t._1, tilemap)
    }), image._2)
  }

  /**
   * Returns 1 iff both values are non-zero for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def and(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val andRDD = image1NoBand.join(image2NoBand)
      (andRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "And"), And(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, And(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns 1 iff either values are non-zero for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def or(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val orRDD = image1NoBand.join(image2NoBand)
      (orRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Or"), Or(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Or(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns 0 if the input is non-zero, and 1 otherwise
   *
   * @param image the image rdd for operation
   * @return
   */
  def not(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Not(t._2))
    }), image._2)
  }

  /**
   * Computes the sine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def sin(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Sin(t._2))
    }), image._2)
  }

  /**
   * Computes the cosine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def cos(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Cos(t._2))
    }), image._2)
  }

  /**
   * Computes the hyperbolic sine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def sinh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Sinh(t._2))
    }), image._2)
  }

  /**
   * Computes the hyperbolic cosine of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def cosh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Cosh(t._2))
    }), image._2)
  }

  /**
   * Computes the arc sine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def asin(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Asin(t._2))
    }), image._2)
  }

  /**
   * Computes the arc cosine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def acos(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Acos(t._2))
    }), image._2)
  }

  /**
   * Computes the atan sine in radians of the input.
   *
   * @param image the image rdd for operation
   * @return
   */
  def atan(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Atan(t._2))
    }), image._2)
  }

  /**
   * Operation to get the Arc Tangent2 of values. The first raster holds the y-values, and the second holds the x values.
   * The arctan is calculated from y/x.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to oprate
   * @return
   */
  def atan2(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
            image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val atan2RDD = image1NoBand.join(image2NoBand)
      (atan2RDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Atan2"), Atan2(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Atan2(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Computes the smallest integer greater than or equal to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def ceil(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Ceil(t._2))
    }), image._2)
  }

  /**
   * Computes the largest integer less than or equal to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def floor(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Floor(t._2))
    }), image._2)
  }

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def eq(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val eqRDD = image1NoBand.join(image2NoBand)
      (eqRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Eq"), Equal(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Equal(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def gt(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gtRDD = image1NoBand.join(image2NoBand)
      (gtRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Gt"), Greater(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Greater(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def gte(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gteRDD = image1NoBand.join(image2NoBand)
      (gteRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Gte"), GreaterOrEqual(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, GreaterOrEqual(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns an image containing all bands copied from the first input and selected bands from the second input,
   * optionally overwriting bands in the first image with the same name.
   *
   * @param image1    first image
   * @param image2    second image
   * @param names     the name of selected bands in image2
   * @param overwrite if true, overwrite bands in the first image with the same name
   * @return
   */
  def addBands(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
               names: List[String], overwrite: Boolean = false): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val image1BandNames = bandNames(image1)
    val image2BandNames = bandNames(image2)
    val intersectBandNames = image1BandNames.intersect(image2BandNames)
    //select bands from image2
    val image2SelectBand: RDD[(SpaceTimeBandKey, Tile)] = image2._1.filter(t => {
      names.contains(t._1._measurementName)
    })
    if (!overwrite) {
      //image2不重写image1中相同的波段，把image2中与image1相交的波段过滤掉
      (image2SelectBand.filter(t => {
        !intersectBandNames.contains(t._1._measurementName)
      }).union(image1._1), image1._2)
    }
    else {
      //image2重写image1中相同的波段，把image1中与image2相交的波段过滤掉
      (image1._1.filter(t => {
        !intersectBandNames.contains(t._1._measurementName)
      }).union(image2SelectBand), image1._2)
    }
  }

  /**
   * get all the bands in rdd
   *
   * @param image rdd for getting bands
   * @return
   */
  def bandNames(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): List[String] = {
    image._1.map(t => t._1._measurementName).distinct().collect().toList
  }

  def deepLearning(geom: String): Unit = {
    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
      t.toDouble
    }).to[ListBuffer]
    val metaData = Preprocessing.queryGF2()
    val time = Preprocessing.load(metaData._1, metaData._2, geom)
    val writeFile = new File("/home/geocube/oge/oge-server/dag-boot/output.txt")
    val writer = new BufferedWriter(new FileWriter(writeFile))
    val path_img = "http://125.220.153.26:8093/ogedemooutput/DL_" + time + ".png"
    val outputString = "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + geomReplace(1) + "," + geomReplace(0) + "," + geomReplace(3) + "," + geomReplace(2) + "]}]}";
    writer.write(outputString)
    writer.close()
  }

  def visualize(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, min: Int = 0, max: Int = 255,
                palette: String = null): Unit = {
    val executorOutputDir = "D:/"
//    val executorOutputDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/ogedemooutput/"
    val writeFile = new File("D:/output.txt")
//    val writeFile = new File("/home/geocube/oge/oge-server/dag-boot/output.txt")

    if (method == null) {
      val tileLayerArray = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      }).collect()
      val layout = image._2.layout
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
      val stitchedTile = Raster(tile, layout.extent)
      val time = System.currentTimeMillis();
      val path = executorOutputDir + "oge_" + time + ".png"
      val path_img = "http://125.220.153.26:8093/ogedemooutput/oge_" + time + ".png"
      if (palette == null) {
        val colorMap = {
          ColorMap(
            scala.Predef.Map(
              max -> RGBA(255, 255, 255, 255),
              min -> RGBA(0, 0, 0, 20),
              -1 -> RGBA(255, 255, 255, 255)
            ),
            ColorMap.Options(
              classBoundaryType = Exact,
              noDataColor = 0x00000000, // transparent
              fallbackColor = 0x00000000, // transparent
              strict = false
            )
          )
        }
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else if (palette == "green") {
        val colorMap = {
          ColorMap(
            scala.Predef.Map(
              max -> RGBA(127, 255, 170, 255),
              min -> RGBA(0, 0, 0, 20)
            ),
            ColorMap.Options(
              classBoundaryType = Exact,
              noDataColor = 0x00000000, // transparent
              fallbackColor = 0x00000000, // transparent
              strict = false
            )
          )
        }
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else if ("histogram".equals(palette)) {
        //set color
        val histogram = stitchedTile.tile.histogramDouble()
        val colorMap = geotrellis.raster.render.ColorMap.fromQuantileBreaks(histogram, geotrellis.raster.render.ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))
        stitchedTile.tile.renderPng(colorMap).write(path)
      }
      else {
        val colorRamp = ColorRamp(
          0xD76B27FF,
          0xE68F2DFF,
          0xF9B737FF,
          0xF5CF7DFF,
          0xF0E7BBFF,
          0xEDECEAFF,
          0xC8E1E7FF,
          0xADD8EAFF,
          0x7FB8D4FF,
          0x4EA3C8FF,
          0x2586ABFF
        )
        stitchedTile.tile.renderPng(colorRamp).write(path)
      }
      val writer = new BufferedWriter(new FileWriter(writeFile))
      if ("EPSG:4326".equals(image._2.crs.toString)) {
        val pointDestLD = new Coordinate(layout.extent.xmin, layout.extent.ymin)
        val pointDestRU = new Coordinate(layout.extent.xmax, layout.extent.ymax)
        val outputString = "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + pointDestLD.x + "," + pointDestLD.y + "," + pointDestRU.x + "," + pointDestRU.y + "]}]}";
        writer.write(outputString)
        writer.close()
      }
      else {
        val crsSource = org.geotools.referencing.CRS.decode(image._2.crs.toString)
        val crsTarget = org.geotools.referencing.CRS.decode("EPSG:4326")
        val transform = org.geotools.referencing.CRS.findMathTransform(crsSource, crsTarget)
        val pointSrcLD = new Coordinate(layout.extent.xmin, layout.extent.ymin)
        val pointSrcRU = new Coordinate(layout.extent.xmax, layout.extent.ymax)
        val pointDestLD = new Coordinate()
        val pointDestRU = new Coordinate()
        JTS.transform(pointSrcLD, pointDestLD, transform)
        JTS.transform(pointSrcRU, pointDestRU, transform)
        val outputString = "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + pointDestLD.x + "," + pointDestLD.y + "," + pointDestRU.x + "," + pointDestRU.y + "]}]}";
        writer.write(outputString)
        writer.close()
      }
    }
    else if ("timeseries".equals(method)) {
      val result = mutable.Map[String, Any]()
      val vectorList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterList = new ArrayBuffer[mutable.Map[String, Any]]()
      val time = System.currentTimeMillis();

      val writer = new BufferedWriter(new FileWriter(writeFile))
      val imageTimeSeries = image._1.groupBy(t => {
        t._1.spaceTimeKey.instant
      })
      val imagePng = imageTimeSeries.map(t => {
        val tileLayerArray = t._2.map(x => (x._1.spaceTimeKey.spatialKey, x._2)).toArray
        val layout = image._2.layout
        val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
        val stitchedTile = Raster(tile, layout.extent)
        if(palette == null){
          val colorMap = {
            ColorMap(
              scala.Predef.Map(
                max -> RGBA(255, 255, 255, 255),
                min -> RGBA(0, 0, 0, 20),
                -1 -> RGBA(255, 255, 255, 255)
              ),
              ColorMap.Options(
                classBoundaryType = Exact,
                noDataColor = 0x00000000, // transparent
                fallbackColor = 0x00000000, // transparent
                strict = false
              )
            )
          }
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else if (palette == "green") {
          val colorMap = {
            ColorMap(
              scala.Predef.Map(
                max -> RGBA(127, 255, 170, 255),
                min -> RGBA(0, 0, 0, 20)
              ),
              ColorMap.Options(
                classBoundaryType = Exact,
                noDataColor = 0x00000000, // transparent
                fallbackColor = 0x00000000, // transparent
                strict = false
              )
            )
          }
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else if ("histogram".equals(palette)) {
          //set color
          val histogram = stitchedTile.tile.histogramDouble()
          val colorMap = geotrellis.raster.render.ColorMap.fromQuantileBreaks(histogram, geotrellis.raster.render.ColorRamp(0x000000FF, 0xFFFFFFFF).stops(100))
          (stitchedTile.tile.renderPng(colorMap), stitchedTile.extent, t._1)
        }
        else {
          val colorRamp = ColorRamp(
            0xD76B27FF,
            0xE68F2DFF,
            0xF9B737FF,
            0xF5CF7DFF,
            0xF0E7BBFF,
            0xEDECEAFF,
            0xC8E1E7FF,
            0xADD8EAFF,
            0x7FB8D4FF,
            0x4EA3C8FF,
            0x2586ABFF
          )
          (stitchedTile.tile.renderPng(colorRamp), stitchedTile.extent, t._1)
        }
      }).collect()
      imagePng.sortBy(t=>t._3).foreach(t => {
        val path = executorOutputDir + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"
        t._1.write(path)
        if ("EPSG:4326".equals(image._2.crs.toString)) {
          val pointDestLD = new Coordinate(t._2.xmin, t._2.ymin)
          val pointDestRU = new Coordinate(t._2.xmax, t._2.ymax)
          rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"), "extent" -> Array(pointDestLD.x, pointDestLD.y, pointDestRU.x, pointDestRU.y)))
        }
        else {
          val crsSource = org.geotools.referencing.CRS.decode(image._2.crs.toString)
          val crsTarget = org.geotools.referencing.CRS.decode("EPSG:4326")
          val transform = org.geotools.referencing.CRS.findMathTransform(crsSource, crsTarget)
          val pointSrcLD = new Coordinate(t._2.xmin, t._2.ymin)
          val pointSrcRU = new Coordinate(t._2.xmax, t._2.ymax)
          val pointDestLD = new Coordinate()
          val pointDestRU = new Coordinate()
          JTS.transform(pointSrcLD, pointDestLD, transform)
          JTS.transform(pointSrcRU, pointDestRU, transform)
          rasterList.append(mutable.Map("url" -> ("http://125.220.153.26:8093/ogedemooutput/" + "oge_" + time + "_" + t._3 + "_timeseries" + ".png"), "extent" -> Array(pointDestLD.x, pointDestLD.y, pointDestRU.x, pointDestRU.y)))
        }
      })
      result += ("vector" -> vectorList)
      result += ("raster" -> rasterList)
      implicit val formats = DefaultFormats
      val jsonStr: String = write(result)
      writer.write(jsonStr)
      writer.close()
    }
  }
}