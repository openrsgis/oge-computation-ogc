package whu.edu.cn.application.oge

import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.render.{ColorMap, Exact}
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, CellType, ColorRamp, Histogram, RGBA, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.{FileLayerManager, FileLayerWriter}
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.geotools.geometry.jts.JTS
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.locationtech.jts.geom.Coordinate
import whu.edu.cn.application.oge.Tiffheader_parse._
import whu.edu.cn.application.tritonClient.Preprocessing
import whu.edu.cn.core.entity
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.util.TileMosaicImage.tileMosaic
import whu.edu.cn.util.TileSerializerImage.deserializeTileData

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math._

object Image {

  /**
   * load the images
   *
   * @param sc              spark context
   * @param productName     product name to query
   * @param sensorName      sensor name to query
   * @param measurementName measurement name to query
   * @param dateTime        array of start time and end time to query
   * @param geom            geom of the query window
   * @param crs             crs of the images to query
   * @return ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile])
   */
  def load(implicit sc: SparkContext, productName: String = null, sensorName: String = null, measurementName: String = null, dateTime: String = null, geom: String = null, geom2: String = null, crs: String = null, level: Int = 0): ((RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), RDD[RawTile]) = {
    val geomReplace = geom.replace("[", "").replace("]", "").split(",").map(t => {
      t.toDouble
    }).to[ListBuffer]
    if (geom2 != null) {
      val geom2Replace = geom2.replace("[", "").replace("]", "").split(",").map(t => {
        t.toDouble
      }).to[ListBuffer]
      if (geom2Replace(0) > geomReplace(0)) {
        geomReplace(0) = geom2Replace(0)
      }
      if (geom2Replace(1) > geomReplace(1)) {
        geomReplace(1) = geom2Replace(1)
      }
      if (geom2Replace(2) < geomReplace(2)) {
        geomReplace(2) = geom2Replace(2)
      }
      if (geom2Replace(3) < geomReplace(3)) {
        geomReplace(3) = geom2Replace(3)
      }
    }
    val dateTimeArray = if (dateTime != null) dateTime.replace("[", "").replace("]", "").split(",") else null
    val StartTime = if (dateTimeArray != null) {
      if (dateTimeArray.length == 2) dateTimeArray(0) else null
    } else null
    val EndTime = if (dateTimeArray != null) {
      if (dateTimeArray.length == 2) dateTimeArray(1) else null
    } else null
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
    if (metaData.isEmpty) {
      throw new RuntimeException("No data to compute!")
    }

    val imagePathRdd = sc.parallelize(metaData, metaData.length)
    val tileRDDNoData: RDD[mutable.Buffer[RawTile]] = imagePathRdd.map(t => {
      val tiles = tileQuery(level, t._1, t._2, t._3, t._4, t._5, t._6, productName, query_extent)
      if (tiles.size() > 0) {
        asScalaBuffer(tiles)
      }
      else {
        mutable.Buffer.empty[RawTile]
      }
    }).persist() // TODO 转化成Scala的可变数组并赋值给tileRDDNoData
    val tileNum = tileRDDNoData.map(t => t.length).reduce((x, y) => {
      x + y
    })
    println("tileNum = " + tileNum)
    val tileRDDFlat: RDD[RawTile] = tileRDDNoData.flatMap(t => t)
    var repNum = tileNum
    if (repNum > 90) {
      repNum = 90
    }
    val tileRDDReP = tileRDDFlat.repartition(repNum).persist()
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
    val tl = TileLayout(Math.round((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.round((extents._4 - extents._2) / firstTile.getResolution / 256).toInt, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDType)
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val tileRDD = tileRDDReP.map(t => {
      val tile = getTileBuf(t)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val phenomenonTime = sdf.parse(tile.getTime).getTime
      val measurement = tile.getMeasurement
      val rowNum = tile.getRow
      val colNum = tile.getCol
      val Tile = deserializeTileData("", tile.getTilebuf, 256, tile.getDType)
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colNum - colRowInstant._1, rowNum - colRowInstant._2, phenomenonTime), measurement)
      val v = Tile
      (k, v)
    })
    (tileRDD, tileLayerMetadata)
  }

  def mosaic(implicit sc: SparkContext, tileRDDReP: RDD[RawTile], method: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO
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
    val firstTile = tileRDDReP.take(1)(0)
    val tl = TileLayout(Math.round((extents._3 - extents._1) / firstTile.getResolution / 256.0).toInt, Math.round((extents._4 - extents._2) / firstTile.getResolution / 256.0).toInt, 256, 256)
    println(tl)
    val extent = geotrellis.vector.Extent(extents._1, extents._4 - tl.layoutRows * 256 * firstTile.getResolution, extents._1 + tl.layoutCols * 256 * firstTile.getResolution, extents._4)
    println(extent)
    val ld = LayoutDefinition(extent, tl)
    val cellType = CellType.fromName(firstTile.getDType)
    val crs = CRS.fromEpsgCode(firstTile.getCrs)
    val bounds = Bounds(SpaceTimeKey(0, 0, colRowInstant._3), SpaceTimeKey(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, colRowInstant._6))
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
    val tileBox = new ListBuffer[((Extent, SpaceTimeKey), List[RawTile])]
    for (i <- 0 until ColSum) {
      for (j <- 0 until RowSum) {
        val rawTiles = new ListBuffer[RawTile]
        val tileExtent = new Extent(extents._1 + i * 256 * firstTile.getResolution, extents._4 - (j + 1) * 256 * firstTile.getResolution, extents._1 + (i + 1) * 256 * firstTile.getResolution, extents._4 - j * 256 * firstTile.getResolution)
        for (rawtile <- rawtileArray) {
          if (Extent(rawtile.getP_bottom_leftX, rawtile.getP_bottom_leftY, rawtile.getP_upper_rightX, rawtile.getP_upper_rightY).intersects(tileExtent))
            rawTiles.append(rawtile)
        }
        if (rawTiles.nonEmpty) {
          val now = "1000-01-01 00:00:00"
          val date = sdf.parse(now).getTime
          tileBox.append(((tileExtent, SpaceTimeKey(i, j, date)), rawTiles.toList))
        }
      }
    }
    val TileRDDUnComputed = sc.parallelize(tileBox, tileBox.length)
    tileMosaic(TileRDDUnComputed, method, tileLayerMetadata, firstTile.getDType)
  }


  def query(productName: String = null, sensorName: String = null, measurementName: String = null, startTime: String = null, endTime: String = null, geom: String = null, crs: String = null): (ListBuffer[(String, String, String, String, String, String)]) = {
    val metaData = new ListBuffer[(String, String, String, String, String, String)]
    val postgresqlUtil = new PostgresqlUtil("")
    val conn = postgresqlUtil.getConnection()
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val sql = new mutable.StringBuilder
        sql ++= "select oge_image.*, oge_data_resource_product.name, oge_data_resource_product.dtype"
        sql ++= ", oge_product_measurement.band_num, oge_product_measurement.resolution"
        sql ++= " from oge_image "
        if (productName != "" && productName != null) {
          sql ++= "join oge_data_resource_product on oge_image.product_key= oge_data_resource_product.id "
        }
        if (sensorName != "" && sensorName != null) {
          sql ++= " join oge_sensor on oge_data_resource_product.sensor_Key=oge_sensor.sensor_Key "
        }
        sql ++= "join oge_product_measurement on oge_product_measurement.product_key=oge_data_resource_product.id join oge_measurement on oge_product_measurement.measurement_key=oge_measurement.measurement_key "
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
            extentResults.getString("path") + "/" + extentResults.getString("image_identification") + "_" + extentResults.getString("band_num") + ".tif"
          }
          else {
            extentResults.getString("path") + "/" + extentResults.getString("image_identification") + ".tif"
          }
          println("path = " + path)
          val time = extentResults.getString("phenomenon_time")
          val srcID = extentResults.getString("crs")
          val dtype = extentResults.getString("dtype")
          val measurement = if (measurementName != "" && measurementName != null) extentResults.getString("band_num") else null
          val resolution = extentResults.getString("resolution")
          metaData.append((path, time, srcID, measurement, dtype, resolution))
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
   * Computes the tangent of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def tan(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Tan(t._2))
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
   * Computes the hyperbolic tangent of the input in radians.
   *
   * @param image the image rdd for operation
   * @return
   */
  def tanh(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Tanh(t._2))
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
   * Computes the integer nearest to the input.
   *
   * @param image rdd for operation
   * @return
   */
  def round(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Round(t._2))
    }), image._2)
  }

  /**
   * Computes the natural logarithm of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def log(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Log(t._2))
    }), image._2)
  }

  /**
   * Computes the base-10 logarithm of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def log10(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Log10(t._2))
    }), image._2)
  }

  /**
   * Computes the square root of the input.
   *
   * @param image rdd for operation
   * @return
   */
  def sqrt(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Sqrt(t._2))
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
   * Returns 1 iff the first value is greater than the second for each matched pair of bands in image1 and image2.
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
   * Returns 1 iff the first value is equal or greater to the second for each matched pair of bands in image1 and image2.
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
   * Returns 1 iff the first value is less than the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def lt(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
         image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gtRDD = image1NoBand.join(image2NoBand)
      (gtRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Lt"), Less(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Less(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Returns 1 iff the first value is less or equal to the second for each matched pair of bands in image1 and image2.
   * if both have only 1 band, the 2 band will match
   *
   * @param image1 first image rdd to operate
   * @param image2 second image rdd to operate
   * @return
   */
  def lte(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val gteRDD = image1NoBand.join(image2NoBand)
      (gteRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Lte"), LessOrEqual(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, LessOrEqual(t._2._1, t._2._2))
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

  /**
   * get the target bands from original image
   *
   * @param image       the image for operation
   * @param targetBands the intersting bands
   * @return
   */
  def select(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), targetBands: List[String])
  : (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.filter(t => targetBands.contains(t._1._measurementName)), image._2)
  }

  /**
   * Returns a map of the image's band types.
   *
   * @param image The image from which the left operand bands are taken.
   * @return
   */
  def bandTypes(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): Map[String, String] = {
    val bandTypesArray = image._1.map(t => (t._1.measurementName, t._2.cellType)).distinct().collect()
    var bandTypesMap = Map[String, String]()
    for (band <- bandTypesArray) {
      bandTypesMap = bandTypesMap + (band._1 -> band._2.toString())
    }
    bandTypesMap
  }

  /**
   * Computes the absolute value of the input.
   *
   * @param image The image to which the operation is applied.
   * @return
   */
  def abs(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, Abs(t._2))
    }), image._2)
  }

  /**
   * Returns 1 iff the first value is not equal to the second for each matched pair of bands in image1 and image2.
   *
   * @param image1 The image from which the left operand bands are taken.
   * @param image2 The image from which the right operand bands are taken.
   * @return
   */
  def neq(image1: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
          image2: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNum1 = bandNames(image1).length
    val bandNum2 = bandNames(image2).length
    if (bandNum1 == 1 && bandNum2 == 1) {
      val image1NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image1._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val image2NoBand: RDD[(SpaceTimeKey, (String, Tile))] = image2._1.map(t => (t._1.spaceTimeKey, (t._1.measurementName, t._2)))
      val eqRDD = image1NoBand.join(image2NoBand)
      (eqRDD.map(t => {
        (entity.SpaceTimeBandKey(t._1, "Ueq"), Unequal(t._2._1._2, t._2._2._2))
      }), image1._2)
    }
    else {
      val matchRDD = image1._1.join(image2._1)
      (matchRDD.map(t => {
        (t._1, Unequal(t._2._1, t._2._2))
      }), image1._2)
    }
  }

  /**
   * Computes the signum function (sign) of the input; zero if the input is zero, 1 if the input is greater than zero, -1 if the input is less than zero.
   *
   * @param image The image to which the operation is applied.
   * @return
   */
  def signum(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.map(u => {
        if (u > 0) 1
        else if (u < 0) -1
        else 0
      }))
    }), image._2)
  }

  /**
   * Rename the bands of an image.Returns the renamed image.
   *
   * @param image The coverage to which to apply the operations.
   * @param name  The new names for the bands. Must match the number of bands in the Image.
   * @return
   */
  def rename(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]),
             name: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val nameList = name.replace("[", "").replace("]", "").split(",")
    val bandnames = bandNames(image)
    if (bandnames.length == nameList.length) {
      val namesMap = (bandnames zip nameList).toMap
      (image._1.map(t => {
        (SpaceTimeBandKey(t._1.spaceTimeKey, namesMap(t._1.measurementName)), t._2)
      }), image._2)
    }
    else {
      image
    }
  }


  /**
   * Return the histogram of the image, a map of bin label value and its associated count.
   *
   * @param image The coverage to which to compute the histogram.
   * @return
   */
  def histogram(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): Map[Int, Long] = {
    val histRDD: RDD[Histogram[Int]] = image._1.map(t => {
      t._2.histogram
    })
    histRDD.reduce((a, b) => {
      a.merge(b)
    }).binCounts().toMap[Int, Long]
  }

  /**
   * Returns the projection of an Image.
   *
   * @param image The coverage to which to get the projection.
   * @return
   */
  def projection(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): String = {
    image._2.crs.toString()
  }

  /**
   * 重采样！！
   *
   * @param image
   * @param sourceZoom
   * @param targetZoom
   * @param mode
   * @return
   */
  def resample(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), sourceZoom: Int, targetZoom: Int,
               mode: String): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    val resampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }
    val level: Int = targetZoom - sourceZoom
    if (level > 0 && level < 8) {
      val imageResampled = image._1.map(t => {
        (t._1, t._2.resample(t._2.cols * (level + 1), t._2.rows * (level + 1), resampleMethod))
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols * (level + 1),
        image._2.tileRows * (level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else if (level < 0 && level > (-8)) {
      val imageResampled = image._1.map(t => {
        val tileResampled = t._2.resample(t._2.cols / (-level + 1), t._2.rows / (-level + 1), resampleMethod)
        (t._1, tileResampled)
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols / (-level + 1),
        image._2.tileRows / (-level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else {
      image
    }
  }


  /**
   * Resample the image
   *
   * @param image The coverage to resample
   * @param level The resampling level. eg:1 for up-sampling and -1 for down-sampling.
   * @param mode  The interpolation mode to use
   * @return
   */
  def resample(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), level: Int, mode: String
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO 重采样
    val resampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }
    if (level > 0 && level < 8) {
      val imageResampled = image._1.map(t => {
        (t._1, t._2.resample(t._2.cols * (level + 1), t._2.rows * (level + 1), resampleMethod))
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols * (level + 1),
        image._2.tileRows * (level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else if (level < 0 && level > (-8)) {
      val imageResampled = image._1.map(t => {
        val tileResampled = t._2.resample(t._2.cols / (-level + 1), t._2.rows / (-level + 1), resampleMethod)
        (t._1, tileResampled)
      })
      (imageResampled, TileLayerMetadata(image._2.cellType, LayoutDefinition(image._2.extent, TileLayout(image._2.layoutCols, image._2.layoutRows, image._2.tileCols / (-level + 1),
        image._2.tileRows / (-level + 1))), image._2.extent, image._2.crs, image._2.bounds))
    }
    else {
      image
    }
  }

  /**
   * Casts the input value to a signed 8-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt8(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
            ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("int8")))
    }), image._2)
  }

  /**
   * Casts the input value to a unsigned 8-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toUint8(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("uint8")))
    }), image._2)
  }

  /**
   * Casts the input value to a signed 16-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt16(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("int16")))
    }), image._2)
  }

  /**
   * Casts the input value to a unsigned 16-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toUint16(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("uint16")))
    }), image._2)
  }

  /**
   * Casts the input value to a signed 32-bit integer.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toInt32(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("int32")))
    }), image._2)
  }
  /**
   * Casts the input value to a 32-bit float.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toFloat(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("float32")))
    }), image._2)
  }

  /**
   * Casts the input value to a 64-bit float.
   *
   * @param image The coverage to which the operation is applied.
   * @return
   */
  def toDouble(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    (image._1.map(t => {
      (t._1, t._2.convert(CellType.fromName("float64")))
    }), image._2)
  }

  def deepLearning(implicit sc: SparkContext, geom: String, fileName: String): Unit = {
    val metaData = Preprocessing.queryGF2()
    val time = Preprocessing.load(sc, metaData._1, metaData._2, geom)
    val writeFile = new File(fileName)
    val writer = new BufferedWriter(new FileWriter(writeFile))
    val path_img = "http://oge.whu.edu.cn/api/oge-python/ogeoutput/DL_" + time + ".png"
    val outputString = "{\"vectorCube\":[],\"rasterCube\":[],\"table\":[],\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\"}]}";
    writer.write(outputString)
    writer.close()
  }

  def deepLearningOnTheFly(implicit sc: SparkContext, level: Int, geom: String, geom2: String = null, fileName: String): Unit = {
    val metaData = Preprocessing.queryGF2()
    Preprocessing.loadOnTheFly(sc, level, metaData._1, metaData._2, geom2, fileName)
  }

  //Precipitation Anomaly Percentage
  def PAP(image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), time: String, n: Int): (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = {
    var m = n
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //val now = "1000-01-01 00:00:00"
    val date = sdf.parse(time).getTime
    val timeYear = time.substring(0, 4)
    var timeBeginYear = timeYear.toInt - n
    if (timeYear.toInt - n < 2000) {
      timeBeginYear = 2000
      m = timeYear.toInt - 2000
    }
    val timeBegin = timeBeginYear.toString + time.substring(4, 19)
    val dateBegin = sdf.parse(timeBegin).getTime


    val imageReduced = image._1.filter(t => {
      val timeResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t._1.spaceTimeKey.instant)
      timeResult.substring(5, 7) == time.substring(5, 7)
    })
    val imageAverage = imageReduced.filter(t => {
      t._1.spaceTimeKey.instant >= dateBegin && t._1.spaceTimeKey.instant < date
    }).groupBy(t => t._1.spaceTimeKey.spatialKey)
      .map(t => {
        val tiles = t._2.map(x => x._2).reduce((a, b) => Add(a, b))
        val tileAverage = tiles.mapDouble(x => {
          x / t._2.size
        })
        (t._1, tileAverage)
      })
    val imageCal = imageReduced.filter(t => t._1.spaceTimeKey.instant == date).map(t => (t._1.spaceTimeKey.spatialKey, t._2))
    val PAP = imageAverage.join(imageCal).map(t => {
      val divide = Divide(t._2._2, t._2._1)
      val PAPTile = divide.mapDouble(x => {
        x - 1.0
      })
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "PAP"), PAPTile)
    })
    (PAP, image._2)
  }

  def visualizeOnTheFly(implicit sc: SparkContext, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, min: Int = 0, max: Int = 255,
                        palette: String = null, layerID: Int, fileName: String = null): Unit = {
    val appID = sc.applicationId
    val outputPath = "/home/geocube/oge/on-the-fly" // TODO datas/on-the-fly
    val levelFromJSON: Int = ImageTrigger.level
    if ("timeseries".equals(method)) {
      val TMSList = new ArrayBuffer[mutable.Map[String, Any]]()
      val resampledImage: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = resample(image, Tiffheader_parse.nearestZoom, levelFromJSON, "Bilinear")

      println("Tiffheader_parse.nearestZoom = " + Tiffheader_parse.nearestZoom)
      val timeList = resampledImage._1.map(t => t._1.spaceTimeKey.instant).distinct().collect()
      resampledImage._1.persist()
      timeList.foreach(x => {
        val tiled = resampledImage._1.filter(m => m._1.spaceTimeKey.instant == x).map(t => {
          (t._1.spaceTimeKey.spatialKey, t._2)
        })
        val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
        val cellType = resampledImage._2.cellType
        val srcLayout = resampledImage._2.layout
        val srcExtent = resampledImage._2.extent
        val srcCrs = resampledImage._2.crs
        val srcBounds = resampledImage._2.bounds
        val newBounds = Bounds(srcBounds.get.minKey.spatialKey, srcBounds.get.maxKey.spatialKey)
        val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)

        val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          TileLayerRDD(tiled, rasterMetaData)
            .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

        // Create the attributes store that will tell us information about our catalog.
        val attributeStore = FileAttributeStore(outputPath)
        // Create the writer that we will use to store the tiles in the local catalog.
        val writer = FileLayerWriter(attributeStore)
        val time = System.currentTimeMillis()
        val layerIDAll = appID + "-layer-" + time + "_" + palette + "-" + min + "-" + max
        // Pyramiding up the zoom levels, write our tiles out to the local file system.



        println("zoom = " + zoom)

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


        TMSList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-tms/" + layerIDAll + "/{z}/{x}/{y}")))
      }
      )

      // 写入文件
      //      Serve.runTMS(outputPath)
      val writeFile = new File(fileName)
      val writerOutput = new BufferedWriter(new FileWriter(writeFile))
      val result = mutable.Map[String, Any]()
      result += ("raster" -> TMSList)
      result += ("vector" -> ArrayBuffer.empty[mutable.Map[String, Any]])
      result += ("table" -> ArrayBuffer.empty[mutable.Map[String, Any]])
      val rasterCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      val vectorCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      result += ("rasterCube" -> rasterCubeList)
      result += ("vectorCube" -> vectorCubeList)
      implicit val formats = DefaultFormats
      val jsonStr: String = Serialization.write(result)
      writerOutput.write(jsonStr)
      writerOutput.close()
    }

    else {
      val tiled = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      })

      //      val tiledArray = tiled.collect()
      //      val layoutd = image._2.layout
      //      val (tiledd, (_, _), (_, _)) = TileLayoutStitcher.stitch(tiledArray)
      //      val stitchedTiled = Raster(tiledd, layoutd.extent)
      //      GeoTiff(stitchedTiled, image._2.crs).write("D:\\home\\geocube\\oge\\on-the-fly\\out1.tif")

      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
      val cellType = image._2.cellType
      val srcLayout = image._2.layout
      val srcExtent = image._2.extent
      val srcCrs = image._2.crs
      val srcBounds = image._2.bounds
      val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1 - srcBounds.get.minKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2 - srcBounds.get.minKey.spatialKey._2))
      val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)

      val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
        TileLayerRDD(tiled, rasterMetaData)
          .reproject(WebMercator, layoutScheme, geotrellis.raster.resample.Bilinear)

      //          val tileLayerArray = reprojected.map(t => {
      //            (t._1, t._2)
      //          }).collect()
      //          val layout = reprojected.metadata.layout
      //          val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
      //          val stitchedTile = Raster(tile, reprojected.metadata.extent)
      //          stitchedTile.tile.renderPng().write("D:/on-the-fly/out.png")
      //          GeoTiff(stitchedTile, CRS.fromEpsgCode(3857)).write("D:\\home\\geocube\\oge\\on-the-fly\\out2.tif")

      // Create the attributes store that will tell us information about our catalog.
      val attributeStore = FileAttributeStore(outputPath)
      // Create the writer that we will use to store the tiles in the local catalog.
      val writer = FileLayerWriter(attributeStore)
      val time = System.currentTimeMillis()
      val layerIDAll = appID + "-layer-" + time + "_" + palette + "-" + min + "-" + max
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
      //            Serve.runTMS(outputPath)
    }
  }

  def visualizeBatch(implicit sc: SparkContext, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, layerID: Int, fileName: String = null): Unit = {
    val executorOutputDir = "datas/"  // TODO
    val writeFile = new File(fileName)

    if (method == null) {
      val tileLayerArray = image._1.map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      }).collect() // TODO
      val layout = image._2.layout
      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)  // TODO
      val stitchedTile = Raster(tile, layout.extent)
      val time = System.currentTimeMillis();
      val path = executorOutputDir + "oge_" + time + ".tif"
      val path_img = "http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_" + time + ".tif"
      GeoTiff(stitchedTile, image._2.crs).write(path)
      val writer = new BufferedWriter(new FileWriter(writeFile))
      val outputString = "{\"vectorCube\":[],\"rasterCube\":[],\"table\":[],\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\"}]}";
      writer.write(outputString)
      writer.close()
    }
    else if ("timeseries".equals(method)) {
      val result = mutable.Map[String, Any]()
      val tableList = new ArrayBuffer[mutable.Map[String, Any]]()
      val vectorList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterList = new ArrayBuffer[mutable.Map[String, Any]]()
      val rasterCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
      val vectorCubeList = new ArrayBuffer[mutable.Map[String, Any]]()
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
        (stitchedTile, t._1)
      }).collect()
      imagePng.sortBy(t => t._2).foreach(t => {
        val path = executorOutputDir + "oge_" + time + "_" + t._2 + "_timeseries" + ".tif"
        GeoTiff(t._1, image._2.crs).write(path)
        rasterList.append(mutable.Map("url" -> ("http://oge.whu.edu.cn/api/oge-python/ogeoutput/" + "oge_" + time + "_" + t._2 + "_timeseries" + ".tif")))
      })
      result += ("table" -> tableList)
      result += ("vector" -> vectorList)
      result += ("raster" -> rasterList)
      result += ("rasterCube" -> rasterCubeList)
      result += ("vectorCube" -> vectorCubeList)
      implicit val formats = DefaultFormats
      val jsonStr: String = Serialization.write(result)
      writer.write(jsonStr)
      writer.close()
    }
  }


  def visualizeBak(implicit sc: SparkContext, image: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]), method: String = null, min: Int = 0, max: Int = 255,
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
      val pathReproject = executorOutputDir + "oge_" + time + "_reproject.png"
      val path_img = "http://125.220.153.26:8093/ogedemooutput/oge_" + time + ".png"
      if (palette == null) {
        val colorMap = {
          ColorMap(
            scala.Predef.Map(
              max -> geotrellis.raster.render.RGBA(255, 255, 255, 255),
              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20),
              -1 -> geotrellis.raster.render.RGBA(255, 255, 255, 255)
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
              max -> geotrellis.raster.render.RGBA(127, 255, 170, 255),
              min -> geotrellis.raster.render.RGBA(0, 0, 0, 20)
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
        if (palette == null) {
          val colorMap = {
            ColorMap(
              scala.Predef.Map(
                max -> geotrellis.raster.render.RGBA(255, 255, 255, 255),
                min -> geotrellis.raster.render.RGBA(0, 0, 0, 20),
                -1 -> geotrellis.raster.render.RGBA(255, 255, 255, 255)
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
                max -> geotrellis.raster.render.RGBA(127, 255, 170, 255),
                min -> geotrellis.raster.render.RGBA(0, 0, 0, 20)
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
      imagePng.sortBy(t => t._3).foreach(t => {
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
      val jsonStr: String = Serialization.write(result)
      writer.write(jsonStr)
      writer.close()
    }
  }
}