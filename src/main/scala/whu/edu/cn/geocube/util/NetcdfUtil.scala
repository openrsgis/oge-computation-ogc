package whu.edu.cn.geocube.util

import geotrellis.layer.stitch.TileLayoutStitcher
import ucar.nc2.{Dimension, NetcdfFileWriter}
import whu.edu.cn.geocube.core.entity.{GcDimension, QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import ucar.ma2.{ArrayDouble, ArrayFloat, ArrayInt, ArrayLong, ArrayString, DataType, Index}
import whu.edu.cn.geocube.application.gdc.gdcCoverage.{getCropExtent, scaleCoverage}
import java.io.IOException
import java.text.SimpleDateFormat
import java.time.{LocalTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Collections, Date}
import scala.collection.mutable.ArrayBuffer

object NetcdfUtil {

  def rasterRDD2Netcdf(measurements: ArrayBuffer[String], rasterTileLayerRdd: (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]),
                       queryParams: QueryParams, scaleSize: Array[java.lang.Integer],
                       scaleAxes: Array[java.lang.Double], scaleFactor: java.lang.Double, outputDir: String): String = {
    val time = System.currentTimeMillis()
    val sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val date = new Date(time)
    val instantRet = sdf.format(date)
    val outputImagePath = outputDir + "Coverage_" + instantRet + ".nc"
    val ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, outputImagePath)
    ncFile.create()
    try {
      val metadata: TileLayerMetadata[SpaceTimeKey] = rasterTileLayerRdd._2.tileLayerMetadata
      val spatialMetadata: TileLayerMetadata[SpatialKey] = TileLayerMetadata(
        metadata.cellType,
        metadata.layout,
        metadata.extent,
        metadata.crs,
        metadata.bounds.get.toSpatial)
      val rasterTileRddWithDimensions: RDD[((ZonedDateTime, String, Seq[GcDimension]), Raster[Tile])] = rasterTileLayerRdd._1.map(v => ((v._1.spaceTimeKey.time, v._1.measurementName, v._1.additionalDimensions.toSeq), (v._1.spaceTimeKey.spatialKey, v._2)))
        .groupBy(_._1).mapValues(iterable => iterable.map(_._2)).map(v => {
        var stitched: Raster[Tile] = Raster(TileLayoutStitcher.stitch(v._2)._1, spatialMetadata.extent)
        var stitchedExtent = stitched.extent
        val queryExtentCoordinates = queryParams.extentCoordinates
        val cropExtent = getCropExtent(queryExtentCoordinates, stitchedExtent, stitched)
        val scaleExtent = scaleCoverage(stitched.tile, scaleSize, scaleAxes, scaleFactor)
        if (cropExtent != null) {
          stitched = stitched.crop(cropExtent._1, cropExtent._2, cropExtent._3, cropExtent._4)
          stitchedExtent = cropExtent._5
        }
        if (scaleExtent != null) {
          stitched = stitched.resample(scaleExtent._1, scaleExtent._2)
        }
        (v._1, stitched)
      })
      val dimensionsRDD = rasterTileLayerRdd._1.map(v => (v._1.spaceTimeKey.time, v._1.measurementName, v._1.additionalDimensions))
      val timeDimensionArray: Array[String] = dimensionsRDD.map(_._1).distinct().map(zonedDateTime => {
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z") // 定义日期时间格式
        zonedDateTime.format(formatter) // 将 ZonedDateTime 转换为字符串
      }).collect()
      ncFile.setRedefineMode(true)
      val timeDimension: Dimension = ncFile.addDimension("validTime", timeDimensionArray.length)
      //      ncFile.addVariable("validTime", DataType.STRING, Collections.singletonList(timeDimension))
      ncFile.addVariable("validTime", DataType.LONG, Collections.singletonList(timeDimension))
      ncFile.addVariableAttribute("validTime", "unit", "ms")
      ncFile.addVariableAttribute("validTime", "description", "The time in milliseconds since January 1,1970")
      val timeArray: ArrayLong = new ArrayLong.D1(timeDimensionArray.length, true)
      var index = 0
      for (time: String <- timeDimensionArray) {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date: Date = dateFormat.parse(time)
        timeArray.setLong(index, date.getTime)
        index = index + 1
      }
      ncFile.setRedefineMode(false)
      ncFile.write("validTime", timeArray)

      ncFile.setRedefineMode(true)
      val firstRasterTileRddWithDimensions = rasterTileRddWithDimensions.first()._2
      val lonLatArray = getLonLatFromRasterTile(firstRasterTileRddWithDimensions)
      val lonDimension: Dimension = ncFile.addDimension("lon", lonLatArray._1.length)
      val latDimension: Dimension = ncFile.addDimension("lat", lonLatArray._2.length)
      ncFile.addVariable("lon", DataType.DOUBLE, Collections.singletonList(lonDimension))
      ncFile.addVariable("lat", DataType.DOUBLE, Collections.singletonList(latDimension))
      ncFile.setRedefineMode(false)
      val lonArray: ArrayDouble = new ArrayDouble.D1(lonLatArray._1.length)
      index = 0
      for (lon: Double <- lonLatArray._1) {
        lonArray.setDouble(index, lon)
        index = index + 1
      }
      ncFile.write("lon", lonArray)
      val latArray: ArrayDouble = new ArrayDouble.D1(lonLatArray._2.length)
      index = 0
      for (lat: Double <- lonLatArray._2) {
        latArray.setDouble(index, lat)
        index = index + 1
      }
      ncFile.write("lat", latArray)
      ncFile.setRedefineMode(true)
      ncWriteAdditionalDimensions(rasterTileRddWithDimensions, ncFile)
      ncFile.setRedefineMode(true)
      ncWriteMeasurements(rasterTileRddWithDimensions, ncFile, timeDimensionArray, measurements, lonLatArray._3, lonLatArray._4)
      ncFile.flush()
      outputImagePath
    } finally try {
      if (ncFile != null) ncFile.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  def getLonLatFromRasterTile(rasterTile: Raster[Tile]): (Array[Double], Array[Double], ArrayBuffer[Int], ArrayBuffer[Int]) = {
    val lonSet: scala.collection.mutable.Set[Double] = scala.collection.mutable.Set.empty
    val latSet: scala.collection.mutable.Set[Double] = scala.collection.mutable.Set.empty

    val extent: Extent = rasterTile.extent
    val cellSize = rasterTile.rasterExtent.cellSize
    val cols = rasterTile.cols
    val rows = rasterTile.rows
    val colArray: ArrayBuffer[Int] = ArrayBuffer.range(0, cols)
    val rowArray: ArrayBuffer[Int] = ArrayBuffer.range(0, rows)
    for (col <- 0 until cols; row <- 0 until rows) {
      val x: Double = extent.xmin + col * cellSize.width
      val y: Double = extent.ymax - row * cellSize.height
      lonSet.add(x)
      latSet.add(y)
    }

    // 将经纬度坐标转换为排序后的数组
    val sortedLonArray: Array[Double] = lonSet.toSeq.sorted.toArray
    val sortedLatArray: Array[Double] = latSet.toSeq.sorted.toArray
    (sortedLonArray, sortedLatArray, colArray, rowArray)
  }

  def ncWriteAdditionalDimensions(rasterTileRddWithDimensions: RDD[((ZonedDateTime, String, Seq[GcDimension]), Raster[Tile])],
                                  ncFile: NetcdfFileWriter): Unit = {
    val additionalDimensions: Seq[GcDimension] = rasterTileRddWithDimensions.first()._1._3
    for (gcDimension <- additionalDimensions) {
      val dimensionCoordinatesStr = gcDimension.getCoordinates
      val dimension: Dimension = ncFile.addDimension(gcDimension.getDimensionName, dimensionCoordinatesStr.length)
      if (gcDimension.getMemberType.contains("int")) {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.INT, Collections.singletonList(dimension))
        val dimensionArray: ArrayInt = new ArrayInt.D1(dimensionCoordinatesStr.length, true)
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          dimensionArray.setInt(index, coordinate.toInt)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      } else if (gcDimension.getMemberType.contains("float")) {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.FLOAT, Collections.singletonList(dimension))
        val dimensionArray: ArrayFloat = new ArrayFloat.D1(dimensionCoordinatesStr.length)
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          dimensionArray.setFloat(index, coordinate.toFloat)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      } else if (gcDimension.getMemberType.contains("double")) {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.DOUBLE, Collections.singletonList(dimension))
        val dimensionArray: ArrayDouble = new ArrayDouble.D1(dimensionCoordinatesStr.length)
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          dimensionArray.setDouble(index, coordinate.toDouble)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      } else if (gcDimension.getMemberType.equals("date")) {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.LONG, Collections.singletonList(dimension))
        val dimensionArray: ArrayLong = new ArrayLong.D1(dimensionCoordinatesStr.length, true)
        ncFile.addVariableAttribute(gcDimension.getDimensionName, "unit", "ms")
        ncFile.addVariableAttribute(gcDimension.getDimensionName, "description", "The time in milliseconds since January 1,1970")
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
          val date: Date = dateFormat.parse(coordinate)
          dimensionArray.setLong(index, date.getTime)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      } else if (gcDimension.getMemberType.equals("time")) {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.LONG, Collections.singletonList(dimension))
        val dimensionArray: ArrayLong = new ArrayLong.D1(dimensionCoordinatesStr.length, true)
        ncFile.addVariableAttribute(gcDimension.getDimensionName, "unit", "s")
        ncFile.addVariableAttribute(gcDimension.getDimensionName, "description", "Seconds since midnight")
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          val localTime: LocalTime = LocalTime.parse(coordinate)
          dimensionArray.setLong(index, localTime.toSecondOfDay)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      } else {
        ncFile.addVariable(gcDimension.getDimensionName, DataType.STRING, Collections.singletonList(dimension))
        val dimensionArray: ArrayString = new ArrayString.D1(dimensionCoordinatesStr.length)
        var index = 0
        for (coordinate: String <- dimensionCoordinatesStr) {
          dimensionArray.setObject(index, coordinate)
          index = index + 1
        }
        ncFile.setRedefineMode(false)
        ncFile.write(gcDimension.getDimensionName, dimensionArray)
        ncFile.setRedefineMode(true)
      }
    }
  }

  def isAddDimensionCombineSame(additionalDimensions: Seq[GcDimension],
                                input: Seq[GcDimension], inputIndex: ArrayBuffer[Int]): Boolean = {
    //    val additionalDimensions: Array[GcDimension] = rasterTileRddWithDimensions.first()._1._3
    var index = 0
    for (gcDimension <- additionalDimensions) {
      val dimensionCoordinatesStr = gcDimension.getCoordinates
      if (!dimensionCoordinatesStr(inputIndex(index)).equals(input(index).value.toString)) return false
      index = index + 1
    }
    true
  }

  def isAddDimensionSame(additionalDimensions: Array[GcDimension]): Boolean = {
      for(additionalDimension <- additionalDimensions){
        val minCoordinates = additionalDimension.coordinates(0)
        if(!(additionalDimension.value.toString == minCoordinates)){
          return false
        }
      }
    return true
  }

  def ncWriteMeasurements(rasterTileRddWithDimensions: RDD[((ZonedDateTime, String, Seq[GcDimension]), Raster[Tile])],
                          ncFile: NetcdfFileWriter, timeArray: Array[String], measurementArray: ArrayBuffer[String],
                          cols: ArrayBuffer[Int], rows: ArrayBuffer[Int]): Unit = {
    val additionalDimensions: Seq[GcDimension] = rasterTileRddWithDimensions.first()._1._3
    val dimensionList: util.ArrayList[Dimension] = new util.ArrayList[Dimension]()
    dimensionList.add(ncFile.findDimension("validTime"))
    dimensionList.add(ncFile.findDimension("lon"))
    dimensionList.add(ncFile.findDimension("lat"))
    val variableSizeArray: Array[Int] = new Array[Int](3 + additionalDimensions.length)
    variableSizeArray(0) = timeArray.length
    variableSizeArray(1) = cols.length
    variableSizeArray(2) = rows.length
    val addDimensionRange = new ArrayBuffer[ArrayBuffer[Int]]()
    var index = 1
    for (additionalDimension: GcDimension <- additionalDimensions) {
      dimensionList.add(ncFile.findDimension(additionalDimension.dimensionName))
      val dimensionLength = ncFile.findDimension(additionalDimension.dimensionName).getLength
      variableSizeArray(index + 2) = dimensionLength
      addDimensionRange.append(ArrayBuffer.range(0, dimensionLength))
      index = index + 1
    }
    val combinedAddDimensionArray = new ArrayBuffer[ArrayBuffer[Int]]()
    generateCombinations(0, addDimensionRange, ArrayBuffer[Int](), combinedAddDimensionArray)

    for ((measurement, measurementKey) <- measurementArray.zipWithIndex) {
      ncFile.addVariable(measurement, DataType.DOUBLE, dimensionList)
      val variableArray = new ArrayDouble(variableSizeArray)
      val variableIndex: Index = variableArray.getIndex
      for ((time, timeKey) <- timeArray.zipWithIndex) {
        for (combinedAddDimension: ArrayBuffer[Int] <- combinedAddDimensionArray) {
          // 过滤符合条件的Tile
          val matchingRasters: RDD[Raster[Tile]] = rasterTileRddWithDimensions.filter {
            case ((zdt, str, gcDims), _) => {
              val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")
              val zonedDateTime: ZonedDateTime = ZonedDateTime.parse(time, formatter)
              zdt == zonedDateTime && str == measurement && isAddDimensionCombineSame(additionalDimensions, gcDims, combinedAddDimension)
            }
          }.map(_._2)
          val firstMatchingRaster = matchingRasters.first()
          if (!matchingRasters.isEmpty()) {
            for (col <- cols) {
              for (row <- rows) {
                val indexArray: Array[Int] = new Array[Int](3 + additionalDimensions.length)
                indexArray(0) = timeKey
                indexArray(1) = col
                indexArray(2) = rows.length - 1 - row
                for (i <- additionalDimensions.indices) {
                  indexArray(3 + i) = combinedAddDimension(i)
                }
                variableArray.setDouble(variableIndex.set(indexArray), firstMatchingRaster.tile.getDouble(col, row))
              }
            }
          }
        }
      }
      ncFile.setRedefineMode(false)
      ncFile.write(measurement, variableArray)
    }
  }

  def generateCombinations(index: Int, input: ArrayBuffer[ArrayBuffer[Int]], combination: ArrayBuffer[Int], output: ArrayBuffer[ArrayBuffer[Int]]): Unit = {
    if (index >= input.length) {
      // 打印生成的组合
      output.append(combination.clone())
      println(combination.mkString(", "))
    } else {
      // 获取当前元素
      val currentElement = input(index)
      // 遍历当前元素的每个值
      for (value <- currentElement) {
        // 将当前值添加到组合中
        combination += value
        // 递归生成下一个元素的组合
        generateCombinations(index + 1, input, combination, output)
        // 移除当前值，以便尝试下一个值
        combination.remove(combination.length - 1)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val A1 = new ArrayBuffer[ArrayBuffer[Int]]()
    val a1 = new ArrayBuffer[Int]()
    a1.append(1)
    a1.append(2)
    val a2 = new ArrayBuffer[Int]()
    a2.append(3)
    a2.append(4)
    a2.append(5)
    val a3 = new ArrayBuffer[Int]()
    a3.append(7)
    a3.append(8)
    A1.append(a1)
    A1.append(a2)
    A1.append(a3)
    val combineArray = new ArrayBuffer[Int]()
    val combineArray2 = new ArrayBuffer[ArrayBuffer[Int]]()
    generateCombinations(0, A1, combineArray, combineArray2)
    println("s")
    //    val A1 = ArrayBuffer(ArrayBuffer(1, 2), ArrayBuffer(3, 4, 5), ArrayBuffer(6))
    //    val combinations = enumerateCombinations(A1)
    //    combinations.foreach(combination => println(combination.mkString(", ")))
    //
    //    println('s')
  }
}
