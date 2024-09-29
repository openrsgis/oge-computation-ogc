package whu.edu.cn.algorithms.ImageProcess.core

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, LayoutDefinition, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RDDTransformerUtil {

  /**
   * store TIFF to disk
   *
   * @param input raster data in [[RDDImage]] format
   * @param outputTiffPath output path
   */
  def saveRasterRDDToTif(
      input: RDDImage,
      outputTiffPath: String
  ): Unit = {
    val tileLayerArray = input._1
      .map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      })
      .collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    println("成功落地tif")
  }


  /**
   * 修复原来的 makeChangedRasterRDDFromTif 方法存在的偏移问题
   *
   * @param sc             Spark 上下文
   * @param sourceTiffPath TIFF 路径
   * @return RDDImage
   * @author 2024-08 Tank
   */
  def makeChangedRasterRDDFromTifNew(
                                   sc: SparkContext,
                                   sourceTiffPath: String
                                 ): RDDImage = {
    val hadoopPath = sourceTiffPath
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(new Path(hadoopPath))(sc)
    // 1. 指定划分的单张瓦片的大小
    val localLayoutScheme = FloatingLayoutScheme(256)
    // 2. 根据布局方案计算并收集影像的元数据信息，包括布局、空间范围、坐标系等
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    // 3. 将影像数据按照指定的布局方案划分为瓦片，并缓存已经更正空间范围为新瓦片布局下的空间范围到内存中以提高后续计算的效率
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    // 4. 添加时间维度元数据信息
    val srcBounds = metadata.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(
      SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, date),
      SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, date)
    )
    /* 5.
       步骤2没有修正空间范围，
       我认为是 geotrellis 框架本身源码写的有疏漏，
       更正空间范围为新瓦片布局下的空间范围，左上角原点不变,
       然后更新布局参数信息
     */
    val newLeft = metadata.extent.xmin
    val newTop = metadata.extent.ymax
    val newRight = newLeft + metadata.cellheight * metadata.cols
    val newBottom = newTop - metadata.cellwidth * metadata.rows
    val newExtent = Extent(newLeft, newBottom, newRight, newTop)
    val newLayout = LayoutDefinition(newExtent, metadata.tileLayout)
    // 6. 更新元数据信息
    val newMetaData = TileLayerMetadata(metadata.cellType, newLayout, newExtent, metadata.crs, newBounds)
    // 7. 将原有的瓦片数据映射为具有空间、时间和波段键的RDD
    // measurementName从"B1"开始依次为各波段命名，解决用户上传数据无法筛选波段的问题（没有波段名称）
    val bandCount: Int = tiled.first()._2.bandCount
    val measurementName = ListBuffer.empty[String]
    for (i <- 1 to bandCount) measurementName.append(s"B$i")
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), measurementName), t._2)
    })
    println("成功读取tif")
    (tiledOut, newMetaData)
  }


  /**
   * read TIFF from disk use specified Spark Context
   *
   * @param sc Spark上下文
   * @param sourceTiffPath TIFF路径
   * @return RDDImage
   */
  def makeChangedRasterRDDFromTif(
      sc: SparkContext,
      sourceTiffPath: String
  ): RDDImage = {
    val hadoopPath = sourceTiffPath
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(new Path(hadoopPath))(sc)
    val localLayoutScheme = FloatingLayoutScheme(256)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    val srcLayout = metadata.layout
    val srcExtent = metadata.extent
    val srcCrs = metadata.crs
    val srcBounds = metadata.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(
      SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, date),
      SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, date)
    )
    val metaData =
      TileLayerMetadata(metadata.cellType, srcLayout, srcExtent, srcCrs, newBounds)
    // measurementName从"B1"开始依次为各波段命名，解决用户上传数据无法筛选波段的问题（没有波段名称）
    val bandCount: Int = tiled.first()._2.bandCount
    val measurementName = ListBuffer.empty[String]
    for (i <- 1 to bandCount) measurementName.append(s"B$i")
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), measurementName), t._2)
    })
    println("成功读取tif")
    (tiledOut, metaData)
  }
  /**
    * Expand the pixels outward along the edge of the tile
    *
    * @param coverage The input DEM
    * @param radius The radius of the kernel to generate.
    * @param borderType  "0"：BORDER_CONSTANT;"1": BORDER_REPLICATE；"2": BORDER_REFLECT;"3": BORDER_WRAP。
    * @param borderValue Fill by fix value
    * @return
    */
  def paddingRDD(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), radius: Int, borderType: String = "0", borderValue: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage1 = coverage._1.collect().toMap

    val borderTypeInput: String = Map(
      "0" -> "0",
      "1" -> "1",
      "2" -> "2",
      "3" -> "3"
    ).getOrElse(borderType, "0")

    val convolvedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {
      val col = t._1.spaceTimeKey.col
      val row = t._1.spaceTimeKey.row
      val time0 = t._1.spaceTimeKey.time

      val cols = t._2.cols
      val rows = t._2.rows

      // 创建一个可变的ArrayBuffer，用于存储卷积后的波段数据
      var arrayBuffer: mutable.ArrayBuffer[Tile] = new mutable.ArrayBuffer[Tile] {}
      for (index <- t._2.bands.indices) {
        // 对于每个波段，创建一个大小为(cols + 2 * radius) * (rows + 2 * radius)的arrBuffer数组
        val arrBuffer: Array[Double] = Array.ofDim[Double]((cols + 2 * radius) * (rows + 2 * radius))
        //arr转换为Tile，并拷贝原tile数据
        val tilePadded: Tile = ArrayTile(arrBuffer, cols + 2 * radius, rows + 2 * radius).convert(t._2.cellType)
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            tilePadded.mutable.setDouble(i + radius, j + radius, t._2.bands(index).getDouble(i, j)) //mutable转成可变的形式
          }
        }

        //填充八邻域数据及自身数据，使用mutable进行性能优化
        //左上
        var tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row - 1, time0), t._1.measurementName))  // 获取当前瓦片左上的瓦片
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, rows - radius + y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(0 + x, 0 + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(0 + x, 0 + y, t._2.bands(index).getDouble(0, 0))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(0 + x, 0 + y, t._2.bands(index).getDouble(radius - 1 - x, radius - 1 - y))  //ok
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(0 + x, 0 + y, t._2.bands(index).getDouble(x + cols - radius, y + rows - radius))
              }
            }
          }
        }

        //上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            //BORDER_CONSTANT
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, 0 + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, 0 + y, t._2.bands(index).getDouble(x, 0))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, 0 + y, t._2.bands(index).getDouble(x, radius - 1 - y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, 0 + y, t._2.bands(index).getDouble(x, y + rows - radius))
              }
            }
          }
        }

        //右上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            //赋0
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, 0 + y, borderValue)

              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, 0 + y, t._2.bands(index).getDouble(cols - 1, 0))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, 0 + y, t._2.bands(index).getDouble(cols - 1 - x, radius - 1 - y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, 0 + y, t._2.bands(index).getDouble(x, y + rows - radius))
              }
            }
          }
        }

        //左
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(0 + x, radius + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(0 + x, radius + y, t._2.bands(index).getDouble(0, y))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(0 + x, radius + y, t._2.bands(index).getDouble(radius - 1 - x, y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(0 + x, radius + y, t._2.bands(index).getDouble(x + cols - radius, y))
              }
            }
          }
        }

        //右
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(cols + radius + x, radius + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(cols + radius + x, radius + y, t._2.bands(index).getDouble(cols - 1, y))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(cols + radius + x, radius + y, t._2.bands(index).getDouble(cols - 1 - x, y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (rows)) {
                tilePadded.mutable.setDouble(cols + radius + x, radius + y, t._2.bands(index).getDouble(x, y))
              }
            }
          }
        }

        //左下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            //赋0
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(x, rows + radius + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(x, rows + radius + y, t._2.bands(index).getDouble(0, rows - 1))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(x, rows + radius + y, t._2.bands(index).getDouble(radius - 1 - x, rows - 1 - y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(x, rows + radius + y, t._2.bands(index).getDouble(x + cols - radius, y))
              }
            }
          }
        }

        //下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            //赋0
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, rows + radius + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, rows + radius + y, t._2.bands(index).getDouble(x, rows - 1))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, rows + radius + y, t._2.bands(index).getDouble(x, rows - 1 - y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (cols)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(radius + x, rows + radius + y, t._2.bands(index).getDouble(x, y))
              }
            }
          }
        }
        //右下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          if (borderTypeInput == "0") {
            //赋0
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, borderValue)
              }
            }
          } else if (borderTypeInput == "1") {
            // BORDER_REPLICATE
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, t._2.bands(index).getDouble(cols - 1, rows - 1))
              }
            }
          } else if (borderTypeInput == "2") {
            // BORDER_REFLECT
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, t._2.bands(index).getDouble(cols - 1 - x, rows - 1 - y))
              }
            }
          } else if (borderTypeInput == "3") {
            // BORDER_WRAP
            for (x <- 0 until (radius)) {
              for (y <- 0 until (radius)) {
                tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, t._2.bands(index).getDouble(x, y))
              }
            }
          }
        }
        arrayBuffer.append(tilePadded)
      }
      (t._1, MultibandTile(arrayBuffer))
    })
    (convolvedRDD, coverage._2)

  }

  def getTileFromCoverage(coverage: Map[SpaceTimeBandKey, MultibandTile]
                          ,spaceTimeBandKey: SpaceTimeBandKey) :Option[MultibandTile]={
    coverage.get(spaceTimeBandKey)
  }

}
