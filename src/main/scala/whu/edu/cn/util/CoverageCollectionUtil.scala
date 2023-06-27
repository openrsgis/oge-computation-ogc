package whu.edu.cn.util

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{ArrayTile, CellType, DoubleConstantNoDataCellType, MultibandTile, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.{OGEDataType, RawTile, SpaceTimeBandKey}
import whu.edu.cn.util.CoverageUtil.makeCoverageRDD

import java.time.Instant
import scala.collection.{immutable, mutable}
import scala.util.Random

object CoverageCollectionUtil {
  def checkMapping(coverage: String, algorithm: (String, String, mutable.Map[String, String])): (String, String, mutable.Map[String, String]) = {
    for (tuple <- algorithm._3) {
      if (tuple._2.contains("MAPPING")) {
        algorithm._3.remove(tuple._1)
        algorithm._3.put(tuple._1, coverage)
      }
    }
    algorithm
  }

  // TODO: lrx: 函数的RDD大写，变量的Rdd小写，为了开源全局改名，提升代码质量
  // 不能用嵌套RDD，因为makeCoverageRDD需要MinIOClient，这个类不能序列化
  def makeCoverageCollectionRDD(rawTileRdd: Map[String, RDD[RawTile]]): Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    rawTileRdd.map(t => {
      (t._1, makeCoverageRDD(t._2))
    })
  }

  // TODO lrx: 检查相同的影像被写入同一个CoverageCollection，这样是不用拼的
  // 如果每个Coverage波段相同就拼，不相同就报错
  // TODO lrx: 黑边的拼接是不是不需要进行运算？否则会让值变化
  // TODO lrx: 这里的NoData是不是需要定义？感觉在Load取数据的时候就要规定好NoData到底是多少，后面更好的区分黑边和NoData
  // TODO lrx: 黑边是0，能不能变成其他值？不然不好识别黑边
  def coverageCollectionMosaicTemplate(coverageCollection: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])], method: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    if (coverageCollection.size != 1) {
      val time: Long = Instant.now.getEpochSecond
      val extents: (Double, Double, Double, Double) = coverageCollection.map(t => t._2._1.map(layer => {
        val key: SpatialKey = layer._1.spaceTimeKey.spatialKey
        val extentR: Extent = t._2._2.mapTransform(key)
        (extentR.xmin, extentR.ymin, extentR.xmax, extentR.ymax)
      }).reduce((a, b) => {
        (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
      })).reduce((a, b) => {
        (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
      })

      // 定义最广泛的数据类型
      val cellType: CellType = coverageCollection.map(t => t._2._1.first()._2.cellType).reduce((a, b) => {
        CellType.fromName(OGEDataType.compareAndGetType(OGEDataType.withName(a.toString()), OGEDataType.withName(b.toString())).toString)
      })

      val bandList: List[mutable.ListBuffer[String]] = coverageCollection.map(t => t._2._1.first()._1.measurementName).toList
      val allSame: Boolean = bandList.forall(_.equals(bandList.head))

      if (allSame) {

        val resoCrsMap: Map[Double, CRS] = coverageCollection.map(t => (t._2._2.cellSize.resolution, t._2._2.crs))
        val resoMin: Double = resoCrsMap.keys.min
        val crs: CRS = resoCrsMap(resoMin)

        val layoutCols: Int = math.max(math.ceil((extents._3 - extents._1) / resoMin / 256.0).toInt, 1)
        val layoutRows: Int = math.max(math.ceil((extents._4 - extents._2) / resoMin / 256.0).toInt, 1)

        val tl: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
        // Extent必须进行重新计算，因为layoutCols和layoutRows加了黑边，导致范围变化了
        val extent: Extent = new Extent(extents._1, extents._2, extents._1 + resoMin * 256.0 * layoutCols, extents._2 + resoMin * 256.0 * layoutRows)
        val ld: LayoutDefinition = LayoutDefinition(extent, tl)

        val coverageCollectionRetiled: List[RDD[(SpatialKey, MultibandTile)]] = coverageCollection.map(coverage => {
          // TODO lrx: 这里要转数据类型

          val coveragetileRDD: RDD[(SpatialKey, MultibandTile)] = coverage._2._1.map(t => {
            (t._1.spaceTimeKey.spatialKey, t._2)
          })
          val srcBounds: Bounds[SpaceTimeKey] = coverage._2._2.bounds
          val newBounds: Bounds[SpatialKey] = Bounds(SpatialKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2), SpatialKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2))
          val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverage._2._2.cellType, coverage._2._2.layout, coverage._2._2.extent, coverage._2._2.crs, newBounds)
          val coverageRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = MultibandTileLayerRDD(coveragetileRDD, rasterMetaData)

          var coveragetileLayerRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = coverageRdd

          val reso: Double = coverage._2._2.cellSize.resolution
          val resoRatio: Double = reso / resoMin

          // 对影像进行瓦片大小重切分
          val srcExtent: Extent = coverage._2._2.layout.extent
          val tileLayout: TileLayout = coverage._2._2.layout.tileLayout
          val tileRatio: Int = (math.log(resoRatio) / math.log(2)).toInt
          if (tileRatio != 0) {
            val newTileSize: Int = 256 / math.pow(2, tileRatio).toInt
            val newTileLayout: TileLayout = TileLayout(tileLayout.layoutCols * math.pow(2, tileRatio).toInt, tileLayout.layoutRows * math.pow(2, tileRatio).toInt, newTileSize, newTileSize)
            val newLayout: LayoutDefinition = LayoutDefinition(srcExtent, newTileLayout)
            val (_, coverageRetiled) = coverageRdd.reproject(coverageRdd.metadata.crs, newLayout)

            val cropExtent: Extent = extent.reproject(crs, coverageRetiled.metadata.crs)
            coveragetileLayerRdd = coverageRetiled.crop(cropExtent)
          }

          val (_, reprojectedRdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
            coveragetileLayerRdd.reproject(crs, ld)

          // Filter配合extent的强制修改，达到真正裁剪到我们想要的Layout的目的
          val reprojFilter: RDD[(SpatialKey, MultibandTile)] = reprojectedRdd.filter(layer => {
            val key: SpatialKey = layer._1
            val extentR: Extent = ld.mapTransform(key)
            extentR.xmin >= extent.xmin && extentR.xmax <= extent.xmax && extentR.ymin >= extent.ymin && extentR.ymax <= extent.ymax
          })

          // 这里LayerMetadata直接使用reprojectRdd的，尽管SpatialKey有负值也不影响
          reprojFilter
        }).toList

        val rddUnion: RDD[(SpatialKey, MultibandTile)] = coverageCollectionRetiled.reduce((a, b) => {
          a.union(b)
        })

        val groupedRdd: RDD[(SpatialKey, Iterable[MultibandTile])] = rddUnion.groupByKey()
        val coverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = groupedRdd.map(t => {
          (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, time), bandList.head), funcMulti(t._2, method, cellType))
        })

        val spatialKeys: (Int, Int, Int, Int) = coverageRdd.map(t => {
          val spatialKey: SpatialKey = t._1.spaceTimeKey.spatialKey
          (spatialKey.col, spatialKey.row, spatialKey.col, spatialKey.row)
        }).reduce((a, b) => {
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        })
        val bounds: Bounds[SpaceTimeKey] = Bounds(SpaceTimeKey(spatialKeys._1, spatialKeys._2, time), SpaceTimeKey(spatialKeys._3, spatialKeys._4, time))
        val coverageMetadata: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(cellType, ld, extent, crs, bounds)
        (coverageRdd, coverageMetadata)
      }
      else {
        throw new IllegalArgumentException("Error: 波段数量不相等，波段的名称、顺序和个数必须完全相同")
      }

    }
    else {
      coverageCollection.head._2
    }

  }

  def funcMulti(multibandTile: Iterable[MultibandTile], method: String, cellType: CellType): MultibandTile = {
    val flatTiles: Iterable[(Tile, Int)] = multibandTile.flatMap(t => {
      t.bands.zipWithIndex
    })
    val groupedTiles: Map[Int, Iterable[Tile]] = flatTiles.groupBy(t => t._2).map(t => (t._1, t._2.map(x => x._1.convert(cellType))))

    val mapComputed: Map[Int, Tile] =
      method match {
        case "min" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Min(a, b)))
          })
        case "max" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Max(a, b)))
          })
        case "sum" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Add(Mean(a, b), Mean(a, b))))
          })
        case "or" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Or(a, b)))
          })
        case "and" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => And(a, b)))
          })
        case "mean" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            (t._1, tiles.reduce((a, b) => Mean(a, b)))
          })
        case "median" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            tiles.head.cellType.toString() match {
              case "int32" | "int32raw" =>
                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Int] = tile.toArray()
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val medianValues: Array[Int] = bandArrays.transpose.map(t => {
                  if (t.length % 2 == 1) {
                    t.sorted.apply(bandArrays.length / 2)
                  }
                  else {
                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2
                  }
                })
                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
                (t._1, medianTile)
              case "float32" | "float32raw" =>
                val bandArrays: Array[Array[Float]] = Array.ofDim[Float](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Float] = tile.toArrayDouble().map(_.toFloat)
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val medianValues: Array[Float] = bandArrays.transpose.map(t => {
                  if (t.length % 2 == 1) {
                    t.sorted.apply(bandArrays.length / 2)
                  }
                  else {
                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2.0f
                  }
                })
                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
                (t._1, medianTile)
              case "float64" | "float64raw" =>
                val bandArrays: Array[Array[Double]] = Array.ofDim[Double](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Double] = tile.toArrayDouble()
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val medianValues: Array[Double] = bandArrays.transpose.map(t => {
                  if (t.length % 2 == 1) {
                    t.sorted.apply(bandArrays.length / 2)
                  }
                  else {
                    (t.sorted.apply(bandArrays.length / 2) + t.sorted.apply(bandArrays.length / 2 - 1)) / 2.0
                  }
                })
                val medianTile: Tile = ArrayTile(medianValues, tiles.head.cols, tiles.head.rows)
                (t._1, medianTile)
            }
          })
        case "mode" =>
          groupedTiles.map(t => {
            val tiles: Iterable[Tile] = t._2
            tiles.head.cellType.toString() match {
              case "int32" | "int32raw" =>
                val bandArrays: Array[Array[Int]] = Array.ofDim[Int](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Int] = tile.toArray()
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val modeValues: Array[Int] = bandArrays.transpose.map(array => {
                  val counts: Map[Int, Int] = array.groupBy(identity).mapValues(_.length)
                  val maxCount: Int = counts.values.max
                  val modes: List[Int] = counts.filter(_._2 == maxCount).keys.toList
                  modes(Random.nextInt(modes.size))
                })
                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
                (t._1, modeTile)
              case "float32" | "float32raw" =>
                val bandArrays: Array[Array[Float]] = Array.ofDim[Float](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Float] = tile.toArrayDouble().map(_.toFloat)
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val modeValues: Array[Float] = bandArrays.transpose.map(array => {
                  val counts: Map[Float, Int] = array.groupBy(identity).mapValues(_.length)
                  val maxCount: Int = counts.values.max
                  val modes: List[Float] = counts.filter(_._2 == maxCount).keys.toList
                  modes(Random.nextInt(modes.size))
                })
                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
                (t._1, modeTile)
              case "float64" | "float64raw" =>
                val bandArrays: Array[Array[Double]] = Array.ofDim[Double](tiles.size, tiles.head.rows * tiles.head.cols)
                tiles.zipWithIndex.foreach { case (tile, bandIndex) =>
                  val data: Array[Double] = tile.toArrayDouble()
                  Array.copy(data, 0, bandArrays(bandIndex), 0, data.length)
                }
                val modeValues: Array[Double] = bandArrays.transpose.map(array => {
                  val counts: Map[Double, Int] = array.groupBy(identity).mapValues(_.length)
                  val maxCount: Int = counts.values.max
                  val modes: List[Double] = counts.filter(_._2 == maxCount).keys.toList
                  modes(Random.nextInt(modes.size))
                })
                val modeTile: Tile = ArrayTile(modeValues, tiles.head.cols, tiles.head.rows)
                (t._1, modeTile)
            }
          })
        case _ =>
          throw new IllegalArgumentException("Error: 该拼接方法不存在:" + method)
      }
    MultibandTile(mapComputed.toArray.sortBy(t => t._1).map(t => t._2))
  }

}
