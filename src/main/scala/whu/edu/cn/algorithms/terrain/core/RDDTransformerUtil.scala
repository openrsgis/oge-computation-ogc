package whu.edu.cn.algorithms.terrain.core

import TypeAliases.RDDImage
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, DoubleArrayTile, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, Raster, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import whu.edu.cn.entity.SpaceTimeBandKey

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
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), ListBuffer("Aspect")), t._2)
    })
    println("成功读取tif")
    (tiledOut, metaData)
  }

  /**
   * @param image RDDImage
   * @param radius padding的栅格数量
   * @return RDDImage
   *
   * @todo need optimization
   */
  def paddingRDDImage(
      image: RDDImage,
      radius: Int
  ): RDDImage = {
    if (radius == 0) {
      return image
    }

    val nodata: Double = image._2.cellType match {
      case x: DoubleUserDefinedNoDataCellType => x.noDataValue
      case x: FloatUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: IntUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case ByteConstantNoDataCellType => ByteConstantNoDataCellType.noDataValue.toDouble
      case UByteConstantNoDataCellType => UByteConstantNoDataCellType.noDataValue.toDouble
      case ShortConstantNoDataCellType => ShortConstantNoDataCellType.noDataValue.toDouble
      case UShortConstantNoDataCellType => UShortConstantNoDataCellType.noDataValue.toDouble
      case IntConstantNoDataCellType => IntConstantNoDataCellType.noDataValue.toDouble
      case FloatConstantNoDataCellType => FloatConstantNoDataCellType.noDataValue.toDouble
      case DoubleConstantNoDataCellType => DoubleConstantNoDataCellType.noDataValue
      case _ => Double.NaN
    }

    val leftNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 1), t._2)
      )
    })

    val rightNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 1), t._2)
      )
    })

    val upNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 0), t._2)
      )
    })

    val downNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 2), t._2)
      )
    })

    val leftUpNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 0), t._2)
      )
    })

    val upRightNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 0), t._2)
      )
    })

    val rightDownNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 2), t._2)
      )
    })

    val downLeftNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 2), t._2)
      )
    })

    val midNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 1), t._2)
      )
    })

    // 合并邻域RDD
    val unionRDD = leftNeighborRDD
      .union(rightNeighborRDD)
      .union(upNeighborRDD)
      .union(downNeighborRDD)
      .union(leftUpNeighborRDD)
      .union(upRightNeighborRDD)
      .union(rightDownNeighborRDD)
      .union(downLeftNeighborRDD)
      .union(midNeighborRDD)
      .filter(t => {
        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 &&
          t._1.spaceTimeKey.spatialKey._1 < image._2.layout.layoutCols &&
          t._1.spaceTimeKey.spatialKey._2 < image._2.layout.layoutRows
      })

    val groupRDD = unionRDD
      .groupByKey()
      .map(t => {
        val listBuffer = new ListBuffer[(SpatialKey, MultibandTile)]()
        val list = t._2.toList
        val listKey = List(
          SpatialKey(0, 0),
          SpatialKey(0, 1),
          SpatialKey(0, 2),
          SpatialKey(1, 0),
          SpatialKey(1, 1),
          SpatialKey(1, 2),
          SpatialKey(2, 0),
          SpatialKey(2, 1),
          SpatialKey(2, 2)
        )
        for (key <- listKey) {
          var flag = false
          breakable {
            for (tile <- list) {
              if (key.equals(tile._1)) {
                listBuffer.append(tile)
                flag = true
                break
              }
            }
          }
          if (!flag) {
            listBuffer.append((key, MultibandTile(DoubleArrayTile(Array.fill[Double](256 * 256)(nodata), 256, 256, nodata))))
          }
        }
        val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
        (
          t._1,
          tile
            .crop(256 - radius, 256 - radius, 511 + radius, 511 + radius)
            .withNoData(Option(nodata))
        )
      })

    (groupRDD, image._2)

  }

}
