package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local._
import io.minio.MinioClient
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{reproject => _, _}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import javafx.scene.paint.Color
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import redis.clients.jedis.Jedis
import whu.edu.cn.entity.{CoverageMetadata, RawTile, SpaceTimeBandKey, VisualizationParam}
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.COGUtil.{getDouble, getTileBuf, tileQuery}
import whu.edu.cn.util.CoverageUtil.{checkProjResoExtent, coverageTemplate, focalMethods, makeCoverageRDD}
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage
import whu.edu.cn.util._
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.mapalgebra.focal.{CellwiseCalculation, CellwiseMedianCalc, Cursor, CursorCalculation, CursorMedianCalc, DoubleArrayTileResult, FocalCalculation, IntArrayTileResult, Median, MedianModeCalculation, Neighborhood, Square, TargetCell}
import geotrellis.spark.partition.SpacePartitioner
import jp.ne.opt.chronoscala.Imports.richZonedDateTime
import whu.edu.cn.geocube.util.EntropyUtil
import whu.edu.cn.util.HttpRequestUtil.sendPost

import java.time.{Instant, ZonedDateTime}
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

// TODO lrx: 后面和GEE一个一个的对算子，看看哪些能力没有，哪些算子考虑的还较少
// TODO lrx: 要考虑数据类型，每个函数一般都会更改数据类型
object Coverage {

  def load(implicit sc: SparkContext, coverageId: String, level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val zIndexStrArray: mutable.ArrayBuffer[String] = Trigger.zIndexStrArray

    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId)
    val queryGeometry: Geometry = metaList.head.getGeom

    // TODO lrx: 改造前端瓦片转换坐标、行列号的方式
    val unionTileExtent: Geometry = zIndexStrArray.map(zIndexStr => {
      val xy: Array[Int] = ZCurveUtil.zCurveToXY(zIndexStr, level)
      val lonMinOfTile: Double = ZCurveUtil.tile2Lon(xy(0), level)
      val latMinOfTile: Double = ZCurveUtil.tile2Lat(xy(1) + 1, level)
      val lonMaxOfTile: Double = ZCurveUtil.tile2Lon(xy(0) + 1, level)
      val latMaxOfTile: Double = ZCurveUtil.tile2Lat(xy(1), level)

      val minCoordinate = new Coordinate(lonMinOfTile, latMinOfTile)
      val maxCoordinate = new Coordinate(lonMaxOfTile, latMaxOfTile)
      val envelope: Envelope = new Envelope(minCoordinate, maxCoordinate)
      val geometry: Geometry = new GeometryFactory().toGeometry(envelope)
      geometry
    }).reduce((a, b) => {
      a.union(b)
    })

    val union: Geometry = unionTileExtent.intersection(queryGeometry)

    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)

    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .mapPartitions(par => {
        val minIOUtil = new MinIOUtil()
        val client: MinioClient = minIOUtil.getMinioClient
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val client: MinioClient = minIOUtil.getMinioClient
            val tiles: mutable.ArrayBuffer[RawTile] = tileQuery(client, level, t, union)
            minIOUtil.releaseMinioClient(client)
            tiles
          }
          val time2: Long = System.currentTimeMillis()
          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
        minIOUtil.releaseMinioClient(client)
        result
      }).flatMap(t => t).persist()

    val tileNum: Int = tileRDDFlat.count().toInt
    println("tileNum = " + tileNum)
    tileRDDFlat.unpersist()
    val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 16))
    val rawTileRdd: RDD[RawTile] = tileRDDRePar.map(t => {
      val time1: Long = System.currentTimeMillis()
      val client: MinioClient = new MinIOUtil().getMinioClient
      val tile: RawTile = getTileBuf(client, t)
      val time2: Long = System.currentTimeMillis()
      println("Get Tile Time is " + (time2 - time1))
      tile
    })
    makeCoverageRDD(rawTileRdd)
  }

  /*
  生成虚拟coverage
  测试函数，不进入正式版本中 TODO：在master分支中删除该函数
  Int类型的默认NoData值是-2147483648，Double类型的默认NoData值是Double.NaN
  可以使用isNoData方法来判断一个值是否是NoData值
  */
  def makeFakeCoverage(implicit sc: SparkContext, array: Array[Int], names: mutable.ListBuffer[String], cols: Int, rows: Int):
  (RDD[
    (SpaceTimeBandKey,
      MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    val tile = MultibandTile(ArrayTile(arr = array, cols, rows),
      ArrayTile(arr = array, cols, rows),
    ArrayTile(arr = array, cols, rows))
    val key = SpaceTimeBandKey(SpaceTimeKey(0, 0, ZonedDateTime.now()), names)
    val rdd: RDD[(SpaceTimeBandKey, MultibandTile)] = sc.parallelize(Seq((key, tile)))
    val metadata = TileLayerMetadata[SpaceTimeKey](
      cellType = IntConstantNoDataCellType,
      layout = LayoutDefinition(Extent(0.0, 0.0, 1.0, 1.0), TileLayout(1, 1, 3, 3)),
      extent = Extent(0.0, 0.0, 1.0, 1.0),
      crs = CRS.fromEpsgCode(4326),
      bounds = KeyBounds(SpaceTimeKey(0, 0, 0), SpaceTimeKey(0, 0, 0))
    )
    (rdd, metadata)
  }

  def makeFakeCoverage(implicit sc: SparkContext, array: Array[Double], names: mutable.ListBuffer[String], cols: Int, rows: Int):
  (RDD[
    (SpaceTimeBandKey,
      MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    val tile = MultibandTile(ArrayTile(arr = array, cols, rows),
      ArrayTile(arr = array, cols, rows)
      , ArrayTile(arr = array, cols, rows))
    val key = SpaceTimeBandKey(SpaceTimeKey(0, 0, ZonedDateTime.now()), names)
    val rdd: RDD[(SpaceTimeBandKey, MultibandTile)] = sc.parallelize(Seq((key, tile)))
    val metadata = TileLayerMetadata[SpaceTimeKey](
      cellType = DoubleConstantNoDataCellType,
      layout = LayoutDefinition(Extent(0.0, 0.0, 1.0, 1.0), TileLayout(1, 1, 3, 3)),
      extent = Extent(0.0, 0.0, 1.0, 1.0),
      crs = CRS.fromEpsgCode(4326),
      bounds = KeyBounds(SpaceTimeKey(0, 0, 0), SpaceTimeKey(0, 0, 0))
    )
    (rdd, metadata)
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, add operation is applied between the 2 bands
   * if not, add the first value to the second for each matched pair of bands in coverage1 and coverage2
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def add(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Add(tile1, tile2))
  }

  def add(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Add(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Add(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, subtarct the second tile from the first tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def subtract(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Subtract(tile1, tile2))
  }

  def subtract(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Subtract(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Subtract(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, divide operation is applied between the 2 bands
   * if not, divide the first tile by the second tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def divide(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Divide(tile1, tile2))
  }

  def divide(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Divide(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Divide(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, multiply operation is applied between the 2 bands
   * if not, multipliy the first tile by the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def multiply(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Multiply(tile1, tile2))
  }

  def multiply(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Multiply(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Multiply(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }


  /**
   * if both coverage1 and coverage2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, mod the second tile from the first tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def mod(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Mod(tile1, tile2))
  }

  def mod(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Mod(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Mod(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * Compute a polynomial at each pixel using the given coefficients.
   *
   * @param coverage The input coverage.
   * @param l        The polynomial coefficients in increasing order of degree starting with the constant term.
   * @return
   */
  def polynomial(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), l: List[Double]):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var resCoverage = multiply(coverage, 0)
    for (i <- 0 until (l.length)) {
      resCoverage = add(resCoverage, multiply(pow(coverage, i), l(i)))
    }
    resCoverage
  }

  def normalizedDifference(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bandNames: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (bandNames.length != 2) {
      throw new IllegalArgumentException(s"输入的波段数量不为2，输入了${bandNames.length}个波段")
    }
    else {
      val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverage, List(bandNames.head))
      val coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverage, List(bandNames(1)))
      coverageTemplate(coverage1, coverage2, (tile1, tile2) => {
        Divide(Subtract(tile1, tile2), Add(tile1, tile2))
      })
    }
  }

  /**
   * coverage Binarization
   *
   * @param coverage  coverage rdd for operation
   * @param threshold threshold
   * @return
   */
  // 自动转成uint8数据类型
  def binarization(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), threshold: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val cellType = coverage._1.first()._2.cellType
    (coverage._1.map(t => {
      (t._1, t._2.mapBands((_, tile) => {
        val tileConverted: Tile = tile.convert(DoubleConstantNoDataCellType)
        val tileMap: Tile = tileConverted.mapDouble(pixel => {
          if (pixel > threshold) {
            255.0
          }
          else {
            0.0
          }
        })
        tileMap.convert(cellType)
      }))
    }), TileLayerMetadata(cellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  }

  /**
   * Returns 1 iff both values are non-zero for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match.
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def and(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => And(tile1, tile2))
  }

  /**
   * Returns 1 iff one coverage is non-zero and the other is non-zero for each matched pair of bands in coverage1 and coverage2.
   * If both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def xor(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Xor(tile1, tile2))
  }

  /**
   * Returns 1 iff either values are non-zero for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def or(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Or(tile1, tile2))
  }

  /**
   * Returns 0 if the input is non-zero, and 1 otherwise
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def not(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Not(tile))
  }

  /**
   * Computes the sine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def sin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sin(tile))
  }

  /**
   * Computes the cosine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def cos(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Cos(tile))
  }

  /**
   * Computes the tangent of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def tan(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Tan(tile))
  }

  /**
   * Computes the hyperbolic sine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def sinh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sinh(tile))
  }

  /**
   * Computes the hyperbolic cosine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def cosh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Cosh(tile))
  }

  /**
   * Computes the hyperbolic tangent of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def tanh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Tanh(tile))
  }

  /**
   * Computes the arc sine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def asin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Asin(tile))
  }

  /**
   * Computes the arc cosine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def acos(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Acos(tile))
  }

  /**
   * Computes the atan sine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def atan(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Atan(tile))
  }

  /**
   * Operation to get the Arc Tangent2 of values. The first raster holds the y-values, and the second holds the x values.
   * The arctan is calculated from y/x.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to oprate
   * @return
   */
  def atan2(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
            coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Atan2(tile1, tile2))
  }

  /**
   * Computes the smallest integer greater than or equal to the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def ceil(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Ceil(tile))
  }

  /**
   * Computes the largest integer less than or equal to the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def floor(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Floor(tile))
  }

  /**
   * Computes the integer nearest to the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def round(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Round(tile))
  }

  /**
   * Computes the natural logarithm of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def log(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Log(tile))
  }

  /**
   * Computes the base-10 logarithm of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def log10(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Log10(tile))
  }

  /**
   * Computes the square root of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def sqrt(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sqrt(tile))
  }

  /**
   * Computes the signum function (sign) of the input; zero if the input is zero, 1 if the input is greater than zero, -1 if the input is less than zero.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def signum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => tile.map(u => {
      if (u == NODATA) NODATA
      else if (u > 0) 1
      else if (u < 0) -1
      else 0
    }))
  }

  /**
   * Computes the absolute value of the input.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def abs(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Abs(tile))
  }

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def eq(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Equal(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is greater than the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def gt(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Greater(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is equal or greater to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def gte(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => GreaterOrEqual(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is less than the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def lt(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Less(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is less or equal to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def lte(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => LessOrEqual(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is not equal to the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def neq(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Unequal(tile1, tile2))
  }

  /**
   * Returns an coverage containing all bands copied from the first input and selected bands from the second input,
   * optionally overwriting bands in the first coverage with the same name.
   *
   * @param dstCoverage first coverage
   * @param srcCoverage second coverage
   * @param names       the name of selected bands in srcCoverage
   * @param overwrite   if true, overwrite bands in the first coverage with the same name. Otherwise the bands in the first coverage will be kept.
   * @return
   */
  //这里与GEE逻辑存在出入，GEE会在overwrite==false时将重名的band加上后缀，而不是直接忽略
  def addBands(dstCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               srcCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               names: List[String] = List(""), overwrite: Boolean = false): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    val selectedBands = selectBands(srcCoverage, names)
    if (selectedBands.isEmpty()) {
      return dstCoverage
    }

    if (!overwrite) {
      var tiles: Vector[Tile] = dstCoverage._1.first()._2.bands
      var newBandNames = dstCoverage._1.first()._1.measurementName
      for (i <- selectedBands._1.first()._1.measurementName.indices) {
        if (!newBandNames.contains(selectedBands._1.first()._1.measurementName(i))) {
          tiles = tiles :+ selectedBands._1.first()._2.bands(i)
          newBandNames = newBandNames :+ selectedBands._1.first()._1.measurementName(i)
        }
      }
      (dstCoverage._1.map(t => {
        (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(tiles))
      }), dstCoverage._2)
    } else {
      var tiles: Vector[Tile] = selectedBands._1.first()._2.bands
      var newBandNames = selectedBands._1.first()._1.measurementName
      for (i <- dstCoverage._1.first()._1.measurementName.indices) {
        if (!newBandNames.contains(dstCoverage._1.first()._1.measurementName(i))) {
          tiles = tiles :+ dstCoverage._1.first()._2.bands(i)
          newBandNames = newBandNames :+ dstCoverage._1.first()._1.measurementName(i)
        }
      }
      (dstCoverage._1.map(t => {
        (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(tiles))
      }), dstCoverage._2)
    }
  }

  /**
   * get all the bands in rdd
   *
   * @param coverage rdd for getting bands
   * @return
   */
  def bandNames(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): List[String] = {
    coverage._1.first()._1.measurementName.toList
  }

  def bandNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Int = {
    coverage._1.first()._1.measurementName.length
  }

  /**
   * get the target bands from original coverage
   *
   * @param coverage    the coverage for operation
   * @param targetBands the intersting bands
   * @return
   */
  def selectBands(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bands: List[String])
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandsAlready: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    val duplicates: List[String] = bands.diff(bands.distinct)
    if (duplicates.nonEmpty) {
      val errorMessage = s"Error: Duplicate bands found: ${duplicates.mkString(", ")}"
      throw new IllegalArgumentException(errorMessage)
    }
    val missingElements: List[String] = bands.filterNot(bandsAlready.contains)
    if (missingElements.nonEmpty) {
      //      val errorMessage = s"Error: Bands not found: ${missingElements.mkString(", ")}"
      return (coverage._1.filter(t => false), coverage._2)
    }

    val indexList: List[Int] = bands.map(bandsAlready.indexOf)

    (coverage._1.map(t => {
      val bandNames: mutable.ListBuffer[String] = t._1.measurementName
      val tileBands: Vector[Tile] = t._2.bands
      val newBandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      for (index <- indexList) {
        newBandNames += bandNames(index)
        newTileBands :+= tileBands(index)
      }
      (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(newTileBands))
    }), coverage._2)

  }

  /**
   * Returns a map of the coverage's band types.
   *
   * @param coverage The coverage from which the left operand bands are taken.
   * @return Map[String, String]  key: band name, value: band type
   */
  def bandTypes(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Map[String, String] = {
    var bandTypesMap: Map[String, String] = Map()
    var bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    coverage._1.first()._2.bands.foreach(tile => {
      bandTypesMap = bandTypesMap + (bandNames.head -> tile.cellType.toString())
      bandNames = bandNames.tail
    })
    bandTypesMap
  }


  /**
   * Rename the bands of a coverage.Returns the renamed coverage.
   *
   * @param coverage The coverage to which to apply the operations.
   * @param name     :List[String]     The new names for the bands. Must match the number of bands in the coverage.
   * @return
   */
  def rename(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             name: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName


    if (bandNames.length != name.length) {
      val errorMessage = s"Error: The number of bands in the coverage is ${bandNames.length}, but the number of names is ${name.length}."
      throw new IllegalArgumentException(errorMessage)
    }

    val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {
      val tileBands: Vector[Tile] = t._2.bands
      val newBandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      for (index <- name.indices) {
        newBandNames += name(index)
        newTileBands :+= tileBands(index)
      }
      (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(newTileBands))
    })
    (newCoverageRdd, coverage._2)

  }

  /**
   * Raises the first value to the power of the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def pow(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Pow(tile1, tile2))
  }

  def pow(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Pow(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Pow(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * Selects the minimum of the first and second values for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def mini(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
           coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Min(tile1, tile2))
  }

  /**
   * Selects the maximum of the first and second values for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def maxi(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
           coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Max(tile1, tile2))
  }

  /**
   * Applies a morphological mean filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMean(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Mean.apply, radius)
  }

  /**
   * Applies a morphological median filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMedian(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  kernelType: String,
                  radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Median.apply, radius)
  }

  /**
   * Applies a morphological max filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use. Options include: 'circle', 'square'. It only supports square and
   *                   circle kernels by now.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMax(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Max.apply, radius)
  }

  /**
   * Applies a morphological min filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use. Options include: 'circle', 'square'. It only supports square and
   *                   circle kernels by now.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Min.apply, radius)
  }

  /**
   * Applies a morphological mode filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMode(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Mode.apply, radius)
  }

  /**
   * Selects a contiguous group of bands from a coverage by position.
   *
   * @param coverage The coverage from which to select bands.
   * @param start    Where to start the selection.
   * @param end      Where to end the selection.
   */
  def slice(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), start: Int, end: Int):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (start < 0 || start > coverage._1.count())
      throw new IllegalArgumentException("Start index out of range!")
    if (end < 0 || end > coverage._1.count())
      throw new IllegalArgumentException("End index out of range!")
    if (end <= start)
      throw new IllegalArgumentException("End index should be greater than the start index!")
    val bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName.slice(start, end)
    val newBands: Vector[Tile] = coverage._1.first()._2.bands.slice(start, end)
    (coverage._1.map(t => {
      (SpaceTimeBandKey(t._1.spaceTimeKey, bandNames), MultibandTile(newBands))
    }), coverage._2)
  }

  /**
   * Maps from input values to output values, represented by two parallel lists. Any input values not included in the input list are either set to defaultValue if it is given, or masked if it isn't.
   *
   * @param coverage
   * @param from
   * @param to
   * @param defaultValue
   * @return
   */
  def remap(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), from: List[Int],
            to: List[Double], defaultValue: Option[Double] = None): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    if (to.length != from.length) {
      throw new IllegalArgumentException("The length of two lists not same!")
    }
    if (defaultValue == None) {
      coverageTemplate(coverage, (tile) => RemapWithoutDefaultValue(tile, from.zip(to).toMap))
    } else {
      coverageTemplate(coverage, (tile) => RemapWithDefaultValue(tile, from.zip(to).toMap, defaultValue.get))
    }
  }


  /**
   * Convolves each band of an coverage with the given kernel. Coverages will be padded with Zeroes.
   *
   * @param coverage The coverage to convolve.
   * @param kernel   The kernel to convolve with.
   * @return
   */
  def convolve(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernel: focal
  .Kernel): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val convolvedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage.map(t => {
      (t._1, t._2.mapBands((_, tile) => {
        focal.Convolve(tile, kernel, None, TargetCell.All)
      }))
    })
    (convolvedRDD, coverage._2)

  }

  /**
   * Return the histogram of the coverage, a map of bin label value and its associated count.
   *
   * @param coverage The coverage to which to compute the histogram.
   * @return
   */
  def histogram(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Map[Int, Long] = {

    val histRDD: RDD[Histogram[Int]] = coverage._1.map(t => {
      t._2.histogram
    }).flatMap(t => {
      t
    })

    histRDD.reduce((a, b) => {
      a.merge(b)
    }).binCounts().toMap[Int, Long]

  }

  /**
   * Returns the projection of an coverage.
   *
   * @param coverage The coverage to which to get the projection.
   * @return
   */
  def projection(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = coverage._2.crs.toString()

  /**
   * Force an coverage to be computed in a given projection
   *
   * @param coverage          The coverage to reproject.
   * @param newProjectionCode The code of new projection
   * @param resolution        The resolution of reprojected coverage
   * @return
   */
  //  def reproject(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
  //                newProjectionCode: Int, resolution: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
  //    val band = coverage._1.first()._1.measurementName
  //    val resampleTime = Instant.now.getEpochSecond
  //    val coveragetileRDD = coverage._1.map(t => {
  //      (t._1.spaceTimeKey.spatialKey, t._2)
  //    })
  //    val extent = coverage._2.extent.reproject(coverage._2.crs, CRS.fromEpsgCode(newProjectionCode))
  //    val tl = TileLayout(((extent.xmax - extent.xmin) / (resolution * 256)).toInt, ((extent.ymax - extent.ymin) / (resolution * 256)).toInt, 256, 256)
  //    val ld = LayoutDefinition(extent, tl)
  //    val cellType = coverage._2.cellType
  //    val srcLayout = coverage._2.layout
  //    val srcExtent = coverage._2.extent
  //    val srcCrs = coverage._2.crs
  //    val srcBounds = coverage._2.bounds
  //    val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2))
  //    val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
  //    val coveragetileLayerRDD = TileLayerRDD(coveragetileRDD, rasterMetaData);
  //    val (_, coveragetileRDDWithMetadata) = coveragetileLayerRDD.reproject(CRS.fromEpsgCode(newProjectionCode), ld, geotrellis.raster.resample.Bilinear)
  //    val coverageNoband = (coveragetileRDDWithMetadata.mapValues(tile => tile: Tile), coveragetileRDDWithMetadata.metadata)
  //    val newBound = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(coverageNoband._2.tileLayout.layoutCols - 1, coverageNoband._2.tileLayout.layoutRows - 1, resampleTime))
  //    val newMetadata = TileLayerMetadata(coverageNoband._2.cellType, coverageNoband._2.layout, coverageNoband._2.extent, coverageNoband._2.crs, newBound)
  //    val coverageRDD = coverageNoband.map(t => {
  //      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band), t._2)
  //    })
  //    (coverageRDD, newMetadata)
  //  }

  /**
   * 自定义重采样方法，功能有局限性，勿用
   *
   * @param coverage     需要被重采样的图像
   * @param sourceZoom   原图像的 Zoom 层级
   * @param targetZoom   输出图像的 Zoom 层级
   * @param mode         插值方法
   * @param downSampling 是否下采样，如果sourceZoom > targetZoom，true则采样，false则不处理
   * @return 和输入图像同类型的新图像
   */
  //  def resampleToTargetZoom(
  //                            coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
  //                            targetZoom: Int,
  //                            mode: String,
  //                            downSampling: Boolean = true
  //                          )
  //  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
  //    val resampleMethod: PointResampleMethod = mode match {
  //      case "Bilinear" => geotrellis.raster.resample.Bilinear
  //      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
  //      case _ => geotrellis.raster.resample.NearestNeighbor
  //    }
  //
  //    // 求解出原始影像的zoom
  //    //    val myAcc2: LongAccumulator = sc.longAccumulator("myAcc2")
  //    //    //    println("srcAcc0 = " + myAcc2.value)
  //    //
  //    //    reprojected.map(t => {
  //    //
  //    //
  //    //      //          println("srcRows = " + t._2.rows) 256
  //    //      //          println("srcRows = " + t._2.cols) 256
  //    //      myAcc2.add(1)
  //    //      t
  //    //    }).collect()
  //    //    println("srcNumOfTiles = " + myAcc2.value) // 234
  //    //
  //    //
  //
  //    val level: Int = COGUtil.tileDifference
  //    println("tileDifference = " + level)
  //    if (level > 0) {
  //      val time1 = System.currentTimeMillis()
  //      val coverageResampled: RDD[(SpaceTimeBandKey, Tile)] = coverage._1.map(t => {
  //        (t._1, t._2.resample(t._2.cols * (1 << level), t._2.rows * (1 << level), resampleMethod))
  //      })
  //      val time2 = System.currentTimeMillis()
  //      println("Resample Time is " + (time2 - time1))
  //      (coverageResampled,
  //        TileLayerMetadata(coverage._2.cellType, LayoutDefinition(
  //          coverage._2.extent,
  //          TileLayout(coverage._2.layoutCols, coverage._2.layoutRows,
  //            coverage._2.tileCols * (1 << level), coverage._2.tileRows * (1 << level))
  //        ), coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  //    }
  //    else if (level < 0) {
  //      val time1 = System.currentTimeMillis()
  //      val coverageResampled: RDD[(SpaceTimeBandKey, Tile)] = coverage._1.map(t => {
  //        val tileResampled: Tile = t._2.resample(Math.ceil(t._2.cols.toDouble / (1 << -level)).toInt, Math.ceil(t._2.rows.toDouble / (1 << -level)).toInt, resampleMethod)
  //        (t._1, tileResampled)
  //      })
  //      val time2 = System.currentTimeMillis()
  //      println("Resample Time is " + (time2 - time1))
  //      (coverageResampled, TileLayerMetadata(coverage._2.cellType, LayoutDefinition(coverage._2.extent, TileLayout(coverage._2.layoutCols, coverage._2.layoutRows, coverage._2.tileCols / (1 << -level),
  //        coverage._2.tileRows / (1 << -level))), coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  //      println("coverage._2.tileCols.toDouble = " + coverage._2.tileCols.toDouble)
  //      (coverageResampled,
  //        TileLayerMetadata(coverage._2.cellType, LayoutDefinition(
  //          coverage._2.extent,
  //          TileLayout(coverage._2.layoutCols, coverage._2.layoutRows,
  //            Math.ceil(coverage._2.tileCols.toDouble / (1 << -level)).toInt, Math.ceil(coverage._2.tileRows.toDouble / (1 << -level)).toInt)),
  //          coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  //    }
  //    else coverage
  //  }


  /**
   * Resample the coverage
   *
   * @param coverage The coverage to resample
   * @param level    The resampling level. eg:1 for up-sampling and -1 for down-sampling.
   * @param mode     The interpolation mode to use
   * @return
   */

  def resample(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), mode: String
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO 重采样
    val resampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }

    val coverageResampled = coverage._1.map(t => {
      (t._1, t._2.resample(t._2.cols * 3, t._2.rows * 3, resampleMethod))
    })
    (coverageResampled, TileLayerMetadata(coverage._2.cellType, LayoutDefinition(coverage._2.extent, TileLayout(coverage._2.layoutCols, coverage._2.layoutRows, coverage._2.tileCols * 3,
      coverage._2.tileRows * 3)), coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  }

  /**
   * Calculates the gradient.
   *
   * @param coverage The coverage to compute the gradient.
   * @return
   */

  def gradient(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val sobelx = geotrellis.raster.mapalgebra.focal.Kernel(IntArrayTile(Array[Int](-1, 0, 1, -2, 0, 2, -1, 0, 1), 3, 3))
    val sobely = geotrellis.raster.mapalgebra.focal.Kernel(IntArrayTile(Array[Int](1, 2, 1, 0, 0, 0, -1, -2, -1), 3, 3))
    val tilex = convolve(coverage, sobelx)
    val tiley = convolve(coverage, sobely)
    add(abs(tilex), abs(tiley))
  }


  /**
   * Clip the raster with the geometry (with the same crs).
   *
   * @param coverage The coverage to clip.
   * @param geom     The geometry used to clip.
   * @return
   */

  def clip(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), geom: Geometry
          ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val RDDExtent: Extent = coverage._2.extent
    val reso: Double = coverage._2.cellSize.resolution
    val coverageRDDWithExtent = coverage._1.map(t => {
      val tileExtent = Extent(RDDExtent.xmin + t._1.spaceTimeKey.col * reso * 256, RDDExtent.ymax - (t._1.spaceTimeKey.row + 1) * 256 * reso,
        RDDExtent.xmin + (t._1.spaceTimeKey.col + 1) * reso * 256, RDDExtent.ymax - t._1.spaceTimeKey.row * 256 * reso)
      (t._1, (t._2, tileExtent))
    })
    val tilesIntersectedRDD: RDD[(SpaceTimeBandKey, (MultibandTile, Extent))] = coverageRDDWithExtent.filter(t => {
      t._2._2.intersects(geom)
    })
    val tilesClippedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = tilesIntersectedRDD.map(t => {
      val tileClipped = t._2._1.mask(t._2._2, geom)
      (t._1, tileClipped)
    })
    val extents = tilesIntersectedRDD
      .map(t => (t._2._2.xmin, t._2._2.ymin, t._2._2.xmax, t._2._2.ymax))
      .reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4)))
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val colRowInstant = tilesIntersectedRDD
      .map(t => (t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, t._1.spaceTimeKey.instant, t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, t._1.spaceTimeKey.instant))
      .reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.min(a._3, b._3), math.max(a._4, b._4), math.max(a._5, b._5), math.max(a._6, b._6)))
    val tl = TileLayout(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val newbounds = Bounds(SpaceTimeKey(colRowInstant._1, colRowInstant._2, colRowInstant._3), SpaceTimeKey(colRowInstant._4, colRowInstant._5, colRowInstant._6))
    val newlayerMetaData = TileLayerMetadata(coverage._2.cellType, ld, extent, coverage._2.crs, newbounds)
    (tilesClippedRDD, newlayerMetaData)
  }


  /**
   * Clamp the raster between low and high
   *
   * @param coverage The coverage to clamp.
   * @param low      The low value.
   * @param high     The high value.
   * @return
   */

  def clamp(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), low: Int, high: Int
           ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageRddClamped = coverage._1.map(t =>
      (t._1, t._2.mapBands((t1, t2) => t2.map(t3 => {
        if (t3 > high) {
          high
        }
        else if (t3 < low) {
          low
        }
        else {
          t3
        }
      }
      ))))

    (coverageRddClamped, coverage._2)
  }


  /**
   * Transforms the coverage from the RGB color space to the HSV color space.
   * Expects a 3 band coverage in the range [0, 255],
   * and produces three bands: hue, saturation and value with values in the range [0, 1].
   *
   * @param coverage The coverage with three bands: R, G, B.
   * @return
   */
  def rgbToHsv(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    // TODO: 如何区分 RGB? 我这里默认索引 RGB 顺序

    if (!coverage._1.first()._2.bandCount.equals(3)) {
      throw new
          IllegalArgumentException("Tile' s bandCount must be three!")
    }

    // 保留原来的 SpaceTimeBandKey, 分离三通道
    val coverageRRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(0)))
    val coverageGRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(1)))
    val coverageBRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(2)))

    // 得到三波段 Tile
    val joinedRGBRdd: RDD[(SpaceTimeBandKey, (Tile, Tile, Tile))] =
      coverageRRdd.join(coverageGRdd).join(coverageBRdd)
        .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2)))
    val hsvRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = joinedRGBRdd.map(t => {
      // 得到每个 Tile 对应的 HSV
      val hTile: MutableArrayTile = t._2._1.convert(CellType.fromName("float32")).mutable
      val sTile: MutableArrayTile = t._2._2.convert(CellType.fromName("float32")).mutable
      val vTile: MutableArrayTile = t._2._3.convert(CellType.fromName("float32")).mutable

      val tileRows: Int = t._2._1.rows
      val tileCols: Int = t._2._1.cols
      for (i <- 0 until tileRows; j <- 0 until tileCols) {
        val r: Double = t._2._1.getDouble(i, j) / 255.0
        val g: Double = t._2._2.getDouble(i, j) / 255.0
        val b: Double = t._2._3.getDouble(i, j) / 255.0
        val cMax: Double = math.max(math.max(r, g), b)
        val cMin: Double = math.min(math.min(r, g), b)
        if (cMax.equals(cMin)) {
          hTile.setDouble(i, j, 0.0)
        } else {
          cMax match {
            case r =>
              hTile.setDouble(i, j,
                if (g >= b) {
                  60 * ((g - b) / (cMax - cMin))
                } else {
                  60 * ((g - b) / (cMax - cMin)) + 360
                })

            case g =>
              hTile.setDouble(i, j,
                60 * ((b - r) / (cMax - cMin)) + 120)
            case b =>
              hTile.setDouble(i, j,
                60 * ((r - g) / (cMax - cMin)) + 240)
          }
        }

        if (math.abs(cMax - 0.0) < 1e-7) {
          sTile.setDouble(i, j, 0.0)
        } else {
          sTile.setDouble(i, j,
            (cMax - cMin) / cMax)
        }
        vTile.setDouble(i, j, cMax)

      }


      (t._1, MultibandTile(Array(hTile, sTile, vTile)))

    })

    // 使用原来的 TileLayerMetadata
    (hsvRdd, coverage._2)
  }


  /**
   * Transforms the coverage from the HSV color space to the RGB color space.
   * Expects a 3 band coverage in the range [0, 1],
   * and produces three bands: red, green and blue with values in the range [0, 255].
   *
   * @param coverage The coverage with three bands: H, S, V
   * @return
   */
  def hsvToRgb(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    if (!coverage._1.first()._2.bandCount.equals(3)) {
      throw new
          IllegalArgumentException("Tile' s bandCount must be three!")
    }


    def isCoverageEqual(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                        coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
    : Boolean = {
      // 相减后判断是否全 0
      subtract(coverage1, coverage2).map(multiTile =>
        multiTile._2.bands.filter(
          tile => math.abs(tile.findMinMax._2) < 1e-6)
      ).isEmpty()

    }

    // TODO: 默认索引 hsv
    val coverageHRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(0)))
    val coverageSRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(1)))
    val coverageVRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(2)))


    // 得到三波段 Tile (HSV)
    val joinedHSVRdd: RDD[(SpaceTimeBandKey, (Tile, Tile, Tile))] =
      coverageHRdd.join(coverageSRdd).join(coverageVRdd)
        .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2)))

    val rgbRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = joinedHSVRdd.map(t => {
      val rTile: MutableArrayTile = t._2._1.convert(CellType.fromName("uint8")).mutable
      val gTile: MutableArrayTile = t._2._2.convert(CellType.fromName("uint8")).mutable
      val bTile: MutableArrayTile = t._2._3.convert(CellType.fromName("uint8")).mutable

      val tileRows: Int = t._2._1.rows
      val tileCols: Int = t._2._1.cols
      for (i <- 0 until tileRows; j <- 0 until tileCols) {
        val h: Double = t._2._1.getDouble(i, j) * 360.0
        val s: Double = t._2._2.getDouble(i, j)
        val v: Double = t._2._3.getDouble(i, j)

        val f: Double = (h / 60) - ((h / 60).toInt % 6)
        val p: Double = v * (1 - s)
        val q: Double = v * (1 - f * s)
        val u: Double = v * (1 - (1 - f) * s)

        ((h / 60).toInt % 6) match {
          case 0 =>
            rTile.set(i, j, (v * 255).toInt)
            gTile.set(i, j, (u * 255).toInt)
            bTile.set(i, j, (p * 255).toInt)
          case 1 =>
            rTile.set(i, j, (q * 255).toInt)
            gTile.set(i, j, (v * 255).toInt)
            bTile.set(i, j, (p * 255).toInt)
          case 2 =>
            rTile.set(i, j, (p * 255).toInt)
            gTile.set(i, j, (v * 255).toInt)
            bTile.set(i, j, (u * 255).toInt)
          case 3 =>
            rTile.set(i, j, (p * 255).toInt)
            gTile.set(i, j, (q * 255).toInt)
            bTile.set(i, j, (v * 255).toInt)
          case 4 =>
            rTile.set(i, j, (u * 255).toInt)
            gTile.set(i, j, (p * 255).toInt)
            bTile.set(i, j, (v * 255).toInt)
          case 5 =>
            rTile.set(i, j, (v * 255).toInt)
            gTile.set(i, j, (p * 255).toInt)
            bTile.set(i, j, (q * 255).toInt)

          case _ =>
          //            throw new RuntimeException("Error illegal Hue...")
        } // end match
      } // end for


      (t._1, MultibandTile(Array(rTile, gTile, bTile)))
    })
    (rgbRdd, coverage._2)
  }

  /**
   * Computes the windowed entropy using the specified kernel centered on each input pixel.
   * Entropy is computed as
   * -sum(p * log2(p)), where p is the normalized probability
   * of occurrence of the values encountered in each window.
   *
   * @param coverage The coverage to compute the entropy.
   * @param radius   The radius of the square neighborhood to compute the entropy,
   *                 1 for a 3×3 square.
   * @return
   */
  def entropy(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
              kernelType: String,
              radius: Int)
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    focalMethods(coverage, kernelType, EntropyUtil.apply, radius)
  }

  protected def stack(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), nums: Int):
  (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (coverage._1.first()._1.measurementName.length != 1) {
      coverage
    } else {
      val name: String = coverage._1.first()._1.measurementName(0)
      var names = mutable.ListBuffer.empty[String]
      var tiles: Vector[Tile] = Vector[Tile]()
      for (i <- 0 until (nums)) {
        tiles = tiles :+ coverage._1.first()._2.bands(0)
        names += name
      }
      (coverage._1.map(t => {
        (SpaceTimeBandKey(t._1.spaceTimeKey, names), MultibandTile(tiles))
      }), coverage._2)
    }
  }

  /**
   * Computes the cubic root of the input.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def cbrt(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, (tile) => Cbrt(tile))
  }

  /**
   * Return the metadata of the input coverage.
   *
   * @param coverage The coverage to get the metadata
   * @return
   */
  def metadata(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : String = {
    TileLayerMetadata.toString
  }

  /**
   * Casts the input value to a signed 8-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted =
      (coverage._1.map(t => {
        (t._1, t._2.convert(ByteCellType))
      }), TileLayerMetadata(ByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }

  /**
   * Casts the input value to a unsigned 8-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteCellType))
    }), TileLayerMetadata(UByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)

  }


  /**
   * Casts the input value to a signed 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteCellType))
    }), TileLayerMetadata(UByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }

  /**
   * Casts the input value to a unsigned 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteCellType))
    }), TileLayerMetadata(UByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }


  /**
   * Casts the input value to a signed 32-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt32(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteCellType))
    }), TileLayerMetadata(UByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }

  /**
   * Casts the input value to a 32-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toFloat(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteCellType))
    }), TileLayerMetadata(UByteCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }

  /**
   * Casts the input value to a 64-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toDouble(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(DoubleCellType))
    }), TileLayerMetadata(DoubleCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    and(coverageConverted, coverage)
  }

  /**
   * Generate a coverage with the values from the first coverage, but only include cells in which the corresponding
   * cell in the second coverage *are not* set to the "readMask" value. Otherwise, the value of the cell will be set
   * to the "writeMask".
   * Both coverages are required to have the same number of bands, or the coverage2 must have 1 bands.
   * If the coverage2 has a band count of 1, the mask is applied to each band of coverage1.
   *
   * @param coverage1 The input coverage.
   * @param coverage2 The Mask.
   * @param readMask  The number to be masked in the Mask.
   * @param writeMask The number will be set if the values from coverage2 is equal to readMask.
   */
  def mask(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2: (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), readMask: Int, writeMask: Int): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    def maskTemplate(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2:
    (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), func: (Tile, Tile, Int, Int) => Tile): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

      def funcMulti(multibandTile1: MultibandTile, multibandTile2: MultibandTile): MultibandTile = {
        val bands1: Vector[Tile] = multibandTile1.bands
        val bands2: Vector[Tile] = multibandTile2.bands
        val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
        for (i <- bands1.indices) {
          bandsFunc.append(func(bands1(i), bands2(i), readMask, writeMask))
        }
        MultibandTile(bandsFunc)
      }

      var cellType: CellType = null
      val resampleTime: Long = Instant.now.getEpochSecond
      val (coverage1Reprojected, coverage2Reprojected) = checkProjResoExtent(coverage1, coverage2)
      val bandList1: mutable.ListBuffer[String] = coverage1Reprojected._1.first()._1.measurementName
      val bandList2: mutable.ListBuffer[String] = coverage2Reprojected._1.first()._1.measurementName
      if (bandList1.length == bandList2.length) {
        val coverage1NoTime: RDD[(SpatialKey, MultibandTile)] = coverage1Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
        val coverage2NoTime: RDD[(SpatialKey, MultibandTile)] = coverage2Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
        val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1NoTime.join(coverage2NoTime)
        if (bandList1.diff(bandList2).isEmpty && bandList2.diff(bandList1).isEmpty) {
          val indexMap: Map[String, Int] = bandList2.zipWithIndex.toMap
          val indices: mutable.ListBuffer[Int] = bandList1.map(indexMap)
          (rdd.map(t => {
            val bands1: Vector[Tile] = t._2._1.bands
            val bands2: Vector[Tile] = t._2._2.bands
            cellType = func(bands1.head, bands2(indices.head), readMask, writeMask).cellType
            val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
            for (i <- bands1.indices) {
              bandsFunc.append(func(bands1(i), bands2(indices(i)), readMask, writeMask))
            }
            (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), MultibandTile(bandsFunc))
          }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
        }
        else {
          cellType = funcMulti(coverage1Reprojected._1.first()._2, coverage2Reprojected._1.first()._2).cellType
          (rdd.map(t => {
            (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), funcMulti(t._2._1, t._2._2))
          }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
        }
      }
      else {
        throw new IllegalArgumentException("Error: 波段数量不相等")
      }
    }

    if (coverage2._1.first()._1.measurementName.length == 1) {
      val coverageStacked = stack(coverage2, coverage1._1.first()._1.measurementName.length)
      maskTemplate(coverage1, coverageStacked, (tile1, tile2, readMask, writeMask) => Mask(tile1, tile2, readMask,
        writeMask))
    }
    else if (coverage1._1.first()._1.measurementName.length == coverage2._1.first()._1.measurementName.length) {
      maskTemplate(coverage1, coverage2, (tile1, tile2, readMask, writeMask) => Mask(tile1, tile2, readMask, writeMask))
    } else {
      throw new IllegalArgumentException("输入Coverage与掩膜Coverage波段数不匹配")
    }

  }

  def visualizeOnTheFly(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): Unit = {
    var coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (coverage._1.map(t => (t._1, t._2.convert(DoubleConstantNoDataCellType))), TileLayerMetadata(DoubleConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))

    // 从波段开始
    val bands: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    // 如果用户选择了波段
    if (visParam.getBands.nonEmpty) {
      // 首先验证选择的波段数量是不是1-3之间
      // 如果在1-3之间
      if (visParam.getBands.length <= 3) {
        // 判断选择的波段是不是在已经的波段中都存在
        // 如果都存在
        if (visParam.getBands.forall(bands.contains)) {
          // 选择的波段数量
          val bandNum: Int = visParam.getBands.length
          // min的数量
          val minNum: Int = visParam.getMin.length
          // max的数量
          val maxNum: Int = visParam.getMax.length
          // gain的数量
          val gainNum: Int = visParam.getGain.length
          // bias的数量
          val biasNum: Int = visParam.getBias.length
          // gamma的数量
          val gammaNum: Int = visParam.getGamma.length
          // 分类讨论
          // 如果波段数量是1
          if (bandNum == 1) {
            // 判断数量是否对应
            if (minNum > 1 || maxNum > 1 || gainNum > 1 || biasNum > 1 || gammaNum > 1) {
              throw new IllegalArgumentException("波段数量与参数数量不相符")
            }
            else {
              // 判断是否同时存在最小最大值拉伸和线性拉伸
              if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
                throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
              }
              else {
                // 先把波段挑出来
                coverageVis = selectBands(coverageVis, visParam.getBands)
                // 如果存在最小最大值拉伸
                if (minNum * maxNum != 0) {
                  // 首先找到现有的最小最大值
                  val minMaxBand: (Double, Double) = coverageVis._1.map(t => {
                    val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                    if (noNaNArray.nonEmpty) {
                      (noNaNArray.min, noNaNArray.max)
                    }
                    else {
                      (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                    }
                  }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                  val minVis: Double = visParam.getMin.headOption.getOrElse(0.0)
                  val maxVis: Double = visParam.getMax.headOption.getOrElse(1.0)
                  val gainBand: Double = (maxVis - minVis) / (minMaxBand._2 - minMaxBand._1)
                  val biasBand: Double = (minMaxBand._2 * minVis - minMaxBand._1 * maxVis) / (minMaxBand._2 - minMaxBand._1)
                  coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
                    Add(Multiply(tile, gainBand), biasBand)
                  }))), coverageVis._2)
                }
                // 如果存在线性拉伸
                else if (gainNum * biasNum != 0) {
                  coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
                    Add(Multiply(tile, visParam.getGain.headOption.getOrElse(1.0)), visParam.getBias.headOption.getOrElse(0.0))
                  }))), coverageVis._2)
                }
                // 如果存在gamma值
                if (gammaNum != 0) {
                  coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
                    Pow(tile, visParam.getGamma.headOption.getOrElse(1.0))
                  }))), coverageVis._2)
                }
                // 如果存在palette
                if (visParam.getPalette.nonEmpty) {
                  val paletteVis: List[String] = visParam.getPalette
                  val colorVis: List[Color] = paletteVis.map(t => {
                    try {
                      val color: Color = Color.valueOf(t)
                      color
                    } catch {
                      case e: Exception =>
                        throw new IllegalArgumentException(s"输入颜色有误，无法识别$t")
                    }
                  })
                  val colorRGB: List[(Double, Double, Double)] = colorVis.map(t => (t.getRed, t.getGreen, t.getBlue))
                  val minMaxBand: (Double, Double) = coverageVis._1.map(t => {
                    val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                    if (noNaNArray.nonEmpty) {
                      (noNaNArray.min, noNaNArray.max)
                    }
                    else {
                      (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                    }
                  }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                  val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
                  coverageVis = (coverageVis._1.map(t => {
                    val bandR: Tile = t._2.bands.head.mapDouble(d => {
                      var R: Double = 0.0
                      for (i <- colorRGB.indices) {
                        if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                          R = colorRGB(i)._1 * 255.0
                        }
                      }
                      R
                    })
                    val bandG: Tile = t._2.bands.head.mapDouble(d => {
                      var G: Double = 0.0
                      for (i <- colorRGB.indices) {
                        if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                          G = colorRGB(i)._2 * 255.0
                        }
                      }
                      G
                    })
                    val bandB: Tile = t._2.bands.head.mapDouble(d => {
                      var B: Double = 0.0
                      for (i <- colorRGB.indices) {
                        if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                          B = colorRGB(i)._3 * 255.0
                        }
                      }
                      B
                    })
                    (t._1, MultibandTile(bandR, bandG, bandB))
                  }), coverage._2)
                }
              }
            }
          }
          // 如果波段数量是2
          else if (bandNum == 2) {
            // 排除palette
            if (visParam.getPalette.nonEmpty) {
              throw new IllegalArgumentException("palette不能应用于2个波段的影像！")
            }
            else {
              // 判断数量是否对应
              if (minNum > 2 || maxNum > 2 || gainNum > 2 || biasNum > 2 || gammaNum > 2) {
                throw new IllegalArgumentException("波段数量与参数数量不相符")
              }
              else {
                // 判断是否同时存在最小最大值拉伸和线性拉伸
                if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
                  throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
                }
                else {
                  // 先把波段挑出来
                  coverageVis = selectBands(coverageVis, visParam.getBands)
                  // 如果存在最小最大值拉伸
                  if (minNum * maxNum != 0) {
                    // 首先找到现有的最小最大值
                    val minMaxBand0: (Double, Double) = coverageVis._1.map(t => {
                      val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                      if (noNaNArray.nonEmpty) {
                        (noNaNArray.min, noNaNArray.max)
                      }
                      else {
                        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                      }
                    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                    val minMaxBand1: (Double, Double) = coverageVis._1.map(t => {
                      val noNaNArray: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
                      if (noNaNArray.nonEmpty) {
                        (noNaNArray.min, noNaNArray.max)
                      }
                      else {
                        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                      }
                    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                    var minVis0: Double = 0.0
                    var minVis1: Double = 0.0
                    var maxVis0: Double = 0.0
                    var maxVis1: Double = 0.0
                    // 判断min的数量
                    if (minNum == 1) {
                      minVis0 = visParam.getMin.head
                      minVis1 = visParam.getMin.head
                    }
                    else if (minNum == 2) {
                      minVis0 = visParam.getMin.head
                      minVis1 = visParam.getMin(1)
                    }
                    if (maxNum == 1) {
                      maxVis0 = visParam.getMax.head
                      maxVis1 = visParam.getMax.head
                    }
                    else if (maxNum == 2) {
                      maxVis0 = visParam.getMax.head
                      maxVis1 = visParam.getMax(1)
                    }
                    val gainBand0: Double = (maxVis0 - minVis0) / (minMaxBand0._2 - minMaxBand0._1)
                    val biasBand0: Double = (minMaxBand0._2 * minVis0 - minMaxBand0._1 * maxVis0) / (minMaxBand0._2 - minMaxBand0._1)
                    val gainBand1: Double = (maxVis1 - minVis1) / (minMaxBand1._2 - minMaxBand1._1)
                    val biasBand1: Double = (minMaxBand1._2 * minVis1 - minMaxBand1._1 * maxVis1) / (minMaxBand1._2 - minMaxBand1._1)

                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Add(Multiply(tile, gainBand0), biasBand0)
                      }
                      else {
                        Add(Multiply(tile, gainBand1), biasBand1)
                      }
                    }))), coverageVis._2)
                  }
                  // 如果存在线性拉伸
                  else if (gainNum * biasNum != 0) {
                    var gainVis0: Double = 1.0
                    var gainVis1: Double = 1.0
                    var biasVis0: Double = 0.0
                    var biasVis1: Double = 0.0
                    if (gainNum == 1) {
                      gainVis0 = visParam.getGain.head
                      gainVis1 = visParam.getGain.head
                    }
                    else if (gainNum == 2) {
                      gainVis0 = visParam.getGain.head
                      gainVis1 = visParam.getGain(1)
                    }
                    if (biasNum == 1) {
                      biasVis0 = visParam.getBias.head
                      biasVis1 = visParam.getBias.head
                    }
                    else if (biasNum == 2) {
                      biasVis0 = visParam.getBias.head
                      biasVis1 = visParam.getBias(1)
                    }
                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Add(Multiply(tile, gainVis0), biasVis0)
                      }
                      else {
                        Add(Multiply(tile, gainVis1), biasVis1)
                      }
                    }))), coverageVis._2)
                  }
                  // 如果存在gamma值
                  if (gammaNum != 0) {
                    var gammaVis0: Double = 1.0
                    var gammaVis1: Double = 1.0
                    if (gammaNum == 1) {
                      gammaVis0 = visParam.getGamma.head
                      gammaVis1 = visParam.getGamma.head
                    }
                    else if (gammaNum == 2) {
                      gammaVis0 = visParam.getGamma.head
                      gammaVis1 = visParam.getGamma(1)
                    }
                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Pow(tile, gammaVis0)
                      }
                      else {
                        Pow(tile, gammaVis1)
                      }
                    }))), coverageVis._2)
                  }
                }
              }
            }
          }
          // 如果波段数量是3
          else {
            // 排除palette
            if (visParam.getPalette.nonEmpty) {
              throw new IllegalArgumentException("palette不能应用于3个波段的影像！")
            }
            else {
              // 判断数量是否对应
              if (minNum > 3 || minNum == 2 || maxNum > 3 || maxNum == 2 || gainNum > 3 || gainNum == 2 || biasNum > 3 || biasNum == 2 || gammaNum > 3 || gammaNum == 2) {
                throw new IllegalArgumentException("波段数量与参数数量不相符")
              }
              else {
                // 判断是否同时存在最小最大值拉伸和线性拉伸
                if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
                  throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
                }
                else {
                  // 先把波段挑出来
                  coverageVis = selectBands(coverageVis, visParam.getBands)
                  // 如果存在最小最大值拉伸
                  if (minNum * maxNum != 0) {
                    // 首先找到现有的最小最大值
                    val minMaxBand0: (Double, Double) = coverageVis._1.map(t => {
                      val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                      if (noNaNArray.nonEmpty) {
                        (noNaNArray.min, noNaNArray.max)
                      }
                      else {
                        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                      }
                    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                    val minMaxBand1: (Double, Double) = coverageVis._1.map(t => {
                      val noNaNArray: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
                      if (noNaNArray.nonEmpty) {
                        (noNaNArray.min, noNaNArray.max)
                      }
                      else {
                        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                      }
                    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                    val minMaxBand2: (Double, Double) = coverageVis._1.map(t => {
                      val noNaNArray: Array[Double] = t._2.bands(2).toArrayDouble().filter(!_.isNaN)
                      if (noNaNArray.nonEmpty) {
                        (noNaNArray.min, noNaNArray.max)
                      }
                      else {
                        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                      }
                    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                    var minVis0: Double = 0.0
                    var minVis1: Double = 0.0
                    var minVis2: Double = 0.0
                    var maxVis0: Double = 0.0
                    var maxVis1: Double = 0.0
                    var maxVis2: Double = 0.0
                    // 判断min的数量
                    if (minNum == 1) {
                      minVis0 = visParam.getMin.head
                      minVis1 = visParam.getMin.head
                      minVis2 = visParam.getMin.head
                    }
                    else if (minNum == 3) {
                      minVis0 = visParam.getMin.head
                      minVis1 = visParam.getMin(1)
                      minVis2 = visParam.getMin(2)
                    }
                    if (maxNum == 1) {
                      maxVis0 = visParam.getMax.head
                      maxVis1 = visParam.getMax.head
                      maxVis2 = visParam.getMax.head
                    }
                    else if (maxNum == 3) {
                      maxVis0 = visParam.getMax.head
                      maxVis1 = visParam.getMax(1)
                      maxVis2 = visParam.getMax(2)
                    }
                    val gainBand0: Double = (maxVis0 - minVis0) / (minMaxBand0._2 - minMaxBand0._1)
                    val biasBand0: Double = (minMaxBand0._2 * minVis0 - minMaxBand0._1 * maxVis0) / (minMaxBand0._2 - minMaxBand0._1)
                    val gainBand1: Double = (maxVis1 - minVis1) / (minMaxBand1._2 - minMaxBand1._1)
                    val biasBand1: Double = (minMaxBand1._2 * minVis1 - minMaxBand1._1 * maxVis1) / (minMaxBand1._2 - minMaxBand1._1)
                    val gainBand2: Double = (maxVis2 - minVis2) / (minMaxBand2._2 - minMaxBand2._1)
                    val biasBand2: Double = (minMaxBand2._2 * minVis2 - minMaxBand2._1 * maxVis2) / (minMaxBand2._2 - minMaxBand2._1)

                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Add(Multiply(tile, gainBand0), biasBand0)
                      }
                      else if (i == 1) {
                        Add(Multiply(tile, gainBand1), biasBand1)
                      }
                      else {
                        Add(Multiply(tile, gainBand2), biasBand2)
                      }
                    }))), coverageVis._2)
                  }
                  // 如果存在线性拉伸
                  else if (gainNum * biasNum != 0) {
                    var gainVis0: Double = 1.0
                    var gainVis1: Double = 1.0
                    var gainVis2: Double = 1.0
                    var biasVis0: Double = 0.0
                    var biasVis1: Double = 0.0
                    var biasVis2: Double = 0.0

                    if (gainNum == 1) {
                      gainVis0 = visParam.getGain.head
                      gainVis1 = visParam.getGain.head
                      gainVis2 = visParam.getGain.head
                    }
                    else if (gainNum == 3) {
                      gainVis0 = visParam.getGain.head
                      gainVis1 = visParam.getGain(1)
                      gainVis2 = visParam.getGain(2)
                    }
                    if (biasNum == 1) {
                      biasVis0 = visParam.getBias.head
                      biasVis1 = visParam.getBias.head
                      biasVis2 = visParam.getBias.head
                    }
                    else if (biasNum == 3) {
                      biasVis0 = visParam.getBias.head
                      biasVis1 = visParam.getBias(1)
                      biasVis2 = visParam.getBias(2)
                    }
                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Add(Multiply(tile, gainVis0), biasVis0)
                      }
                      else if (i == 1) {
                        Add(Multiply(tile, gainVis1), biasVis1)
                      }
                      else {
                        Add(Multiply(tile, gainVis2), biasVis2)
                      }
                    }))), coverageVis._2)
                  }
                  // 如果存在gamma值
                  if (gammaNum != 0) {
                    var gammaVis0: Double = 1.0
                    var gammaVis1: Double = 1.0
                    var gammaVis2: Double = 1.0
                    if (gammaNum == 1) {
                      gammaVis0 = visParam.getGamma.head
                      gammaVis1 = visParam.getGamma.head
                      gammaVis2 = visParam.getGamma.head
                    }
                    else if (gammaNum == 3) {
                      gammaVis0 = visParam.getGamma.head
                      gammaVis1 = visParam.getGamma(1)
                      gammaVis2 = visParam.getGamma(2)
                    }
                    coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                      if (i == 0) {
                        Pow(tile, gammaVis0)
                      }
                      else if (i == 1) {
                        Pow(tile, gammaVis1)
                      }
                      else {
                        Pow(tile, gammaVis2)
                      }
                    }))), coverageVis._2)
                  }
                }
              }
            }
          }
        }
        // 如果不存在，则报错
        else {
          throw new IllegalArgumentException("Error: 选择的波段不存在，请重新选择")
        }
      }
      // 如果不在1-3之间，则报错
      else {
        throw new IllegalArgumentException("Error: 选择的波段数量不是1-3之间的范围")
      }
    }
    // 如果用户没选择波段
    else {
      // 波段数量
      val bandNumber: Int = bandNum(coverage)
      // min的数量
      val minNum: Int = visParam.getMin.length
      // max的数量
      val maxNum: Int = visParam.getMax.length
      // gain的数量
      val gainNum: Int = visParam.getGain.length
      // bias的数量
      val biasNum: Int = visParam.getBias.length
      // gamma的数量
      val gammaNum: Int = visParam.getGamma.length
      // 如果只有一个波段
      if (bandNumber == 1) {
        if (minNum > 1 || maxNum > 1 || gainNum > 1 || biasNum > 1 || gammaNum > 1) {
          throw new IllegalArgumentException("波段数量与参数数量不相符")
        }
        else {
          // 如果存在最小最大值拉伸
          if (minNum * maxNum != 0) {
            // 首先找到现有的最小最大值
            val minMaxBand: (Double, Double) = coverageVis._1.map(t => {
              val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
              if (noNaNArray.nonEmpty) {
                (noNaNArray.min, noNaNArray.max)
              }
              else {
                (Int.MaxValue.toDouble, Int.MinValue.toDouble)
              }
            }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
            val minVis: Double = visParam.getMin.headOption.getOrElse(0.0)
            val maxVis: Double = visParam.getMax.headOption.getOrElse(1.0)
            val gainBand: Double = (maxVis - minVis) / (minMaxBand._2 - minMaxBand._1)
            val biasBand: Double = (minMaxBand._2 * minVis - minMaxBand._1 * maxVis) / (minMaxBand._2 - minMaxBand._1)
            coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
              Add(Multiply(tile, gainBand), biasBand)
            }))), coverageVis._2)
          }
          // 如果存在线性拉伸
          else if (gainNum * biasNum != 0) {
            coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
              Add(Multiply(tile, visParam.getGain.headOption.getOrElse(1.0)), visParam.getBias.headOption.getOrElse(0.0))
            }))), coverageVis._2)
          }
          // 如果存在gamma值
          if (gammaNum != 0) {
            coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((_, tile) => {
              Pow(tile, visParam.getGamma.headOption.getOrElse(1.0))
            }))), coverageVis._2)
          }
          // 如果存在palette
          if (visParam.getPalette.nonEmpty) {
            val paletteVis: List[String] = visParam.getPalette
            val colorVis: List[Color] = paletteVis.map(t => {
              try {
                val color: Color = Color.valueOf(t)
                color
              } catch {
                case e: Exception =>
                  throw new IllegalArgumentException(s"输入颜色有误，无法识别$t")
              }
            })
            val colorRGB: List[(Double, Double, Double)] = colorVis.map(t => (t.getRed, t.getGreen, t.getBlue))
            val minMaxBand: (Double, Double) = coverageVis._1.map(t => {
              val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
              if (noNaNArray.nonEmpty) {
                (noNaNArray.min, noNaNArray.max)
              }
              else {
                (Int.MaxValue.toDouble, Int.MinValue.toDouble)
              }
            }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
            val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
            coverageVis = (coverageVis._1.map(t => {
              val bandR: Tile = t._2.bands.head.mapDouble(d => {
                var R: Double = 0.0
                for (i <- colorRGB.indices) {
                  if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                    R = colorRGB(i)._1 * 255.0
                  }
                }
                R
              })
              val bandG: Tile = t._2.bands.head.mapDouble(d => {
                var G: Double = 0.0
                for (i <- colorRGB.indices) {
                  if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                    G = colorRGB(i)._2 * 255.0
                  }
                }
                G
              })
              val bandB: Tile = t._2.bands.head.mapDouble(d => {
                var B: Double = 0.0
                for (i <- colorRGB.indices) {
                  if (d >= minMaxBand._1 + i * interval && d < minMaxBand._1 + (i + 1) * interval) {
                    B = colorRGB(i)._3 * 255.0
                  }
                }
                B
              })
              (t._1, MultibandTile(bandR, bandG, bandB))
            }), coverage._2)
          }
        }
      }
      // 如果有2个波段
      else if (bandNumber == 2) {
        // 排除palette
        if (visParam.getPalette.nonEmpty) {
          throw new IllegalArgumentException("palette不能应用于2个波段的影像！")
        }
        else {
          // 判断数量是否对应
          if (minNum > 2 || maxNum > 2 || gainNum > 2 || biasNum > 2 || gammaNum > 2) {
            throw new IllegalArgumentException("波段数量与参数数量不相符")
          }
          else {
            // 判断是否同时存在最小最大值拉伸和线性拉伸
            if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
              throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
            }
            else {
              // 如果存在最小最大值拉伸
              if (minNum * maxNum != 0) {
                // 首先找到现有的最小最大值
                val minMaxBand0: (Double, Double) = coverageVis._1.map(t => {
                  val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                  if (noNaNArray.nonEmpty) {
                    (noNaNArray.min, noNaNArray.max)
                  }
                  else {
                    (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                  }
                }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                val minMaxBand1: (Double, Double) = coverageVis._1.map(t => {
                  val noNaNArray: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
                  if (noNaNArray.nonEmpty) {
                    (noNaNArray.min, noNaNArray.max)
                  }
                  else {
                    (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                  }
                }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                var minVis0: Double = 0.0
                var minVis1: Double = 0.0
                var maxVis0: Double = 0.0
                var maxVis1: Double = 0.0
                // 判断min的数量
                if (minNum == 1) {
                  minVis0 = visParam.getMin.head
                  minVis1 = visParam.getMin.head
                }
                else if (minNum == 2) {
                  minVis0 = visParam.getMin.head
                  minVis1 = visParam.getMin(1)
                }
                if (maxNum == 1) {
                  maxVis0 = visParam.getMax.head
                  maxVis1 = visParam.getMax.head
                }
                else if (maxNum == 2) {
                  maxVis0 = visParam.getMax.head
                  maxVis1 = visParam.getMax(1)
                }
                val gainBand0: Double = (maxVis0 - minVis0) / (minMaxBand0._2 - minMaxBand0._1)
                val biasBand0: Double = (minMaxBand0._2 * minVis0 - minMaxBand0._1 * maxVis0) / (minMaxBand0._2 - minMaxBand0._1)
                val gainBand1: Double = (maxVis1 - minVis1) / (minMaxBand1._2 - minMaxBand1._1)
                val biasBand1: Double = (minMaxBand1._2 * minVis1 - minMaxBand1._1 * maxVis1) / (minMaxBand1._2 - minMaxBand1._1)

                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Add(Multiply(tile, gainBand0), biasBand0)
                  }
                  else {
                    Add(Multiply(tile, gainBand1), biasBand1)
                  }
                }))), coverageVis._2)
              }
              // 如果存在线性拉伸
              else if (gainNum * biasNum != 0) {
                var gainVis0: Double = 1.0
                var gainVis1: Double = 1.0
                var biasVis0: Double = 0.0
                var biasVis1: Double = 0.0
                if (gainNum == 1) {
                  gainVis0 = visParam.getGain.head
                  gainVis1 = visParam.getGain.head
                }
                else if (gainNum == 2) {
                  gainVis0 = visParam.getGain.head
                  gainVis1 = visParam.getGain(1)
                }
                if (biasNum == 1) {
                  biasVis0 = visParam.getBias.head
                  biasVis1 = visParam.getBias.head
                }
                else if (biasNum == 2) {
                  biasVis0 = visParam.getBias.head
                  biasVis1 = visParam.getBias(1)
                }
                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Add(Multiply(tile, gainVis0), biasVis0)
                  }
                  else {
                    Add(Multiply(tile, gainVis1), biasVis1)
                  }
                }))), coverageVis._2)
              }
              // 如果存在gamma值
              if (gammaNum != 0) {
                var gammaVis0: Double = 1.0
                var gammaVis1: Double = 1.0
                if (gammaNum == 1) {
                  gammaVis0 = visParam.getGamma.head
                  gammaVis1 = visParam.getGamma.head
                }
                else if (gammaNum == 2) {
                  gammaVis0 = visParam.getGamma.head
                  gammaVis1 = visParam.getGamma(1)
                }
                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Pow(tile, gammaVis0)
                  }
                  else {
                    Pow(tile, gammaVis1)
                  }
                }))), coverageVis._2)
              }
            }
          }
        }
      }
      // 如果有3个波段
      else {
        coverageVis = (coverageVis.map(t => {
          (SpaceTimeBandKey(t._1.spaceTimeKey, t._1.measurementName.take(3)), MultibandTile(t._2.bands.take(3)))
        }), coverageVis._2)
        // 排除palette
        if (visParam.getPalette.nonEmpty) {
          throw new IllegalArgumentException("palette不能应用于3个波段的影像！")
        }
        else {
          // 判断数量是否对应
          if (minNum > 3 || minNum == 2 || maxNum > 3 || maxNum == 2 || gainNum > 3 || gainNum == 2 || biasNum > 3 || biasNum == 2 || gammaNum > 3 || gammaNum == 2) {
            throw new IllegalArgumentException("波段数量与参数数量不相符")
          }
          else {
            // 判断是否同时存在最小最大值拉伸和线性拉伸
            if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
              throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
            }
            else {
              // 如果存在最小最大值拉伸
              if (minNum * maxNum != 0) {
                // 首先找到现有的最小最大值
                val minMaxBand0: (Double, Double) = coverageVis._1.map(t => {
                  val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
                  if (noNaNArray.nonEmpty) {
                    (noNaNArray.min, noNaNArray.max)
                  }
                  else {
                    (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                  }
                }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                val minMaxBand1: (Double, Double) = coverageVis._1.map(t => {
                  val noNaNArray: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
                  if (noNaNArray.nonEmpty) {
                    (noNaNArray.min, noNaNArray.max)
                  }
                  else {
                    (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                  }
                }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                val minMaxBand2: (Double, Double) = coverageVis._1.map(t => {
                  val noNaNArray: Array[Double] = t._2.bands(2).toArrayDouble().filter(!_.isNaN)
                  if (noNaNArray.nonEmpty) {
                    (noNaNArray.min, noNaNArray.max)
                  }
                  else {
                    (Int.MaxValue.toDouble, Int.MinValue.toDouble)
                  }
                }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
                var minVis0: Double = 0.0
                var minVis1: Double = 0.0
                var minVis2: Double = 0.0
                var maxVis0: Double = 0.0
                var maxVis1: Double = 0.0
                var maxVis2: Double = 0.0
                // 判断min的数量
                if (minNum == 1) {
                  minVis0 = visParam.getMin.head
                  minVis1 = visParam.getMin.head
                  minVis2 = visParam.getMin.head
                }
                else if (minNum == 3) {
                  minVis0 = visParam.getMin.head
                  minVis1 = visParam.getMin(1)
                  minVis2 = visParam.getMin(2)
                }
                if (maxNum == 1) {
                  maxVis0 = visParam.getMax.head
                  maxVis1 = visParam.getMax.head
                  maxVis2 = visParam.getMax.head
                }
                else if (maxNum == 3) {
                  maxVis0 = visParam.getMax.head
                  maxVis1 = visParam.getMax(1)
                  maxVis2 = visParam.getMax(2)
                }
                val gainBand0: Double = (maxVis0 - minVis0) / (minMaxBand0._2 - minMaxBand0._1)
                val biasBand0: Double = (minMaxBand0._2 * minVis0 - minMaxBand0._1 * maxVis0) / (minMaxBand0._2 - minMaxBand0._1)
                val gainBand1: Double = (maxVis1 - minVis1) / (minMaxBand1._2 - minMaxBand1._1)
                val biasBand1: Double = (minMaxBand1._2 * minVis1 - minMaxBand1._1 * maxVis1) / (minMaxBand1._2 - minMaxBand1._1)
                val gainBand2: Double = (maxVis2 - minVis2) / (minMaxBand2._2 - minMaxBand2._1)
                val biasBand2: Double = (minMaxBand2._2 * minVis2 - minMaxBand2._1 * maxVis2) / (minMaxBand2._2 - minMaxBand2._1)

                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Add(Multiply(tile, gainBand0), biasBand0)
                  }
                  else if (i == 1) {
                    Add(Multiply(tile, gainBand1), biasBand1)
                  }
                  else {
                    Add(Multiply(tile, gainBand2), biasBand2)
                  }
                }))), coverageVis._2)
              }
              // 如果存在线性拉伸
              else if (gainNum * biasNum != 0) {
                var gainVis0: Double = 1.0
                var gainVis1: Double = 1.0
                var gainVis2: Double = 1.0
                var biasVis0: Double = 0.0
                var biasVis1: Double = 0.0
                var biasVis2: Double = 0.0

                if (gainNum == 1) {
                  gainVis0 = visParam.getGain.head
                  gainVis1 = visParam.getGain.head
                  gainVis2 = visParam.getGain.head
                }
                else if (gainNum == 3) {
                  gainVis0 = visParam.getGain.head
                  gainVis1 = visParam.getGain(1)
                  gainVis2 = visParam.getGain(2)
                }
                if (biasNum == 1) {
                  biasVis0 = visParam.getBias.head
                  biasVis1 = visParam.getBias.head
                  biasVis2 = visParam.getBias.head
                }
                else if (biasNum == 3) {
                  biasVis0 = visParam.getBias.head
                  biasVis1 = visParam.getBias(1)
                  biasVis2 = visParam.getBias(2)
                }
                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Add(Multiply(tile, gainVis0), biasVis0)
                  }
                  else if (i == 1) {
                    Add(Multiply(tile, gainVis1), biasVis1)
                  }
                  else {
                    Add(Multiply(tile, gainVis2), biasVis2)
                  }
                }))), coverageVis._2)
              }
              // 如果存在gamma值
              if (gammaNum != 0) {
                var gammaVis0: Double = 1.0
                var gammaVis1: Double = 1.0
                var gammaVis2: Double = 1.0
                if (gammaNum == 1) {
                  gammaVis0 = visParam.getGamma.head
                  gammaVis1 = visParam.getGamma.head
                  gammaVis2 = visParam.getGamma.head
                }
                else if (gammaNum == 3) {
                  gammaVis0 = visParam.getGamma.head
                  gammaVis1 = visParam.getGamma(1)
                  gammaVis2 = visParam.getGamma(2)
                }
                coverageVis = (coverageVis._1.map(t => (t._1, t._2.mapBands((i, tile) => {
                  if (i == 0) {
                    Pow(tile, gammaVis0)
                  }
                  else if (i == 1) {
                    Pow(tile, gammaVis1)
                  }
                  else {
                    Pow(tile, gammaVis2)
                  }
                }))), coverageVis._2)
              }
            }
          }
        }
      }
    }

    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

    // TODO lrx:后面要不要考虑直接从MinIO读出来的数据进行上下采样？
    // 大于0是上采样
    if (COGUtil.tileDifference > 0) {
      // 首先对其进行上采样
      // 上采样必须考虑范围缩小，不然非常占用内存
      val levelUp: Int = COGUtil.tileDifference
      val layoutOrigin: LayoutDefinition = coverageTMS.metadata.layout
      val extentOrigin: Extent = layoutOrigin.extent
      val extentIntersect: Extent = extentOrigin.intersection(COGUtil.extent).orNull
      val layoutCols: Int = math.max(math.ceil((extentIntersect.xmax - extentIntersect.xmin) / 256.0 / layoutOrigin.cellSize.width * (1 << levelUp)).toInt, 1)
      val layoutRows: Int = math.max(math.ceil((extentIntersect.ymax - extentIntersect.ymin) / 256.0 / layoutOrigin.cellSize.height * (1 << levelUp)).toInt, 1)
      val extentNew: Extent = Extent(extentIntersect.xmin, extentIntersect.ymin, extentIntersect.xmin + layoutCols * 256.0 * layoutOrigin.cellSize.width / (1 << levelUp), extentIntersect.ymin + layoutRows * 256.0 * layoutOrigin.cellSize.height / (1 << levelUp))

      val tileLayout: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
      val layoutNew: LayoutDefinition = LayoutDefinition(extentNew, tileLayout)
      coverageTMS = coverageTMS.reproject(coverageTMS.metadata.crs, layoutNew)._2
    }

    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      coverageTMS.reproject(tmsCrs, layoutScheme)

    val outputPath: String = "/mnt/storage/on-the-fly"
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
    // Create the writer that we will use to store the tiles in the local catalog.
    val writer: FileLayerWriter = FileLayerWriter(attributeStore)

    if (zoom < Trigger.level) {
      throw new InternalError("内部错误，切分瓦片层级没有前端TMS层级高")
    }

    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      if (z == Trigger.level) {
        val layerId: LayerId = LayerId(Trigger.dagId, z)
        println(layerId)
        // If the layer exists already, delete it out before writing
        if (attributeStore.layerExists(layerId)) {
          //        new FileLayerManager(attributeStore).delete(layerId)
          try {
            writer.overwrite(layerId, rdd)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
        else {
          writer.write(layerId, rdd, ZCurveKeyIndexMethod)
        }
      }
    }

    // 回调服务
    val jsonObject: JSONObject = new JSONObject
    val rasterJsonObject: JSONObject = new JSONObject
    if (visParam.getFormat == "png") {
      rasterJsonObject.put(Trigger.layerName, "http://oge.whu.edu.cn/api/oge-tms-png/" + Trigger.dagId + "/{z}/{x}/{y}.png")
    }
    else {
      rasterJsonObject.put(Trigger.layerName, "http://oge.whu.edu.cn/api/oge-tms-jpg/" + Trigger.dagId + "/{z}/{x}/{y}.jpg")
    }
    jsonObject.put("raster", rasterJsonObject)

    val outJsonObject: JSONObject = new JSONObject
    outJsonObject.put("workID", Trigger.dagId)
    outJsonObject.put("json", jsonObject)

    sendPost(GlobalConstantUtil.DAG_ROOT_URL + "/deliverUrl", outJsonObject.toJSONString)

    val zIndexStrArray: mutable.ArrayBuffer[String] = Trigger.zIndexStrArray
    val jedis: Jedis = new JedisUtil().getJedis
    jedis.select(1)
    zIndexStrArray.foreach(zIndexStr => {
      val key: String = Trigger.dagId + ":solvedTile:" + Trigger.level + zIndexStr
      jedis.sadd(key, "cached")
      jedis.expire(key, GlobalConstantUtil.REDIS_CACHE_TTL)
    })
    jedis.close()


    // 清空list
    Trigger.optimizedDagMap.clear()
    Trigger.coverageCollectionMetadata.clear()
    Trigger.lazyFunc.clear()
    Trigger.coverageCollectionRddList.clear()
    Trigger.coverageRddList.clear()
    Trigger.zIndexStrArray.clear()
    JsonToArg.dagMap.clear()
    // TODO lrx: 以下为未检验
    Trigger.tableRddList.clear()
    Trigger.kernelRddList.clear()
    Trigger.featureRddList.clear()
    Trigger.cubeRDDList.clear()
    Trigger.cubeLoad.clear()

    if (sc.master.contains("local")) {
      whu.edu.cn.debug.load.makeTIFF(reprojected, "lsOrigin")
    }

  }

  def visualizeBatch(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Unit = {
  }


}
