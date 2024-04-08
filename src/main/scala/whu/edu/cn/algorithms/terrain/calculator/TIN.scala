package whu.edu.cn.algorithms.terrain.calculator

import whu.edu.cn.algorithms.terrain.core.OgeTerrainTile
import whu.edu.cn.algorithms.terrain.core.TypeAliases.{RDDImage, RDDFeature}
import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.paddingRDDImage
import com.alibaba.fastjson.JSON
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.Tile
import geotrellis.vector.reproject.Reproject
import org.apache.commons.lang3.StringUtils
import org.locationtech.jts.geom._
import org.locationtech.jts.triangulate.ConformingDelaunayTriangulationBuilder

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object TIN {

  /** call the Calculator
    *
    * @note the input rddImage doesn't require padding
    * @param rddImage      raster data in RDD format with metadata
    * @param radius        radius of padding, default is 1
    * @param zFactor       z position factor for calculation
    * @param vipValue      VIP algorithm threshold
    * @param geometryType  type of result (1: points, 2: lines, 3: polygons)
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      radius: Int = 1,
      zFactor: Double = 1.0,
      vipValue: Double = 10.0,
      geometryType: Int = 2
  ): RDDFeature = {
    val geometryFactory = new GeometryFactory()
    val shapes: ListBuffer[(String, (Geometry, mutable.Map[String, Any]))] = ListBuffer()

    val layoutCols = rddImage._2.layoutCols
    val layoutRows = rddImage._2.layoutRows

    // extent.width and extent.height are unAligned data. got original numCols and numRows from it.
    val maxCols = (rddImage._2.extent.width / rddImage._2.cellwidth).toInt - 1
    val maxRows = (rddImage._2.extent.height / rddImage._2.cellheight).toInt - 1

    val corners = rddImage._1.map(t => {
      // 0: topLeft, 1: topRight, 2: bottomLeft, 3: bottomRight, -1: others
      val sk = t._1.spaceTimeKey
      val tile = t._2.band(0)
      var res = (-1, 0.0)
      if (sk.row == 0 && sk.col == 0) {
        res = (0, tile.getDouble(0, 0))
      } else if (sk.row == 0 && sk.col == layoutCols - 1) {
        val c = maxCols % tile.cols
        res = (1, tile.getDouble(c, 0))
      } else if (sk.row == layoutRows - 1 && sk.col == 0) {
        val r = maxRows % tile.rows
        res = (2, tile.getDouble(0, r))
      } else if (sk.row == layoutRows - 1 && sk.col == layoutCols - 1) {
        val r = maxRows % tile.rows
        val c = maxCols % tile.cols
        res = (3, tile.getDouble(c, r))
      }
      res
    })

    var Za, Zb, Zc, Zd = 0.0
    val cornersCollect = corners.collect()
    for (elem <- cornersCollect) {
      elem._1 match {
        case 0 => Za = if (elem._2.isNaN) 0 else elem._2
        case 1 => Zb = if (elem._2.isNaN) 0 else elem._2
        case 2 => Zc = if (elem._2.isNaN) 0 else elem._2
        case 3 => Zd = if (elem._2.isNaN) 0 else elem._2
        case _ =>
      }
    }

    val (rddPadding, metaData) = paddingRDDImage(rddImage, radius)
    val result = rddPadding.flatMap(t => {
      val calculator =
        VIPCalculator(
          t._2.band(0),
          t._1.spaceTimeKey.col,
          t._1.spaceTimeKey.row,
          metaData,
          radius,
          zFactor,
          vipValue
        )
      calculator()
    })

    var points = result.collect()

    val xMin = metaData.extent.xmin
    val xMax = metaData.extent.xmax
    val yMin = metaData.extent.ymin
    val yMax = metaData.extent.ymax

    val a = new Coordinate(xMin, yMax, Za) // topLeft
    val b = new Coordinate(xMax, yMax, Zb) // topRight
    val c = new Coordinate(xMin, yMin, Zc) // bottomLeft
    val d = new Coordinate(xMax, yMin, Zd) // bottomRight

    points ++= Array(
      geometryFactory.createPoint(a),
      geometryFactory.createPoint(b),
      geometryFactory.createPoint(c),
      geometryFactory.createPoint(d)
    )

    points.foreach(point =>
      point.getCoordinate.setZ(point.getCoordinate.getZ * zFactor)
    )

    val conformTriBuilder = new ConformingDelaunayTriangulationBuilder()
    conformTriBuilder.setSites(geometryFactory.createMultiPoint(points))

    val tin = conformTriBuilder.getTriangles(geometryFactory)
    val pointIndexMap: mutable.HashMap[Coordinate, Int] =
      new mutable.HashMap[Coordinate, Int]()
    pointIndexMap.sizeHint(points.length)

    for (i <- points.indices) {
      val point: Point = points(i)
      pointIndexMap.put(point.getCoordinate, i)
    }

    val lineStrings: ArrayBuffer[LineString] = ArrayBuffer()
    val polygons: ArrayBuffer[Polygon] = ArrayBuffer()

    var curCount = points.length
    for (i <- 0 until tin.getNumGeometries) {
      val polygon = tin.getGeometryN(i)

      polygon match {
        case polygon: Polygon =>
          val lineString: LineString = polygon.getExteriorRing
          lineStrings += lineString
          polygons += polygon
      }

      val pts = tin.getGeometryN(i).getCoordinates
      val idx = Array(0, 0, 0)
      for (j <- 0 until 3) {
        val index = pointIndexMap.get(pts(j))
        index match {
          case Some(x) => idx(j) = x
          case None =>
            if (pts(j).getZ.isNaN) {
              pts(j).setZ(0)
            }
            idx(j) = curCount
            pointIndexMap.put(pts(j), curCount)
            curCount += 1
        }
      }
    }

    geometryType match {
      case 1 =>
        for (i <- points.indices) {
          val point = points(i)
          point.setSRID(4326)
          val properties: String = f"{'id' : '$i', " +
            f"'x' : '${point.getCoordinate.x}', " +
            f"'y' : '${point.getCoordinate.y}', " +
            f"'z' : '${point.getCoordinate.getZ}'}"
          val shape: (String, (Geometry, mutable.Map[String, Any])) = (
            UUID.randomUUID().toString,
            (point, getMapFromJsonStr(properties))
          )
          shapes += shape
        }
      case 2 =>
        for (i <- lineStrings.indices) {
          val lineString = lineStrings(i)
          lineString.setSRID(4326)
          val properties: String = f"{'id' : '$i'}"
          val shape: (String, (Geometry, mutable.Map[String, Any])) = (
            UUID.randomUUID().toString,
            (lineString, getMapFromJsonStr(properties))
          )
          shapes += shape
        }
      case 3 =>
        for (i <- polygons.indices) {
          val polygon = polygons(i)
          polygon.setSRID(4326)
          val properties: String = f"{'id' : '$i'}"
          val shape: (String, (Geometry, mutable.Map[String, Any])) = (
            UUID.randomUUID().toString,
            (polygon, getMapFromJsonStr(properties))
          )
          shapes += shape
        }
    }

    val sc = rddImage._1.sparkContext
    val rddResult: RDDFeature = sc.parallelize(shapes)
    rddResult
  }

  private def getMapFromJsonStr(json: String): mutable.Map[String, Any] = {
    val map = mutable.Map.empty[String, Any]
    if (StringUtils.isNotEmpty(json) && !json.equals("")) {
      val jsonObject = JSON.parseObject(json)
      val sIterator = jsonObject.keySet.iterator
      while (sIterator.hasNext) {
        val key = sIterator.next()
        val value = jsonObject.getString(key)
        map += (key -> value)
      }
    }
    map
  }

  private case class VIPCalculator(
      tile: Tile,
      col: Int,
      row: Int,
      metaData: TileLayerMetadata[SpaceTimeKey],
      paddingSize: Int,
      zFactor: Double,
      vipValue: Double
  ) {
    private val DEM: OgeTerrainTile =
      OgeTerrainTile.from(tile, metaData, paddingSize)
    private val FeaturePoint: OgeTerrainTile =
      OgeTerrainTile.fill(tile, metaData, paddingSize, 0)
    private val algorithmType: Int = 0
    private val VIPValue: Double = vipValue

    def apply(): ListBuffer[Point] = {
      algorithmType match {
        case 0 => doVIPPointSelect(VIPValue)
      }

      val result = FeaturePoint.refine()

      val geometryFactory = new GeometryFactory()

      val points: ListBuffer[Point] = ListBuffer()
      val crs: CRS = DEM.metaData.crs

      val nx = result.cols
      val ny = result.rows
      for (y <- 0 until ny) {
        for (x <- 0 until nx) {
          if (result.getDouble(x, y) > 0) {
            val xMin =
              metaData.extent.xmin + (nx * metaData.cellSize.width) * col
            val yMax =
              metaData.extent.ymax - (ny * metaData.cellSize.width) * row
            val xVector: Double = xMin + (x + 0.5) * metaData.cellSize.width
            val yVector: Double = yMax - (y + 0.5) * metaData.cellSize.width

            val pixelPoint = geotrellis.vector.Point(xVector, yVector)
            val reprojectedPoint = Reproject(pixelPoint, crs, LatLng)

            val coordinate =
              new Coordinate(reprojectedPoint.getX, reprojectedPoint.getY, DEM.getDouble(x, ny - y - 1))
            val point = geometryFactory.createPoint(coordinate)
            point.setSRID(4326)
            points += point
          }
        }
      }
      points
    }

    private def doVIPPointSelect(VIPValue: Double): Boolean = {
      val z: Array[Double] = new Array[Double](9)
      var t: Double = 0
      var max: Double = -1e20
      var min: Double = 1e20
      val pv: Array[Double] = new Array[Double](DEM.nX * DEM.nY)

      for (i <- 1 until DEM.nY - 1) {
        for (j <- 1 until DEM.nX - 1) {
          for (k <- -1 to 1) {
            for (l <- -1 to 1)
              z((k + 1) * 3 + l + 1) = DEM.getDouble(j + l, DEM.nY - 1 - i - k)
          }
          t = calPointVIP(z)
          if (min > t) min = t
          if (max < t) max = t
          pv(i * DEM.nX + j) = t
        }
      }

      for (i <- 0 until DEM.nX) {
        pv(i) = min
        pv((DEM.nY - 1) * DEM.nX + i) = min
      }

      for (i <- 0 until DEM.nY) {
        pv(i * DEM.nX) = min
        pv(i * DEM.nX + DEM.nX - 1) = min
      }

      for (i <- 1 until DEM.nY - 1) {
        for (j <- 1 until DEM.nX - 1) {
          if (pv(i * DEM.nX + j) < min)
            pv(i * DEM.nX + j) = min
          if (pv(i * DEM.nX + j) >= VIPValue)
            FeaturePoint.setDouble(j, DEM.nY - 1 - i, pv(i * DEM.nX + j))
          else
            FeaturePoint.setDouble(j, DEM.nY - 1 - i, 0)
        }
      }
      true

    }

    private def calPointVIP(z: Array[Double]): Double = {
      val z0: Double = z(4)
      val a: Array[Double] = new Array[Double](4)
      val dis: Double = math.sqrt(DEM.cellArea + DEM.cellArea)

      a(0) = (z0 - (z(6) + z(2)) / 2) * math.sqrt(
        4 * (DEM.cellArea + DEM.cellArea) - (z(6) - z(2)) * (z(6) - z(2))
      ) / (2 * dis)
      a(1) = (z0 - (z(3) + z(5)) / 2) * math.sqrt(
        4 * DEM.cellSize * DEM.cellSize - (z(3) - z(5)) * (z(3) - z(5))
      ) / (2 * DEM.cellSize)
      a(2) = (z0 - (z(0) + z(8)) / 2) * math.sqrt(
        4 * (DEM.cellArea + DEM.cellArea) - (z(0) - z(8)) * (z(0) - z(8))
      ) / (2 * dis)
      a(3) = (z0 - (z(1) + z(7)) / 2) * math.sqrt(
        4 * DEM.cellSize * DEM.cellSize - (z(1) - z(7)) * (z(1) - z(7))
      ) / (2 * DEM.cellSize)

      math.abs(a(0) + a(1) + a(2) + a(3)) / 4
    }

  }

}
