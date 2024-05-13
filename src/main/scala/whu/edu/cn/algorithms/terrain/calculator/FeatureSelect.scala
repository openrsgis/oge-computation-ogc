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
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FeatureSelect {

  /** call the Calculator
    *
    * @note the input rddImage doesn't require padding
    * @param rddImage raster data in RDD format with metadata
    * @param radius   radius of padding, default is 1
    * @param zFactor  z position factor for calculation
    * @return result
    */
  def apply(
      rddImage: RDDImage,
      radius: Int = 1,
      zFactor: Double = 1.0,
      vipValue: Double = 10.0
  ): RDDFeature = {
    val (rddPadding, metaData) = paddingRDDImage(rddImage, radius)
    val result = rddPadding.map(t => {
      val calculator =
        FeatureSelectCalculator(
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
    val result1: RDDFeature = result.flatMap(list => list)
    result1
  }

  private case class FeatureSelectCalculator(
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

    def apply(): ListBuffer[(String, (Geometry, mutable.Map[String, Any]))] = {
      algorithmType match {
        case 0 => doVIPPointSelect(VIPValue)
      }

      val result = FeaturePoint.refine()

      val geometryFactory = new GeometryFactory()

      val points: ListBuffer[(String, (Geometry, mutable.Map[String, Any]))] =
        ListBuffer()
      val crs: CRS = DEM.metaData.crs

      val nx = result.cols
      val ny = result.rows
      var count = 0
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
              new Coordinate(reprojectedPoint.getX, reprojectedPoint.getY)
            val point = geometryFactory.createPoint(coordinate)
            point.setSRID(4326)

            count += 1
            val properties: String = f"{'id' : '$count', " +
              f"'x' : '$x', " +
              f"'y' : '$y', " +
              f"'dem' : '${result.getDouble(x, y)}'}"
            val result1: (String, (Geometry, mutable.Map[String, Any])) = (
              UUID.randomUUID().toString,
              (point, getMapFromJsonStr(properties))
            )
            points += result1
          }
        }
      }
      points
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
