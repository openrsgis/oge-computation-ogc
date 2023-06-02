package whu.edu.cn.application.oge.terrainAnalysis

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{CellSize, Tile}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString}
import whu.edu.cn.core.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveFeatureRDDToShp}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TerrainFeature {
  var inputPath: String = _
  private var rddImage: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey]) = _

  def main(args: Array[String]): Unit = {
    inputPath = "/home/cgd/Documents/DEM/SAGA/rc/data/dem64.tif"
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TerrainFeature")
    val sc = new SparkContext(conf)
    rddImage = makeChangedRasterRDDFromTif(sc, inputPath)
    val contours = generateContour(rddImage)
    saveFeatureRDDToShp(contours, "/home/cgd/Documents/DEM/Output/contour.shp")
  }

  private def generateContour(input: (RDD[(SpaceTimeBandKey, Tile)], TileLayerMetadata[SpaceTimeKey])): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val rddImage = input._1
    val metadata = input._2
    val featureRDD = rddImage.flatMap { case (key, tile) =>
      val extent = metadata.layout.extent
      val cellSize = metadata.layout.cellSize

      val contours = generateContours(tile, extent, cellSize)

      val attributes = mutable.Map[String, Any]("key" -> key.toString)

      contours.map { case (lineString, id, elevation) =>
        val geometry: Geometry = lineString
        val attributeMap: mutable.Map[String, Any] = attributes + ("id" -> id, "elevation" -> elevation)
        ("contour", (geometry, attributeMap))
      }
    }


    featureRDD
  }

  private def generateContours(tile: Tile, extent: Extent, cellSize: CellSize, threshold: Double = 1.0): Seq[(LineString, String, Double)] = {
    // 使用 JTS 创建 GeometryFactory 对象
    val geometryFactory = new GeometryFactory()

    val points = tile
      .toArrayDouble()
      .zipWithIndex
      .filter { case (value, _) => value >= 0 }
      .map { case (value, index) =>
        val col = index % tile.cols
        val row = index / tile.cols
        val x = extent.xmin + col * cellSize.width
        val y = extent.ymax - row * cellSize.height
        val coordinate = new Coordinate(x, y)
        val elevation = value
        (coordinate, elevation)
      }
    val sortedPoints = points.sortBy(_._2)

    val lineStrings = new scala.collection.mutable.ListBuffer[(LineString, String, Double)]
    var currentLineString: LineString = null
    var currentElevation: Double = Double.NaN
    val currentLineStringCoordinates = ListBuffer[Coordinate]()
    sortedPoints.foreach { case (coordinate, elevation) =>
      if (elevation != currentElevation) {
        if (currentLineString != null || (elevation - currentElevation>=threshold)) {
          if (currentLineStringCoordinates.size >= 2) {
            val line = geometryFactory.createLineString(currentLineStringCoordinates.toArray)
            lineStrings += ((line, s"contour_${lineStrings.size + 1}", currentElevation))
          }
          currentLineStringCoordinates.clear()
        }
        currentLineStringCoordinates += coordinate
        currentElevation = elevation
      } else {
        currentLineStringCoordinates += coordinate
        }
      }
    // Add the last line string
    if (currentLineStringCoordinates.size >= 2) {
      val line = geometryFactory.createLineString(currentLineStringCoordinates.toArray)
      lineStrings += ((line, s"contour_${lineStrings.size + 1}", currentElevation))
    }
    lineStrings.toList
  }

}
