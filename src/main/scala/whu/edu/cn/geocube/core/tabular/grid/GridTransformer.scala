package whu.edu.cn.geocube.core.tabular.grid

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.TileLayout
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import geotrellis.vector.Extent
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import whu.edu.cn.geocube.core.cube.tabular.TabularRecord
import whu.edu.cn.geocube.core.vector.grid.GridConf

import scala.collection.mutable.ArrayBuffer
import org.geonames.Toponym
import org.geonames.ToponymSearchCriteria
import org.geonames.ToponymSearchResult
import org.geonames.WebService

class GridTransformer {

}

object GridTransformer {
  def getGeomGridInfo(geom: Geometry, gridDimX: Long, gridDimY: Long, extent: Extent,
                      colRow: Array[Int], longLat: Array[Double]): Unit = {
    val spatialKeyResults: ArrayBuffer[SpatialKey] = new ArrayBuffer[SpatialKey]()
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    //tile: 1° × 1°
    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt)
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      // In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
      // while here y-axis is from bottom to top, thus inverse y-axix.
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        spatialKeyResults.append(SpatialKey(i, geotrellis_j))
      }
    }
    val cols = spatialKeyResults.map(_.col)
    val minCol = cols.min
    val maxCol = cols.max
    val rows = spatialKeyResults.map(_.row)
    val minRow = rows.min
    val maxRow = rows.max
    colRow(0) = minCol; colRow(1) = minRow;colRow(2) = maxCol; colRow(3) = maxRow

    val longtitude = spatialKeyResults.flatMap(x => Array(x.extent(ld).xmin, x.extent(ld).xmax))
    val minLongtitude = longtitude.min
    val maxLongtitude = longtitude.max
    val latititude = spatialKeyResults.flatMap(x => Array(x.extent(ld).ymin, x.extent(ld).ymax))
    val minLatititude = latititude.min
    val maxLatititude = latititude.max
    longLat(0) = minLongtitude; longLat(1) = minLatititude; longLat(2) = maxLongtitude; longLat(3) = maxLatititude
  }

  /**
   * Group tabular into grid, grid is represented with ZOrder code.
   * Return an array of pair(ZOrderIndex,TabularID).
   * Using GeoNames library
   *
   * @param x Input geometry which is represented by GeoObject
   * @param gridConf A grid layout
   * @return An array of pair(ZOrderIndex, GeomID)
   */
  def groupTabularId2ZCgrid(x: TabularRecord, gridConf: GridConf): Array[(Long, String)] = {
    val results: ArrayBuffer[(Long, String)] = new ArrayBuffer[(Long, String)]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //using GeoNames web service
    /*val geo_name = x.getAttributes.get("geo_name").get
    val geo_addr = x.getAttributes.get("geo_address").get
    val (longitude, latitude) = getGeogpraphicCoordinate(geo_name)*/
    val longitude = x.getAttributes.get("longitude").get.toDouble
    val latitude = x.getAttributes.get("latitude").get.toDouble

    //get geometry
    val coord = new Coordinate(longitude, latitude)
    val geom = new GeometryFactory().createPoint(coord)

    //get roughly intersected grids
    val mbr = (geom.getEnvelopeInternal.getMinX, geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    //get accurately intersected grids
    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt)
    val ld = LayoutDefinition(extent, tl)
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      // In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
      // while here y-axis is from bottom to top, thus inverse y-axix.
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        val pair = (index.toLong, x.id)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * 问题：注册后这里仍然提示用户不存在
   * @return
   */
  def getGeogpraphicCoordinate(geoName: String): (Double, Double) = {
    WebService.setUserName("fgao")
    val searchCriteria = new ToponymSearchCriteria()
    searchCriteria.setQ(geoName) //"neijiang"
    searchCriteria.setLanguage("zh")

    var searchResult: ToponymSearchResult = null
    try{
      searchResult = WebService.search(searchCriteria)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        System.err.println("exception===>:" + ex.toString)
      }
    }

    var toponym: Toponym = null
    val count = searchResult.getTotalResultsCount
    var latitude: Double = 0.0
    var longtitude: Double = 0.0
    for(i <- 0 until count){
      toponym = searchResult.getToponyms.get(i)
      //println(toponym.getName + " " + toponym.getCountryName + toponym.getAdminCode1)
      latitude = toponym.getLatitude
      longtitude = toponym.getLongitude
    }
    Tuple2(toponym.getLatitude, toponym.getLongitude)
  }


  def main(args: Array[String]): Unit = {
    //WebService.setGeoNamesServer("api.geonames.org")

  }

}
