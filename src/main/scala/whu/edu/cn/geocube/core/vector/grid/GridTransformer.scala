package whu.edu.cn.geocube.core.vector.grid

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.TileLayout
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import geotrellis.vector.Extent
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.geocube.core.cube.vector.GeoObject

import scala.collection.mutable.ArrayBuffer

/**
 *  A transformer for mapping geometry to spatial grid.
 *
 */
object GridTransformer {
  /**
   * Group geometry into grid, grid is represented with ZOrder code.
   * Return an array of pair(ZOrderIndex,(geomId, geometry)).
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A grid layout
   * @return An array of pair(ZOrderIndex,(geomId, geometry))
   */
  def groupGeom2ZCgrid(x: (String, Geometry), gridConf: GridConf): Array[(Long, (String, Geometry))] = {
    val results: ArrayBuffer[(Long, (String, Geometry))] = new ArrayBuffer[(Long, (String, Geometry))]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY
    /*val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble*/

    //get roughly intersected grids
    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMaxX,
      x._2.getEnvelopeInternal.getMinY, x._2.getEnvelopeInternal.getMaxY)
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
      if (gridExtent.intersects(x._2)) {
        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        val pair = (index.toLong, x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid, grid is represented with ZOrder code.
   * Return an array of pair(ZOrderIndex,GeoObject).
   *
   * @param x Input geometry which is represented by GeoObject
   * @param gridConf A grid layout
   * @return An array of pair(ZOrderIndex, GeoObject)
   */
  def groupGeom2ZCgrid(x: GeoObject, gridConf: GridConf): Array[(Long, GeoObject)] = {
    val results: ArrayBuffer[(Long, GeoObject)] = new ArrayBuffer[(Long, GeoObject)]()

    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //get geometry
    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]

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
        val pair = (index.toLong, x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid, grid is represented with SpatialKey.
   * Return an array of pair(SpatialKey,(geomId, geometry)).
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A grid layout
   * @return An array of pair(SpatialKey,(geomId, geometry))
   */
  def groupGeom2SKgrid(x: (String, Geometry), gridConf: GridConf): Array[(SpatialKey, (String, Geometry))] = {
    val results: ArrayBuffer[(SpatialKey, (String, Geometry))] = new ArrayBuffer[(SpatialKey, (String, Geometry))]()

    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //get roughly intersected grids
    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMaxX,
      x._2.getEnvelopeInternal.getMinY, x._2.getEnvelopeInternal.getMaxY)
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    //get accurately intersected grids
    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt)
    val ld = LayoutDefinition(extent, tl)
    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      // In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
      // while here y-axis is from bottom to top, thus inverse y-axix.
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(x._2)) {
        val pair = (SpatialKey(i, geotrellis_j), x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid, grid is represented with SpatialKey.
   * Return an array of pair(SpatialKey,GeoObject).
   *
   * @param x Input geometry which is represented by GeoObject
   * @param gridConf A grid layout
   * @return An array of pair(SpatialKey,GeoObject)
   */
  def groupGeom2SKgrid(x: GeoObject, gridConf: GridConf): Array[(SpatialKey, GeoObject)] = {
    val results: ArrayBuffer[(SpatialKey, GeoObject)] = new ArrayBuffer[(SpatialKey, GeoObject)]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //get geometry
    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]

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
    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      // In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
      // while here y-axis is from bottom to top, thus inverse y-axix.
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        val pair = (SpatialKey(i, geotrellis_j), x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid, grid is represented with ZOrder code.
   * Return an array of pair(ZOrderIndex,GeomID).
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A grid layout
   * @return An array of pair(ZOrderIndex, GeomID)
   */
  def groupGeomId2ZCgrid(x: (String, Geometry), gridConf: GridConf): Array[(Long, String)] = {
    val results: ArrayBuffer[(Long, String)] = new ArrayBuffer[(Long, String)]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    val geom = x._2

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
        val pair = (index.toLong, x._1)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid, grid is represented with ZOrder code.
   * Return an array of pair(ZOrderIndex,GeomID).
   *
   * @param x Input geometry which is represented by GeoObject
   * @param gridConf A grid layout
   * @return An array of pair(ZOrderIndex, GeomID)
   */
  def groupGeomId2ZCgrid(x: GeoObject, gridConf: GridConf): Array[(Long, String)] = {
    val results: ArrayBuffer[(Long, String)] = new ArrayBuffer[(Long, String)]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //get geometry
    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]

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
   * Collect grid z-codes that intersects with the input geometry.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A grid layout
   * @return An array of grid z-codes that intersects with the input geometry.
   */
  def getGeomZcodes(x: (String, Geometry), gridConf: GridConf): Array[Long] = {
    val results: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    val geom = x._2

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
        results.append(index.toLong)
      }
    }
    results.toArray
  }

  /**
   * Collect grid z-codes that intersects with the input geometry.
   *
   * @param x Input geometry which is represented by GeoObject
   * @param gridConf A grid layout
   * @return An array of grid z-codes that intersects with the input geometry.
   */
  def getGeomZcodes(x: GeoObject, gridConf: GridConf): Array[Long] = {
    val results: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    //get grid layout info
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val gridSizeX = gridConf.gridSizeX
    val gridSizeY = gridConf.gridSizeY

    //get geometry
    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]

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
        results.append(index.toLong)
      }
    }
    results.toArray
  }

  def getGeomZcodes(geom: Geometry, gridDimX: Long, gridDimY: Long, extent: geotrellis.vector.Extent,cellSize:Double): ArrayBuffer[String] = {
    println(gridDimX)
    println(gridDimY)
    println(extent)
    val spatialKeyResults: ArrayBuffer[SpatialKey] = new ArrayBuffer[SpatialKey]()
    val gridcodeResults: ArrayBuffer[String] = new ArrayBuffer[String]()
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)
    println(mbr)
    //get roughly intersected grids
    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    println(gridSizeX)
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt
    println(_xmin)
    println(_xmax)
    println(_ymin)
    println(_ymax)
    //set layout using geotrellis
    val tl = TileLayout(((extent.xmax - extent.xmin) / cellSize).toInt, ((extent.ymax - extent.ymin) / cellSize).toInt, gridDimX.toInt, gridDimY.toInt)
    val ld = LayoutDefinition(extent, tl)

    //get accurately intersected grids
    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      // In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
      // while here y-axis is from bottom to top, thus inverse y-axix.
      val geotrellis_j = (gridDimY - 1 - j).toInt
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        spatialKeyResults.append(SpatialKey(i, geotrellis_j))
      }
    }

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    spatialKeyResults.foreach(sk=>gridcodeResults.append(keyIndex.toIndex(sk).toString()))
    gridcodeResults
  }

  /**
   * Calculate the (minCol, minRow, maxCol, maxRow)
   * and covered grid extent of the input geometry
   * under the grid layout ($extent, $gridDimX, $gridDimY)
   *
   * @param geom
   * @param gridDimX
   * @param gridDimY
   * @param extent
   * @param colRow
   * @param longLat
   */
  def getGeomGridInfo(geom: Geometry, gridDimX: Long, gridDimY: Long, extent: Extent,
                      colRow: Array[Int], longLat: Array[Double],cellSize:Double): Unit = {
    val spatialKeyResults: ArrayBuffer[SpatialKey] = new ArrayBuffer[SpatialKey]()
    print("geom" + geom.toString)
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    //tile: 1° × 1°
//    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt)
    val tl = TileLayout(((extent.xmax - extent.xmin) / cellSize).toInt, ((extent.ymax - extent.ymin) / cellSize).toInt, gridDimX.toInt, gridDimY.toInt)
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
   * The function is used for OsGeo pyramid tile zcoding.
   *
   * Note: In the Geotrellis, x-axis is from left to right and y-axis is from top to bottom,
   * while y-axis is from bottom to top in OsGeo tile. So OsGeo tile are encoded by inverse y
   * in ingestion process. Thus, in the process of query, OsGeo tile zcode can be calculated
   * directly with request OsGeo x (i.e. column) and y (i.e. row), instead of using inverse y.
   *
   * @param column
   * @param row
   * @param level
   *
   * @return OsGeo pyramid tile zcode
   */
  def xy2GridCode(column: Int, row: Int, level: Int): String = {
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    val sk = SpatialKey(column, row)
    keyIndex.toIndex(sk).toString()
  }

}
