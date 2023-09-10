package whu.edu.cn.geocube.core.entity

import com.fasterxml.jackson.databind.annotation.JsonSerialize

import java.sql.{DriverManager, ResultSet}
import geotrellis.layer.LayoutDefinition
import geotrellis.raster.TileLayout
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon}

import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomZcodes
import whu.edu.cn.util.PostgresqlUtil

import scala.beans.BeanProperty
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

/**
 * A wrapper of query parameters.
 *
 */
class QueryParams extends Serializable {
  @JsonSerialize
  var cubeId: String = ""
  @JsonSerialize
  var rasterProductName: String = ""
  @JsonSerialize
  var rasterProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var vectorProductName: String = ""
  @JsonSerialize
  var vectorProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var tabularProductName: String = ""
  @JsonSerialize
  var platform: String = ""
  @JsonSerialize
  var instruments: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var measurements: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var highLevelMeasurement: String = ""
  @JsonSerialize
  var CRS: String = ""
  @JsonSerialize
  var tileSize: String = ""
  @JsonSerialize
  var cellRes: String = ""
  @JsonSerialize
  var level: String = ""
  @JsonSerialize
  var phenomenonTime: String = ""
  @JsonSerialize
  var startTime: String = ""
  @JsonSerialize
  var endTime: String = ""
  @JsonSerialize
  var nextStartTime: String = ""
  @JsonSerialize
  var nextEndTime: String = ""
  @JsonSerialize
  var month: Int = -1
  @JsonSerialize
  var year: Int = -1
  @JsonSerialize
  var gridCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var cityCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var cityNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  @JsonSerialize
  var cityName: String = ""
  @JsonSerialize
  var provinceName: String = ""
  @JsonSerialize
  var districtName: String = ""
  @JsonSerialize
  var cloudMax: String = ""
  @JsonSerialize
  var cloudShadowMax: String = ""
  @JsonSerialize
  var polygon: Polygon = null
  @JsonSerialize
  var extentCoordinates: ArrayBuffer[Double] = new ArrayBuffer[Double]()
  @JsonSerialize
  var subsetQuery: ArrayBuffer[SubsetQuery] = new ArrayBuffer[SubsetQuery]()
  //  var subsetQuery: java.util.List[SubsetQuery] = new java.util.ArrayList[SubsetQuery]()

  def this(cubeId: String, rasterProductName: String, rasterProductNames: ArrayBuffer[String], vectorProductName: String,
           vectorProductNames: ArrayBuffer[String], tabularProductName: String, platform: String, instruments: ArrayBuffer[String],
           measurements: ArrayBuffer[String], highLevelMeasurement: String, CRS: String, tileSize: String, cellRes: String,
           level: String, phenomenonTime: String, startTime: String, endTime: String, nextStartTime: String, nextEndTime: String,
           month: Int, year: Int, gridCodes: ArrayBuffer[String], cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String],
           cityName: String, provinceName: String, districtName: String, cloudMax: String, cloudShadowMax: String, polygon: Polygon,
           extentCoordinates: ArrayBuffer[Double], subsetQuery: ArrayBuffer[SubsetQuery]) {
    this()
    this.cubeId = cubeId
    this.rasterProductName = rasterProductName
    this.rasterProductNames = rasterProductNames
    this.vectorProductName = vectorProductName
    this.vectorProductNames = vectorProductNames
    this.tabularProductName = tabularProductName
    this.platform = platform
    this.instruments = instruments
    this.measurements = measurements
    this.highLevelMeasurement = highLevelMeasurement
    this.CRS = CRS
    this.tileSize = tileSize
    this.cellRes = cellRes
    this.level = level
    this.phenomenonTime = phenomenonTime
    this.startTime = startTime
    this.endTime = endTime
    this.nextStartTime = nextStartTime
    this.nextEndTime = nextEndTime
    this.month = month
    this.year = year
    this.gridCodes = gridCodes
    this.cityCodes = cityCodes
    this.cityNames = cityNames
    this.cityName = cityName
    this.provinceName = provinceName
    this.districtName = districtName
    this.cloudMax = cloudMax
    this.cloudShadowMax = cloudShadowMax
    this.polygon = polygon
    this.extentCoordinates = extentCoordinates
    this.subsetQuery = subsetQuery
  }

  //  def this(platform: String, measurements: ArrayBuffer[String],
  //           gridCodes: ArrayBuffer[String], cloud: String, level: String) {
  //    this()
  //    this.platform = platform
  //    this.measurements = measurements
  //    this.gridCodes = gridCodes
  //    this.cloudMax = cloud
  //    this.level = level
  //  }
  //
  //  def this(rasterProductName: String, platform: String, instruments: ArrayBuffer[String],
  //           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
  //           cellRes: String, level: String, phenomenonTime: String,
  //           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
  //           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String) {
  //    this()
  //    this.rasterProductName = rasterProductName
  //    this.platform = platform
  //    this.instruments = instruments
  //    this.measurements = measurements
  //    this.CRS = CRS
  //    this.tileSize = tileSize
  //    this.cellRes = cellRes
  //    this.startTime = startTime
  //    this.endTime = endTime
  //    this.nextStartTime = nextStartTime
  //    this.nextEndTime = nextEndTime
  //    this.phenomenonTime = phenomenonTime
  //    this.gridCodes = gridCodes
  //    this.cityCodes = cityCodes
  //    this.cityNames = cityNames
  //    this.cloudMax = cloud
  //    this.cloudShadowMax = cloudShadow
  //    this.level = level
  //  }
  //
  //  def this(rasterProductNames: ArrayBuffer[String], platform: String, instruments: ArrayBuffer[String],
  //           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
  //           cellRes: String, level: String, phenomenonTime: String,
  //           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
  //           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String) {
  //    this()
  //    this.rasterProductNames = rasterProductNames
  //    this.platform = platform
  //    this.instruments = instruments
  //    this.measurements = measurements
  //    this.CRS = CRS
  //    this.tileSize = tileSize
  //    this.cellRes = cellRes
  //    this.startTime = startTime
  //    this.endTime = endTime
  //    this.nextStartTime = nextStartTime
  //    this.nextEndTime = nextEndTime
  //    this.phenomenonTime = phenomenonTime
  //    this.gridCodes = gridCodes
  //    this.cityCodes = cityCodes
  //    this.cityNames = cityNames
  //    this.cloudMax = cloud
  //    this.cloudShadowMax = cloudShadow
  //    this.level = level
  //  }

  /**
   * Set grid codes with input extent.
   *
   * @param leftBottomLong
   * @param LeftBottomLat
   * @param rightUpperLong
   * @param rightUpperLat
   */
  def setExtent(leftBottomLong: Double, leftBottomLat: Double, rightUpperLong: Double, rightUpperLat: Double): Unit = {
    this.extentCoordinates.append(leftBottomLong, leftBottomLat, rightUpperLong, rightUpperLat)
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(leftBottomLong, leftBottomLat),
      new Coordinate(leftBottomLong, rightUpperLat),
      new Coordinate(rightUpperLong, rightUpperLat),
      new Coordinate(rightUpperLong, leftBottomLat),
      new Coordinate(leftBottomLong, leftBottomLat)
    ))
    this.polygon = queryGeometry
    val id = this.getCubeId
    PostgresqlUtil.get()
    val SRE = getSizeResAndExtentByCubeId(id, PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password).toList.head
    val extent = geotrellis.vector.Extent(SRE._2._1, SRE._2._2, SRE._2._3, SRE._2._4)
    println(SRE)
    val tl = TileLayout(((SRE._2._3 - SRE._2._1) / SRE._1._1).toInt, ((SRE._2._4 - SRE._2._2) / SRE._1._1).toInt, SRE._1._2.toInt, SRE._1._2.toInt) //1024 can be replaced by any value
    val ld = LayoutDefinition(extent, tl)
    val gridCodeArray = getGeomZcodes(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent, SRE._1._1)
    this.setGridCodes(gridCodeArray)
  }

    def setPolygon(polygon: Polygon): Unit = {
      this.polygon = polygon
    }

  def updatePolygon(): Unit = {
    if (!this.extentCoordinates.isEmpty) {
      val leftBottomLong = this.extentCoordinates(0)
      val leftBottomLat = this.extentCoordinates(1)
      val rightUpperLong = this.extentCoordinates(2)
      val rightUpperLat = this.extentCoordinates(3)
      val queryGeometry = new GeometryFactory().createPolygon(Array(
        new Coordinate(leftBottomLong, leftBottomLat),
        new Coordinate(leftBottomLong, rightUpperLat),
        new Coordinate(rightUpperLong, rightUpperLat),
        new Coordinate(rightUpperLong, leftBottomLat),
        new Coordinate(leftBottomLong, leftBottomLat)
      ))
      this.polygon = queryGeometry
    }
  }

  /**
   * Set city or province name.
   * Support roll up and drill down operation.
   *
   * @param districtName
   * @param districtType
   */
  def setExtent(districtName: String, districtType: String): Unit = {
    districtType match {
      case "city" => this.setCityName(districtName)
      case "province" => this.setProvinceName(districtName)
      case _ => throw new RuntimeException("No support for " + districtType)
    }
  }


  /**
   * Set query time range.
   *
   * @param startTime
   * @param endTime
   */
  def setTime(startTime: String, endTime: String): Unit = {
    this.setStartTime(startTime)
    this.setEndTime(endTime)
  }

  /**
   * Set previous time range, which is used in two-period analysis,
   * e.g. water change detection.
   *
   * @param previousStartTime
   * @param previousEndTime
   */
  def setPreviousTime(previousStartTime: String, previousEndTime: String): Unit = {
    this.setTime(previousStartTime, previousEndTime)
  }


  /**
   * Set next time range, which is used in two-period analysis,
   * e.g. water change detection.
   *
   * @param nextStartTime
   * @param nextEndTime
   */
  def setNextTime(nextStartTime: String, nextEndTime: String): Unit = {
    this.setNextStartTime(nextStartTime)
    this.setNextEndTime(nextEndTime)
  }


  def setMonth(month: Int): Unit = {
    this.month = month
  }

  def setYear(year: Int): Unit = {
    this.year = year
  }

  def setRasterProductName(rasterProductName: String): Unit = {
    this.rasterProductName = rasterProductName
  }

  def setRasterProductNames(rasterProductNames: ArrayBuffer[String]): Unit = {
    this.rasterProductNames = rasterProductNames
  }

  def setRasterProductNames(rasterProductNames: Array[String]): Unit = {
    this.rasterProductNames = rasterProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }


  def setVectorProductName(vectorProductName: String): Unit = {
    this.vectorProductName = vectorProductName
  }

  def setVectorProductNames(vectorProductNames: ArrayBuffer[String]): Unit = {
    this.vectorProductNames = vectorProductNames
  }

  def setVectorProductNames(vectorProductNames: Array[String]): Unit = {
    this.vectorProductNames = vectorProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }


  def setTabularProductName(tabularProductName: String): Unit = {
    this.tabularProductName = tabularProductName
  }

  def setPlatform(platform: String): Unit = {
    this.platform = platform
  }

  def setInstruments(instruments: ArrayBuffer[String]): Unit = {
    this.instruments = instruments
  }

  def setMeasurements(measurements: Array[String]): Unit = {
    this.measurements = measurements.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }

  def setMeasurements(measurements: ArrayBuffer[String]): Unit = {
    this.measurements = measurements
  }

  def setHighLevelMeasurement(highLevelMeasurement: String): Unit = {
    this.highLevelMeasurement = highLevelMeasurement
  }

  def setLevel(level: String): Unit = {
    this.level = level
  }

  def setCRS(CRS: String): Unit = {
    this.CRS = CRS
  }

  def setCloudMax(cloudMax: String): Unit = {
    this.cloudMax = cloudMax
  }

  def setCloudShadowMax(cloudShadowMax: String): Unit = {
    this.cloudShadowMax = cloudShadowMax
  }

  def setStartTime(startTime: String): Unit = {
    this.startTime = startTime
  }

  def setEndTime(endTime: String): Unit = {
    this.endTime = endTime
  }

  def setNextStartTime(nextStartTime: String): Unit = {
    this.nextStartTime = nextStartTime
  }

  def setNextEndTime(nextEndTime: String): Unit = {
    this.nextEndTime = nextEndTime
  }

  def setGridCodes(gridCodes: ArrayBuffer[String]): Unit = {
    this.gridCodes = gridCodes
  }

  def setGridCodes(gridCodes: Array[String]): Unit = {
    this.gridCodes = gridCodes.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }

  def setCellRes(cellRes: String): Unit = {
    this.cellRes = cellRes
  }

  def setCityCodes(cityCodes: ArrayBuffer[String]): Unit = {
    this.cityCodes = cityCodes
  }

  def setCityNames(cityNames: ArrayBuffer[String]): Unit = {
    this.cityNames = cityNames
  }

  def setDistrictName(districtName: String): Unit = {
    this.districtName = districtName
  }

  def setCityName(cityName: String): Unit = {
    this.cityName = cityName
  }

  def setProvinceName(provinceName: String): Unit = {
    this.provinceName = provinceName
  }

  def setTileSize(tileSize: String): Unit = {
    this.tileSize = tileSize
  }

  def setCubeId(cubeId: String): Unit = {
    this.cubeId = cubeId
  }

  def setExtentCoordinates(extentCoordinates: ArrayBuffer[Double]): Unit = {
    this.extentCoordinates = extentCoordinates
  }

  def setSubsetQuery(subsetQuery: ArrayBuffer[SubsetQuery]): Unit = {
    this.subsetQuery = subsetQuery
  }

  def setSubsetQuery(subsetQuery: java.util.List[SubsetQuery]): Unit = {
    this.subsetQuery = ArrayBuffer(subsetQuery.asScala: _*)
  }

  def getPolygon: Polygon = polygon

  def getPlatform: String = platform

  def getPhenomenonTime: String = phenomenonTime

  def getRasterProductName: String = rasterProductName

  def getRasterProductNames: ArrayBuffer[String] = rasterProductNames

  def getVectorProductName: String = vectorProductName

  def getVectorProductNames: ArrayBuffer[String] = vectorProductNames

  def getTabularProductName: String = tabularProductName

  def getStartTime: String = startTime

  def getEndTime: String = endTime

  def getNextStartTime: String = nextStartTime

  def getNextEndTime: String = nextEndTime

  def getMonth: Int = month

  def getYear: Int = year

  def getLevel: String = level

  def getTileSize: String = tileSize

  def getGridCodes: ArrayBuffer[String] = gridCodes

  def getCloudMax: String = cloudMax

  def getCloudShadowMax: String = cloudShadowMax

  def getCRS: String = CRS

  def getCellRes: String = cellRes

  def getMeasurements: ArrayBuffer[String] = measurements

  def getHighLevelMeasurement: String = highLevelMeasurement

  def getCityNames: ArrayBuffer[String] = cityNames

  def getCityCodes: ArrayBuffer[String] = cityCodes

  def getInstruments: ArrayBuffer[String] = instruments

  def getDistrictName: String = districtName

  def getProvinceName: String = provinceName

  def getCityName: String = cityName

  def getCubeId: String = cubeId

  def getExtentCoordinates: ArrayBuffer[Double] = extentCoordinates

  def getSubsetQuery: ArrayBuffer[SubsetQuery] = this.subsetQuery

  /**
   * Get cell size and resolution info from SensorLevelAndProduct view, and return a HashMap object.
   *
   * @param cubeId
   * @param connAddr
   * @param user
   * @param password
   * @return a HashMap object.
   */
  def getSizeResAndExtentByCubeId(cubeId: String, connAddr: String, user: String, password: String): mutable.HashMap[(Double, Double), (Double, Double, Double, Double)] = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select distinct cell_size,cell_res,slice_minx,slice_miny,slice_maxx,slice_maxy " +
          "from gc_cube where id = " + cubeId + ";"
        val rs = statement.executeQuery(sql)
        val resHashMap = new mutable.HashMap[(Double, Double), (Double, Double, Double, Double)]
        //add every measurementName
        while (rs.next) {
          resHashMap.put((rs.getString(1).toDouble, rs.getDouble(2)), (rs.getDouble(3), rs.getDouble(4), rs.getDouble(5), rs.getDouble(6)))
        }
        resHashMap
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }
}
