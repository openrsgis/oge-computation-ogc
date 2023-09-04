package whu.edu.cn.geocube.core.entity

import java.sql.{DriverManager, ResultSet}
import geotrellis.layer.LayoutDefinition
import geotrellis.raster.TileLayout
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon}

import scala.collection.mutable.ArrayBuffer
import whu.edu.cn.geocube.core.vector.grid.GridTransformer.getGeomZcodes
import whu.edu.cn.util.GlobalConstantUtil.{POSTGRESQL_PWD, POSTGRESQL_URL, POSTGRESQL_USER}
import whu.edu.cn.util.PostgresqlUtil

import scala.collection.mutable

/**
 * A wrapper of query parameters.
 *
 */
class QueryParams extends Serializable {
  var cubeId:String = ""
  var rasterProductName: String = ""
  var rasterProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var vectorProductName: String = ""
  var vectorProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var tabularProductName: String = ""
  var platform: String = ""
  var instruments: ArrayBuffer[String] = new ArrayBuffer[String]()
  var measurements: ArrayBuffer[String] = new ArrayBuffer[String]()
  var highLevelMeasurement: String = ""
  var CRS: String = ""
  var tileSize: String = ""
  var cellRes: String = ""
  //  var level: String = "4000"
  var level: String = ""
  var phenomenonTime: String = ""
  var startTime: String = ""
  var endTime: String = ""
  var nextStartTime: String = ""
  var nextEndTime: String = ""
  var month: Int = -1
  var year: Int = -1
  var gridCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  var cityCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  var cityNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var cityName: String = ""
  var provinceName: String = ""
  var districtName: String = ""
  var cloudMax: String = ""
  var cloudShadowMax: String = ""
  var polygon: Polygon = null

  def this(platform: String, measurements: ArrayBuffer[String],
           gridCodes: ArrayBuffer[String], cloud: String, level: String) {
    this()
    this.platform = platform
    this.measurements = measurements
    this.gridCodes = gridCodes
    this.cloudMax = cloud
    this.level = level
  }

  def this(rasterProductName: String, platform: String, instruments: ArrayBuffer[String],
           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
           cellRes: String, level: String, phenomenonTime: String,
           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String) {
    this()
    this.rasterProductName = rasterProductName
    this.platform = platform
    this.instruments = instruments
    this.measurements = measurements
    this.CRS = CRS
    this.tileSize = tileSize
    this.cellRes = cellRes
    this.startTime = startTime
    this.endTime = endTime
    this.nextStartTime = nextStartTime
    this.nextEndTime = nextEndTime
    this.phenomenonTime = phenomenonTime
    this.gridCodes = gridCodes
    this.cityCodes = cityCodes
    this.cityNames = cityNames
    this.cloudMax = cloud
    this.cloudShadowMax = cloudShadow
    this.level = level
  }

  def this(rasterProductNames: ArrayBuffer[String], platform: String, instruments: ArrayBuffer[String],
           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
           cellRes: String, level: String, phenomenonTime: String,
           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String) {
    this()
    this.rasterProductNames = rasterProductNames
    this.platform = platform
    this.instruments = instruments
    this.measurements = measurements
    this.CRS = CRS
    this.tileSize = tileSize
    this.cellRes = cellRes
    this.startTime = startTime
    this.endTime = endTime
    this.nextStartTime = nextStartTime
    this.nextEndTime = nextEndTime
    this.phenomenonTime = phenomenonTime
    this.gridCodes = gridCodes
    this.cityCodes = cityCodes
    this.cityNames = cityNames
    this.cloudMax = cloud
    this.cloudShadowMax = cloudShadow
    this.level = level
  }

  /**
   * Set grid codes with input extent.
   *
   * @param leftBottomLong
   * @param LeftBottomLat
   * @param rightUpperLong
   * @param rightUpperLat
   */
  def setExtent(leftBottomLong: Double, LeftBottomLat: Double, rightUpperLong: Double, rightUpperLat: Double): Unit = {
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(leftBottomLong, LeftBottomLat),
      new Coordinate(leftBottomLong, rightUpperLat),
      new Coordinate(rightUpperLong, rightUpperLat),
      new Coordinate(rightUpperLong, LeftBottomLat),
      new Coordinate(leftBottomLong, LeftBottomLat)
    ))
    this.polygon = queryGeometry
    val id = this.getCubeId
    val SRE = getSizeResAndExtentByCubeId(id,POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PWD).toList.head
    //    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    //    val extent = geotrellis.vector.Extent(113, 23, 114, 24)
    val extent = geotrellis.vector.Extent(SRE._2._1, SRE._2._2, SRE._2._3, SRE._2._4)
    //    val tl = TileLayout(360, 180, 1024, 1024) //1024 can be replaced by any value
    println(SRE)
    //    val extent = geotrellis.vector.Extent(113, 23, 114, 24)
    val tl = TileLayout(((SRE._2._3 - SRE._2._1) / SRE._1._1).toInt, ((SRE._2._4 - SRE._2._2) / SRE._1._1).toInt, SRE._1._2.toInt,  SRE._1._2.toInt) //1024 can be replaced by any value
    val ld = LayoutDefinition(extent, tl)
    //    this.setLevel(SRE._1._2.toInt.toString)
    val gridCodeArray = getGeomZcodes(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent, SRE._1._1)
    this.setGridCodes(gridCodeArray)
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
    if (measurements != null) {
      this.measurements = measurements.toBuffer.asInstanceOf[ArrayBuffer[String]]
    }
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
  def getCityName:String = cityName
  def getCubeId:String = cubeId

  /**
   * Get cell size and resolution info from SensorLevelAndProduct view, and return a HashMap object.
   * @param cubeId
   * @param connAddr
   * @param user
   * @param password
   *
   * @return a HashMap object.
   */
  def getSizeResAndExtentByCubeId(cubeId:String,  connAddr: String, user: String, password: String): mutable.HashMap[(Double,Double),(Double,Double,Double,Double)] = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select distinct cell_size,cell_res,slice_minx,slice_miny,slice_maxx,slice_maxy " +
          "from gc_cube where id = " +cubeId+  ";"
        val rs = statement.executeQuery(sql)
        val resHashMap = new mutable.HashMap[(Double,Double),(Double,Double,Double,Double)]
        //add every measurementName
        while (rs.next) {
          //          val res = rs.getString(1).split("/").head
          //          println(res.length)
          //
          //          val res1 = res.substring(1,res.length-1)
          //          res1.foreach(e=>{
          //            print("-----"+e)
          //          })
          //          println(res1)
          //          val small = "0.0025"
          //          println(res1==small)
          //
          //          println(small)
          //          println(small.toDouble)
          resHashMap.put((rs.getString(1).toDouble,rs.getDouble(2)),(rs.getDouble(3),rs.getDouble(4),rs.getDouble(5),rs.getDouble(6)))
        }
        resHashMap
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }
}
