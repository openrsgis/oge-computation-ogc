package whu.edu.cn.util

import com.alibaba.fastjson.JSON.parseObject
import com.alibaba.fastjson.JSONObject
import geotrellis.vector.GeometryCollection
import org.geotools.geojson.GeoJSONUtil
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.{ParseException, WKTReader, WKTWriter}

import java.io.{IOException, StringWriter}

/**
 * wktutil
 *
 * @author tree
 * @date 2022/01/21
 */
object WKTUtil {
  private val reader = new WKTReader
  private val GEO_JSON_TYPE = "GeometryCollection"
  private val WKT_TYPE = "GEOMETRYCOLLECTION"

  def geomToWkt(geometry: Geometry): String = {
    val writer = new WKTWriter
    writer.write(geometry)
  }

  @throws[ParseException]
  def wktToGeom(wkt: String): Geometry = {
    var geometry: Geometry = null
    val reader = new WKTReader
    geometry = reader.read(wkt)
    geometry
  }

  def main(args: Array[String]): Unit = {
    val wkt = "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))"
    val wkt0 = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2))"
    val jsonObject = wktToJson(wkt)
    println(jsonObject)
    val s = jsonToWkt(jsonObject)
    println("s = " + s)
  }

  /**
   * wkt转Json
   *
   * @param wkt
   * @return
   */
  def wktToJson(wkt: String): JSONObject = {
    var json: String = null
    var jsonObject = new JSONObject
    try {
      val geometry = reader.read(wkt)
      val writer = new StringWriter
      val geometryJSON = new GeometryJSON
      geometryJSON.write(geometry, writer)
      json = writer.toString
      jsonObject = parseObject(json)
    } catch {
      case e: Exception =>
        System.out.println("WKT转GeoJson出现异常")
        e.printStackTrace()
    }
    jsonObject
  }

  /**
   * geoJson转wkt
   *
   * @param jsonObject
   * @return
   */
  def jsonToWkt(jsonObject: JSONObject): String = {
    var wkt: String = null
    val `type` = jsonObject.getString("type")
    val gJson = new GeometryJSON
    try // {"geometries":[{"coordinates":[4,6],"type":"Point"},{"coordinates":[[4,6],[7,10]],"type":"LineString"}],"type":"GeometryCollection"}
      if (GEO_JSON_TYPE == `type`) { // 由于解析上面的json语句会出现这个geometries属性没有采用以下办法
        val geometriesArray = jsonObject.getJSONArray("geometries")
        // 定义一个数组装图形对象
        val size = geometriesArray.size
        val geometries = new Array[Geometry](size)
        for (i <- 0 until size) {
          val str = geometriesArray.get(i).toString
          // 使用GeoUtil去读取str
          val reader = GeoJSONUtil.toReader(str)
          val geometry = gJson.read(reader)
          geometries(i) = geometry
        }
        val geometryCollection = new GeometryCollection(geometries, new GeometryFactory())
        wkt = geometryCollection.toText
      }
      else {
        val reader = GeoJSONUtil.toReader(jsonObject.toString)
        val read = gJson.read(reader)
        wkt = read.toText
      }
    catch {
      case e: IOException =>
        System.out.println("GeoJson转WKT出现异常")
        e.printStackTrace()
    }
    wkt
  }

}
