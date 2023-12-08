package whu.edu.cn.oge

import java.io.{StringReader, StringWriter}
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.json.JSONObject
import org.locationtech.jts.geom._

object Geometry {
  //TODO:关于几何对象的初始投影是否需要讨论一下？构建的几何对象默认是WGS84坐标系？
  private val geometryFactory :GeometryFactory= new GeometryFactory()

  private val POINT="Point"
  private val LINESTRING="LineString"
  private val LINEARRING="LinearRing"
  private val POLYGON="Polygon"
  private val MULTI_POINT="MultiPoint"
  private val MULTI_LINESTRING="MultiLineString"
  private val MULTI_POLYGON="MultiPolygon"

  /**
   * construct a point in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def point(coors:String,crs:String="EPSG:4326"):Point={
    val geom=geometryFactory.createPoint(getCoorFromStr(coors).head)
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a LineString in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def lineString(coors:String,crs:String="EPSG:4326"):LineString={
    val geom=geometryFactory.createLineString(getCoorFromStr(coors).toArray)
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a LinearRing in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def linearRing(coors:String,crs:String="EPSG:4326"):LinearRing={
    val geom=geometryFactory.createLinearRing(getCoorFromStr(coors).toArray)
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a polygon in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def polygon(coors:String,crs:String="EPSG:4326"):Polygon={
    val geom=geometryFactory.createPolygon(getCoorFromStr(coors).toArray)
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a MultiPoint in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def multiPoint(coors:String,crs:String="EPSG:4326"):MultiPoint={
    val geom=getGeomFromCoors(coors,MULTI_POINT).asInstanceOf[MultiPoint]
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a MultiLineString in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def multiLineString(coors:String,crs:String="EPSG:4326"):MultiLineString={
    val geom=getGeomFromCoors(coors,MULTI_LINESTRING).asInstanceOf[MultiLineString]
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct a MulitiPolygon in a crs
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param coors the coordinates.Comply with the coordinate format in geojson
   * @param crs the crs of the geometry
   * @return
   */
  def multiPolygon(coors:String,crs:String="EPSG:4326"):MultiPolygon={
    val geom=getGeomFromCoors(coors,MULTI_POLYGON).asInstanceOf[MultiPolygon]
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * construct geometry collection from geomtery list
   *
   * @param geomteries the geometry array to construct geometry collection
   * @return
   */
  def geometryCollection(geomteries:List[Geometry]):GeometryCollection={
    val geom=geometryFactory.createGeometryCollection(geomteries.toArray)
    geom
  }

  /**
   * construct a geometry from geojson
   * if crs is unspecified, the crs will be "EPSG:4326"
   *
   * @param gjson the geojson.
   * @param crs the crs of the geometry
   * @return
   */
  def geometry(gjson:com.alibaba.fastjson.JSONObject, crs:String="EPSG:4326"):Geometry={
    val coors=gjson.getJSONObject("geometry").getJSONArray("coordinates")
    val geomtype=gjson.getJSONObject("geometry").get("type")
    val geom=getGeomFromCoors(coors.toString,geomtype.toString,isGeoJson = false)
    val srid=crs.split(":")(1).toInt
    geom.setSRID(srid)
    geom
  }

  /**
   * get the area of the geometry
   * default to compute the area of geometry in "EPSG:3857"
   *
   * @param geom the geometry to compute area
   * @param crs the projection
   * @return
   */
  def area(geom:Geometry,crs:String="EPSG:3857"):Double={
    val srcCrsCode="EPSG:"+geom.getSRID
    if(srcCrsCode.equals(crs)){
      geom.getArea
    }
    else{
      reproject(geom,crs).getArea
    }
  }

  /**
   * get the length of the geometry
   * default to compute the length of geometry in "EPSG:3857"
   *
   * @param geom the geometry to compute length
   * @param crs the projection
   * @return
   */
  def length(geom:Geometry,crs:String="EPSG:3857"):Double={
    val srcCrsCode="EPSG:"+geom.getSRID
    if(srcCrsCode.equals(crs)){
      geom.getLength
    }
    else{
      reproject(geom,crs).getLength
    }
  }

  /**
   * Returns the bounding rectangle of the geometry.
   *
   * @param geom the geometry to operate
   * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def bounds(geom:Geometry,crs:String="EPSG:4326"):Geometry={
    var resGeom=geom.getEnvelope
    resGeom.setSRID(geom.getSRID)
    reproject(resGeom,crs)
  }

  /**
   * Returns the centroid of geometry
   *
   * @param geom the geometry to operate
   * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def centroid(geom:Geometry,crs:String="EPSG:4326"):Geometry={
    var resGeom=geom.getCentroid
    resGeom.setSRID(geom.getSRID)
    reproject(resGeom,crs)
  }

  /**
   * Returns the convex hull of the given geometry.
   *
   * @param geom the geometry to operate
   * @param crs If specified, the result will be in this projection. Otherwise it will be in WGS84.
   * @return
   */
  def convexHull(geom:Geometry,crs:String="EPSG:4326"):Geometry={
    var resGeom=geom.convexHull()
    resGeom.setSRID(geom.getSRID)
    reproject(resGeom,crs)
  }

  /**
   * Computes the union of all the elements of this geometry.
   * only for GeometryCollection
   *
   * @param geom the geometry to operate
   * @param crs
   * @return
   */
  def dissolve(geom:Geometry,crs:String="EPSG:4326"):Geometry={
    var resGeom=geom.union()
    resGeom.setSRID(geom.getSRID)
    reproject(resGeom,crs)
  }

  /**
   * reproject the geometry to target crs.
   *
   *
   * @param geom the geometry to reproject
   * @param tarCrsCode the target crs
   * @return
   */
  def reproject(geom:Geometry,tarCrsCode:String):Geometry={
    val srcCrsCode="EPSG:"+geom.getSRID
    if(srcCrsCode.equals(tarCrsCode)){
      geom
    }
    else{
      //Note:When processing coordinate transform, geotools defaults to x as latitude and y as longitude.it may cuase false transform
      //CRS.decode(srcCrsCode,true) can solve the problem
      val sourceCrs=CRS.decode(srcCrsCode,true)
      val targetCrs=CRS.decode(tarCrsCode,true)
      val transform=CRS.findMathTransform(sourceCrs,targetCrs)
      val transGeom=JTS.transform(geom,transform)
      val tarSRID=tarCrsCode.split(":")(1).toInt
      transGeom.setSRID(tarSRID)
      transGeom
    }
  }

  /**
   * Returns a GeoJSON string representation of the geometry.
   *
   * @param geom
   * @return
   */
  def toGeoJSONString(geom: Geometry):String={
    var gjsonStr:String=null
    val gjson:GeometryJSON=new GeometryJSON()
    val writer:StringWriter=new StringWriter()
    gjson.write(geom,writer)
    gjsonStr=writer.toString
    writer.close()
    gjsonStr
  }

  def buffer(geom:Geometry,distance:Double,crs:String="EPSG:3857"):Geometry={
    val srcCrsCode="EPSG:"+geom.getSRID
    var bufferGeom:Geometry=null
    if(srcCrsCode.equals(crs)){
      bufferGeom=geom.buffer(distance)
      bufferGeom.setSRID(geom.getSRID)
      bufferGeom
    }
    else{
      bufferGeom=reproject(geom,crs).buffer(distance)
      val tarSRID=crs.split(":")(1).toInt
      bufferGeom.setSRID(tarSRID)
      bufferGeom
    }
  }

  /**
   * Returns true iff one geometry contains the other.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def contains(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).contains(reproject(geom2,crs))
  }

  /**
   * Returns true iff one geometry is contained in the other.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def containedIn(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom2,crs).contains(reproject(geom1,crs))
  }

  /**
   * Returns true iff the geometries are disjoint.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def disjoint(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).disjoint(reproject(geom2,crs))
  }

  /**
   * Returns the minimum distance between two geometries.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
   * @return
   */
  def distance(geom1:Geometry,geom2:Geometry,crs:String="EPSG:3857"):Double={
    reproject(geom1,crs).distance(reproject(geom2,crs))
  }

  /**
   * Returns the result of subtracting the 'right' geometry from the 'left' geometry.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def difference(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Geometry={
    val resGeom=reproject(geom1,crs).difference(reproject(geom2,crs))
    resGeom.setSRID(crs.split(":")(1).toInt)
    resGeom
  }

  /**
   * Returns the intersection of the two geometries.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def intersection(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Geometry={
    val resGeom=reproject(geom1,crs).intersection(reproject(geom2,crs))
    resGeom.setSRID(crs.split(":")(1).toInt)
    resGeom
  }

  /**
   * Returns true iff the geometries intersect.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def intersects(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Boolean={
    reproject(geom1,crs).intersects(reproject(geom2,crs))
  }

  /**
   * Returns the symmetric difference between two geometries.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def symmetricDifference(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Geometry={
    val resGeom=reproject(geom1,crs).symDifference(reproject(geom2,crs))
    resGeom.setSRID(crs.split(":")(1).toInt)
    resGeom
  }

  /**
   * Returns the union of the two geometries.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:4326
   * @return
   */
  def union(geom1:Geometry,geom2:Geometry,crs:String="EPSG:4326"):Geometry={
    val resGeom=reproject(geom1,crs).union(reproject(geom2,crs))
    resGeom.setSRID(crs.split(":")(1).toInt)
    resGeom
  }

  /**
   * Returns true iff the geometries are within a specified distance.
   *
   * @param geom1 left geometry
   * @param geom2 right geometry
   * @param distance The distance threshold.
   * @param crs The projection in which to perform the operation.  If not specified, the operation will be performed in EPSG:3857
   * @return
   */
  def withDistance(geom1:Geometry,geom2:Geometry,distance:Double,crs:String="EPSG:3857"):Boolean={
    reproject(geom1,crs).isWithinDistance(reproject(geom2,crs),distance)
  }



  /**
   *get Coordinate list from string
   *
   * @param coors original string. eg:[[-109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]
   * @return eg:List(-109.05, 41, -109.05, 37, -102.05, 37,-102.05, 41)
   */
  def getCoorFromStr(coors:String):List[Coordinate]={
    val lonlat=coors.replace("[","").replace("]","").split(",")
    var coorList:List[Coordinate]=List.empty
    for(i <- 0 until lonlat.length by 2){
      val coor=new Coordinate(lonlat(i).toDouble,lonlat(i+1).toDouble)
      coorList=coorList:+coor
    }
    coorList
  }

  /**
   * acoording to coordinates and geomType ,build the correct geojson
   * and then, get Geometry from geojson
   *
   * @param coors the coordinate    这里的coors传的是gjson？
   * @param geomType  Geometry Type
   * @param isGeoJson if true ,coors is a geojson.if false coors is a array of coordinates
   *             * @return
   */
  def getGeomFromCoors(coors:String,geomType:String,isGeoJson:Boolean=false):Geometry={
    val jsonStr="{\"geometry\":{\"type\":\""+geomType+"\",\"coordinates\":"+coors+"}}"
    val gjson:GeometryJSON=new GeometryJSON()
    var reader:StringReader=null
    if(!isGeoJson){
      reader=new StringReader(jsonStr)
    }
    else{
      reader=new StringReader(coors)
    }
    var geom:Geometry=null
    try{
      geom=gjson.read(reader)
    }catch{
      case ex:Exception=>{ex.printStackTrace()}
    }
    geom
  }

  def main(args: Array[String]): Unit ={
    //    val coors="[[-109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]"
    var coors="[[[119.283461766823521,35.113845473433457], [119.285033114198498,35.11405167501087]],  [[119.186893667167482,34.88690637041627], [119.186947247282234,34.890273599368562]]]"
    //coors="[35.113845473433457,119.283461766823521]"
    //coors="[[[0,0],[0,20],[20,20],[20,0],[0,0]]]"
    coors="[[[108.09876, 37.200787],[106.398901, 33.648651],[114.972103, 33.340483],[113.715685, 37.845557],[108.09876, 37.200787]]]"
    val geom=polygon(coors)
    val p=polygon("[[[0, 0], [40, 0], [40, 40], [0, 40], [0, 0]]]")
    val rp=reproject(p,"EPSG:3857")
    val jsonStr="{\n\"name\":\"网站\",\n\"num\":3,\n\"sites\":[ \"Google\", \"Runoob\", \"Taobao\" ]\n}"
    val p1=point("[10,10]")
    val coordiantes=p1.getCoordinates
    println("test")

  }

}
