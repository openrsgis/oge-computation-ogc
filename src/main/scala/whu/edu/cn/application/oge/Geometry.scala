package whu.edu.cn.application.oge

import java.io.StringReader

import org.geotools.geojson.geom.GeometryJSON
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
    * construct a point
    *
    * @param coors the coordinate to construct a point
    * @return
    */
  def point(coors:String):Point={
    geometryFactory.createPoint(getCoorFromStr(coors).head)
  }

  /**
    * construct a LineString
    *
    * @param coors the coordinates to construct a lineString
    * @return
    */
  def lineString(coors:String):LineString={
    geometryFactory.createLineString(getCoorFromStr(coors).toArray)
  }

  /**
    * construct a LinearRing
    *
    * @param coors the coordinates to construct a linearRing
    * @return
    */
  def linearRing(coors:String):LinearRing={
    geometryFactory.createLinearRing(getCoorFromStr(coors).toArray)
  }

  /**
    * construct a Polygon
    *
    * @param coors the coordinates to construct a polygon
    * @return
    */
  def polygon(coors:String):Polygon={
    geometryFactory.createPolygon(getCoorFromStr(coors).toArray)
  }

  /**
    * construct MultiPoint
    *
    * @param coors the coordinates to construct multiPoint
    * @return
    */
  def multiPoint(coors:String):MultiPoint={
    getGeomFromCoors(coors,MULTI_POINT).asInstanceOf[MultiPoint]
  }

  /**
    * construct MultiLineSting
    *
    * @param coors the coordinates to construct multiLineString
    * @return
    */
  def multiLineString(coors:String):MultiLineString={
    getGeomFromCoors(coors,MULTI_LINESTRING).asInstanceOf[MultiLineString]
  }

  /**
    * construct MultiPolygon
    *
    * @param coors the coordinates to construct multiPolygon
    * @return
    */
  def multiPolygon(coors:String):MultiPolygon={
    getGeomFromCoors(coors,MULTI_POLYGON).asInstanceOf[MultiPolygon]
  }

  /**
    * construct a geometry from geojson
    *
    * @param gjson the geojson to construct geometry
    * @return
    */
  def geometry(gjson:String):Geometry={
    getGeomFromCoors(gjson,"")
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
    * get Geometry from geojson
    *
    * @param coors the coordinate
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

//  def main(args: Array[String]): Unit ={
////    val coors="[[-109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]"
//    val coors="[[[119.283461766823521,35.113845473433457], [119.285033114198498,35.11405167501087]],  [[119.186893667167482,34.88690637041627], [119.186947247282234,34.890273599368562]]]"
//    println(multiLineString(coors).getLength)
//  }

}
