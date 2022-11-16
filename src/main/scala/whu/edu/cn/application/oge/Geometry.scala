package whu.edu.cn.application.oge

import java.io.StringReader

import org.geotools.geojson.geom.GeometryJSON
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
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
    * construct a geometry from geojson
    * if crs is unspecified, the crs will be "EPSG:4326"
    *
    * @param gjson the geojson.
    * @param crs the crs of the geometry
    * @return
    */
  def geometry(gjson:String,crs:String="EPSG:4326"):Geometry={
    val geom=getGeomFromCoors(gjson,"")
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
      JTS.transform(geom,transform)
    }
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

  def main(args: Array[String]): Unit ={
//    val coors="[[-109.05, 41], [-109.05, 37], [-102.05, 37], [-102.05, 41]]"
    var coors="[[[119.283461766823521,35.113845473433457], [119.285033114198498,35.11405167501087]],  [[119.186893667167482,34.88690637041627], [119.186947247282234,34.890273599368562]]]"
    //coors="[35.113845473433457,119.283461766823521]"
    val geom=multiLineString(coors)

    println(geom)
    println(reproject(geom,"EPSG:3857"))
  }

}
