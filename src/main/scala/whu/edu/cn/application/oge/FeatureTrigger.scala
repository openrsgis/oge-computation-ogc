package whu.edu.cn.application.oge

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.application.oge.ImageTrigger.lamda
import whu.edu.cn.jsonparser.JsonToArgLocal

import scala.collection.mutable.Map
import scala.io.Source

object FeatureTrigger {
  var rdd_list:Map[String, Any] = Map.empty[String, Any]
  def argOrNot(args:Map[String, String],name:String):String = {
    if(args.contains(name)){
      args(name)
    }
    else{
      null
    }
  }
  def func(implicit sc: SparkContext, UUID: String, name: String, args: Map[String, String]): Unit ={
    if(name=="Feature.point"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.point(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.point(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.lineString"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.lineString(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.lineString(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.linearRing"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.linearRing(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.linearRing(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.polygon"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.polygon(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.polygon(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.multiPoint"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.multiPoint(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.multiPoint(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.multiLineString"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.multiLineString(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.multiLineString(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.multiPolygon"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.multiPolygon(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.multiPolygon(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.geometry"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.geometry(sc,args("coors"),args("properties"), args("crs")))
      else
        rdd_list+=(UUID->Feature.geometry(sc,args("coors"),args("properties")))
    }
    if(name=="Feature.area"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.area(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.area(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.bounds"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.bounds(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.bounds(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.centroid"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.centroid(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.centroid(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.buffer"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.buffer(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("distance").toDouble, args("crs")))
      else
        rdd_list+=(UUID->Feature.buffer(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("distance").toDouble))
    }
    if(name=="Feature.convexHull"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.convexHull(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.convexHull(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.coordinates"){
      rdd_list+=(UUID->Feature.coordinates(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.reproject"){
      rdd_list+=(UUID->Feature.reproject(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("tarCrsCode")))
    }
    if(name=="Feature.isUnbounded"){
      rdd_list+=(UUID->Feature.isUnbounded(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.getType"){
      rdd_list+=(UUID->Feature.getType(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.projection"){
      rdd_list+=(UUID->Feature.projection(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.toGeoJSONString"){
      rdd_list+=(UUID->Feature.toGeoJSONString(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.getLength"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.getLength(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.getLength(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.geometries"){
      rdd_list+=(UUID->Feature.geometries(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.dissolve"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.dissolve(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("crs")))
      else
        rdd_list+=(UUID->Feature.dissolve(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.contains"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.contains(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.contains(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.containedIn"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.containedIn(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.containedIn(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.disjoint"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.disjoint(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.disjoint(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.distance"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.distance(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.distance(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.difference"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.difference(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.difference(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.intersection"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.intersection(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.intersection(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.intersects"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.intersects(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.intersects(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.symmetricDifference"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.symmetricDifference(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.symmetricDifference(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.union"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.union(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("crs")))
      else
        rdd_list+=(UUID->Feature.union(sc,rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.withDistance"){
      if(argOrNot(args,"crs")!=null)
        rdd_list+=(UUID->Feature.withDistance(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("distance").toDouble,args("crs")))
      else
        rdd_list+=(UUID->Feature.withDistance(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
          rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],args("distance").toDouble))
    }
    if(name=="Feature.copyProperties"){
      val propertyList=args("properties").replace("[","").replace("]","")
        .replace("\"","").split(",").toList
      rdd_list+=(UUID->Feature.copyProperties(rdd_list(args("featureRDD1")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
        rdd_list(args("featureRDD2")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],propertyList))
    }
    if(name=="Feature.get"){
      rdd_list+=(UUID->Feature.get(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("property")))
    }
    if(name=="Feature.getNumber"){
      rdd_list+=(UUID->Feature.getNumber(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("property")))
    }
    if(name=="Feature.getString"){
      rdd_list+=(UUID->Feature.getString(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("property")))
    }
    if(name=="Feature.getArray"){
      rdd_list+=(UUID->Feature.getArray(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("property")))
    }
    if(name=="Feature.propertyNames"){
      rdd_list+=(UUID->Feature.propertyNames(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }
    if(name=="Feature.set"){
      rdd_list+=(UUID->Feature.set(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]], args("property")))
    }
    if(name=="Feature.setGeometry"){
      rdd_list+=(UUID->Feature.setGeometry(rdd_list(args("featureRDD")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]],
        rdd_list(args("geometry")).asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]]))
    }


  }

  def lamda(implicit sc: SparkContext, list: List[Tuple3[String, String, Map[String, String]]]) = {
    for (i <- list.indices) {
      func(sc, list(i)._1, list(i)._2, list(i)._3)
    }
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)
    val line: String = Source.fromFile("src/main/scala/whu/edu/cn/application/oge/testVector.json").mkString
    val jsonObject = JSON.parseObject(line)
    println(jsonObject.size())
    println(jsonObject)

    val a = JsonToArgLocal.trans(jsonObject)
    println(a.size)
    a.foreach(println(_))
    lamda(sc, a)


//    var properties="{\"name\":\"haha\",\"value\":10}"
//    var properties2="{\n\"name\":\"网站\",\n\"num\":3,\n\"sites\":[ \"Google\", \"Runoob\", \"Taobao\" ]\n}"
//    var a: List[Tuple3[String, String, Map[String, String]]] = List.empty[Tuple3[String, String, Map[String, String]]]
//    a=a:+("00000","Feature.point",Map("coors" -> "[5,5]", "properties" -> properties))
//    a=a:+("00001","Feature.lineString",Map("coors" -> "[[20,20],[25,30],[40,37]]", "properties" -> properties2))
//    a=a:+("00002","Feature.linearRing",Map("coors" -> "[[20,20],[30,30],[30,20],[20,20]]", "properties" -> properties))
//    a=a:+("00003","Feature.polygon",Map("coors" -> "[[[0,0],[0,10],[10,10],[10,0],[0,0]]]", "properties" -> properties))
//    a=a:+("00004","Feature.multiPoint",Map("coors" -> "[[105,31],[110,42]]", "properties" -> properties))
//    a=a:+("00005","Feature.multiLineString",Map("coors" -> "[[[20,20],[25,30],[40,37]],[47,56],[73,21]]", "properties" -> properties))
//    a=a:+("00006","Feature.multiPolygon",Map("coors" -> "[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[20,20],[20,30],[30,30],[30,20],[20,20]]]", "properties" -> properties))
//    a=a:+("00007","Feature.lineString",Map("coors" -> "[[0,0],[5,20]]", "properties" -> properties))
//    a=a:+("00008","Feature.polygon",Map("coors" -> "[[[5,0],[5,10],[15,10],[15,0],[5,0]]]", "properties" -> properties))
//    a=a:+("0000","Feature.area",Map("featureRDD" -> "00003"))
//    a=a:+("0001","Feature.area",Map("featureRDD" -> "00003","crs"->"EPSG:4326"))
//    a=a:+("0002","Feature.bounds",Map("featureRDD" -> "00003"))
//    a=a:+("0003","Feature.bounds",Map("featureRDD" -> "00003","crs"->"EPSG:3857"))
//    a=a:+("0004","Feature.centroid",Map("featureRDD" -> "00003"))
//    a=a:+("0005","Feature.centroid",Map("featureRDD" -> "00003","crs"->"EPSG:3857"))
//    a=a:+("0006","Feature.convexHull",Map("featureRDD" -> "00003"))
//    a=a:+("0007","Feature.convexHull",Map("featureRDD" -> "00003","crs"->"EPSG:3857"))
//    a=a:+("0008","Feature.buffer",Map("featureRDD" -> "00003","distance"->"10000"))
//    a=a:+("0009","Feature.buffer",Map("featureRDD" -> "00003","distance"->"5","crs"->"EPSG:4326"))
//    a=a:+("0010","Feature.coordinates",Map("featureRDD" -> "00003"))
//    a=a:+("0011","Feature.reproject",Map("featureRDD" -> "00003","tarCrsCode"->"EPSG:3857"))
//    a=a:+("0012","Feature.isUnbounded",Map("featureRDD" -> "00000"))
//    a=a:+("0013","Feature.isUnbounded",Map("featureRDD" -> "00003"))
//    a=a:+("0014","Feature.getType",Map("featureRDD" -> "00003"))
//    a=a:+("0015","Feature.projection",Map("featureRDD" -> "0002"))
//    a=a:+("0016","Feature.projection",Map("featureRDD" -> "0011"))
//    a=a:+("0017","Feature.toGeoJSONString",Map("featureRDD" -> "00003"))
//    a=a:+("0018","Feature.getLength",Map("featureRDD" -> "00003"))
//    a=a:+("0019","Feature.getLength",Map("featureRDD" -> "00003","crs"->"EPSG:4326"))
//    a=a:+("0020","Feature.contains",Map("featureRDD1" -> "00003","featureRDD2" -> "00000"))
//    a=a:+("0021","Feature.contains",Map("featureRDD1" -> "00003","featureRDD2" -> "00000","crs"->"EPSG:3857"))
//    a=a:+("0022","Feature.containedIn",Map("featureRDD1" -> "00003","featureRDD2" -> "00000"))
//    a=a:+("0023","Feature.containedIn",Map("featureRDD1" -> "00003","featureRDD2" -> "00000","crs"->"EPSG:3857"))
//    a=a:+("0024","Feature.disjoint",Map("featureRDD1" -> "00003","featureRDD2" -> "00000"))
//    a=a:+("0025","Feature.disjoint",Map("featureRDD1" -> "00003","featureRDD2" -> "00000","crs"->"EPSG:3857"))
//    a=a:+("0026","Feature.distance",Map("featureRDD1" -> "00001","featureRDD2" -> "00007"))
//    a=a:+("0027","Feature.distance",Map("featureRDD1" -> "00001","featureRDD2" -> "00007","crs"->"EPSG:4316"))
//    a=a:+("0028","Feature.difference",Map("featureRDD1" -> "00003","featureRDD2" -> "00008"))
//    a=a:+("0029","Feature.difference",Map("featureRDD1" -> "00003","featureRDD2" -> "00008","crs"->"EPSG:3857"))
//    a=a:+("0030","Feature.intersection",Map("featureRDD1" -> "00003","featureRDD2" -> "00008"))
//    a=a:+("0031","Feature.intersection",Map("featureRDD1" -> "00003","featureRDD2" -> "00008","crs"->"EPSG:3857"))
//    a=a:+("0032","Feature.intersects",Map("featureRDD1" -> "00003","featureRDD2" -> "00008"))
//    a=a:+("0033","Feature.intersects",Map("featureRDD1" -> "00003","featureRDD2" -> "00008","crs"->"EPSG:3857"))
//    a=a:+("0034","Feature.symmetricDifference",Map("featureRDD1" -> "00003","featureRDD2" -> "00008"))
//    a=a:+("0035","Feature.symmetricDifference",Map("featureRDD1" -> "00003","featureRDD2" -> "00008","crs"->"EPSG:3857"))
//    a=a:+("0036","Feature.union",Map("featureRDD1" -> "00003","featureRDD2" -> "00008"))
//    a=a:+("0037","Feature.union",Map("featureRDD1" -> "00003","featureRDD2" -> "00008","crs"->"EPSG:3857"))
//    a=a:+("0038","Feature.withDistance",Map("featureRDD1" -> "00001","featureRDD2" -> "00007","distance"->"10000"))
//    a=a:+("0039","Feature.withDistance",Map("featureRDD1" -> "00001","featureRDD2" -> "00007","distance"->"16","crs"->"EPSG:4326"))
//    a=a:+("0040","Feature.copyProperties",Map("featureRDD1" -> "00000","featureRDD2" -> "00001","properties"->"[\"name\",\"num\"]"))
//    a=a:+("0041","Feature.get",Map("featureRDD" -> "0040","property"->"name"))
//    a=a:+("0042","Feature.getNumber",Map("featureRDD" -> "0040","property"->"num"))
//    a=a:+("0043","Feature.getString",Map("featureRDD" -> "0040","property"->"name"))
//    a=a:+("0044","Feature.getArray",Map("featureRDD" -> "00001","property"->"sites"))
//    a=a:+("0045","Feature.set",Map("featureRDD" -> "00001","property"->"{\"name\":\"haha\",\"value\":10}"))
//    a=a:+("0046","Feature.setGeometry",Map("featureRDD" -> "00001","geometry"->"00000"))
//
//    lamda(sc,a)
//    println(rdd_list("0000").asInstanceOf[List[Double]])
//    println(rdd_list("0001").asInstanceOf[List[Double]])
//    println(rdd_list("0002").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0003").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0004").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0005").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0006").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0007").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0008").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0009").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0010").asInstanceOf[List[Array[Coordinate]]])
//    println(rdd_list("0011").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0012").asInstanceOf[List[Boolean]])
//    println(rdd_list("0013").asInstanceOf[List[Boolean]])
//    println(rdd_list("0014").asInstanceOf[List[String]])
//    println(rdd_list("0015").asInstanceOf[List[String]])
//    println(rdd_list("0016").asInstanceOf[List[String]])
//    println(rdd_list("0017").asInstanceOf[List[String]])
//    println(rdd_list("0018").asInstanceOf[List[Double]])
//    println(rdd_list("0019").asInstanceOf[List[Double]])
//    println(rdd_list("0020").asInstanceOf[Boolean])
//    println(rdd_list("0021").asInstanceOf[Boolean])
//    println(rdd_list("0022").asInstanceOf[Boolean])
//    println(rdd_list("0023").asInstanceOf[Boolean])
//    println(rdd_list("0024").asInstanceOf[Boolean])
//    println(rdd_list("0025").asInstanceOf[Boolean])
//    println(rdd_list("0026").asInstanceOf[Double])
//    println(rdd_list("0027").asInstanceOf[Double])
//    println(rdd_list("0028").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0029").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0030").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0031").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0032").asInstanceOf[Boolean])
//    println(rdd_list("0033").asInstanceOf[Boolean])
//    println(rdd_list("0034").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0035").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0036").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0037").asInstanceOf[RDD[(String,(Geometry, Map[String, Any]))]].first()._2._1.toText)
//    println(rdd_list("0038").asInstanceOf[Boolean])
//    println(rdd_list("0039").asInstanceOf[Boolean])
//    println(rdd_list("0041").asInstanceOf[List[Any]])
//    println(rdd_list("0042").asInstanceOf[List[Double]])
//    println(rdd_list("0043").asInstanceOf[List[String]])
//    println(rdd_list("0044").asInstanceOf[List[Array[String]]])

  }

}
