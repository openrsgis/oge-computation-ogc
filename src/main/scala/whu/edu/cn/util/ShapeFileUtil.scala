package whu.edu.cn.util

import com.alibaba.fastjson.JSON.parseObject
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.{FeatureWriter, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import whu.edu.cn.util.WKTUtil.{jsonToWkt, wktToGeom}
import java.io._
import java.nio.charset.Charset
import java.util
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Objects, UUID}

import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.JavaConverters._

object ShapeFileUtil {
  val DEF_GEOM_KEY = "the_geom"
  val DEF_ENCODE = "uft-8"

  /**
   * 图形信息写入shp文件。shape文件中的geometry附带属性类型仅支持String（最大255）、Integer、Double、Boolean、Date(只包含日期，不包含时间)；
   * 附带属性的name仅支持15字符，多余的自动截取。
   *
   * @param shpPath  shape文件路径，包括shp文件名称 如：D:\data\tmp\test.shp
   * @param geomType 图形信息类型 Geometry类型，如Point.class、Polygon.class等
   * @param data     图形信息集合
   */
  def createShp(shpPath: String, geomType: Class[_], data: util.List[util.Map[String, Any]]): Unit = {
    try createShp(shpPath, DEF_ENCODE, geomType, data)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 图形信息写入shp文件。shape文件中的geometry附带属性类型仅支持String（最大255）、Integer、Double、Boolean、Date(只包含日期，不包含时间)；
   * 附带属性的name仅支持15字符，多余的自动截取。
   *
   * @param shpPath  shape文件路径，包括shp文件名称 如：D:\data\tmp\test.shp
   * @param encode   shp文件编码
   * @param geomType 图形信息类型 Geometry类型，如Point.class、Polygon.class等
   * @param data     图形信息集合
   */
  def createShp(shpPath: String, encode: String, geomType: Class[_], data: util.List[util.Map[String, Any]]): Unit = {
    try {
      if (StringUtils.isEmpty(shpPath)) throw new Exception("shp文件的路径不能为空，shpPath: " + shpPath)
      if (StringUtils.isEmpty(encode)) throw new Exception("shp文件的编码不能为空，encode: " + encode)
      if (Objects.isNull(geomType)) throw new Exception("shp文件的图形类型不能为空，geomType: " + geomType)
      if (CollectionUtils.isEmpty(data)) throw new Exception("shp文件的图形数据不能为空，data: " + data)
      if (!data.get(0).containsKey(DEF_GEOM_KEY)) throw new Exception("shp文件的图形数据中必须包含the_geom的属性，data: " + data)
      //创建shape文件对象+
      val file = new File(shpPath)
      val params = new util.HashMap[String, Serializable]
      val geom:Geometry = data.get(0).get("the_geom").asInstanceOf[Geometry]
      val srid :Int = geom.getSRID()
      val cood = "EPSG:" + srid.toString
      val crs: CoordinateReferenceSystem = CRS.decode(cood)
      params.put(ShapefileDataStoreFactory.URLP.key, file.toURI.toURL)
      val ds = new ShapefileDataStoreFactory().createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      //定义图形信息和属性信息
      ds.createSchema(builderFeatureType(geomType, if (CollectionUtils.isEmpty(data)) null
      else data.get(0)))
      //设置编码
      val charset = Charset.forName(encode)
      ds.setCharset(charset)
      ds.forceSchemaCRS(crs)
      //设置Writer
      val writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)
      import scala.collection.JavaConversions._
      for (map <- data) { //写下一条
        val feature = writer.next
        import scala.collection.JavaConversions._
        for (key <- map.keySet) {
          if (DEF_GEOM_KEY == key) feature.setAttribute(key, map.get(key))
          else feature.setAttribute(key.toUpperCase, map.get(key))
        }
      }
      writer.write()
      writer.close()
      ds.dispose()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 构建Feature模板
   *
   * @param geomType 图形信息类型 Geometry类型，如Point.class、Polygon.class等
   * @param data     图形信息具体的属性
   * @return featureType
   */
  def builderFeatureType(geomType: Class[_], data: util.Map[String, _]): SimpleFeatureType = {
    val ftb = new SimpleFeatureTypeBuilder
    ftb.setCRS(DefaultGeographicCRS.WGS84)
    ftb.setName("shapefile")
    ftb.add(DEF_GEOM_KEY, geomType)
    if (MapUtils.isNotEmpty(data)) {
      import scala.collection.JavaConversions._
      for (key <- data.keySet) {
        if (Objects.nonNull(data.get(key))) ftb.add(key.toUpperCase, data.get(key).getClass)
      }
    }
    ftb.buildFeatureType
  }

  /**
   * 压缩shape文件
   *
   * @param shpPath shape文件路径（包含shape文件名称）
   */
  def zipShapeFile(shpPath: String): Unit = {
    try {
      val shpFile = new File(shpPath)
      val shpRoot = shpFile.getParentFile.getPath
      val shpName = shpFile.getName.substring(0, shpFile.getName.lastIndexOf("."))
      val zipPath = shpRoot + File.separator + shpName + ".zip"
      val zipFile = new File(zipPath)
      var input: FileInputStream = null
      val zipOut = new ZipOutputStream(new FileOutputStream(zipFile))
      // zip的名称为
      zipOut.setComment(shpName)
      val shpFiles = Array[String](shpRoot + File.separator + shpName + ".dbf", shpRoot + File.separator + shpName + ".prj", shpRoot + File.separator + shpName + ".shp", shpRoot + File.separator + shpName + ".shx", shpRoot + File.separator + shpName + ".fix")
      for (i <- shpFiles.indices) {
        val file = new File(shpFiles(i))
        input = new FileInputStream(file)
        zipOut.putNextEntry(new ZipEntry(file.getName))
        var temp = 0
        while ( {
          temp = input.read;
          temp != -1
        }) zipOut.write(temp)
        input.close()
      }
      zipOut.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def readShp(implicit sc: SparkContext, shpPath: String, encode: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    val shapeFile = new File(shpPath)
    val store = new ShapefileDataStore(shapeFile.toURI.toURL)
    //设置编码
    val charset = Charset.forName(encode)
    store.setCharset(charset)
    val sfSource = store.getFeatureSource
    val coor = sfSource.getSchema.getCoordinateReferenceSystem
    val crs: String = CRS.lookupIdentifier(coor, false)
    val srid=crs.split(":")(1).toInt
    val sfIter = sfSource.getFeatures.features
    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
    val preRdd: ArrayBuffer[(String, (Geometry, Map[String, Any]))] = ArrayBuffer.empty[(String, (Geometry, Map[String, Any]))]
    while ( {
      sfIter.hasNext
    }) {
      val feature = sfIter.next
      // Feature转GeoJSON
      val fJson = new FeatureJSON
      val writer = new StringWriter()
      fJson.writeFeature(feature, writer)
      val sJson = parseObject(writer.toString)
      val wkt = jsonToWkt(sJson)
      val geom = wktToGeom(wkt)
      geom.setSRID(srid)
      val properties = sJson.getJSONObject("properties")
      val props = mutable.Map.empty[String, Any]
      val keys = properties.keySet().asScala.toSet
      for (key <- keys) {
        val element = properties.getString(key)
        props += (key -> element)
      }
      preRdd += Tuple2(UUID.randomUUID().toString, Tuple2(geom, props))
    }
    sc.makeRDD(preRdd)
  }
}
