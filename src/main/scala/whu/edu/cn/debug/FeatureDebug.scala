package whu.edu.cn.debug

import com.alibaba.fastjson.JSON.parseObject
import com.alibaba.fastjson.JSONObject
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.raster.Raster
import geotrellis.raster.render.ColorRamps
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.data.store.ContentFeatureSource
import org.geotools.data.{FeatureWriter, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.{Geometry, LineString}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.WKTUtil.{jsonToWkt, wktToGeom}

import java.io._
import java.nio.charset.Charset
import java.util
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Objects, UUID}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FeatureDebug {
  val DEF_GEOM_KEY = "the_geom"
  val DEF_ENCODE = "uft-8"

  def main(args: Array[String]): Unit = {

    //启动spark并读取测试数据
    val t1 = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    val points = Feature.load(sc, "China_MainRoad_Vector")
    println("Finish")
//    val maskGeom = Feature.load(sc, "China_ADM_Country_Vector")
//    val t2 = System.currentTimeMillis()
//    println("启动spark时间:" + (t2 - t1) / 1000)
//
//
//
//    //原始数据矢量转栅格
//    saveFeatureRDDToShp(points, "D:/cog/out/points.shp")
//    val pointsToRaster = Feature.rasterize(points, "PM2.5")
//    CoverageDubug.makeTIFF(pointsToRaster, "pointsToRaster")
//
//    //反距离加权插值
//    val idw = Feature.inverseDistanceWeighted(sc, points, "PM2.5", maskGeom)
//    //结果生成 tiff 和 png
//    CoverageDubug.makeTIFF(idw, "idw")
//    CoverageDubug.makePNG(idw, "idw")
//
//
//    //TODO 简单克里金插值(奇异矩阵报错)
//    val skg = Feature.simpleKriging(sc, points, "PM2.5","模型未知")
//    CoverageDubug.makeTIFF(skg, "skg")
//    CoverageDubug.makePNG(skg, "skg")


    //    val t1 = System.currentTimeMillis()
    //    val conf = new SparkConf()
    //      .setMaster("local[*]")
    //      .setAppName("query")
    //    val sc = new SparkContext(conf)
    //    val maskGeom = load(sc, "China_ADM_Country_Vector")
    //    val t2 = System.currentTimeMillis()
    //    println("启动spark、查元数据的时间:" + (t2 - t1) / 1000)
    //    val mask = maskGeom.first()
    //    val t3 = System.currentTimeMillis()
    //    println("从hbase中取数据的时间:" + (t3 - t2) / 1000)
  }



  def saveFeatureRDDToShp(input: RDD[(String, (Geometry, mutable.Map[String, Any]))], outputShpPath: String): Unit = {
    val data: util.List[util.Map[String, Any]] = input.map(t => {
      t._2._2 + (DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava
    createShp(outputShpPath, "utf-8", classOf[LineString], data)
    println("成功落地shp")
  }

  def makeFeatureRDDFromShp(sc: SparkContext, sourceShpPath: String): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = readShp(sc, sourceShpPath, "utf-8")
    println("成功读取shp")
    featureRDD
  }


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
      params.put(ShapefileDataStoreFactory.URLP.key, file.toURI.toURL)
      val ds: ShapefileDataStore = new ShapefileDataStoreFactory().createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      //定义图形信息和属性信息
      ds.createSchema(builderFeatureType(geomType, if (CollectionUtils.isEmpty(data)) null
      else data.get(0)))
      //设置编码
      val charset: Charset = Charset.forName(encode)
      ds.setCharset(charset)
      //设置Writer
      val writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)

      for (map <- data) { //写下一条
        val feature: SimpleFeature = writer.next
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
      val shpRoot: String = shpFile.getParentFile.getPath
      val shpName: String = shpFile.getName.substring(0, shpFile.getName.lastIndexOf("."))
      val zipPath: String = shpRoot + File.separator + shpName + ".zip"
      val zipFile = new File(zipPath)
      var input: FileInputStream = null
      val zipOut = new ZipOutputStream(new FileOutputStream(zipFile))
      // zip的名称为
      zipOut.setComment(shpName)
      val shpFiles: Array[String] = Array[String](shpRoot + File.separator + shpName + ".dbf", shpRoot + File.separator + shpName + ".prj", shpRoot + File.separator + shpName + ".shp", shpRoot + File.separator + shpName + ".shx", shpRoot + File.separator + shpName + ".fix")
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


  def readShp(implicit sc: SparkContext, shpPath: String, encode: String): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val shapeFile = new File(shpPath)
    val store = new ShapefileDataStore(shapeFile.toURI.toURL)
    //设置编码
    val charset: Charset = Charset.forName(encode)
    store.setCharset(charset)
    val sfSource: ContentFeatureSource = store.getFeatureSource
    val sfIter: SimpleFeatureIterator = sfSource.getFeatures.features
    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
    val preRdd: ArrayBuffer[(String, (Geometry, mutable.Map[String, Any]))] = ArrayBuffer.empty[(String, (Geometry, mutable.Map[String, Any]))]
    while ( {
      sfIter.hasNext
    }) {
      val feature: SimpleFeature = sfIter.next
      // Feature转GeoJSON
      val fJson = new FeatureJSON
      val writer = new StringWriter()
      fJson.writeFeature(feature, writer)
      val sJson: JSONObject = parseObject(writer.toString)
      val wkt: String = jsonToWkt(sJson)
      val geom: Geometry = wktToGeom(wkt)
      val properties: JSONObject = sJson.getJSONObject("properties")
      val props = mutable.Map.empty[String, Any]
      val keys: Set[String] = properties.keySet().asScala.toSet
      for (key <- keys) {
        val element: String = properties.getString(key)
        props += (key -> element)
      }
      preRdd += Tuple2(UUID.randomUUID().toString, Tuple2(geom, props))
    }
    sc.makeRDD(preRdd)
  }
}
