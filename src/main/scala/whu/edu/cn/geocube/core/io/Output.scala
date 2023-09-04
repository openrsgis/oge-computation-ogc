package whu.edu.cn.geocube.core.io

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter, Serializable}
import java.nio.charset.Charset
import java.util
import java.util.UUID

import org.geotools.data.Transaction
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import whu.edu.cn.geocube.core.cube.vector.GeoObject

import scala.collection.mutable.ArrayBuffer

/**
 * Only for development.
 * */
@deprecated(message = "Only for development!")
object Output{
  /**
   * Write an array of simple features to geojson
   *
   * @param features An array of simple features
   * @param outputPath geojson outputPath
   *
   * @return
   */
  def saveAsGeojson(features: Array[SimpleFeature], outputPath: String):Unit = {
    if(features.length <= 0) return
    val fjson = new FeatureJSON()
    val simpleFeatureType: SimpleFeatureType = features(0).getFeatureType
    val simpleFeatureList: util.ArrayList[SimpleFeature] = new util.ArrayList[SimpleFeature]()
    for(i <- features)
      simpleFeatureList.add(i)
    val featureCollection:SimpleFeatureCollection = new ListFeatureCollection(simpleFeatureType, simpleFeatureList)
    val fileOutputStream = new FileOutputStream(new File(outputPath))
    fjson.writeFeatureCollection(featureCollection, fileOutputStream)
    fileOutputStream.close()
  }

  /**
   * Write an array of simple features to esri shapefile
   *
   * @param features An array of simple features
   * @param outputPath shapefile outputPath
   *
   * @return
   */
  def saveAsShapefile(features: Array[SimpleFeature], outputPath: String):Unit = {
    if(features.length <= 0) return
    val simpleFeatureList: util.ArrayList[SimpleFeature] = new util.ArrayList[SimpleFeature]()
    for(i <- features)
      simpleFeatureList.add(i)

    val params = new util.HashMap[String, Serializable]()
    params.put(ShapefileDataStoreFactory.URLP.key, new File(outputPath).toURI().toURL())

    val factory = new ShapefileDataStoreFactory()
    val ds = factory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
    ds.createSchema(SimpleFeatureTypeBuilder.retype(simpleFeatureList.get(0).getFeatureType, DefaultGeographicCRS.WGS84))
    ds.setCharset(Charset.forName("UTF-8"))
    val it = simpleFeatureList.iterator()
    val writer = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)

    while(it.hasNext){
      val f = it.next()
      val fNew = writer.next()
      fNew.setAttribute("the_geom", f.getDefaultGeometry.asInstanceOf[Geometry])
      val fProperties = f.getProperties.iterator()
      val fNewProperties = fNew.getProperties.iterator()
      fNewProperties.next()
      while(fProperties.hasNext && fNewProperties.hasNext){
        fNewProperties.next()
        val property = fProperties.next()
        fNew.setAttribute(property.getName, property.getValue)
      }
      //println(f.toString)
      //println(fNew.toString)
      writer.write()
    }
    writer.close()
    ds.dispose()
  }

  /**
   * 转换vector为tabular，去掉经纬度信息
   */
  def vectorShp2TabularTxt(inPath: String, outPath: String): Unit = {
    val outfile = new File(outPath)
    val bw = new BufferedWriter(new FileWriter(outfile))
    bw.write("longitude latitude geo_name time population households male female geo_address")
    bw.newLine()

    val dataStoreFactory = new ShapefileDataStoreFactory()
    val sds = dataStoreFactory.createDataStore(new File(inPath).toURI.toURL)
      .asInstanceOf[ShapefileDataStore]
    sds.setCharset(Charset.forName("GBK"))
    val featureSource = sds.getFeatureSource()

    val iterator: SimpleFeatureIterator = featureSource.getFeatures().features()
    while (iterator.hasNext) {
      val feature = iterator.next()
      val longitude = feature.getAttribute("lon")
      val latitude = feature.getAttribute("lat")
      val geoName = feature.getAttribute("geo_name")
      val time = "2016-07-02"
      val population = feature.getAttribute("Population")
      val households = feature.getAttribute("Households")
      val male = feature.getAttribute("Male")
      val female = feature.getAttribute("Female")
      val geoAdrr = feature.getAttribute("geo_addr")

      bw.write(longitude + " " + latitude + " " + geoName + " " + time + " " + population + " " + households + " " + male + " " + female + " " + geoAdrr)
      bw.newLine()
    }
    iterator.close()
    sds.dispose()
    bw.close()
  }

  def main(args: Array[String]): Unit = {
    /*vectorShp2TabularTxt("E:\\VectorData\\Hainan_Daguangba\\sourceVillage\\mz_Village.shp",
      "E:\\TabularData\\Hainan_Daguangba\\mz_Village.txt")*/
    val dataStoreFactory = new ShapefileDataStoreFactory()
    val sds = dataStoreFactory.createDataStore(new File("E:\\VectorData\\Administry District\\gadm36_CHN_shp\\gadm36_CHN_1.shp").toURI.toURL)
      .asInstanceOf[ShapefileDataStore]
    sds.setCharset(Charset.forName("GBK"))
    val featureSource = sds.getFeatureSource()
    val iterator: SimpleFeatureIterator = featureSource.getFeatures().features()
    var count = 0
    val featureArray = new ArrayBuffer[SimpleFeature]()
    while (iterator.hasNext) {
      count += 1
      val feature = iterator.next()
      /*if (feature.getAttribute("NAME_2").equals("Wuhan") ||feature.getAttribute("NAME_2").equals("Chongqing") ||feature.getAttribute("NAME_2").equals("Hangzhou"))
        featureArray.append(feature)*/
      if (feature.getAttribute("NAME_1").equals("Hubei"))
        featureArray.append(feature)

    }
    iterator.close()
    sds.dispose()
    saveAsShapefile(featureArray.toArray, "E:\\VectorData\\Administry District\\gadm36_CHN_shp\\test1.shp")

  }
}
