package whu.edu.cn.geocube.core.cube.vector

import java.io._
import java.nio.charset.Charset
import org.geotools.data.shapefile._
import org.geotools.data.simple._
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature}
import scala.beans.BeanProperty

/**
 * A vector wrapper which represents vector data
 * by org.opengis.feature.simple.SimpleFeature and UUID
 *
 */
class GeoObject(_id: String,
                _feature: SimpleFeature) extends Serializable {
  @BeanProperty
  var id: String = _id
  @BeanProperty
  var feature: SimpleFeature = _feature
}

object GeoObject{
  /**
   * Transform shapefile to fjson file.
   *
   * @param inputPath shapefile path
   * @param outputPath fjson file path
   */
  def shpToFeatureJson(inputPath: String, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    val dataStoreFactory = new ShapefileDataStoreFactory()
    try{
      val sds = dataStoreFactory.createDataStore(new File(inputPath).toURI.toURL)
        .asInstanceOf[ShapefileDataStore]
      sds.setCharset(Charset.forName("GBK"))
      val featureSource = sds.getFeatureSource()
      val iterator:SimpleFeatureIterator = featureSource.getFeatures().features()
      var count = 0
      while(iterator.hasNext && count < 6000000){
        val feature = iterator.next()
        val fj = new FeatureJSON
        val featureJson = fj.toString(feature)
        writer.println(featureJson)
        count += 1
      }
      iterator.close()
      sds.dispose()
    } catch {
      case ex: Exception =>{
        println(ex.printStackTrace())
      }
    }
    writer.close()
  }

  /**
   * Transform shapefile to WKT file.
   *
   * @param inputPath
   * @param outputPath
   */
  def shpToWkt(inputPath: String, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    val dataStoreFactory = new ShapefileDataStoreFactory()
    try{
      val sds = dataStoreFactory.createDataStore(new File(inputPath).toURI.toURL)
        .asInstanceOf[ShapefileDataStore]
      sds.setCharset(Charset.forName("GBK"))
      val featureSource = sds.getFeatureSource()
      val iterator:SimpleFeatureIterator = featureSource.getFeatures().features()
      //var count = 0
      while(iterator.hasNext /*&& count < 10000000*/){
        val feature = iterator.next()
        val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val geomWKT = geom.toText
        writer.println(geomWKT)
        //count += 1
      }
      iterator.close()
      sds.dispose()
    } catch {
      case ex: Exception =>{
        println(ex.printStackTrace())
      }
    }
    writer.close()
  }
}
