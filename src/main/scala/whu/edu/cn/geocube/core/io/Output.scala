package whu.edu.cn.geocube.core.io

import java.io.{File, FileOutputStream, Serializable}
import java.nio.charset.Charset
import java.util

import org.geotools.data.Transaction
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
}
