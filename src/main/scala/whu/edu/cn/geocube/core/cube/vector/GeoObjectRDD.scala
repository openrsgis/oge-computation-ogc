package whu.edu.cn.geocube.core.cube.vector

import java.io.{File, FileInputStream}
import java.nio.charset.Charset
import java.util.UUID

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn


/**
 * RDD class which represents a series of GeoObjects.
 *
 */
class GeoObjectRDD  (val rddPrev: RDD[GeoObject])extends RDD[GeoObject](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[GeoObject] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)
}

object GeoObjectRDD{
  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  /**
   * Generate a GeoObjectRDD using shapefile on local file system.
   *
   * @param sc A Spark Context
   * @param filePath Shapefile path
   * @param numPartition Num of RDD partitions
   *
   * @return A GeoObjectRDD[GeoObject]
   */
  def createGeoObjectRDDFromShp(implicit sc: SparkContext,
                                filePath: String,
                                numPartition: Int): GeoObjectRDD = {
    val geomArray: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
    val dataStoreFactory = new ShapefileDataStoreFactory()
    val sds = dataStoreFactory.createDataStore(new File(filePath).toURI.toURL)
      .asInstanceOf[ShapefileDataStore]
    sds.setCharset(Charset.forName("GBK"))
    val featureSource = sds.getFeatureSource()

    val iterator: SimpleFeatureIterator = featureSource.getFeatures().features()
    while (iterator.hasNext) {
      val feature = iterator.next()
      val uuid = UUID.randomUUID().toString
      val geoObject = new GeoObject(uuid, feature)
      geomArray += geoObject
    }
    iterator.close()
    sds.dispose()
    val rddShpData = sc.parallelize(geomArray, numPartition)
    new GeoObjectRDD(rddShpData)
  }

  /**
   * Generate a GeoObjectRDD using FeatureJson on local file system.
   *
   * @param sc A Spark Context
   * @param filePath FeatureJson path
   * @param numPartition Num of RDD partitions
   *
   * @return A GeoObjectRDD[GeoObject]
   */
  def createGeoObjectRDDFromFeatureJson(implicit sc: SparkContext,
                                        filePath: String,
                                        numPartition: Int): GeoObjectRDD = {
    val geomArray: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
    val objectMapper=new ObjectMapper()
    val fjson = new FeatureJSON()
    val node = objectMapper.readTree(new FileInputStream(filePath))
    if(node != null && node.has("features")) {
      val featureNodes = node.get("features").asInstanceOf[ArrayNode]
      val features = featureNodes.elements()
      while(features.hasNext){
        val arrayNode:JsonNode = features.next()
        val fcWkt = arrayNode.toString()
        val feature:SimpleFeature = fjson.readFeature(fcWkt)
        val uuid = UUID.randomUUID().toString
        val geoObject = new GeoObject(uuid, feature)
        geomArray += geoObject
      }
    }
    val rddShpData = sc.parallelize(geomArray, numPartition)
    new GeoObjectRDD(rddShpData)
  }

  /**
   *
   * Generate a GeoObjectRDD using re-formatted FeatureJson on hdfs file system.
   *
   * Native FeatureJson is not suitable for accessing from hdfs file system,
   * which can be transformed to txt format where each line contains a vector FeatureJson.
   *
   * @param sc A Spark Context
   * @param hdfsPath FeatureJson hdfs path
   * @param numPartition Num of RDD partitions
   *
   * @return A GeoObjectRDD[GeoObject]
   */
  def createGeoObjectRDDFromReformattedFeatureJson(implicit sc: SparkContext,
                                                   hdfsPath: String,
                                                   numPartition: Int): GeoObjectRDD = {
    val rddHDFS = sc.textFile(hdfsPath, numPartition)
    val geoObjects = rddHDFS.map{line =>
      val fjson = new FeatureJSON()
      val feature: SimpleFeature = fjson.readFeature(line)
      val uuid = UUID.randomUUID().toString
      val geoObject = new GeoObject(uuid, feature)
      geoObject
    }
    new GeoObjectRDD(geoObjects)
  }

}

