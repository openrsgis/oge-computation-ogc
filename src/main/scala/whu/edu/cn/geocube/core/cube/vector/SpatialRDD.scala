package whu.edu.cn.geocube.core.cube.vector

import java.io.File
import java.nio.charset.Charset
import java.util.UUID

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.geotools.data.simple.SimpleFeatureIterator
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ArrayBuffer

/**
 * RDD class which represents a series of (ID, Geometry)
 *
 */
class SpatialRDD (val rddPrev: RDD[(String, Geometry)])extends RDD[(String, Geometry)](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, Geometry)] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)
}

object SpatialRDD {
  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  /**
   * Generate a SpatialRDD using shapefile on local file system.
   *
   * @param sc Spark context
   * @param filePath local file path
   * @param numPartition
   * @return
   */
  def createSpatialRDDFromShp(implicit sc: SparkContext,
                              filePath: String,
                              numPartition: Int): SpatialRDD = {

    val geomArray: ArrayBuffer[(String, Geometry)] = new ArrayBuffer[(String, Geometry)]()
    val sf = new ShpFiles(filePath)
    val sfReader = new ShapefileReader(sf, false, false, new GeometryFactory())
    while(sfReader.hasNext){
      val uuid = UUID.randomUUID().toString
      val pair = (uuid, (sfReader.nextRecord().shape()).asInstanceOf[Geometry])
      geomArray += pair
    }
    val rddShpData = sc.parallelize(geomArray, numPartition)

    new SpatialRDD(rddShpData)
  }

  /**
   * Generate a SpatialRDD using WKT on local file system.
   *
   * @param sc Spark context
   * @param filePath hdfs path
   * @param numPartition
   * @return
   */
  def createSpatialRDDFromLocalWKT(sc: SparkContext,
                                   filePath: String,
                                   numPartition: Int): SpatialRDD = {

    // read data from WKT file
    var rddHDFSData = sc.textFile(filePath).filter(line => {
      val wkt = line.split(SEPARATOR)(0)
      if (wkt.contains("POLYGON") || wkt.contains("LINESTRING") || wkt.contains("POINT")) {
        val geom = new WKTReader().read(wkt)
        if (geom != null)
          if(geom.isValid())
            true
          else
            false
        else
          false
      } else {
        false
      }
    }).map(line => {
      val uuid = UUID.randomUUID().toString
      (uuid, line)
    })

    //repartition
    rddHDFSData = rddHDFSData.repartition(numPartition)

    val rddSpatialData = rddHDFSData.map(lineWithUUID => {
      val wkt = lineWithUUID._2.split(SEPARATOR)(0)
      val geom = new WKTReader().read(wkt)
      (lineWithUUID._1, geom)

    })

    new SpatialRDD(rddSpatialData)
  }

  /**
   *
   * Generate a SpatialRDD using WKT on hdfs file system.
   *
   * @param sc A Spark Context
   * @param hdfsPath WKT hdfs path
   * @param numPartition Num of RDD partitions
   * @return A SpatialRDD[(String, Geometry)]
   */
  def createSpatialRDDFromHdfsWKT(implicit sc: SparkContext,
                                  hdfsPath: String,
                                  numPartition: Int): RDD[(String, Geometry)] = {
    val rddHDFS = sc.textFile(hdfsPath, 16)
    rddHDFS.map{line =>
      val uuid = UUID.randomUUID().toString
      val geom: Geometry = new WKTReader().read(line)
      (uuid, geom)
    }
  }
}
