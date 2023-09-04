package whu.edu.cn.geocube.core.cube.tabular

import java.io.File
import java.nio.charset.Charset
import java.util.UUID

import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import whu.edu.cn.geocube.core.cube.vector.GeoObjectRDD

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class TabularRecordRDD (val rddPrev: RDD[TabularRecord])extends RDD[TabularRecord](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[TabularRecord] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)
}

object TabularRecordRDD {
  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  /**
   * Generate a GeoObjectRDD using shapefile on local file system.
   *
   * @param sc           A Spark Context
   * @param filePath     Shapefile path
   * @param numPartition Num of RDD partitions
   * @return A GeoObjectRDD[GeoObject]
   */
  def createTabularRecordRDD(implicit sc: SparkContext,
                                filePath: String,
                                numPartition: Int): TabularRecordRDD = {
    val results = new ArrayBuffer[TabularRecord]()

    val file=Source.fromFile(filePath)
    val attributeArr = file.getLines().next().split(" ")
    for(line <- file.getLines) {
      val valueArr = line.split(" ")
      var result: Map[String, String] = Map()
      (0 until attributeArr.length).foreach{ i =>
        result += (attributeArr(i)->valueArr(i))
      }
      val uuid = UUID.randomUUID().toString
      results.append(new TabularRecord(uuid, result))
    }
    file.close
    val rddData = sc.parallelize(results, numPartition)
    new TabularRecordRDD(rddData)
  }
}
