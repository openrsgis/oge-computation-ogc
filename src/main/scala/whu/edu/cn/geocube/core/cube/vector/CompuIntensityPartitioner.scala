package whu.edu.cn.geocube.core.cube.vector

import geotrellis.layer.SpatialKey
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

/**
 * An partitioner of how a pair-RDD is partitioned based on computational intensity.
 *
 * @param partitions partition num
 * @param partitionContainers defines elements in each partition
 */
class CompuIntensityPartitioner (partitions: Int, partitionContainers: Array[ArrayBuffer[BigInt]]) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val spatialKey = key.asInstanceOf[SpatialKey]
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    val zorderCode = keyIndex.toIndex(spatialKey)
    (0 until partitions).foreach{ partitionID =>
      if(partitionContainers(partitionID).contains(zorderCode))
        return partitionID
    }
    throw new RuntimeException("Repartition error:" + zorderCode)
  }
}


object CompuIntensityPartitioner{

  /**
   * Assign element one by one to the partition container that has minimum computational intensity.
   * This function can derive balanced partitions, but has a low-efficiency.
   *
   * @param gridZOrderCodeWithCI A pair array, key is grid zorder code and value is computational intensity
   * @param partitions Partition nums
   *
   * @return Partition containers where each partition contains assigned grid zorder code
   */
  def getBoundary2(gridZOrderCodeWithCI: Array[(BigInt, Long)], partitions: Int): Array[ArrayBuffer[BigInt]] = {
    def getArrayIndex(value: Long, arr: Array[Long]): Int = {
      val length = arr.length
      (0 until length).foreach{index =>
        if(arr(index).equals(value))
          return index
      }
      throw new RuntimeException("No value matched")
    }
    val sortGridZOrderCodeWithCI = gridZOrderCodeWithCI.sortBy(_._2).reverse
    val avgCI = sortGridZOrderCodeWithCI.map(_._2).sum/partitions
    val partitionContainers = new Array[ArrayBuffer[BigInt]](partitions)
    val partitionCompuIntensity = Array.fill(partitions)(0L)
    (0 until partitions).foreach { i =>
      partitionContainers(i) = new ArrayBuffer[BigInt]()
      partitionContainers(i).append(sortGridZOrderCodeWithCI(i)._1)
      partitionCompuIntensity(i) += sortGridZOrderCodeWithCI(i)._2
    }
    (partitions until sortGridZOrderCodeWithCI.length).foreach{ i =>
      val minPartitionId = getArrayIndex(partitionCompuIntensity.min, partitionCompuIntensity)
      partitionContainers(minPartitionId).append(sortGridZOrderCodeWithCI(i)._1)
      partitionCompuIntensity(minPartitionId) += sortGridZOrderCodeWithCI(i)._2
    }

    print("partitions computational intensity: ")
    partitionCompuIntensity.foreach(x => print(x + " "))
    println()

    var checkCount = 0
    partitionContainers.foreach(partition => checkCount += partition.length)
    assert(checkCount == sortGridZOrderCodeWithCI.length)

    partitionContainers
  }

  /**
   * Assign head and tail elements one by one to the partition containers.
   *
   * This function can derive not so balanced partitions, but has a high-efficiency.
   *
   * @param gridZOrderCodeWithCI A pair array, key is grid zorder code and value is computational intensity
   * @param partitions Partition nums
   *
   * @return Partition containers where each partition contains assigned grid zorder code
   */
  def getBoundary1(gridZOrderCodeWithCI: Array[(BigInt, Long)], partitions: Int): Array[ArrayBuffer[BigInt]] = {
    val sortGridZOrderCodeWithCI = gridZOrderCodeWithCI.sortBy(_._2)
    val partitionContainers = new Array[ArrayBuffer[BigInt]](partitions)
    (0 until partitions).foreach(i => partitionContainers(i) = new ArrayBuffer[BigInt]())

    for (i <- Range(0, gridZOrderCodeWithCI.length / 2, partitions)){
      breakable{
        (i until (i + partitions)).foreach{ j =>
          val partitionID = j - i
          val firstIndex = j
          val lastIndex = gridZOrderCodeWithCI.length - j - 1
          if(lastIndex >= firstIndex){
            if(lastIndex > firstIndex){
              partitionContainers(partitionID).append(sortGridZOrderCodeWithCI(firstIndex)._1)
              partitionContainers(partitionID).append(sortGridZOrderCodeWithCI(lastIndex)._1)
            }else
              partitionContainers(partitionID).append(sortGridZOrderCodeWithCI(firstIndex)._1)
          }else break
        }
      }
    }

    val gridCIMap  = gridZOrderCodeWithCI.toMap
    var count = 0
    partitionContainers.foreach{index =>
      var partitionCI:Long = 0
      print("partition " + count + ": ")
      index.foreach{ele =>
        print(ele, gridCIMap.get(ele).get)
        partitionCI += gridCIMap.get(ele).get
      }
      println()
      println("partition " + count +  " sumCI: " + partitionCI)
      count += 1
    }

    partitionContainers
  }

  /**
   * Assign elements one by one to partitions container until the partition's computational intenisty is greater than average computational intenisty.
   *
   * This function can derive not so balanced partitions, but has a high-efficiency.
   *
   * @param gridZOrderCodeWithCI A pair array, key is grid zorder code and value is computational intensity
   * @param partitions Partition nums
   *
   * @return A grid zorder code partition boundary
   */
  def getBoundary(gridZOrderCodeWithCI: Array[(BigInt, Long)], partitions: Int): Array[BigInt] = {
    val sortGridZOrderCodeWithCI = gridZOrderCodeWithCI.sortBy(_._1)
    val compuIntensitySum = sortGridZOrderCodeWithCI.map(_._2).sum
    var str:Float = Float.MaxValue
    val bounds: Array[BigInt] = new Array[BigInt](partitions + 1)
    val partitionsIntensity:Array[Double] = Array.fill(partitions)(0)

    (0 until 500).foreach(count =>{
      val partitionsPara = partitions + count
      val intensityAvg = compuIntensitySum / partitionsPara
      val partitionsIntensityTmp:Array[Double] = Array.fill(partitions)(0)
      val boundsTmp: Array[BigInt] = new Array[BigInt](partitions + 1)
      var split = sortGridZOrderCodeWithCI(0)._1
      var tmp = 0
      (0 until partitions).foreach(i =>{
        boundsTmp(i) = split
        breakable{
          (tmp until sortGridZOrderCodeWithCI.length).foreach(j =>{
            if(partitionsIntensityTmp(i) <= intensityAvg){
              tmp += 1
              split = sortGridZOrderCodeWithCI(j)._1
              partitionsIntensityTmp(i) += sortGridZOrderCodeWithCI(j)._2
            }
            else if(i == partitions - 1){
              tmp += 1
              split = sortGridZOrderCodeWithCI(j)._1
              partitionsIntensityTmp(i) += sortGridZOrderCodeWithCI(j)._2
            }
            else
              break
          })
        }
      })
      boundsTmp(partitions) = split
      var ci_min:Double = Double.MaxValue
      var ci_max:Double = Double.MinValue
      (0 until partitions).foreach(i => {
        if(partitionsIntensityTmp(i) < ci_min)ci_min =  partitionsIntensityTmp(i)
        if(partitionsIntensityTmp(i) > ci_max)ci_max =  partitionsIntensityTmp(i)
      })
      if((ci_max - ci_min)/ci_max < str){
        str = ((ci_max - ci_min)/ci_max).toFloat
        (0 until bounds.length).foreach(i => bounds(i) = boundsTmp(i))
        (0 until partitionsIntensity.length).foreach(i => partitionsIntensity(i) = partitionsIntensityTmp(i))
      }
    })
    print("Partition boundary：")
    (0 until bounds.length).foreach(i=>print(bounds(i)+" "))
    println()
    print("Partition intensity：")
    (0 until partitionsIntensity.length).foreach(i=>print(partitionsIntensity(i)+" "))

    bounds
  }

}
