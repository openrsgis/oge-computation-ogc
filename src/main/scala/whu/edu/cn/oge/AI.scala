package whu.edu.cn.oge

import com.baidubce.http.HttpMethodName
import com.baidubce.services.bos.model.GeneratePresignedUrlRequest
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, FloatArrayTile, FloatCellType, MultibandTile, Tile}
import geotrellis.raster.mapalgebra.local.{Add, Divide}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey

import java.text.SimpleDateFormat

object AI {
//  /**
//   *
//   * @param coverage1
//   * @param coverage2
//   * @return
//   */
//  def classificationDLCUG(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
//                         ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//
//    // 处理 coverage1
//    // 依照 spaceTimeKey 分组
//    val coverageGrouped: RDD[(SpaceTimeKey, Iterable[(SpaceTimeBandKey, Tile)])] = {
//      coverage1._1.groupBy(t => t._1.spaceTimeKey)
//    }
//    // 根据波段序号排序
//    val coverageSorted: RDD[(SpaceTimeKey, Array[(SpaceTimeBandKey, Tile)])] =
//      coverageGrouped.map(t => {
//        val tileArray: Array[(SpaceTimeBandKey, Tile)] = t._2.toArray
//        (t._1, tileArray.sortBy(x => x._1.measurementName.toInt))
//      }).persist()
//    val floatArrayOfcoverage1: RDD[(SpaceTimeKey, Array[Float])] = coverageSorted.map(t => {
//      (t._1,
//        ModelInference.byteToFloatOPT(
//          t._2(0)._2.toBytes(), t._2(1)._2.toBytes(), t._2(2)._2.toBytes(), t._2(3)._2.toBytes()
//        ))
//    }
//    )
//
//    // 处理coverage2
//    val floatArrayOfcoverage2: RDD[(SpaceTimeKey, Array[Float])] = coverage2.map(t => {
//      (t._1.spaceTimeKey,
//        ModelInference.byteToFloatSAR(
//          t._2.toBytes()
//        ))
//    })
//
//
//    // 连接 coverage1 和 coverage2 的 Float 数组
//    val floatArrayInput: RDD[(SpaceTimeKey, (Array[Float], Array[Float]))] =
//      floatArrayOfcoverage1.join(floatArrayOfcoverage2)
//
//
//    // 将float数组对应瓦片合二为一，进行处理并得到返回值
//    val floatArrayOutput: RDD[(SpaceTimeBandKey, Tile)] =
//      floatArrayInput.map(t => (t._1, ModelInference.processOneTile(t._2._1, t._2._2)))
//        .mapPartitions(iter => {
//          // 拆分出三个波段并生成新的 rdd
//          val res = List[(SpaceTimeBandKey, Tile)]()
//          while (iter.hasNext) {
//            val curTuple: (SpaceTimeKey, Array[Float]) = iter.next()
//
//            val band1 = new Array[Byte](512 * 512)
//            val band2 = new Array[Byte](512 * 512)
//            val band3 = new Array[Byte](512 * 512)
//            val prob = new Array[Float](8)
//            for (k <- 0 until 512 * 512) {
//              for (l <- 0 until 8) {
//                prob(l) = FloatArrayTile(curTuple._2, 512, 512, FloatCellType)(k + l * 512 * 512)
//              }
//              val classValue: Int = prob.zipWithIndex.maxBy(_._1)._2 // 根据值的大小进行排序并返回索引
//
//              classValue match {
//                case 0 => {
//                  band1(k) = -1
//                  band2(k) = -1
//                  band3(k) = -1
//                }
//                case 1 => {
//                  band1(k) = -1
//                  band2(k) = -1
//                  band3(k) = 127
//                }
//                case 2 => {
//                  band1(k) = -1
//                  band2(k) = 127
//                  band3(k) = -1
//                }
//                case 3 => {
//                  band1(k) = 127
//                  band2(k) = -1
//                  band3(k) = -1
//                }
//                case 4 => {
//                  band1(k) = 127
//                  band2(k) = 127
//                  band3(k) = -1
//                }
//                case 5 => {
//                  band1(k) = 127
//                  band2(k) = -1
//                  band3(k) = 127
//                }
//                case 6 => {
//                  band1(k) = -1
//                  band2(k) = 127
//                  band3(k) = 127
//                }
//                case 7 => {
//                  band1(k) = 127
//                  band2(k) = 127
//                  band3(k) = 127
//                }
//              }
//            }
//            // 将三个波段的 key 及其对应的tile追加进去
//            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand1DL"),
//              ByteArrayTile(band1, 512, 512, ByteConstantNoDataCellType))
//            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand2DL"),
//              ByteArrayTile(band2, 512, 512, ByteConstantNoDataCellType))
//            res.::(SpaceTimeBandKey(curTuple._1, "ChangeDetectionBand3DL"),
//              ByteArrayTile(band3, 512, 512, ByteConstantNoDataCellType))
//
//          }
//          // 一变三
//          res.iterator
//        })
//
//    // 返回rdd和 TileLayerMetadata
//
//    (
//      // RDD
//      floatArrayOutput,
//      // TileLayerMetadata
//      TileLayerMetadata(
//        ByteConstantNoDataCellType,
//        coverage1._2.layout,
//        coverage1._2.extent,
//        coverage1._2.crs,
//        coverage1._2.bounds
//      )
//
//    )
//    //    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
//  }
//
//  //Precipitation Anomaly Percentage
//  def PAP(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), time: String, n: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//    var m = n
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    //val now = "1000-01-01 00:00:00"
//    val date = sdf.parse(time).getTime
//    val timeYear = time.substring(0, 4)
//    var timeBeginYear = timeYear.toInt - n
//    if (timeYear.toInt - n < 2000) {
//      timeBeginYear = 2000
//      m = timeYear.toInt - 2000
//    }
//    val timeBegin = timeBeginYear.toString + time.substring(4, 19)
//    val dateBegin = sdf.parse(timeBegin).getTime
//
//
//    val coverageReduced = coverage._1.filter(t => {
//      val timeResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t._1.spaceTimeKey.instant)
//      timeResult.substring(5, 7) == time.substring(5, 7)
//    })
//    val coverageAverage = coverageReduced.filter(t => {
//      t._1.spaceTimeKey.instant >= dateBegin && t._1.spaceTimeKey.instant < date
//    }).groupBy(t => t._1.spaceTimeKey.spatialKey)
//      .map(t => {
//        val tiles = t._2.map(x => x._2).reduce((a, b) => Add(a, b))
//        val tileAverage = tiles.mapDouble(x => {
//          x / t._2.size
//        })
//        (t._1, tileAverage)
//      })
//    val coverageCal = coverageReduced.filter(t => t._1.spaceTimeKey.instant == date).map(t => (t._1.spaceTimeKey.spatialKey, t._2))
//    val PAP = coverageAverage.join(coverageCal).map(t => {
//      val divide = Divide(t._2._2, t._2._1)
//      val PAPTile = divide.mapDouble(x => {
//        x - 1.0
//      })
//      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), "PAP"), PAPTile)
//    })
//    (PAP, coverage._2)
//  }
def getTrainingDatasetEncoding(datasetName: String): String = {
  throw new Exception("所请求的数据在Bos中不存在！")
//  val bosClient = BosClientUtil_scala.getClient2 // 假设这个方法返回一个有效的BosClient对象
//
//  try {
//    val bucketName = "pytdml"
//    val key = s"datasetTDEncodes/$datasetName.json"
//    // 生成预签名URL请求
//    val request = new GeneratePresignedUrlRequest(bucketName, key, HttpMethodName.GET)
//    request.setExpiration(3600) // 设置URL的有效时间为3600秒
//
//    // 生成预签名URL
//    val url = bosClient.generatePresignedUrl(request)
//    url.toString // 将返回的URL转换为字符串
//  } catch {
//    case e: Exception =>
//      e.printStackTrace()
//      "" // 在异常情况下返回空字符串
//  }
}


//  def main(args: Array[String]): Unit = {
//    print(getTrainingDatasetEncoding("MBD"))
//  }
}
