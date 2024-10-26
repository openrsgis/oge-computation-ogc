package whu.edu.cn.algorithms.RS
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => hPath}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.entity.SpaceTimeBandKey
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}

import java.io.{BufferedReader, InputStreamReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.immutable
import scala.jdk.CollectionConverters._



object AtmosphericCorrection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("query")
    val sc = new SparkContext(conf)
    /** 黑暗像元法测试代码 */
//    val tiffRDD = makeChangedRasterRDDFromTif(sc, "/mnt/storage/data/LC432.tif")
//    //val resRDD = atmosphericCorrection(sc, tiffRDD, 50000, 45, 45, 60)
//    val resRDD2 = darkObjectSubtraction(sc, tiffRDD)
//    //saveRasterRDDToTif(resRDD, "/mnt/storage/data/restif69.tif")
//    saveRasterRDDToTif(resRDD2, "/mnt/storage/data/restif71.tif")

    /** 6S大气校正测试代码 */
    val RDD1 = makeChangedRasterRDDFromTif(sc, "/mnt/storage/Landsat8/LC08_L1TP_123039_20150331_20170411_01_T1_B1.TIF")
    val RDD2 = makeChangedRasterRDDFromTif(sc, "/mnt/storage/Landsat8/LC08_L1TP_123039_20150331_20170411_01_T1_B2.TIF")
    val RDDCollection: immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map(
      "firstRDD" -> (RDD1),
      "secondRDD" -> (RDD2)
    )
    atmosphericCorrection_6S(sc, RDDCollection, "/mnt/storage/Landsat8/LC08_L1TP_123039_20150331_20170411_01_T1_MTL.txt")

  }

  /**黑暗像元法
   * @author Bolong Yu
   * @param sc Spark Context
   * @param tilesRDD Input landsat8 tileRDD
   * @return CorrectedRDD
   */
  def darkObjectSubtraction(implicit sc: SparkContext,
                            tilesRDD: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
                           ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // 提取瓦片RDD
    val (tiles, metadata) = tilesRDD

    // 计算黑暗像元的最小值
    val minDarkValues: Array[Double] = tiles.map { case (_, multibandTile) =>
      multibandTile.bands.map { bandTile =>
        bandTile.toArrayDouble().min
      }.toArray
    }.reduce((a, b) => a.zip(b).map { case (x, y) => Math.min(x, y) })

    // 遍历瓦片进行校正
    val correctedTiles: RDD[(SpaceTimeBandKey, MultibandTile)] = tiles.map { case (key, multibandTile) =>
      // 遍历波段
      val correctedBands = multibandTile.bands.zipWithIndex.map { case (bandTile, index) =>
        // 从波段的TOA反射率中减去黑暗像元的最小值
        bandTile.mapDouble { toa =>
          if (isData(toa)) {
            val darkValue = minDarkValues(index)
            Math.max(0, toa - darkValue) // 保证反射率不为负值
          } else {
            Double.NaN
          }
        }
        // 转换到uint16防止数值溢出
        bandTile.mapDouble { toa =>
          Math.max(0, Math.min(65535, toa))
        }.convert(UShortConstantNoDataCellType)
      }
      (key, MultibandTile(correctedBands))
    }
    (correctedTiles, metadata)
  }

  // 文件目录
  val landsatData = "/mnt/storage/"

  /**
   * 6S大气校正模型
   * @author Bolong Yu
   * @param sc SaprkContext
   * @param InputTiffs Landsat8-tiffs RDDCollection
   * @param Metadata Landsat8 MTL.txt file
   * @return AtmosphericCorrected RDDCollection
   */
  def atmosphericCorrection_6S(implicit sc: SparkContext,
                               InputTiffs: immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])],
                               Metadata: String): immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    // 配置本地文件系统
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    val fs = FileSystem.get(conf)

    // 指定创建目录
    val time = System.currentTimeMillis()
    val currentDate1 = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val formattedDate1 = currentDate1.format(formatter)


    val folderPath = new hPath("/mnt/landsat8_" + time)
    val outputPath = new hPath("/mnt/landsat8_res_" + time)

    // 创建文件夹存放影像
    if (!fs.exists(folderPath) && !fs.exists(outputPath)) {
      fs.mkdirs(folderPath)
      fs.mkdirs(outputPath)
      println(s"影像文件夹创建成功：$folderPath")
      println(s"结果文件夹创建成功：$outputPath")
    } else {
      println(s"文件夹已存在")
    }

    // 影像落地
    var i = 1
    for (tiff <- InputTiffs) {
      val currentDate2 = LocalDate.now()
      val formattedDate2 = currentDate2.format(formatter)
      // 影像落地为tif
      val tiffPath = folderPath.toString + "/" + "LC08_L1TP_123039_" + formattedDate1 + "_" + formattedDate2 + "_01_T1_" + s"B$i.TIF"
      saveRasterRDDToTif(tiff._2, tiffPath)
      i = i + 1
    }
    val utilsAC = new Utils
    // 元数据落地
    val targetPath = folderPath.toString + "/" + "LC08_L1TP_123039_" + formattedDate1 + "_" + formattedDate1 + "_01_T1" + "_MTL.txt"
    utilsAC.saveTXT(Metadata, targetPath)
    // 启动6S模型
    val command = s"conda activate py6s-env && D: && python /Python_code/AtmosphericCorrection/AtmosphericCorrection_Landsat8.py --Input_dir=${folderPath.toString} --Output_dir=${outputPath.toString}"
    println(s"开始执行命令：$command")

    // 执行命令并捕获输出
    val processBuilder = new ProcessBuilder("cmd", "/c", command)
    processBuilder.redirectErrorStream(true)
    val process = processBuilder.start()
    // 打印输出结果
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    var line: String = null
    while ( {line = reader.readLine(); line != null}) {
      println(line)
    }
    reader.close()
    val exitCode = process.waitFor()
    println(s"Process exited with code: $exitCode")

    //校正结果返回为RDDCollection
    val RDDCollection = utilsAC.TiffsToRDDCollection(sc, outputPath.toString)
    RDDCollection
  }
}






