package whu.edu.cn.algorithms.RS
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity.SpaceTimeBandKey

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.immutable
import scala.math._
import scala.jdk.CollectionConverters._
import whu.edu.cn.util.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}


class Utils extends Serializable{
  // txt文件落地
  def saveTXT(sourcePath: String,
              targetPath: String): Unit = {
    Files.copy(
      Paths.get(sourcePath),
      Paths.get(targetPath),
      StandardCopyOption.REPLACE_EXISTING
    )
    if (Files.exists(Paths.get(targetPath))){
      println(s"txt元数据文件已保存至 $targetPath")
    } else {
      println("txt元数据文件保存失败")
    }
  }

  // 获取大气校正结果路径
  def getTiffFiles(outputPath: String): List[String] = {
    val dirPath = Paths.get(outputPath)
    if (Files.exists(dirPath) && Files.isDirectory(dirPath)){
      // 将所有.TIF文件路径保存为列表
      Files.walk(dirPath).iterator().asScala.filter(path => Files.isRegularFile(path) && path.toString.toUpperCase.endsWith(".TIF")).map(_.toString).toList
    } else {
      println(s"该路径不存在：$dirPath")
      List.empty[String]
    }
  }

  // TIF转RDDCollection
  def TiffsToRDDCollection(implicit sc: SparkContext,
                           dir: String): immutable.Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = {
    val tiffFiles = getTiffFiles(dir)
    tiffFiles.map { filePath =>
      val rasterData = makeChangedRasterRDDFromTif(sc, "/" + filePath)
      (filePath, rasterData)
    }.toMap
  }





}
