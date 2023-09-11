package whu.edu.cn.algorithms.terrain

import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveRasterRDDToTif}
import whu.edu.cn.algorithms.terrain.calculator.{Aspect, Curvature, Ruggedness, Slope, SlopeLength}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object Application {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("TerrainAnalysis")
    val sc = new SparkContext(config)

    val calculator = Slope
    val className = calculator.getClass.getSimpleName.dropRight(1)
    val folder = new File("D:/oge-terrain/data/output/" + className)
    if (!folder.exists()) {
      folder.mkdir()
    }

    for (i <- List("32", "64", "large")) {
      val srcPath  = "D:/oge-terrain/data/dem_" + i + ".tif"
      val dstPath  = "D:/oge-terrain/data/output/"+ className +"/dem_" + i + "_out.tif"
      val rddImage = makeChangedRasterRDDFromTif(sc, srcPath)
      val rddSlope = calculator(rddImage)

      saveRasterRDDToTif(rddSlope, dstPath)
    }

  }

}
