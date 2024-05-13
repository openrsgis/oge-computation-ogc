package whu.edu.cn.algorithms.terrain

import whu.edu.cn.algorithms.terrain.core.RDDTransformerUtil.{makeChangedRasterRDDFromTif, saveFeatureRDDToShp, saveRasterRDDToTif}
import whu.edu.cn.algorithms.terrain.calculator.{Aspect, ChannelNetwork, Curvature, FeatureSelect, FlowAccumulation, FlowConnectivity, FlowDirection, FlowLength, FlowWidth, HillShade, PitEliminator, PitRouter, Ruggedness, Slope, SlopeLength, StrahlerOrder, TIN, WatershedBasins}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Application {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("TerrainAnalysis")
    val sc = new SparkContext(config)

    val calculator = WatershedBasins
    val className = calculator.getClass.getSimpleName.dropRight(1)
    val folder = new File("D:/oge-terrain/data/output/" + className)
    if (!folder.exists()) {
      folder.mkdir()
    }

////    for (i <- List("32", "64", "large")) {
//    for (i <- List("origin")) {
////    for (i <- List("PitEliminator_ogc")) {
//      val srcPath  = "D:/projects/oge-terrain/data/our_test/dem_" + i + ".tif"
////      val srcPath  = "D:/projects/oge-terrain/data/our_test/" + i + ".tif"
//      val dstPath  = "D:/projects/oge-terrain/data/our_test/"+ className +"_ogc.tif"
////      val dstPath  = "D:/projects/oge-terrain/data/our_test/"+ className +"_ogc.shp"
//      val rddImage = makeChangedRasterRDDFromTif(sc, srcPath)
//      val rddSlope = calculator(rddImage)
//
//      saveRasterRDDToTif(rddSlope, dstPath)
////      saveFeatureRDDToShp(rddSlope, dstPath)
//    }

//    //    test_channelnetwork
//    val srcPath = "D:/projects/oge-terrain/data/our_test/PitEliminator_ogc.tif"
//    val accPath = "D:/projects/oge-terrain/data/our_test/FlowAccumulation_ogc.tif"
//    val dirPath = "D:/projects/oge-terrain/data/our_test/FlowDirection_ogc.tif"
//    //    val accPath = "./data/dem_fill_accumulation_crop0.tif"
//    //    val dstPath = "./data/output/" + className + "/dem_out_saga.shp"
//    val dstPath = "D:/projects/oge-terrain/data/our_test/" + className + "_ogc.shp"
//    val rddImage = makeChangedRasterRDDFromTif(sc, srcPath)
//    val accIamge = makeChangedRasterRDDFromTif(sc, accPath)
//    val dirIamge = makeChangedRasterRDDFromTif(sc, dirPath)
//    val rddSlope = calculator(rddImage, accIamge, dirIamge, 1, threshold = 0.01 * 1e4)
//    saveFeatureRDDToShp(rddSlope, dstPath)

        //    test_basin
        val srcPath = "D:/projects/oge-terrain/data/our_test/PitEliminator_ogc.tif"
        val accPath = "D:/projects/oge-terrain/data/our_test/FlowAccumulation_ogc.tif"
        val dstPath = "D:/projects/oge-terrain/data/our_test/" + className + "_ogc.tif"
        val rddImage = makeChangedRasterRDDFromTif(sc, srcPath)
        val accIamge = makeChangedRasterRDDFromTif(sc, accPath)
        val rddSlope = calculator(rddImage, accIamge)
        saveRasterRDDToTif(rddSlope, dstPath)
  }

}
