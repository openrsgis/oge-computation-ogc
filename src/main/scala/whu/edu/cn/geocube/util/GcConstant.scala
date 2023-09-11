package whu.edu.cn.geocube.util

/**
 * Constant config parameters
 */
object GcConstant {
  //data server
  //  final val localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/"
  //  final val httpDataRoot = "http://125.220.153.26:8093/data/temp/"
  final val localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/gdc_api/"
  final val httpDataRoot = "http://125.220.153.26:8093/data/gdc_api/"
  final val localHtmlRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/html/"
  final val algorithmJson = "/home/geocube/kernel/geocube-core/tb19/process_description.json"
//  final val algorithmJson = "/home/geocube/tb19/kernel/process_description.json"

  //  final val algorithmJson = "E:\\LaoK\\githubFolder\\GeoCubeCore\\src\\main\\resource\\process_description.json"

  //landsat-8 pixel value in BQA band from USGS
  final val cloudValueLs8: Array[Int] = Array(2800, 2804, 2808, 2812, 6896, 6900, 6904, 6908)
  final val cloudShadowValueLs8: Array[Int] = Array(2976, 2980, 2984, 2988, 3008, 3012, 3016, 3020, 7072, 7076,
    7080, 7084, 7104, 7108, 7112, 7116)
}
