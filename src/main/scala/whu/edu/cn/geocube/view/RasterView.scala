package whu.edu.cn.geocube.view

import java.io.{File, FileOutputStream, PrintStream}
import java.text.SimpleDateFormat
import java.util.UUID

/**
 * A class for displaying raster data with png format.
 *
 * @param info
 */
class RasterView(info: Array[Info]){
  val htmlDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/"
  val htmlPath = "html/visualize_" + UUID.randomUUID() + ".html"

  /**
   * Display single-instant png.
   */
  def displayResult():Unit = {
    val sb = new StringBuilder
    val printStream = new PrintStream(new FileOutputStream(htmlDir + htmlPath))
    sb.append("<html>")
    sb.append("<head>")
    sb.append("<meta charset=\"utf-8\">")
    sb.append("<title>Raster View</title>")
    sb.append("<style>")
    sb.append("img {border:2px solid #ddd; border-radius:4px; padding:5px;}")
    sb.append("</style>")
    sb.append("</head>")

    sb.append("<body>")
    val header = String.format("<h2>%s</h2>", info(0).header)

    sb.append(header)
    val img = String.format("<img src=%s alt=\"Error\" width=\"256\" height=\"256\">", new File(info(0).path).getName)
    sb.append(img)
    sb.append("</body>")

    sb.append("</html>")

    printStream.println(sb.toString())
    println("Please access http://125.220.153.22:8093/" + htmlPath + " to view the results!")
  }

  /**
   * Display time-series pngs.
   */
  def displayResults():Unit = {
    val sb = new StringBuilder
    val printStream = new PrintStream(new FileOutputStream(htmlDir + htmlPath))
    sb.append("<html>")
    sb.append("<head>")
    sb.append("<meta charset=\"utf-8\">")
    sb.append("<title>Raster View</title>")
    sb.append("<link rel=\"icon\" href=\"../ico/geocube2.ico\" type=\"image/x-icon\">")
    sb.append("<style>")

    sb.append("div.img {border: 1px solid #ddd;}")
    sb.append("div.img:hover { border: 1px solid #777;}")
    sb.append("div.img img { width: 100%; height: auto;}")
    sb.append("div.desc { padding: 15px; text-align: center;}")
    sb.append("* { box-sizing: border-box;}")
    val width = 1.0 / info.length * 100.0
    sb.append(".responsive { padding: 0 6px; float: left; width: " + width + "%;}")
    sb.append("@media only screen and (max-width: 700px) { .responsive { width: 49.99999%; margin: 6px 0;}}")
    sb.append("@media only screen and (max-width: 500px) { .responsive { width: 100%;}.container{transform: none!important;top:0!important;}}")
    sb.append(".clearfix:after {  content: \"\"; display: table; clear: both;}")
    if(info.length == 1){
      sb.append(".container{margin: 0 auto;width: 40%;top:50%;position: relative;transform: translateY(-50%);}")
    }else{
      sb.append(".container{margin: 0 auto;width: 80%;top:50%;position: relative;transform: translateY(-50%);}")
    }


    sb.append("</style>")
    sb.append("</head>")

    sb.append("<body>")
    sb.append("<div class=\"container\">")
    //val header = String.format("<h2 style=\"text-align:justify\">%s</h2>", info(0).header) path_info.get(pathArray(0)).asInstanceOf[Info].header
    val header = String.format("<h2 style=\"text-align:center\">%s</h2>", info(0).header)
    sb.append(header)

    for(i <- 0 until info.length){
      sb.append("<div class=\"responsive\">")
      sb.append("  <div class=\"img\">")
      val aTag = String.format("   <a target=\"_blank\" href=%s>", new File(info(i).path).getName)
      sb.append(aTag)
      val img = String.format("      <img src=%s alt=\"Error\" width=\"256\" height=\"256\";>", new File(info(i).path).getName)
      sb.append(img)
      sb.append("</a>")
      //val desc = String.format("<div class=\"desc\">%s</div>", info(i).instant)path_info.get(path(0)).asInstanceOf[Info]
      val desc = String.format("<div class=\"desc\">%s</div>", info(i).getInstant())
      if(info.length != 1) sb.append(desc)
      sb.append("  </div>")
      sb.append("</div>")
    }
    sb.append("<div class=\"clearfix\"></div>")
    sb.append("<div style=\"padding:6px;\">")
    sb.append("</div>")

    sb.append("</body>")
    sb.append("</html>")

    printStream.println(sb.toString())
    println("Please access http://125.220.153.22:8093/" + htmlPath + " to view the results!")
  }

}

object RasterView{
  /**
   * Display raster data with png.
   *
   * @param _info
   */
  def display(_info: Array[Info]):Unit = {
    val info = _info.sortBy(_.instant)
    val rasterView = new RasterView(info)
    rasterView.displayResults()
  }
}

