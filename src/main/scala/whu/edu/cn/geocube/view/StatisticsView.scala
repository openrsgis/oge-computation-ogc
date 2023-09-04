package whu.edu.cn.geocube.view

import java.io.{FileOutputStream, PrintStream}
import java.util.UUID

/**
 * A class for display statistics data using echarts.
 */
class StatisticsView(info: Array[Info], _xaxisLabel:String, _yaxisLabel:String, _title:String){
  val htmlDir = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/"
  val htmlPath = "html/statistics_" + UUID.randomUUID() + ".html"
  val xaxisLabel = _xaxisLabel
  val yaxisLabel = _yaxisLabel
  val title = _title

  def display:Unit = {
    val sb = new StringBuilder
    val printStream = new PrintStream(new FileOutputStream(htmlDir + htmlPath))
    sb.append("<html>")
    sb.append("<head>")
    sb.append("<meta charset=\"utf-8\">")
    sb.append("<title>Statistics View</title>")
    sb.append("<link rel=\"icon\" href=\"../ico/geocube2.ico\" type=\"image/x-icon\">")
    sb.append("<script src=\"../js/echarts.js\"></script>")
    sb.append("</head>")
    sb.append("<body>")
    sb.append("<div id=\"main\" style=\"width: 1200px;height:400px;;margin: 0 auto;border: solid 1px grey;top:50%;transform: translateY(-50%);\"></div>")
    sb.append("<script type=\"text/javascript\">")
    sb.append("var myChart = echarts.init(document.getElementById('main'));")

    val titleString = String.format("title: {text: '%s', x: 'center'},", title)
    val grid = "grid: {left: '20%',right: '20%',bottom: '3%',containLabel: true},"
    val tooltip = "tooltip: {text:\"this is tool tip\"},"
    val legend = "legend: {" + String.format("data:['%s']", yaxisLabel) + ",textStyle: {fontSize: 14},x: 'left',y: '30px',left: '20%'},"
    val xAxis_data = new StringBuilder
    for(i <- info)
      xAxis_data.append("\"" + i.getInstant() + "\""+ ",")
    xAxis_data.deleteCharAt(xAxis_data.length - 1)
    val xAxis = String.format(" xAxis: {type: 'category', data: [%s], name: '%s',nameTextStyle: {fontSize: 14},axisTick: { alignWithLabel: true},splitLine:{show: true},splitArea : {show : true} },", xAxis_data, xaxisLabel)
    val yAxis = "yAxis: {splitLine:{show: true},splitArea : {show : true} },"
    val series_data = new StringBuilder
    for(i <- info)
      series_data.append(i.value + ",")
    series_data.deleteCharAt(series_data.length - 1)
    val series = String.format("series: [{name: '%s',type: 'line', data: [%s], symbol: 'circle',symbolSize: 10}]", yaxisLabel, series_data)
    val option = new StringBuilder
    option.append("var option = {")
    option.append(titleString + grid + tooltip + legend + xAxis + yAxis + series)
    option.append("};")
    sb.append(option)

    sb.append("myChart.setOption(option);")
    sb.append("</script>")
    sb.append("</body>")
    sb.append("</html>")

    printStream.println(sb.toString())
    println("Please access http://125.220.153.22:8093/" + htmlPath + " to view the results!")
  }
}

object StatisticsView {
  /**
   * Display water body area in a time period.
   * Used in Jupyte Notebook.
   *
   * @param _info
   */
  def displayWaterBodyArea(_info: Array[Info]):Unit = {
    val info = _info.sortBy(_.instant)
    val statisticsView1 = new StatisticsView(info, "date /yyyy-MM-dd HH:mm:ss", "waterbody area /km²", "Water statistics")
    statisticsView1.display
  }

  /**
   * Display vegetation body area in a time period.
   * Used in Jupyte Notebook.
   *
   * @param _info
   */
  def displayVegetationArea(_info: Array[Info]):Unit = {
    val info = _info.sortBy(_.instant)
    val statisticsView1 = new StatisticsView(info, "date /yyyy-MM-dd HH:mm:ss", "vegetation area /km²", "Vegetation statistics")
    statisticsView1.display
  }

}


