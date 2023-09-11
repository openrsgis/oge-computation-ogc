package whu.edu.cn.geocube.core.entity

import org.json4s.native.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization.write

class WorkflowCollectionParam extends Serializable {

  var queryParams: QueryParams = new QueryParams()

  var scaleSize: Array[Int] = null

  var scaleAxes: Array[Double] = null

  var scaleFactor: Option[Double]= None
  //
  //  @BeanProperty
  //  var outputDirectory: String = null

  var imageFormat: String = "tif"

  def this(queryParams: QueryParams, scaleSize: Array[Int], scaleAxes: Array[Double],
           scaleFactor: Option[Double], imageFormat: String) {
    this()
    this.queryParams = queryParams
    this.scaleSize = scaleSize
    this.scaleAxes = scaleAxes
    this.scaleFactor = scaleFactor
    this.imageFormat = imageFormat
  }

  def setQueryParams(queryParams: QueryParams): Unit = {
    this.queryParams = queryParams
  }

  def setScaleSize(scaleSize: Array[Int]): Unit = {
    this.scaleSize = scaleSize
  }

  def setScaleAxes(scaleAxes: Array[Double]): Unit = {
    this.scaleAxes = scaleAxes
  }

  def setScaleFactor(scaleFactor: Option[Double]): Unit = {
    this.scaleFactor = scaleFactor
  }

  def setImageFormat(imageFormat: String): Unit = {
    this.imageFormat = imageFormat
  }

  def getQueryParams: QueryParams = {
    this.queryParams
  }

  def getScaleSize: Array[Int] = {
    this.scaleSize
  }

  def getScaleAxes: Array[Double] = {
    this.scaleAxes
  }

  def getScaleFactor: Option[Double] = {
    this.scaleFactor
  }

  def getImageFormat: String = {
    this.imageFormat
  }

  def toJSONString: String = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    write(this)
  }


}
