package whu.edu.cn.geocube.core.raster.ingest

import java.io.{File, FileOutputStream}
import java.util.UUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import scala.io.StdIn

object DatasetPreparation{
  def main(args:Array[String]):Unit = {
    val datasetPreparation = DatasetPreparation()
    //datasetPreparation.createScript("https://schemas.opendatacube.org/dataset", "E:\\SatelliteImage\\Landsat8", satellite = "Landsat")
    //datasetPreparation.createScript("https://schemas.opendatacube.org/dataset", "E:\\SatelliteImage\\Landsat7", satellite = "Landsat")
    //datasetPreparation.createScript("https://schemas.opendatacube.org/dataset", "E:\\SatelliteImage\\Landsat5", satellite = "Landsat")
    //datasetPreparation.createScript("https://schemas.opendatacube.org/dataset", "E:\\SatelliteImage\\Landsat4", satellite = "Landsat")
    datasetPreparation.createScript("https://schemas.opendatacube.org/dataset", "E:\\SatelliteImage\\GF1", satellite = "Gaofen")
    datasetPreparation.print()

    println("Hit enter to exit")
    StdIn.readLine()
  }
}

case class DatasetPreparation() {
  var id: String = ""
  var schema: String = ""
  var productName: String = ""
  var crs:String = ""
  var geometry:Option[Geometry] = Some(Geometry())
  var grids:Grids = Grids()
  var measurements:Array[PrepareMeasurement] = Array()
  var properties:Properties = Properties()

  case class Geometry(_geomType:String = "", _coordinates:String = ""){
    var geomType = _geomType
    var coordinates = _coordinates

    def getOptional():Option[Geometry] = {
      if(this.equals(Geometry("", "")))
        None
      else
        Some(this)
    }

    override def equals(obj: Any): Boolean = {
      (this.geomType == obj.asInstanceOf[Geometry].geomType) &&
        (this.coordinates == obj.asInstanceOf[Geometry].coordinates)
    }
  }
  case class Grids(_defaultShape:String = "", _defaultTransform:String = "",
                   _panShape:String = "", _panTransform:String = ""){
    var defaultShape = _defaultShape
    var defaultTransform = _defaultTransform
    var panShape = _panShape
    var panTransform = _panTransform
  }

  case class Properties(_dateTime:String = "", _fileFormat:String = "",
                        _processingDatetime:String = ""){
    var dateTime = _dateTime
    var fileFormat = _fileFormat
    var processingDatetime = _processingDatetime

  }

  def init(id: String, schema: String, productName: String, crs: String, geomType: String = "",
           coordinates:String = "", defaultShape:String, defaultTransform:String, panShape:String,
           panTransform:String, measurements: Array[PrepareMeasurement],
           dateTime:String, fileFormat:String = "", processingDatetime:String = ""): Unit = {
    this.id = id
    this.schema = schema
    this.productName = productName
    this.crs = crs
    this.geometry = Geometry(geomType, coordinates).getOptional()
    this.grids = Grids(defaultShape, defaultTransform, panShape, panTransform)
    this.measurements = measurements
    this.properties = Properties(dateTime, fileFormat, processingDatetime)
  }

  def createScript(schema: String, inputDir:String, satellite:String = "Landsat"):Unit = {
    this.id = UUID.randomUUID().toString
    this.schema = schema
    gdal.AllRegister()
    satellite match {
      case "Landsat" =>{
        val dir = new File(inputDir)
        val datasets = dir.listFiles()
        val fileLength = datasets.length
        for(i <- 0 until 1){
          val dataset = datasets(i)
          val metaFile = dataset.listFiles().filter(_.getName.contains("MTL"))
          val parser = whu.edu.cn.geocube.core.raster.ard.landsat8.Parser(metaFile(0).getAbsolutePath, 11)
          parser.collectMetadata("")
          if (parser.satellite.equals("LANDSAT_8") || parser.satellite.equals("LANDSAT_7")) {
            val panBand = dataset.listFiles().filter(_.getName.contains("B8"))
            val srcDataset = gdal.Open(panBand(0).getAbsolutePath, gdalconstConstants.GA_ReadOnly)
            if (srcDataset == null) {
              System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo())
              System.err.println(gdal.GetLastErrorMsg())
              System.exit(1)
            }
            val imgWidth = srcDataset.getRasterXSize
            val imgHeight = srcDataset.getRasterYSize
            val imgCrs = srcDataset.GetProjectionRef
            val imgGeoTrans = new Array[Double](6)
            srcDataset.GetGeoTransform(imgGeoTrans)
            this.productName = parser.satellite
            this.crs = imgCrs

            this.geometry.get.geomType = "Polygon"
            this.geometry.get.coordinates = "[" +
              "[" + parser.geomULLati + ", " + parser.geomULLong + "], " +
              "[" + parser.geomURLati + ", " + parser.geomURLong + "], " +
              "[" + parser.geomLRLati + ", " + parser.geomLRLong + "], " +
              "[" + parser.geomLLLati + ", " + parser.geomLLLong + "], " +
              "[" + parser.geomULLati + ", " + parser.geomULLong + "]" +
              "]"

            /*this.grids.defaultShape = "[" + parser.defaultLines + ", " + parser.defaultSamples + "]"
            this.grids.defaultTransform = "[30, 0, " + parser.defaultULX + ", 0, -30, " + parser.defaultULY + ", 0, 0, 1]"
            this.grids.panShape = "[" + imgHeight + ", " + imgWidth + "]"
            this.grids.panTransform = "[15, 0, " + imgGeoTrans(0) + ", 0, -15, " + imgGeoTrans(3) + ", 0, 0, 1]"*/

            this.grids.defaultShape = parser.defaultLines + ", " + parser.defaultSamples
            this.grids.defaultTransform = "30, 0, " + parser.defaultULX + ", 0, -30, " + parser.defaultULY + ", 0, 0, 1"
            this.grids.panShape = imgHeight + ", " + imgWidth
            this.grids.panTransform = "15, 0, " + imgGeoTrans(0) + ", 0, -15, " + imgGeoTrans(3) + ", 0, 0, 1"

            this.measurements = parser.satellite match {
              case "LANDSAT_8" => Array(
                PrepareMeasurement(_name = "coastal", _path = "B1.tif"),
                PrepareMeasurement(_name = "blue", _path = "B2.tif"),
                PrepareMeasurement(_name = "green", _path = "B3.tif"),
                PrepareMeasurement(_name = "red", _path = "B4.tif"),
                PrepareMeasurement(_name = "NIR", _path = "B5.tif"),
                PrepareMeasurement(_name = "SWIR1", _path = "B6.tif"),
                PrepareMeasurement(_name = "SWIR2", _path = "B7.tif"),
                PrepareMeasurement(_name = "PAN", _path = "B8.tif", _grid = "pan"),
                PrepareMeasurement(_name = "CIRRUS", _path = "B9.tif"),
                PrepareMeasurement(_name = "TIRS1", _path = "B10.tif"),
                PrepareMeasurement(_name = "TIRS2", _path = "B11.tif")
              )
              case "LANDSAT_7" => Array(
                PrepareMeasurement(_name = "blue", _path = "B1.TIF"),
                PrepareMeasurement(_name = "green", _path = "B2.TIF"),
                PrepareMeasurement(_name = "red", _path = "B3.TIF"),
                PrepareMeasurement(_name = "NIR", _path = "B4.TIF"),
                PrepareMeasurement(_name = "SWIR1", _path = "B5.TIF"),
                PrepareMeasurement(_name = "TIR1", _path = "B6_VCID_1.TIF"),
                PrepareMeasurement(_name = "TIR2", _path = "B6_VCID_2.TIF", _grid = "pan"),
                PrepareMeasurement(_name = "SWIR2", _path = "B7.TIF"),
                PrepareMeasurement(_name = "PAN", _path = "B8.TIF")
              )
            }

            this.properties.dateTime = parser.date(0) + "-" + parser.date(1) + "-" + parser.date(2) + "T" + parser.sceneCenterTime
            this.properties.fileFormat = "GeoTIFF"
            this.properties.processingDatetime = this.properties.dateTime

            val outputPath = dataset.getAbsolutePath + "/gc_metadata.json"
            this.createJson(outputPath)
          }
          if (parser.satellite.equals("LANDSAT_5") || parser.satellite.equals("LANDSAT_4")) {
            val defaultBand = dataset.listFiles().filter(_.getName.contains("B1"))
            val srcDataset = gdal.Open(defaultBand(0).getAbsolutePath, gdalconstConstants.GA_ReadOnly)
            if (srcDataset == null) {
              System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo())
              System.err.println(gdal.GetLastErrorMsg())
              System.exit(1)
            }
            val imgCrs = srcDataset.GetProjectionRef

            this.productName = parser.satellite
            this.crs = imgCrs

            this.geometry.get.geomType = "Polygon"
            this.geometry.get.coordinates = "[" +
              "[" + parser.geomULLati + ", " + parser.geomULLong + "], " +
              "[" + parser.geomURLati + ", " + parser.geomURLong + "], " +
              "[" + parser.geomLRLati + ", " + parser.geomLRLong + "], " +
              "[" + parser.geomLLLati + ", " + parser.geomLLLong + "], " +
              "[" + parser.geomULLati + ", " + parser.geomULLong + "]" +
              "]"

            this.grids.defaultShape = parser.defaultLines + ", " + parser.defaultSamples
            this.grids.defaultTransform = "30, 0, " + parser.defaultULX + ", 0, -30, " + parser.defaultULY + ", 0, 0, 1"

            this.measurements = Array(
              PrepareMeasurement(_name = "blue", _path = "B1.tif"),
              PrepareMeasurement(_name = "green", _path = "B2.tif"),
              PrepareMeasurement(_name = "red", _path = "B3.tif"),
              PrepareMeasurement(_name = "NIR", _path = "B4.tif"),
              PrepareMeasurement(_name = "SWIR1", _path = "B5.tif"),
              PrepareMeasurement(_name = "TIR", _path = "B6.tif"),
              PrepareMeasurement(_name = "SWIR2", _path = "B7.tif")
            )

            this.properties.dateTime = parser.date(0) + "-" + parser.date(1) + "-" + parser.date(2) + "T" + parser.sceneCenterTime
            this.properties.fileFormat = "GeoTIFF"
            this.properties.processingDatetime = this.properties.dateTime

            val outputPath = dataset.getAbsolutePath + "/gc_metadata.json"
            this.createJson(outputPath)
          }

        }
      }
      case "Gaofen" =>{
        val dir = new File(inputDir)
        val datasets = dir.listFiles()
        val fileLength = datasets.length
        for(i <- 0 until 1){
          val dataset = datasets(i)
          val metaFile = dataset.listFiles().filter(_.getName.contains(".xml"))
          val parser = whu.edu.cn.geocube.core.raster.ard.gaofen1.Parser(metaFile(0).getAbsolutePath)
          parser.collectMetadata()

          val defaultBand = dataset.listFiles().filter(_.getName.contains("B1"))
          val srcDataset = gdal.Open(defaultBand(0).getAbsolutePath, gdalconstConstants.GA_ReadOnly)
          if (srcDataset == null) {
            System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo())
            System.err.println(gdal.GetLastErrorMsg())
            System.exit(1)
          }
          val imgCrs = srcDataset.GetProjectionRef
          val imgGeoTrans = new Array[Double](6)
          srcDataset.GetGeoTransform(imgGeoTrans)

          this.productName = parser.platform + "_" + parser.sensor
          this.crs = imgCrs

          this.geometry.get.geomType = "Polygon"
          this.geometry.get.coordinates = "[" +
            "[" + parser.topLeftLatitude + ", " + parser.topLeftLongitude + "], " +
            "[" + parser.topRightLatitude + ", " + parser.topRightLongitude + "], " +
            "[" + parser.bottomRightLatitude + ", " + parser.bottomRightLongitude + "], " +
            "[" + parser.bottomLeftLatitude + ", " + parser.bottomLeftLongitude + "], " +
            "[" + parser.topLeftLatitude + ", " + parser.topLeftLongitude + "]" +
            "]"

          this.grids.defaultShape = parser.heightInPixels + ", " + parser.widthInPixels
          //this.grids.defaultTransform = "30, 0, " + parser.defaultULX + ", 0, -30, " + parser.defaultULY + ", 0, 0, 1"
          this.grids.defaultTransform = parser.resolution +  ", 0, " + imgGeoTrans(0) + ", 0, -" + parser.resolution + ", " + imgGeoTrans(3) + ", 0, 0, 1"

          this.measurements = Array(
            PrepareMeasurement(_name = "blue", _path = "B1.tif"),
            PrepareMeasurement(_name = "green", _path = "B2.tif"),
            PrepareMeasurement(_name = "red", _path = "B3.tif"),
            PrepareMeasurement(_name = "NIR", _path = "B4.tif")
          )

          this.properties.dateTime = parser.receiveTime
          this.properties.fileFormat = "GeoTIFF"
          this.properties.processingDatetime = parser.produceTime

          val outputPath = dataset.getAbsolutePath + "/gc_metadata.json"
          this.createJson(outputPath)
        }
      }
    }
  }

  def createJson(outputPath:String):Unit = {
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("id:", this.id)
    node.put("schema", this.schema)
    val productNode = objectMapper.createObjectNode()
    node.put("product", productNode)
    productNode.put("name", this.productName)
    node.put("crs", this.crs)

    if (!geometry.isEmpty) {
      val geometryNode = objectMapper.createObjectNode()
      node.put("geometry", geometryNode)
      geometryNode.put("type", this.geometry.get.geomType)
      geometryNode.put("coordinates", this.geometry.get.coordinates)
    }


    val gridsNode = objectMapper.createObjectNode()
    node.put("grids", gridsNode)
    val defaultNode = objectMapper.createObjectNode()
    gridsNode.put("default", defaultNode)
    val defaultShapeNode = objectMapper.createArrayNode()
    val defaultShapeParams = this.grids.defaultShape.split(",")
    for(i <- defaultShapeParams)
      defaultShapeNode.add(i.trim.toInt)
    val defaultTransformNode = objectMapper.createArrayNode()
    val defaultTransformParams = this.grids.defaultTransform.split(",")
    for(i <- defaultTransformParams)
      defaultTransformNode.add(i.trim.toDouble)
    defaultNode.put("shape", defaultShapeNode)
    defaultNode.put("transform", defaultTransformNode)
    if(!this.grids.panShape.isEmpty && !this.grids.panTransform.isEmpty ){
      val panNode = objectMapper.createObjectNode()
      gridsNode.put("pan", panNode)
      val panShapeNode = objectMapper.createArrayNode()
      val panShapeParams = this.grids.panShape.split(",")
      for(i <- panShapeParams)
        panShapeNode.add(i.trim.toInt)
      val panTransformNode = objectMapper.createArrayNode()
      val panTransformParams = this.grids.panTransform.split(",")
      for(i <- panTransformParams)
        panTransformNode.add(i.trim.toDouble)
      panNode.put("shape", panShapeNode)
      panNode.put("transform", panTransformNode)
    }


    val measurementsNode = objectMapper.createObjectNode()
    node.put("measurements", measurementsNode)
    for (measurement <- measurements) {
      val nameNode = objectMapper.createObjectNode()
      measurementsNode.put(measurement.name, nameNode)
      nameNode.put("path", measurement.path)
      if (!measurement.grid.isEmpty)
        nameNode.put("grid", measurement.grid)
      if (!measurement.bandCount.isEmpty)
        nameNode.put("band", measurement.bandCount)
      if (!measurement.layer.isEmpty)
        nameNode.put("layer", measurement.layer)
    }

    val propertiesNode = objectMapper.createObjectNode()
    node.put("properties", propertiesNode)
    propertiesNode.put("datetime", properties.dateTime)
    if (!properties.fileFormat.isEmpty)
      propertiesNode.put("file_format", properties.fileFormat)
    if (!properties.processingDatetime.isEmpty)
      propertiesNode.put("processing_datetime", properties.processingDatetime)

    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputPath), node)
  }

  def print():Unit = {
    println("id:" + id)
    println("schema:" + schema)
    println("productName:" + productName)
    println("crs:" + crs)

    if (!geometry.isEmpty) {
      println("geometry:")
      println(" type:" + geometry.get.geomType)
      println(" coordinates:" + geometry.get.coordinates)
    }

    println("grids:")
    println(" default:")
    println("   shape:" + grids.defaultShape)
    println("   transform:" + grids.defaultTransform)
    println(" pan:")
    println("   shape:" + grids.panShape)
    println("   transform:" + grids.panTransform)

    println("measurements:")
    for (measurement <- measurements) {
      println(" " + measurement.name + ":")
      println("   path:" + measurement.path)
      if (!measurement.grid.isEmpty)
        println("   grid:" + measurement.grid)
      if (!measurement.bandCount.isEmpty)
        println("   band:" + measurement.bandCount)
      if (!measurement.layer.isEmpty)
        println("   layer:" + measurement.layer)
    }

    println("properties:")
    println(" datetime:" + properties.dateTime)
    if (!properties.fileFormat.isEmpty)
      println(" file_format:" + properties.fileFormat)

    if (!properties.processingDatetime.isEmpty)
      println(" processing_datetime:" + properties.processingDatetime)
  }
}

case class PrepareMeasurement(_name:String = "", _grid:String = "", _path:String = "",
                              _bandCount:String = "", _layer:String = ""){
  var name = _name
  var grid = _grid
  var path = _path
  var bandCount = _bandCount
  var layer = _layer
}

