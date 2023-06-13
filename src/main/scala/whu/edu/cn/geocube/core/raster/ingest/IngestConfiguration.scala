package whu.edu.cn.geocube.core.raster.ingest

import java.io.FileOutputStream
import com.fasterxml.jackson.databind.ObjectMapper
import scala.io.StdIn

object IngestConfiguration{
  def main(args:Array[String]):Unit = {
    val ingestConfig = IngestConfiguration()

    //Initialize Measurements, flagsDefinition信息不对，用于测试
    val measurementsParams = Array(
      IngestMeasurement(_name = "blue", _dtype= "Float32",_nodata = "Float.NaN",_resamplingMethod = "nearest",
        _srcVarname = "blue",
        _attrslongName = "Surface Reflectance 0.45-0.52 microns (Blue)",
        _attrsAlias = "band_1"),

      IngestMeasurement(_name = "green", _dtype= "Float32",_nodata = "Float.NaN",_resamplingMethod = "nearest",
        _srcVarname = "green",
        _attrslongName = "Surface Reflectance 0.52-0.60 microns (Green)",
        _attrsAlias = "band_2")
    )

    ingestConfig.init(sourceType = "ls5_ledaps_scene", outputType = "ls5_ledaps_albers", description = "Landsat 5 LEDAPS 25 metre, 100km tile, Australian Albers Equal Area projection (EPSG:3577)",
      location = "/media/simonaoliver/datacube/tiles", file_path_template = "LS5_TM_LEDAPS/LS5_TM_LEDAPS_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.nc",
      title = "CEOS Data Cube Landsat Surface Reflectance", summary = "Landsat 5 Thematic Mapper Analysis Ready data prepared by NASA on behalf of CEOS.",
      source = "Surface reflectance from LEDAPS", platform = "LANDSAT-5", instrument = "TM",
      processingLevel = "L2", productVersion = "2.0.0", institution = "CEOS", keywords = "AU/GA", keywordsVocabulary = "GCMD",
      productSuite = "USGS Landsat", license = "https://creativecommons.org/licenses/by/4.0/",
      driver = "GeoTIFF", crs = "EPSG:3577", tileSizeX = 100000.0, tileSizeY = 100000.0, resX = 25, resY = -25, chunkingX = 200, chunkingY = 200, chunkingTime = 1,
      dimensionOrder = "['time', 'y', 'x']",
      measurements = measurementsParams
    )

    ingestConfig.print()
    ingestConfig.createIngestConfiguration("conf/ingest_configuration/Ls8_ard_ingest.json")

    println("Hit enter to exit")
    StdIn.readLine()
  }
}

case class IngestConfiguration() {
  var sourceType: String = ""
  var outputType: String = ""
  var description: String = ""
  var location: String = "" //deprecated
  var file_path_template: String = "" //deprecated
  var globalAttributes:GlobalAttributes = GlobalAttributes("", "", "", "", "", "", "", "","", "", "", "")
  var storage:Storage = Storage("", "", -1.0, -1.0, -1.0, -1.0)
  var measurements:Array[IngestMeasurement] = Array()

  case class GlobalAttributes(_title:String, _summary:String, _source:String, _platform:String, _instrument:String,
                              _processingLevel:String, _productVersion:String,
                              _institution:String = "",  _keywords:String = "", _keywordsVocabulary:String = "",
                              _productSuite:String = "", _license:String = ""){
    var title = _title
    var summary = _summary
    var source = _source
    var platform = _platform
    var instrument = _instrument
    var processingLevel = _processingLevel
    var productVersion = _productVersion
    var institution = _institution
    var keywords = _keywords
    var keywordsVocabulary = _keywordsVocabulary
    var productSuite = _productSuite
    var license = _license
  }

  case class Storage(_driver:String , _crs:String, _tileSizeX:Double, _tileSizeY:Double,
                     _resX:Double, _resY:Double, _chunkingX:Double = -1.0, _chunkingY:Double = -1.0, _chunkingTime:Double = 0.0,
                     _dimensionOrder:String = "['time', 'y', 'x']"){
    var driver = _driver
    var crs = _crs
    var tileSizeX = _tileSizeX
    var tileSizeY = _tileSizeY
    var resX = _resX
    var resY = _resY
    var chunkingX = _chunkingX
    var chunkingY = _chunkingY
    var chunkingTime = _chunkingTime
    var dimensionOrder = _dimensionOrder
  }

  def init(sourceType: String, outputType: String, description: String, location: String = "", file_path_template:String,
           title:String, summary:String, source:String, platform:String = "", instrument:String,
           processingLevel:String, productVersion:String,
           institution:String = "",  keywords:String = "", keywordsVocabulary:String = "",
           productSuite:String = "", license:String = "",
           driver:String, crs:String, tileSizeX:Double, tileSizeY:Double,
           resX:Double , resY:Double, chunkingX:Double = -1.0, chunkingY:Double = -1.0, chunkingTime:Double = 0.0,
           dimensionOrder:String = "['time', 'y', 'x']",
           measurements: Array[IngestMeasurement]): Unit = {
    this.sourceType = sourceType
    this.outputType = outputType
    this.description = description
    this.location = location
    this.file_path_template = file_path_template
    this.globalAttributes = GlobalAttributes(title, summary, source, platform, instrument,
      processingLevel, productVersion, institution,  keywords, keywordsVocabulary, productSuite, license)
    this.storage= Storage(driver, crs, tileSizeX, tileSizeY,
      resX , resY, chunkingX, chunkingY, chunkingTime, dimensionOrder)
    this.measurements = measurements
  }

  def createIngestConfiguration(outputPath:String):Unit = {
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("source_type", this.sourceType)
    node.put("output_type", this.outputType)
    node.put("description", this.description)
    node.put("location", this.location)
    node.put("file_path_template", this.file_path_template)

    val globalAttributesNode = objectMapper.createObjectNode()
    node.put("global_attributes", globalAttributesNode)
    globalAttributesNode.put("title",this.globalAttributes.title)
    globalAttributesNode.put("summary",this.globalAttributes.summary)
    globalAttributesNode.put("source",this.globalAttributes.source)
    globalAttributesNode.put("institution",this.globalAttributes.institution)
    globalAttributesNode.put("instrument",this.globalAttributes.instrument)
    globalAttributesNode.put("keywords",this.globalAttributes.keywords)
    globalAttributesNode.put("keywords_vocabulary",this.globalAttributes.keywordsVocabulary)
    globalAttributesNode.put("platform",this.globalAttributes.platform)
    globalAttributesNode.put("processing_level",this.globalAttributes.processingLevel)
    globalAttributesNode.put("product_version",this.globalAttributes.productVersion)
    globalAttributesNode.put("product_suite",this.globalAttributes.productSuite)
    globalAttributesNode.put("license",this.globalAttributes.license)

    val storageNode = objectMapper.createObjectNode()
    node.put("storage", storageNode)
    storageNode.put("driver",this.storage.driver)
    storageNode.put("crs",this.storage.crs)
    val tileSizeNode = objectMapper.createObjectNode()
    storageNode.put("tile_size", tileSizeNode)
    tileSizeNode.put("x",this.storage.tileSizeX)
    tileSizeNode.put("y",this.storage.tileSizeY)
    val resolutionNode = objectMapper.createObjectNode()
    storageNode.put("resolution", resolutionNode)
    resolutionNode.put("x",this.storage.resX)
    resolutionNode.put("y",this.storage.resY)
    if(this.storage.chunkingX != -1 && this.storage.chunkingY != -1){
      val chunkingNode = objectMapper.createObjectNode()
      storageNode.put("chunking", chunkingNode)
      chunkingNode.put("x",this.storage.chunkingX)
      chunkingNode.put("y",this.storage.chunkingY)
      chunkingNode.put("time",this.storage.chunkingTime)
    }
    storageNode.put("dimension_order",this.storage.dimensionOrder)

    for(measurement <- this.measurements){
      val measurementsNode = objectMapper.createObjectNode()
      node.put("measurements_" + measurement.name, measurementsNode)
      measurementsNode.put("name", measurement.name)
      measurementsNode.put("dtype", measurement.dtype)
      measurementsNode.put("nodata", measurement.nodata)
      measurementsNode.put("resampling_method", measurement.resamplingMethod)
      measurementsNode.put("src_varname", measurement.srcVarname)

      val attrsNode = objectMapper.createObjectNode()
      measurementsNode.put("attrs", attrsNode)
      attrsNode.put("long_name",measurement.attrslongName)
      attrsNode.put("alias",measurement.attrsAlias)
    }

    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputPath), node)

  }

  def print():Unit = {
    println("source_type:" + sourceType)
    println("output_type:" + outputType)
    println("description:" + description)
    println("location:" + location)
    println("file_path_template:" + file_path_template)

    println("global_attributes:")
    println(" title:" + this.globalAttributes.title)
    println(" summary:" + this.globalAttributes.summary)
    println(" source:" + this.globalAttributes.source)
    println(" institution:" + this.globalAttributes.institution)
    println(" instrument:" + this.globalAttributes.instrument)
    println(" keywords:" + this.globalAttributes.keywords)
    println(" keywords_vocabulary:" + this.globalAttributes.keywordsVocabulary)
    println(" platform:" + this.globalAttributes.platform)
    println(" processing_level:" + this.globalAttributes.processingLevel)
    println(" product_version:" + this.globalAttributes.productVersion)
    println(" product_suite:" + this.globalAttributes.productSuite)
    println(" license:" + this.globalAttributes.license)

    println("storage:")
    println(" driver:" + this.storage.driver)
    println(" crs:" + this.storage.crs)
    println(" tile_size:")
    println("   x:" + this.storage.tileSizeX)
    println("   y:" + this.storage.tileSizeY)
    println(" resolution:")
    println("   x:" + this.storage.resX)
    println("   y:" + this.storage.resY)
    if(this.storage.chunkingX != -1 && this.storage.chunkingY != -1){
      println(" chunking:")
      println("   x:" + this.storage.chunkingX)
      println("   y:" + this.storage.chunkingY)
      println("   time:" + this.storage.chunkingTime)
    }
    println(" dimension_order:" + this.storage.dimensionOrder)

    println("measurements: ")
    measurements.foreach{ x =>
      println(" name:" + x.name, " dtype:" + x.dtype, " nodata:" + x.nodata, " resampling_method:" + x.resamplingMethod, " src_varname:" + x.srcVarname)
      println(" attrs:")
      println("   long_name:" + x.attrslongName)
      println("   alias:" + x.attrsAlias)
    }

  }

}
case class IngestMeasurement(_name: String, _dtype: String, _nodata: String, _resamplingMethod: String,
                             _srcVarname: String, _attrslongName:String = "", _attrsAlias:String = ""){
  var name = _name
  var dtype = _dtype
  var nodata = _nodata
  var resamplingMethod = _resamplingMethod
  var srcVarname = _srcVarname
  val attrslongName = _attrslongName
  val attrsAlias = _attrsAlias
}

