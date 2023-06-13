package whu.edu.cn.geocube.core.raster.ingest

import java.io.FileOutputStream
import com.fasterxml.jackson.databind.ObjectMapper
import scala.io.StdIn

object ProductDefinition{
  def main(args:Array[String]):Unit = {
    val productDefinition = ProductDefinition()

    //Initialize Measurements, flagsDefinition信息不对，用于测试
    val measurements = Array(
      Measurement(_name = "band1", _dtype= "Float32",_nodata = "Float.NaN",_units = "1", _aliases = "[band_1, coastal]",
        _wavelength = "[427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459]",
        _response = "[7.3e-05, 0.000609, 0.001628, 0.003421, 0.008019, 0.024767, 0.085688, 0.254149, 0.517821, 0.765117, 0.908749, 0.958204, 0.977393, 0.98379, 0.989052, 0.986713, 0.993683, 0.993137, 1.0, 0.996969, 0.98278, 0.972692, 0.905808, 0.745606, 0.471329, 0.226412, 0.09286, 0.036603, 0.014537, 0.005829, 0.002414, 0.000984, 0.000255]",
        _flagsDefinitions = Array(FlagsDefinition(_name = "fmask", _bits = "[0,1,2,3,4,5,6,7]", _description = "Fmask", _values = "0:clear, 1: water", _flagType = "true"))),

      Measurement(_name = "band2", _dtype= "Float32",_nodata = "Float.NaN",_units = "1", _aliases = "[band_2, blue]",
        _wavelength = "[436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528]",
        _response = "[1.0e-05, 6.1e-05, 0.000117, 0.000241, 0.000349, 0.000455, 0.000756, 0.001197, 0.00207, 0.003712, 0.006869, 0.013212, 0.02717, 0.058606, 0.130876, 0.27137, 0.493542, 0.723971, 0.85751, 0.894222, 0.903034, 0.910928, 0.90988, 0.899475, 0.897977, 0.889667, 0.883542, 0.877453, 0.881011, 0.874721, 0.879688, 0.886569, 0.891913, 0.88768, 0.861157, 0.848533, 0.840828, 0.828339, 0.844202, 0.865864, 0.868497, 0.890253, 0.912538, 0.910385, 0.918822, 0.931726, 0.931813, 0.954248, 0.955545, 0.96242, 0.956424, 0.953352, 0.978564, 0.989104, 0.985615, 0.989469, 0.982262, 0.968801, 0.967332, 0.976836, 0.988729, 0.980826, 0.967361, 0.954754, 0.964132, 0.966125, 0.966772, 0.981834, 0.98232, 0.965685, 0.963135, 0.972261, 0.996498, 1.0, 0.9556, 0.844893, 0.534592, 0.190738, 0.048329, 0.013894, 0.005328, 0.002611, 0.001557, 0.0011, 0.000785, 0.000516, 0.000321, 0.000162, 7.2e-05, 5.7e-05, 2.3e-05, 3.2e-05, -1.6e-05]\n    - name: '3'",
        _flagsDefinitions = Array(
          FlagsDefinition(_name = "blue_saturated", _bits = "0", _description = "Blue band is saturated", _values = "{0: true, 1: false}", _flagType = "false"),
          FlagsDefinition(_name = "Green_saturated", _bits = "1", _description = "Green band is saturated", _values = "{0: true, 1: false}", _flagType = "false"))
      )
    )

    /**Initialize Other information**/
    productDefinition.init(name = "ls8_ard_data", description = "Landsat 8 ARD Product", metadataType = "Analysis Ready Data",
      platformCode = "LANDSAT_8", instrumentName = "OLI_TIRS", productType = "satellite_ard_product", formatName = "GeoTiff",
      crs = "EPSG:32651", resX = 30.0, resY = 30.0,
      measurements = measurements)

    productDefinition.print()

    productDefinition.createProductDefinition("conf/product_definition/Ls8_ard.json")



    println("Hit enter to exit")
    StdIn.readLine()
  }

}

case class ProductDefinition() {
  var name: String = ""
  var description: String = ""
  var metadataType: String = ""
  var license: String = ""
  var metadata:Metadata = Metadata("", "", "", "")
  var storage:Option[Storage] = None
  var measurements:Array[Measurement] = Array()

  case class Metadata(_platformCode:String, _instrumentName:String, _productType:String, _formatName:String){
    var platformCode = _platformCode
    var instrumentName = _instrumentName
    var productType = _productType
    var formatName = _formatName
  }
  case class Storage(_crs:String = "", _resLongitude:Double = -1.0, _resLatitude:Double = -1.0, _resX:Double = -1.0, _resY:Double = -1.0){
    var crs = _crs
    var resLongitude = _resLongitude
    var resLatitude = _resLatitude
    var resX = _resX
    var resY = _resY

    def getOptional():Option[Storage] = {
      if(this.equals(Storage("", -1.0, -1.0, -1.0, -1.0)))
        None
      else
        Some(this)
    }

    override def equals(obj: Any): Boolean = {
      (this.crs == obj.asInstanceOf[Storage].crs) &&
        (this.resLongitude == obj.asInstanceOf[Storage].resLongitude) &&
        (this.resLatitude == obj.asInstanceOf[Storage].resLatitude)
    }
  }

  def init(name: String, description: String, metadataType: String, license: String = "",
           platformCode:String, instrumentName:String, productType:String, formatName:String,
           crs:String = "", resLongitude:Double = -1.0, resLatitude:Double = -1.0, resX:Double = -1.0, resY:Double = 1.0,
           measurements: Array[Measurement]): Unit = {
    this.name = name
    this.description = description
    this.metadataType = metadataType
    this.license = license
    this.metadata = Metadata(platformCode, instrumentName, productType, formatName)
    this.storage = Storage(crs, resLongitude, resLatitude, resX, resY).getOptional()
    this.measurements = measurements
  }

  def createProductDefinition(outputPath:String):Unit = {
    val objectMapper =new ObjectMapper()
    val node = objectMapper.createObjectNode()
    node.put("name", this.name)
    node.put("description", this.description)
    node.put("metadata_type", this.metadataType)
    node.put("license", this.license)

    //metadata
    val metadataNode = objectMapper.createObjectNode()
    node.put("metadata", metadataNode)
    val platformNode = objectMapper.createObjectNode()
    metadataNode.put("plaform", platformNode)
    platformNode.put("code",this.metadata.platformCode)
    val instrumentNode = objectMapper.createObjectNode()
    metadataNode.put("instrument", instrumentNode)
    instrumentNode.put("name",this.metadata.instrumentName)
    metadataNode.put("product_type", this.metadata.productType)
    val formatNode = objectMapper.createObjectNode()
    metadataNode.put("format", formatNode)
    formatNode.put("name",this.metadata.formatName)

    //storage
    if(!this.storage.isEmpty){
      val storageNode = objectMapper.createObjectNode()
      node.put("storage", storageNode)
      storageNode.put("crs", this.storage.get.crs)
      val resolutionNode = objectMapper.createObjectNode()
      storageNode.put("resolution", resolutionNode)
      resolutionNode.put("x",this.storage.get.resX)
      resolutionNode.put("y",this.storage.get.resY)
    }

    //measurement
    for(measurement <- this.measurements){
      val measurementsNode = objectMapper.createObjectNode()
      node.put("measurements_" + measurement.name, measurementsNode)
      measurementsNode.put("name", measurement.name)
      measurementsNode.put("dtype", measurement.dtype)
      measurementsNode.put("nodata", measurement.nodata)
      measurementsNode.put("units", measurement.units)
      measurementsNode.put("aliases", measurement.aliases)

      if(!measurement.spectralDefinition.isEmpty){
        val spectralDefinitionNode = objectMapper.createObjectNode()
        measurementsNode.put("spectral_definition", spectralDefinitionNode)
        spectralDefinitionNode.put("wavelength",measurement.spectralDefinition.get.wavelength)
        spectralDefinitionNode.put("response",measurement.spectralDefinition.get.response)
      }

      if(!measurement.flagsDefinitions(0).getOptional().isEmpty) {
        for(flagsDefinition <- measurement.flagsDefinitions){
          val flagsDefinitionNode = objectMapper.createObjectNode()
          measurementsNode.put("flags_definition_" + flagsDefinition.name, flagsDefinitionNode)
          val nameNode = objectMapper.createObjectNode()
          flagsDefinitionNode.put(flagsDefinition.name, nameNode)
          nameNode.put("bits",flagsDefinition.bits)
          nameNode.put("description",flagsDefinition.description)
          if(!flagsDefinition.flagType.toBoolean) nameNode.put("values",flagsDefinition.values)
          else{
            val valueNode = objectMapper.createObjectNode()
            nameNode.put("values", valueNode)
            val pairs = flagsDefinition.values.split(",")
            for(pair <- pairs){
              val tmp = pair.split(":")
              valueNode.put(tmp(0), tmp(1))
            }
          }

        }
      }
    }

    objectMapper.writerWithDefaultPrettyPrinter().writeValue(new FileOutputStream(outputPath), node)

  }

  def print():Unit = {
    println("name:" + name)
    println("description:" + description)
    println("metadata_type:" + metadataType)
    println("license:" + license)
    println("metadata:")
    println(" platform:")
    println("   code:" + metadata.platformCode)
    println(" instrument:")
    println("   name:" + metadata.instrumentName)
    println(" product_type:" + metadata.productType)
    println(" format:")
    println("   name:" + metadata.formatName)

    if(!storage.isEmpty){
      println("storage:")
      println(" crs:" + storage.get.crs)
      println(" resolution: ")

      if(!(storage.get.resLongitude == -1)){
        println("   longitude:" + storage.get.resLongitude)
        println("   latitude:" + storage.get.resLatitude)
      }
      if(!(storage.get.resX == -1)){
        println("   x:" + storage.get.resX)
        println("   y:" + storage.get.resY)
      }

    }

    println("measurements: ")
    measurements.foreach{ x =>
      println(" name:" + x.name, " dtype:" + x.dtype, " nodata:" + x.nodata, " units:" + x.units, " aliases:" + x.aliases)
      if(!x.spectralDefinition.isEmpty){
        println("spectral_definition:")
        println(" wavelength:" + x.spectralDefinition.get.wavelength)
        println(" response:" + x.spectralDefinition.get.response)
      }
      if(!x.flagsDefinitions(0).getOptional().isEmpty) {
        println("flags_definition: ")
        for(i <- x.flagsDefinitions){
          println(" " + i.name)
          println("   bits:" + i.bits)
          println("   description:" + i.description)
          println("   values:" + i.values)
        }
      }


    }

  }

}
case class Measurement(_name: String, _dtype: String, _nodata: String, _units: String, _aliases: String,
                       _wavelength:String = "", _response:String = "",
                       _flagsDefinitions: Array[FlagsDefinition]){
  var name = _name
  var dtype = _dtype
  var nodata = _nodata
  var units = _units
  var aliases = _aliases
  val wavelength = _wavelength
  val response = _response
  val spectralDefinition = SpectralDefinition(wavelength,response).getOptional()
  val flagsDefinitions = _flagsDefinitions
  case class SpectralDefinition(_wavelength:String, _response:String){
    val wavelength = _wavelength
    val response = _response

    def getOptional():Option[SpectralDefinition] = {
      if(this.wavelength.isEmpty || this.response.isEmpty)
        None
      else
        Some(SpectralDefinition(wavelength, response))
    }
  }
}

case class FlagsDefinition(_name:String, _bits: String, _description: String, _values: String, _flagType: String){
  var name = _name
  var bits = _bits
  var description = _description
  val values = _values
  val flagType = _flagType

  def getOptional():Option[FlagsDefinition] = {
    if(this.equals(FlagsDefinition("", "", "", "", "")))
      None
    else
      Some(this)
  }

  override def equals(obj: Any): Boolean = {
    (this.bits == obj.asInstanceOf[FlagsDefinition].bits) &&
      (this.description == obj.asInstanceOf[FlagsDefinition].description) &&
      (this.values == obj.asInstanceOf[FlagsDefinition].values) && (this.flagType == obj.asInstanceOf[FlagsDefinition].flagType)
  }
}


