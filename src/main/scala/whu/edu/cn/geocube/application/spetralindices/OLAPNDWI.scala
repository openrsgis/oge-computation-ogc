package whu.edu.cn.geocube.application.spetralindices

import java.io.File
import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.Tile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import sys.process._
import whu.edu.cn.geocube.application.spetralindices.NDWI.ndwi
import whu.edu.cn.geocube.core.entity.{QueryParams, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.raster.ingest.OlapIngestor
import whu.edu.cn.geocube.core.raster.query.{DistributedQueryRasterTiles, OlapQueryPrecomputeRasterTiles}

/**
 * Accessing database for precomputing results,
 * and on-the-fly computing if precomputing results not exist
 */
object OLAPNDWI {
  /**
   * On-the-fly computing
   *
   * @param rasterProductNames
   * @param month
   * @param year
   * @param cityName
   * @param provinceName
   * @param outputDir
   */
  def onTheFlyCompute(rasterProductNames: Array[String],
                      month: Int,
                      year: Int,
                      cityName: String,
                      provinceName: String,
                      outputDir: String): Unit = {
    val conf = new SparkConf()
      .setAppName("NDWI analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rpc.message.maxSize", "1024")
      .set("spark.driver.maxResultSize", "8g")
    val sc = new SparkContext(conf)

    //query and access
    val queryBegin = System.currentTimeMillis()
    val queryParams = new QueryParams
    queryParams.setRasterProductNames(rasterProductNames)
    queryParams.setMonth(month)
    queryParams.setYear(year)
    queryParams.setCityName(cityName)
    queryParams.setProvinceName(provinceName)
    queryParams.setLevel("4000") //分辨率
    queryParams.setHighLevelMeasurement("NDWI") //NDWI分析固定
    val tileLayerRddWithMeta:(RDD[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = DistributedQueryRasterTiles.getRasterTileRDD(sc, queryParams, olap = true)
    val queryEnd = System.currentTimeMillis()

    //ndwi
    val analysisBegin = System.currentTimeMillis()
    ndwi(tileLayerRddWithMeta, 0.01, outputDir)
    val analysisEnd = System.currentTimeMillis()

    println("Query time: " + (queryEnd - queryBegin))
    println("Analysis time: " + (analysisEnd - analysisBegin))
  }

  /**
   * Accessing database for precomputing results,
   * and on-the-fly computing if precomputing results not exist.
   * If $onTheFlyIngest is true, the results of on-the-fly computation
   * will be ingested to database and vice versa.
   */
  def main(args: Array[String]): Unit = {
    /***Using distributed initiated raster tile as input***/
    //Parse the web request params
    val rasterProductNames = args(0).split(",") //必须
    val cityName = args(1) //必须
    val provinceName = if(args(2).equals("null")) "" else args(2)
    val month = if(args(3).equals("null")) -1 else args(3).toInt
    val year = args(4).toInt //必须
    val outputDir = args(5) //必须
    val onTheFlyIngest = false

    println("rasterProductName: " + rasterProductNames.foreach(x=>print(x + "|")))
    println("cityName: " + cityName)
    println("provinceName: " + provinceName)
    println("year: " + year)
    println("month: " + month)

    if(cityName != "" && month != -1){
      val results = OlapQueryPrecomputeRasterTiles.cityMonthHighlevelmeasurement(rasterProductNames, cityName, month, "NDWI", outputDir)
      if(results != null){
        //The SparkContext is created for progress bar only
        val conf = new SparkConf()
          .setAppName("NDWI analysis")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rpc.message.maxSize", "1024")
          .set("spark.driver.maxResultSize", "8g")
        val sc = new SparkContext(conf)
        sc.stop()
      }else{
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        onTheFlyCompute(rasterProductNames, month, year, cityName, provinceName, outputDir)
        if(onTheFlyIngest){
          val preComputeDir = "/home/geocube/kernel/geocube-core/v2/olapndwi/" + month + "_" + cityName.toLowerCase + "/"
          val file = new File(preComputeDir)
          if (!file.exists())file.mkdir
          val cpCommond = "cp " + outputDir + "* " + preComputeDir
          Seq("sh", "-c", cpCommond).!
          OlapIngestor.cityMonthHighlevelmeasurement(rasterProductNames(0), cityName, month, "NDWI", preComputeDir)
        }
      }
    }else if(cityName != "" && month == -1 && year != -1){
      val results = OlapQueryPrecomputeRasterTiles.cityYearHighlevelmeasurement(rasterProductNames, cityName, year, "NDWI", outputDir)
      if(results != null){
        //The SparkContext is created for progress bar only
        val conf = new SparkConf()
          .setAppName("NDWI analysis")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rpc.message.maxSize", "1024")
          .set("spark.driver.maxResultSize", "8g")
        val sc = new SparkContext(conf)
        sc.stop()
      }else{
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        onTheFlyCompute(rasterProductNames, month, year, cityName, provinceName, outputDir)
        if(onTheFlyIngest){
          val preComputeDir = "/home/geocube/kernel/geocube-core/v2/olapndwi/" + year + "_" + cityName.toLowerCase + "/"
          val file = new File(preComputeDir)
          if (!file.exists())file.mkdir
          val cpCommond = "cp " + outputDir + "* " + preComputeDir
          Seq("sh", "-c", cpCommond).!
          OlapIngestor.cityYearHighlevelmeasurement(rasterProductNames(0), cityName, year, "NDWI", preComputeDir)
        }
      }
    }else if(cityName == "" && provinceName != "" && month != -1){
      val results = OlapQueryPrecomputeRasterTiles.provinceMonthHighlevelmeasurement(rasterProductNames, provinceName, month, "NDWI" ,outputDir)
      if(results != null){
        //The SparkContext is created for progress bar only
        val conf = new SparkConf()
          .setAppName("NDWI analysis")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rpc.message.maxSize", "1024")
          .set("spark.driver.maxResultSize", "8g")
        val sc = new SparkContext(conf)
        sc.stop()
      }else{
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        onTheFlyCompute(rasterProductNames, month, year, cityName, provinceName, outputDir)
        if(onTheFlyIngest){
          val preComputeDir = "/home/geocube/kernel/geocube-core/v2/olapndwi/" + month + "_" + provinceName.toLowerCase + "/"
          val file = new File(preComputeDir)
          if (!file.exists())file.mkdir
          val cpCommond = "cp " + outputDir + "* " + preComputeDir
          Seq("sh", "-c", cpCommond).!
          OlapIngestor.provinceMonthHighlevelmeasurement(rasterProductNames(0), provinceName, month, "NDWI", preComputeDir)
        }
      }
    }else if(cityName == "" && provinceName != "" && month == -1 && year != -1){
      val results = OlapQueryPrecomputeRasterTiles.provinceYearHighlevelmeasurement(rasterProductNames, provinceName, year, "NDWI" ,outputDir)
      if(results != null){
        //The SparkContext is created for progress bar only
        val conf = new SparkConf()
          .setAppName("NDWI analysis")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rpc.message.maxSize", "1024")
          .set("spark.driver.maxResultSize", "8g")
        val sc = new SparkContext(conf)
        sc.stop()
      }else{
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        println("######## On-The-Fly ########")
        onTheFlyCompute(rasterProductNames, month, year, cityName, provinceName, outputDir)
        if(onTheFlyIngest){
          val preComputeDir = "/home/geocube/kernel/geocube-core/v2/olapndwi/" + year + "_" + provinceName.toLowerCase + "/"
          val file = new File(preComputeDir)
          if (!file.exists())file.mkdir
          val cpCommond = "cp " + outputDir + "* " + preComputeDir
          Seq("sh", "-c", cpCommond).!
          OlapIngestor.provinceYearHighlevelmeasurement(rasterProductNames(0), provinceName, year, "NDWI", preComputeDir)
        }
      }
    }else{
      throw new RuntimeException("Unresolved OLAP parameters!")
    }


  }

}
