package whu.edu.cn.geocube.application.spatialjoin

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import geotrellis.layer.{LayoutDefinition, SpaceTimeKey, SpatialKey}
import geotrellis.raster.TileLayout
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import java.io.{File, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.WKTReader
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import whu.edu.cn.geocube.core.cube.vector.CompuIntensityPartitioner
import whu.edu.cn.geocube.core.entity.QueryParams
import whu.edu.cn.geocube.core.vector.grid.GridConf
import whu.edu.cn.geocube.core.cube.vector.GeoObject
import whu.edu.cn.geocube.core.vector.query.DistributedQueryVectorObjects._
import whu.edu.cn.geocube.util.HbaseUtil.getVectorMeta
import whu.edu.cn.util.PostgresqlUtil
import whu.edu.cn.geocube.util.PostgresqlService

/**
 * Geospatial data especially vector are always irregular and heterogeneous,
 * which can easily result in serious load imbalance and poor parallel performance.
 *
 * This class uses an AI-based approach in [1] to optimize intersection on vector data.
 *
 * [1] Yue P, Gao F, Shangguan B Y and Yan Z R. 2020. A machine learning approach for
 *     predicting computational intensity and domain decomposition in parallel geoprocessing.
 *     International Journal of Geographical Information Science. 34(11): 2243-2274
 *     [DOI: 10.1080/13658816.2020.1730850]
 */
object Intersection {
  def main(args: Array[String]):Unit = {
    /****Distributed query gridLayerGeoObjectRdd, has accessed GeoObject before sampleGeneration() function****/
    /*val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val queryParams = new QueryParams()
    queryParams.setVectorProductName("OSM_Buildings")
    queryParams.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")
    val gridLayerGeoObjectRdd1: RDD[(SpaceTimeKey, Iterable[GeoObject])] = getGeoObjectsRDD(sc, queryParams, duplicated = false)
    queryParams.setVectorProductName("OSM_Landuse")
    val gridLayerGeoObjectRdd2: RDD[(SpaceTimeKey, Iterable[GeoObject])] = getGeoObjectsRDD(sc, queryParams, duplicated = false)

    val gridCIRdd: RDD[(SpatialKey, (Int, Int, Long))] = sampleGeneration(gridLayerGeoObjectRdd1, gridLayerGeoObjectRdd2)

    val gridCIArray = gridCIRdd.collect()
    gridCIArray.foreach(grid => println(grid._1.toString, grid._2._1, grid._2._2, grid._2._3))*/

    /****Distributed query gridLayerGeoObjectKeysRdd, will access GeoObject in sampleGeneration() function****/
    /*---------下面两步用于样本生成和模型训练, 可提前做好---------*/
    val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val queryParams = new QueryParams()
    queryParams.setVectorProductName("OSM_Buildings7") //queryParams.setVectorProductName("OSM_Buildings")
    queryParams.setExtent(66.000, 1.000, 136.000, 54.000) //queryParams.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams.setTime("2020-10-01 02:30:59.415", "2020-11-01 02:30:59.41") //queryParams.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")
    val gridLayerGeoObjectKeysRdd1: RDD[(SpatialKey, Array[String])] = getGeoObjectKeysRDD(sc, queryParams, duplicated = true)
    queryParams.setVectorProductName("OSM_Landuse")
    val gridLayerGeoObjectKeysRdd2: RDD[(SpatialKey, Array[String])] = getGeoObjectKeysRDD(sc, queryParams, duplicated = true)

    val joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      gridLayerGeoObjectKeysRdd1.join(gridLayerGeoObjectKeysRdd2)
    /*** 1.样本生成 ***/
    sampleGenerator(joinKeysRdd, "/home/geocube/environment_test/AI/osm_china_india_buildings_landuse_sample1.txt")

    /*** 2.模型生成: Python-sklearn生成 ***/

    /*---------下面三步导入新数据计算强度信息, 第4步尚未自动化---------*/
    /*val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val queryParams = new QueryParams()
    queryParams.setVectorProductName("OSM_Buildings7") //queryParams.setVectorProductName("OSM_Buildings")
    queryParams.setExtent(66.000, 1.000, 136.000, 54.000) //queryParams.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams.setTime("2020-10-01 02:30:59.415", "2020-11-01 02:30:59.41") //queryParams.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")
    val gridLayerGeoObjectAndProductFactKeysRdd1: RDD[(SpatialKey, ((String, String), Array[String]))] = getGeoObjectAndProductFactKeysRDD(sc, queryParams, duplicated = true)
    queryParams.setVectorProductName("OSM_Landuse")
    val gridLayerGeoObjectAndProductFactKeysRdd2: RDD[(SpatialKey, ((String, String), Array[String]))] = getGeoObjectAndProductFactKeysRDD(sc, queryParams, duplicated = true)

    val joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      gridLayerGeoObjectAndProductFactKeysRdd1.map(x=>(x._1, x._2._2)).join(gridLayerGeoObjectAndProductFactKeysRdd2.map(x=>(x._1, x._2._2)))*/

    /*** 3.网格特征生成(网格通过zorder标识) ***/
    //featureGenerator(joinKeysRdd, "/home/geocube/environment_test/AI/osm_china_india_buildings_landuse_more_zorderWithFeature.txt")

    /*** 4.网格计算强度预测(网格通过zorder标识): Python-sklearn生成,可采用scala调用python脚本 ***/

    /*** 5.网格计算强度信息入库(根据zorder标识匹配网格) ***/
    /*val gridZOrderCodeWithProductFactKeyRdd1: RDD[(BigInt, (String, String))] = gridLayerGeoObjectAndProductFactKeysRdd1.map{x =>
      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      val zorderCode: BigInt = keyIndex.toIndex(x._1)
      val productKey: String = x._2._1._1
      val factKey: String = x._2._1._2
      (zorderCode, (productKey, factKey))
    }
    val gridZOrderCodeWithProductFactKeyRdd2: RDD[(BigInt, (String, String))] = gridLayerGeoObjectAndProductFactKeysRdd2.map{x =>
      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      val zorderCode: BigInt = keyIndex.toIndex(x._1)
      val productKey: String = x._2._1._1
      val factKey: String = x._2._1._2
      (zorderCode, (productKey, factKey))
    }
    compuIntensityIngester(sc, gridZOrderCodeWithProductFactKeyRdd1, gridZOrderCodeWithProductFactKeyRdd2,
      "/home/geocube/environment_test/AI/osm_china_india_buildings_landuse_zorderWithCompuIntensity1.txt")*/

    /*---------下面为根据预测的计算强度进行重分区分析测试---------*/
    /***测试不重分区 VS 读取存储于文件系统上预测的计算强度重分区***/
    /*val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val queryParams = new QueryParams()
    queryParams.setVectorProductName("OSM_Buildings7") //queryParams.setVectorProductName("OSM_Buildings")
    queryParams.setExtent(66.000, 1.000, 136.000, 54.000) //queryParams.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams.setTime("2020-10-01 02:30:59.415", "2020-11-01 02:30:59.41") //queryParams.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")
    val gridLayerGeoObjectKeysRdd1: RDD[(SpatialKey, Array[String])] = getGeoObjectKeysRDD(sc, queryParams, duplicated = true)
    queryParams.setVectorProductName("OSM_Landuse")
    val gridLayerGeoObjectKeysRdd2: RDD[(SpatialKey, Array[String])] = getGeoObjectKeysRDD(sc, queryParams, duplicated = true)

    val joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      gridLayerGeoObjectKeysRdd1.join(gridLayerGeoObjectKeysRdd2)

    val file = Source.fromFile("/home/geocube/environment_test/AI/osm_china_india_buildings_landuse_more_zorderWithCompuIntensity.txt")
    val gridZOrderCodeWithCI: Array[(BigInt, Long)] = file.getLines().map{ line =>
      val arr = line/*.substring(line.indexOf("(") + 1, line.indexOf(")"))*/.split(",")
      val zorderCode = BigInt(arr(0).toInt)
      val compuIntesity = arr(1).toDouble.toLong
      (zorderCode, compuIntesity)
    }.toArray
    file.close

    val intersectionRdd: RDD[Geometry] = intersection(joinKeysRdd, gridZOrderCodeWithCI)
    println("intersection results sum: " + intersectionRdd.count())*/

    /***测试不重分区 VS 读取存储于数据库上预测的计算强度重分区效果***/
    /*val conf = new SparkConf()
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val queryParams1 = new QueryParams()
    queryParams1.setVectorProductName("OSM_Buildings5") //queryParams1.setVectorProductName("OSM_Buildings")
    queryParams1.setExtent(68.000, 13.000, 136.000, 54.000) //queryParams1.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams1.setTime("2020-10-01 02:30:59.415", "2020-11-01 02:30:59.41") //queryParams1.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")

    val queryParams2 = new QueryParams()
    queryParams2.setVectorProductName("OSM_Landuse")
    queryParams2.setExtent(68.000, 13.000, 136.000, 54.000) //queryParams1.setExtent(70.000, 15.000, 136.000, 54.000)
    queryParams2.setTime("2020-10-01 02:30:59.415", "2020-11-01 02:30:59.41") //queryParams1.setTime("2020-08-01 02:30:59.415", "2020-09-01 02:30:59.41")

    val gridLayerGeoObjectKeysAndCompuIntensityRdd1: RDD[(SpatialKey, (Array[String], Long))] = getGeoObjectKeysAndCompuIntensityRDD(sc, queryParams1, duplicated = true, processName = "intersection", queryParams2)
    val gridLayerGeoObjectKeysRdd2: RDD[(SpatialKey, Array[String])] = getGeoObjectKeysRDD(sc, queryParams2, duplicated = true)

    val gridZOrderCodeWithCI: Array[(BigInt, Long)] = gridLayerGeoObjectKeysAndCompuIntensityRdd1.map{x =>
      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      (keyIndex.toIndex(x._1), x._2._2)
    }.collect()

    val joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      gridLayerGeoObjectKeysAndCompuIntensityRdd1.map(x=>(x._1, x._2._1)).join(gridLayerGeoObjectKeysRdd2)

    val intersectionRdd: RDD[Geometry] = intersection(joinKeysRdd, gridZOrderCodeWithCI)
    println("intersection results sum: " + intersectionRdd.count())*/
  }

  /**
   * Generate computational intensity samples.
   * Using GeoObject keys as input and read GeoObject by keys in the function.
   *
   * @param joinKeysRdd
   * @param samplePath
   */
  def sampleGenerator(joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))], samplePath: String): Unit = {
    //获取feature json后转为Geometry
    /*val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry], Long))] = joinKeysRdd.map{ ele =>
      val gridReadBegin = System.currentTimeMillis()
      val spatialKey = ele._1
      val fjson = new FeatureJSON()
      val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
        val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
        val feature: SimpleFeature = fjson.readFeature(featureStr)
        feature.getDefaultGeometry.asInstanceOf[Geometry]
      }.filter(_.isValid)
      val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
        val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
        val feature: SimpleFeature = fjson.readFeature(featureStr)
        feature.getDefaultGeometry.asInstanceOf[Geometry]
      }.filter(_.isValid)
      val gridReadEnd = System.currentTimeMillis()
      val readTimeCost = gridReadEnd - gridReadBegin
      (spatialKey, (geometries1, geometries2, readTimeCost))
    }.filter(ele => (ele._2._1.length != 0) && (ele._2._2.length != 0))*/

    //获取wkt后转为Geometry
    val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry], Long))] = joinKeysRdd.map{ ele =>
      val gridReadBegin = System.currentTimeMillis()
      val spatialKey = ele._1
      val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      val gridReadEnd = System.currentTimeMillis()
      val readTimeCost = gridReadEnd - gridReadBegin
      (spatialKey, (geometries1, geometries2, readTimeCost))
    }.filter(ele => (ele._2._1.length != 0) && (ele._2._2.length != 0))

    val zorderWithSampleRdd: RDD[(BigInt, Array[String])] = joinRdd.map{ ele =>
      val spatialKey = ele._1
      val geomArray1 = ele._2._1
      val geomArray2 = ele._2._2
      val readTimeCost = ele._2._3
      val geomsSum1 = geomArray1.size
      val geomsSum2 = geomArray2.size
      val geomVerticesSum1 = geomArray1.map(_.getNumPoints).sum
      val geomVerticesSum2 = geomArray2.map(_.getNumPoints).sum
      val centroPoints1:Array[Point] = geomArray1.map(_.getCentroid)
      val centroPoints2:Array[Point] = geomArray1.map(_.getCentroid)
      val centroPointsX1:Array[Double] = centroPoints1.map(_.getX)
      val centroPointsY1:Array[Double] = centroPoints1.map(_.getY)
      val centroPointsX2:Array[Double] = centroPoints2.map(_.getX)
      val centroPointsY2:Array[Double] = centroPoints2.map(_.getY)
      val averageX1:Double = centroPointsX1.sum/geomsSum1
      val averageY1:Double = centroPointsY1.sum/geomsSum1
      val averageX2:Double = centroPointsX2.sum/geomsSum2
      val averageY2:Double = centroPointsY2.sum/geomsSum2
      val variance1 = centroPoints1.map( point =>
        (point.getX - averageX1) * (point.getX - averageX1) + (point.getY - averageY1) * (point.getY - averageY1)).sum / (geomsSum1 + 1)
      val variance2 = centroPoints2.map( point =>
        (point.getX - averageX2) * (point.getX - averageX2) + (point.getY - averageY2) * (point.getY - averageY2)).sum / (geomsSum2 + 1)
      val avgPointDistance = Math.sqrt((averageX1 - averageX2) * (averageX1 - averageX2) + (averageY1 - averageY2) * (averageY1 - averageY2))
      var referencePoint:Int = 0

      val gridIntersectionBegin = System.currentTimeMillis()
      val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
      val tl = TileLayout(360, 180, 4000, 4000)
      val ld = LayoutDefinition(extent, tl)
      val gridExtent = spatialKey.extent(ld)
      geomArray1.foreach { geom1 =>
        val envelope1 = geom1.getEnvelopeInternal
        geomArray2.foreach { geom2 =>
          val envelope2 = geom2.getEnvelopeInternal
          val envIntersection = envelope1.intersection(envelope2)
          if (gridExtent.contains(envIntersection.getMinX, envIntersection.getMaxY)) {
            referencePoint += 1
            if (geom1.intersects(geom2)) {
              val geomIntersection = geom1.intersection(geom2)
            }
          }
        }
      }

      val gridIntersectionEnd = System.currentTimeMillis()
      val intersectionTimeCost = gridIntersectionEnd - gridIntersectionBegin
      val readAndIntersectionTimeCost = intersectionTimeCost + readTimeCost
      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      (keyIndex.toIndex(spatialKey), Array(geomsSum1.toString, geomsSum2.toString, geomVerticesSum1.toString, geomVerticesSum2.toString,
        variance1.toString, variance2.toString, avgPointDistance.toString, referencePoint.toString, readAndIntersectionTimeCost.toString))
    }

    val writer = new PrintWriter(new File(samplePath))
    zorderWithSampleRdd.collect().foreach{ zorderWithSample =>
      val sample = zorderWithSample._2
      writer.println(sample(0) + "  " + sample(1) + "  " + sample(2) + "  " + sample(3) + "  "
        + sample(4) + "  " + sample(5) + "  " + sample(6) + "  " + sample(7) + "  " + sample(8))
    }
    writer.close()
  }

  /**
   * Generate grid features.
   * Using GeoObject keys as input and read GeoObject by keys in the function.
   *
   * @param joinKeysRdd
   * @param featurePath
   */
  def featureGenerator(joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))], featurePath:String): Unit = {
    val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry]))] = joinKeysRdd.map{ ele =>
      val spatialKey = ele._1
      val fjson = new FeatureJSON()
      val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
        val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
        val feature: SimpleFeature = fjson.readFeature(featureStr)
        feature.getDefaultGeometry.asInstanceOf[Geometry]
      }.filter(_.isValid)
      val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
        val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
        val feature: SimpleFeature = fjson.readFeature(featureStr)
        feature.getDefaultGeometry.asInstanceOf[Geometry]
      }.filter(_.isValid)
      (spatialKey, (geometries1, geometries2))
    }

    /*val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry]))] = joinKeysRdd.map{ ele =>
      val spatialKey = ele._1
      val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      (spatialKey, (geometries1, geometries2))
    }*/

    val zorderWithFeatureRdd: RDD[(BigInt, Array[String])] = joinRdd.map{ ele =>
      val spatialKey = ele._1
      val geomArray1 = ele._2._1
      val geomArray2 = ele._2._2
      val geomsSum1 = geomArray1.size
      val geomsSum2 = geomArray2.size
      val geomVerticesSum1 = geomArray1.map(_.getNumPoints).sum
      val geomVerticesSum2 = geomArray2.map(_.getNumPoints).sum
      val centroPoints1:Array[Point] = geomArray1.map(_.getCentroid)
      val centroPoints2:Array[Point] = geomArray1.map(_.getCentroid)
      val centroPointsX1:Array[Double] = centroPoints1.map(_.getX)
      val centroPointsY1:Array[Double] = centroPoints1.map(_.getY)
      val centroPointsX2:Array[Double] = centroPoints2.map(_.getX)
      val centroPointsY2:Array[Double] = centroPoints2.map(_.getY)
      val averageX1:Double = centroPointsX1.sum/geomsSum1
      val averageY1:Double = centroPointsY1.sum/geomsSum1
      val averageX2:Double = centroPointsX2.sum/geomsSum2
      val averageY2:Double = centroPointsY2.sum/geomsSum2
      val variance1 = centroPoints1.map( point =>
        (point.getX - averageX1) * (point.getX - averageX1) + (point.getY - averageY1) * (point.getY - averageY1)).sum / (geomsSum1 + 1)
      val variance2 = centroPoints2.map( point =>
        (point.getX - averageX2) * (point.getX - averageX2) + (point.getY - averageY2) * (point.getY - averageY2)).sum / (geomsSum2 + 1)
      val avgPointDistance = Math.sqrt((averageX1 - averageX2) * (averageX1 - averageX2) + (averageY1 - averageY2) * (averageY1 - averageY2))
      var referencePoint:Int = 0

      val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
      val tl = TileLayout(360, 180, 4000, 4000)
      val ld = LayoutDefinition(extent, tl)
      val gridExtent = spatialKey.extent(ld)
      geomArray1.foreach { geom1 =>
        val envelope1 = geom1.getEnvelopeInternal
        geomArray2.foreach { geom2 =>
          val envelope2 = geom2.getEnvelopeInternal
          val envIntersection = envelope1.intersection(envelope2)
          if (gridExtent.contains(envIntersection.getMinX, envIntersection.getMaxY)) {
            referencePoint += 1
          }
        }
      }

      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      //5 features left after feature selection
      (keyIndex.toIndex(spatialKey), Array(geomsSum1.toString, geomsSum2.toString, geomVerticesSum1.toString, geomVerticesSum2.toString, referencePoint.toString))
    }

    val writer = new PrintWriter(new File(featurePath))
    zorderWithFeatureRdd.collect().foreach{ zorderWithFeature =>
      val zorderCode = zorderWithFeature._1
      val feature = zorderWithFeature._2
      writer.println(zorderCode.toString() + "  " + feature(0) + "  " + feature(1) + "  " + feature(2) + "  " + feature(3) + "  " + feature(4))
    }
    writer.close()

  }

  /**
   * Ingest compuIntensity info to fact table.
   *
   * @param sc Spark context
   * @param gridZOrderCodeWithProductFactKeyRdd1 A pair rdd where key is grid zorder code and value is a tuple(product key, fact key)
   * @param gridZOrderCodeWithProductFactKeyRdd2 A pair rdd where key is grid zorder code and value is a tuple(product key, fact key)
   * @param compuIntensityPath CompuIntensity info path which stores grid zorder code and compuIntensity.
   */
  def compuIntensityIngestor(implicit sc:SparkContext,
                             gridZOrderCodeWithProductFactKeyRdd1: RDD[(BigInt, (String, String))],
                             gridZOrderCodeWithProductFactKeyRdd2: RDD[(BigInt, (String, String))],
                             compuIntensityPath:String):Unit = {
    val file = Source.fromFile(compuIntensityPath)
    val gridZOrderCodeWithCI: Array[(BigInt, Long)] = file.getLines().map{ line =>
      val arr = line.split(",")
      val zorderCode = BigInt(arr(0).toInt)
      val compuIntesity = arr(1).toDouble.toLong
      (zorderCode, compuIntesity)
    }.toArray
    file.close
    val gridZOrderCodeWithCIRdd = sc.parallelize(gridZOrderCodeWithCI, 8)

    //RDD[(zorder, (((productKey1, factKey1), (productKey2, factKey2)), compuIntensity))]
    val joinZCodeWithProductFactKey:RDD[(BigInt, (((String, String), (String, String)), Long))] = gridZOrderCodeWithProductFactKeyRdd1.join(gridZOrderCodeWithProductFactKeyRdd2).join(gridZOrderCodeWithCIRdd)

    joinZCodeWithProductFactKey.foreachPartition{ partition =>
      val conn = DriverManager.getConnection(PostgresqlUtil.url, PostgresqlUtil.user, PostgresqlUtil.password)
      val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val postgresqlService = new PostgresqlService
      partition.foreach{ ele =>
        val zorderCode: BigInt = ele._1

        val productKey1: String = ele._2._1._1._1
        val factKey1: String = ele._2._1._1._2
        val productKey2: String = ele._2._1._2._1
        val factKey2: String = ele._2._1._2._2
        val compuIntensity:Long = ele._2._2

        val objectMapper = new ObjectMapper
        val node = objectMapper.createObjectNode
        val processNode = objectMapper.createObjectNode
        node.set("intersection", processNode)
        processNode.put(productKey2, compuIntensity)
        postgresqlService.insertCompuIntensity(statement, factKey1, node) //insert node to factKey1 CI attribute

        processNode.remove(productKey2)
        processNode.put(productKey1, compuIntensity)
        postgresqlService.insertCompuIntensity(statement, factKey2, node) //insert node to factKey2 CI attribute
      }
      conn.close()
    }


  }

  /**
   * Distributed geometry intersection with balanced partitions.
   *
   * @param joinKeysRdd
   * @param gridZOrderCodeWithCI
   * @return
   */
  def intersection(joinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))], gridZOrderCodeWithCI: Array[(BigInt, Long)]): RDD[Geometry] = {
    /***repartition using getBoundary()***/
    /*val bounds: Array[BigInt] = CompuIntensityPartitioner.getBoundary(gridZOrderCodeWithCI, 8)
    val balancedJoinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      joinKeysRdd.partitionBy(new CompuIntensityPartitioner(8, bounds))
    balancedJoinKeysRdd.foreachPartition{ partition =>
      println(TaskContext.getPartitionId() + " partitions: ")
      val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
      partition.foreach(x => print(keyIndex.toIndex(x._1) + " "))
      println()
    }*/

    /***repartition using getBoundary1() or *getBoundary2()***/
    val partitionContainers: Array[ArrayBuffer[BigInt]] = CompuIntensityPartitioner.getBoundary2(gridZOrderCodeWithCI, 8)
    val balancedJoinKeysRdd: RDD[(SpatialKey, (Array[String], Array[String]))] =
      joinKeysRdd.partitionBy(new CompuIntensityPartitioner(8, partitionContainers))

    /***read Geometry***/
    val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry]))] = balancedJoinKeysRdd.map{ ele =>
       val spatialKey = ele._1
       val fjson = new FeatureJSON()
       val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
         val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
         val feature: SimpleFeature = fjson.readFeature(featureStr)
         feature.getDefaultGeometry.asInstanceOf[Geometry]
       }.filter(_.isValid)
       val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
         val featureStr = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "metaData")
         val feature: SimpleFeature = fjson.readFeature(featureStr)
         feature.getDefaultGeometry.asInstanceOf[Geometry]
       }.filter(_.isValid)
       (spatialKey, (geometries1, geometries2))
     }
    /*val joinRdd: RDD[(SpatialKey, (Array[Geometry], Array[Geometry]))] = balancedJoinKeysRdd.map{ ele =>
      val spatialKey = ele._1
      val fjson = new FeatureJSON()
      val geometries1: Array[Geometry] = ele._2._1.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      val geometries2: Array[Geometry] = ele._2._2.map{ geoObjectkey =>
        val geomWKT = getVectorMeta("hbase_vector_aigis", geoObjectkey, "vectorData", "tile")
        new WKTReader().read(geomWKT)
      }.filter(_.isValid)
      (spatialKey, (geometries1, geometries2))
    }*/

     /***intersection***/
     val intersectionRdd: RDD[Geometry] = joinRdd.flatMap{ ele =>
       val gridIntersectionResults = new ArrayBuffer[Geometry]()
       val spatialKey = ele._1
       val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
       val tl = TileLayout(360, 180, 4000, 4000)
       val ld = LayoutDefinition(extent, tl)
       val gridExtent = spatialKey.extent(ld)
       val geomIter1 = ele._2._1
       val geomIter2 = ele._2._2
       geomIter1.foreach { geom1 =>
         val envelope1 = geom1.getEnvelopeInternal
         geomIter2.foreach { geom2 =>
           val envelope2 = geom2.getEnvelopeInternal
           val envIntersection = envelope1.intersection(envelope2)
           if (gridExtent.contains(envIntersection.getMinX, envIntersection.getMaxY)) {
             if (geom1.intersects(geom2)) {
               gridIntersectionResults.append(geom1.intersection(geom2))
             }
           }
         }
       }
       gridIntersectionResults
     }
     intersectionRdd
  }

}
