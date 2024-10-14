package whu.edu.cn.algorithms.terrain.core

import TypeAliases.RDDImage
import com.alibaba.fastjson.JSON.parseObject
import com.alibaba.fastjson.{JSONArray, JSONObject}
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, DoubleArrayTile, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, Raster, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.{withCollectMetadataMethods, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.data.store.ContentFeatureSource
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geojson.GeoJSONUtil
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.{FeatureWriter, Transaction}

import java.nio.charset.Charset
import java.util.{Objects, UUID}
import whu.edu.cn.entity.SpaceTimeBandKey

import java.io.{File, IOException, Reader, Serializable, StringWriter}
import scala.collection.JavaConversions._


object RDDTransformerUtil {

  val DEF_GEOM_KEY = "the_geom"
  val DEF_ENCODE = "uft-8"

  def saveFeatureRDDToShp(input: RDD[(String, (Geometry, mutable.Map[String, Any]))], outputShpPath: String): Unit = {
    val data: util.List[util.Map[String, Any]] = input.map(t => {
      t._2._2 + (DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava
    val geomClass = input.first()._2._1.getClass
    //    val geomSRID = input.first()._2._1.getSRID
    createShp(outputShpPath, "utf-8", geomClass, 4326, data)
    println("成功落地shp")
  }

  def createShp(shpPath: String, encode: String, geomType: Class[_], geomSRID: Int, data: util.List[util.Map[String, Any]]): Unit = {
    try {
      if (StringUtils.isEmpty(shpPath)) throw new Exception("shp文件的路径不能为空，shpPath: " + shpPath)
      if (StringUtils.isEmpty(encode)) throw new Exception("shp文件的编码不能为空，encode: " + encode)
      if (Objects.isNull(geomType)) throw new Exception("shp文件的图形类型不能为空，geomType: " + geomType)
      if (CollectionUtils.isEmpty(data)) throw new Exception("shp文件的图形数据不能为空，data: " + data)
      if (!data.get(0).containsKey(DEF_GEOM_KEY)) throw new Exception("shp文件的图形数据中必须包含the_geom的属性，data: " + data)
      //创建shape文件对象+
      val file = new File(shpPath)
      val params = new util.HashMap[String, Serializable]
      params.put(ShapefileDataStoreFactory.URLP.key, file.toURI.toURL)
      val ds: ShapefileDataStore = new ShapefileDataStoreFactory().createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      //定义图形信息和属性信息
      ds.createSchema(builderFeatureType(geomType, geomSRID, if (CollectionUtils.isEmpty(data)) null
      else data.get(0)))
      //设置编码
      val charset: Charset = Charset.forName(encode)
      ds.setCharset(charset)
      //设置Writer
      val writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)

      for (map <- data) { //写下一条
        val feature: SimpleFeature = writer.next
        for (key <- map.keySet) {
          if (DEF_GEOM_KEY == key) feature.setAttribute(key, map.get(key))
          else feature.setAttribute(key.toUpperCase, map.get(key))
        }
      }
      writer.write()
      writer.close()
      ds.dispose()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def builderFeatureType(geomType: Class[_], geomSRID: Int, data: util.Map[String, _]): SimpleFeatureType = {
    val ftb = new SimpleFeatureTypeBuilder
    val EPSG = "EPSG:"
    val srid = s"$EPSG$geomSRID"
    val crs: CoordinateReferenceSystem = CRS.decode(srid)

    ftb.setCRS(crs)
    //    ftb.setCRS(DefaultGeographicCRS.WGS84)
    ftb.setName("shapefile")
    ftb.add(DEF_GEOM_KEY, geomType)
    if (MapUtils.isNotEmpty(data)) {
      for (key <- data.keySet) {
        if (Objects.nonNull(data.get(key))) ftb.add(key.toUpperCase, data.get(key).getClass)
      }
    }
    ftb.buildFeatureType
  }

  def readShp(implicit sc: SparkContext, shpPath: String, encode: String): RDD[(String, (Geometry, mutable.Map[String, Any]))] = {
    val shapeFile = new File(shpPath)
    val store = new ShapefileDataStore(shapeFile.toURI.toURL)
    //设置编码
    val charset: Charset = Charset.forName(encode)
    store.setCharset(charset)
    val sfSource: ContentFeatureSource = store.getFeatureSource
    val sfIter: SimpleFeatureIterator = sfSource.getFeatures.features
    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
    val preRdd: ArrayBuffer[(String, (Geometry, mutable.Map[String, Any]))] = ArrayBuffer.empty[(String, (Geometry, mutable.Map[String, Any]))]
    while ( {
      sfIter.hasNext
    }) {
      val feature: SimpleFeature = sfIter.next
      // Feature转GeoJSON
      val fJson = new FeatureJSON
      val writer = new StringWriter()
      fJson.writeFeature(feature, writer)
      val sJson: JSONObject = parseObject(writer.toString)
      val wkt: String = jsonToWkt(sJson)
      val geom: Geometry = wktToGeom(wkt)
      val properties: JSONObject = sJson.getJSONObject("properties")
      val props = mutable.Map.empty[String, Any]
      val keys: Set[String] = properties.keySet().asScala.toSet
      for (key <- keys) {
        val element: String = properties.getString(key)
        props += (key -> element)
      }
      preRdd += Tuple2(UUID.randomUUID().toString, Tuple2(geom, props))
    }
    sc.makeRDD(preRdd)
  }

  def wktToGeom(wkt: String): Geometry = {
    var geometry: Geometry = null
    val reader = new WKTReader
    geometry = reader.read(wkt)
    geometry
  }

  def jsonToWkt(jsonObject: JSONObject): String = {
    var wkt: String = null
    val `type`: String = jsonObject.getString("type")
    val gJson = new GeometryJSON
    val GEO_JSON_TYPE = "GeometryCollection"
    try // {"geometries":[{"coordinates":[4,6],"type":"Point"},{"coordinates":[[4,6],[7,10]],"type":"LineString"}],"type":"GeometryCollection"}
      if (GEO_JSON_TYPE == `type`) { // 由于解析上面的json语句会出现这个geometries属性没有采用以下办法
        val geometriesArray: JSONArray = jsonObject.getJSONArray("geometries")
        // 定义一个数组装图形对象
        val size: Int = geometriesArray.size
        val geometries = new Array[Geometry](size)
        for (i <- 0 until size) {
          val str: String = geometriesArray.get(i).toString
          // 使用GeoUtil去读取str
          val reader: Reader = GeoJSONUtil.toReader(str)
          val geometry: Geometry = gJson.read(reader)
          geometries(i) = geometry
        }
        val geometryCollection = new GeometryCollection(geometries, new GeometryFactory())
        wkt = geometryCollection.toText
      }
      else {
        val reader: Reader = GeoJSONUtil.toReader(jsonObject.toString)
        val read: Geometry = gJson.read(reader)
        wkt = read.toText
      }
    catch {
      case e: IOException =>
        System.out.println("GeoJson转WKT出现异常")
        e.printStackTrace()
    }
    wkt
  }


  /**
   * store TIFF to disk
   *
   * @param input raster data in [[RDDImage]] format
   * @param outputTiffPath output path
   */
  def saveRasterRDDToTif(
      input: RDDImage,
      outputTiffPath: String
  ): Unit = {
    val tileLayerArray = input._1
      .map(t => {
        (t._1.spaceTimeKey.spatialKey, t._2)
      })
      .collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    println("成功落地tif")
  }

  /**
   * read TIFF from disk use specified Spark Context
   *
   * @param sc Spark上下文
   * @param sourceTiffPath TIFF路径
   * @return RDDImage
   */
  def makeChangedRasterRDDFromTif(
      sc: SparkContext,
      sourceTiffPath: String
  ): RDDImage = {
    val hadoopPath = sourceTiffPath
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(new Path(hadoopPath))(sc)
    val localLayoutScheme = FloatingLayoutScheme(256)
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](localLayoutScheme)
    val tiled = inputRdd.tileToLayout[SpatialKey](metadata).cache()
    val srcLayout = metadata.layout
    val srcExtent = metadata.extent
    val srcCrs = metadata.crs
    val srcBounds = metadata.bounds
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(
      SpaceTimeKey(srcBounds.get.minKey._1, srcBounds.get.minKey._2, date),
      SpaceTimeKey(srcBounds.get.maxKey._1, srcBounds.get.maxKey._2, date)
    )
    val metaData =
      TileLayerMetadata(metadata.cellType, srcLayout, srcExtent, srcCrs, newBounds)
    // measurementName从"B1"开始依次为各波段命名，解决用户上传数据无法筛选波段的问题（没有波段名称）
    val bandCount: Int = tiled.first()._2.bandCount
    val measurementName = ListBuffer.empty[String]
    for (i <- 1 to bandCount) measurementName.append(s"B$i")
    val tiledOut = tiled.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), measurementName), t._2)
    })
    println("成功读取tif")
    (tiledOut, metaData)
  }

  /**
   * @param image RDDImage
   * @param radius padding的栅格数量
   * @return RDDImage
   *
   * @todo need optimization
   */
  def paddingRDDImage(
      image: RDDImage,
      radius: Int
  ): RDDImage = {
    if (radius == 0) {
      return image
    }

    val nodata: Double = image._2.cellType match {
      case x: DoubleUserDefinedNoDataCellType => x.noDataValue
      case x: FloatUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: IntUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case ByteConstantNoDataCellType => ByteConstantNoDataCellType.noDataValue.toDouble
      case UByteConstantNoDataCellType => UByteConstantNoDataCellType.noDataValue.toDouble
      case ShortConstantNoDataCellType => ShortConstantNoDataCellType.noDataValue.toDouble
      case UShortConstantNoDataCellType => UShortConstantNoDataCellType.noDataValue.toDouble
      case IntConstantNoDataCellType => IntConstantNoDataCellType.noDataValue.toDouble
      case FloatConstantNoDataCellType => FloatConstantNoDataCellType.noDataValue.toDouble
      case DoubleConstantNoDataCellType => DoubleConstantNoDataCellType.noDataValue
      case _ => Double.NaN
    }

    val leftNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 1), t._2)
      )
    })

    val rightNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 1), t._2)
      )
    })

    val upNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 0), t._2)
      )
    })

    val downNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 2), t._2)
      )
    })

    val leftUpNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 0), t._2)
      )
    })

    val upRightNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 0), t._2)
      )
    })

    val rightDownNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(2, 2), t._2)
      )
    })

    val downLeftNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0),
          t._1.measurementName
        ),
        (SpatialKey(0, 2), t._2)
      )
    })

    val midNeighborRDD = image._1.map(t => {
      (
        SpaceTimeBandKey(
          SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0),
          t._1.measurementName
        ),
        (SpatialKey(1, 1), t._2)
      )
    })

    // 合并邻域RDD
    val unionRDD = leftNeighborRDD
      .union(rightNeighborRDD)
      .union(upNeighborRDD)
      .union(downNeighborRDD)
      .union(leftUpNeighborRDD)
      .union(upRightNeighborRDD)
      .union(rightDownNeighborRDD)
      .union(downLeftNeighborRDD)
      .union(midNeighborRDD)
      .filter(t => {
        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 &&
          t._1.spaceTimeKey.spatialKey._1 < image._2.layout.layoutCols &&
          t._1.spaceTimeKey.spatialKey._2 < image._2.layout.layoutRows
      })

    val groupRDD = unionRDD
      .groupByKey()
      .map(t => {
        val listBuffer = new ListBuffer[(SpatialKey, MultibandTile)]()
        val list = t._2.toList
        val listKey = List(
          SpatialKey(0, 0),
          SpatialKey(0, 1),
          SpatialKey(0, 2),
          SpatialKey(1, 0),
          SpatialKey(1, 1),
          SpatialKey(1, 2),
          SpatialKey(2, 0),
          SpatialKey(2, 1),
          SpatialKey(2, 2)
        )
        for (key <- listKey) {
          var flag = false
          breakable {
            for (tile <- list) {
              if (key.equals(tile._1)) {
                listBuffer.append(tile)
                flag = true
                break
              }
            }
          }
          if (!flag) {
            listBuffer.append((key, MultibandTile(DoubleArrayTile(Array.fill[Double](256 * 256)(nodata), 256, 256, nodata))))
          }
        }
        val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
        (
          t._1,
          tile
            .crop(256 - radius, 256 - radius, 511 + radius, 511 + radius)
            .withNoData(Option(nodata))
        )
      })

    (groupRDD, image._2)

  }

}
