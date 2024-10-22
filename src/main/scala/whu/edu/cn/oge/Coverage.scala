package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import com.baidubce.services.bos.BosClient
import com.baidubce.services.bos.model.GetObjectRequest
import geotrellis.layer._
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.mapalgebra.focal.TargetCell
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.resample.{Bilinear, PointResampleMethod}
import io.minio.{GetObjectArgs, MinioClient, PutObjectArgs, UploadObjectArgs}
import geotrellis.raster.{reproject => _, _}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.Extent
import javafx.scene.paint.Color
import org.apache.commons.math3.distribution.FDistribution
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import redis.clients.jedis.Jedis
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.ClientConf.{CLIENT_NAME, USER_BUCKET_NAME}
import whu.edu.cn.config.GlobalConfig.DagBootConf._
import whu.edu.cn.config.GlobalConfig.MinioConf.MINIO_BUCKET_NAME
import whu.edu.cn.config.GlobalConfig.BosConf.BOS_BUCKET_NAME
import whu.edu.cn.config.GlobalConfig.Others.tempFilePath
import whu.edu.cn.config.GlobalConfig.RedisConf.REDIS_CACHE_TTL
import whu.edu.cn.entity._
import whu.edu.cn.jsonparser.JsonToArg
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.trigger.Trigger.{batchParam, dagId, layerName, tempFileList, userId, windowExtent}
import whu.edu.cn.util.CoverageUtil._
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostgresqlServiceUtil.queryCoverage
import whu.edu.cn.util.RDDTransformerUtil.saveRasterRDDToTif
import whu.edu.cn.util.SSHClientUtil.{runCmd, versouSshUtil}

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.collection.mutable
import scala.language.postfixOps
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import scala.util.control.Breaks
import whu.edu.cn.util.{COGUtil, Cbrt, ClientUtil, CoverageOverloadUtil, Entropy, Mod, PostSender, RDDTransformerUtil, RemapWithDefaultValue, RemapWithoutDefaultValue}

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._
import java.io.File
import sys.process._

// TODO lrx: 后面和GEE一个一个的对算子，看看哪些能力没有，哪些算子考虑的还较少
// TODO lrx: 要考虑数据类型，每个函数一般都会更改数据类型
object Coverage {
  val resolutionTMSArray: Array[Double] = Array(156543.033928, 78271.516964, 39135.758482, 19567.879241, 9783.939621, 4891.969810, 2445.984905, 1222.992453, 611.496226, 305.748113, 152.874057, 76.437028, 38.218514, 19.109257, 9.554629, 4.777314, 2.388657, 1.194329, 0.597164, 0.298582, 0.149291)

  def load(implicit sc: SparkContext, coverageId: String, productKey: String, level: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val time1 = System.currentTimeMillis()
    //    println("WindowExtent",windowExtent.xmin,windowExtent.ymin,windowExtent.xmax,windowExtent.ymax)


    val metaList: mutable.ListBuffer[CoverageMetadata] = queryCoverage(coverageId, productKey)
    if (metaList.isEmpty) {
      throw new Exception("No such coverage in database!")
    }

    var union: Extent = Trigger.windowExtent
    if(union == null){
      union = Extent(metaList.head.getGeom.getEnvelopeInternal)
    }

    val queryGeometry = metaList.head.getGeom

    //    val union: Geometry = unionTileExtent.intersection(queryGeometry)

    val tileMetadata: RDD[CoverageMetadata] = sc.makeRDD(metaList)
    val cogUtil: COGUtil = COGUtil.createCOGUtil(CLIENT_NAME)
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)

    val tileRDDFlat: RDD[RawTile] = tileMetadata
      .mapPartitions(par => {
        val client = clientUtil.getClient
        val result: Iterator[mutable.Buffer[RawTile]] = par.map(t => { // 合并所有的元数据（追加了范围）
          val time1: Long = System.currentTimeMillis()
          val rawTiles: mutable.ArrayBuffer[RawTile] = {
            val tiles: mutable.ArrayBuffer[RawTile] = cogUtil.tileQuery(client, level, t, union,queryGeometry)
            tiles
          }
          val time2: Long = System.currentTimeMillis()
          println("Get Tiles Meta Time is " + (time2 - time1))
          // 根据元数据和范围查询后端瓦片
          if (rawTiles.nonEmpty) rawTiles
          else mutable.Buffer.empty[RawTile]
        })
        result
      }).flatMap(t => t).persist()

    val tileNum: Int = tileRDDFlat.count().toInt
    println("tileNum = " + tileNum)
    if(tileNum <= 0 ){
      throw new Exception("There are no tiles within the visible range!")
    }
    val tileRDDRePar: RDD[RawTile] = tileRDDFlat.repartition(math.min(tileNum, 16))
    tileRDDFlat.unpersist()
    val rawTileRdd: RDD[RawTile] = tileRDDRePar.mapPartitions(par => {

      val client = clientUtil.getClient
      par.map(t => {
        cogUtil.getTileBuf(client, t)
      })
    })
    println("Loading data Time: " + (System.currentTimeMillis() - time1))
    val time2 = System.currentTimeMillis()
    val coverage = makeCoverageRDD(rawTileRdd)
    println("Make RDD Time: " + (System.currentTimeMillis() - time2))
    coverage
  }

  def geoDetector(implicit depVar_In: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  facVar_name_In:String,
                  facVar_In: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  norExtent_sta:Double,norExtent_end:Double,NaN_value:Double) = {
    //将栅格数据中值在范围之外的像素赋值为指定的异常值
    def clamp_NodataV(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), low: Double, high: Double,
                      NodataV: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
      val coverageRddClamped = coverage._1.map(t =>
        (t._1, t._2.mapBands((t1, t2) => t2.mapDouble(t3 => {
          if (t3 > high | t3 < low) {
            NodataV
          }
          else {
            t3
          }
        }
        ))))
      (coverageRddClamped, coverage._2)
    }
    //对因变量栅格数据进行异常值清晰
    val depVar = clamp_NodataV(depVar_In,norExtent_sta,norExtent_end,(norExtent_sta+norExtent_end)/2)
    val facVar = List(facVar_In)
    val facVar_name = List(facVar_name_In)

    //获取因变量depVar的整体方差和样本数
    //首先map reduce计算deVar的栅格数量和栅格均值
    val summary = depVar._1.map { t =>
      val deptile: Tile = t._2.band(0)
      val pixelArray = deptile.toArrayDouble()

      // 过滤掉 NaN 值和正常值范围外的异常值
      var validPixels = pixelArray.filter(_ != null)
      validPixels = validPixels.filter(_ >= norExtent_sta)
      validPixels = validPixels.filter(_ <= norExtent_end)

      val pixelCount = validPixels.length
      val pixelSum = validPixels.sum
      (pixelCount, pixelSum)
    }.reduce { (left, right) =>
      (left._1 + right._1, left._2 + right._2)
    }
    val depVar_N: Int = summary._1
    val depVar_mean: Double = summary._2 / summary._1

    //再根据栅格均值，进行map reduce计算deVar的方差
    val depVar_Var_sum = depVar._1.map { t =>
      val deptile: Tile = t._2.band(0)
      val deptile_arr = deptile.toArrayDouble()
      // 过滤掉 NaN 值
      val validPixels = deptile_arr.filter(!_.isNaN)
      // 计算有效像素的平方差总和
      val devSqSum = validPixels.map(x => math.pow(x - depVar_mean, 2)).sum
      devSqSum
    }.reduce { (a, b) => a + b }
    val depVar_Var: Double = depVar_Var_sum / depVar_N

    val sst = depVar_N*depVar_Var

    //定义一个可变的Map，用于存储每个影响因子的类别数量和对应的栅格数量
    val facVar_claNum = mutable.Map.empty[String, mutable.Map[Double, Double]]
    //定义一个可变的Map，用于存储每个影响因子的类别数量和对应的方差
    val facVar_claVar = mutable.Map.empty[String, mutable.Map[Double, Double]]
    //定义一个可变的Map，用于存储每个影响因子的类别数量和对应的均值
    val facVar_claMean = mutable.Map.empty[String, mutable.Map[Double, Double]]

    //遍历每一个影响因子获取影响因子类别数量
    for (fac <- facVar.indices) {
      //定义一个可变的Map，用于动态存储每一类的像素值和栅格数量
      var fac_class_tmp = mutable.ListBuffer.empty[Double]
      //首先遍历每个rdd
      val fac_class_Sum = facVar(fac)._1.map{ t =>
        val factile: Tile = t._2.band(0)
        //然后再foreach遍历rdd中tile的每个像素,累计类别存入fac_class
        factile.foreachDouble { pix =>
          //如果是非有效值，就不统计为类别
          if (pix != NaN_value & !fac_class_tmp.contains(pix)) {
            fac_class_tmp.append(pix)
          }
        }
        fac_class_tmp
      }.reduce { (a,b) =>a union b}

      val fac_class_Union = fac_class_Sum.distinct
      val claNum_tmp2 = mutable.Map.empty[Double, Double]
      //根据fac_class_Union初始化facVar_claNum
      for(fac_class <- fac_class_Union){
        val tmp2 = fac_class -> 1d
        claNum_tmp2 += tmp2
      }

      facVar_claNum(facVar_name(fac)) = claNum_tmp2
    }

    //基于coverageTemplate进行双栅格数据处理
    //循环遍历每一个影响因子
    for (fac <- facVar.indices) {
      val facRDD = facVar(fac)
      val facType = facVar_name(fac)//该影响因子对应的名称
      val facClassNum = facVar_claNum(facType).size//该影响因子对应的类别数量
      val facClass_List = facVar_claNum(facType).keys.toList//该影响因子对应的类别值list

      //循环遍历每一个影响因子的每一类
      for(fac_Class_ind <- facClass_List.indices){
        val target_class =facClass_List(fac_Class_ind)//该影响因子当前类别
        //联合计算，如果depVar对应的facVar的值不是当前类别，就赋值为0，为后面计算平均值
        val classifiedRDD: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
        = coverageTemplate(depVar, facRDD, (tile1, tile2) => classifyFunction(tile1, tile2, target_class))

        //遍历classifiedRDD获取当前类别下depVar的平均值
        //由于非当前类别下depVar的值被赋值为了0，对求和没有影响
        val summary_1 = classifiedRDD._1.map { t =>
          val classTile: Tile = t._2.band(0)
          val pixelSum = classTile.toArrayDouble().sum
          val nonZeroCount = classTile.toArrayDouble().count(_ != 0)
          (pixelSum,nonZeroCount)
        }.reduce { (a,b) => (a._1+b._1, a._2+ b._2) }

        val class_mean: Double = summary_1._1 / summary_1._2
        val class_num: Double = summary_1._2

        //更新facVar_claMean
        val tmp1 = target_class -> class_mean
        if(facVar_claMean.contains(facType)){
          //这里已有当前因子，也就是有当前的key
          //就要先获取当前类别（如‘dem’）下的map，然后再进行+=，否则由于都是同key会被覆盖
          val Mean_value: mutable.Map[Double, Double] = facVar_claMean(facType)
          Mean_value += tmp1
          facVar_claMean(facType) = Mean_value
        }else{
          //如果没有当前因子，也就是没有当前的key，则可以直接添加
          var tmp1_mutable = mutable.Map.empty[Double, Double]
          tmp1_mutable += tmp1
          val newfacMean = facVar_name(fac) -> tmp1_mutable
          facVar_claMean += newfacMean
        }

        //更新facVar_claNum
        val tmp2_5 = target_class -> class_num
        //这里已有当前因子，也就是有当前的key
        //就要先获取当前类别（如‘dem’）下的map，然后再进行+=，否则由于都是同key会被覆盖
        val Num_value: mutable.Map[Double, Double] = facVar_claNum(facType)
        Num_value += tmp2_5
        facVar_claNum(facType) = Num_value

        val varSumRDD: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
        = coverageTemplate(depVar, facRDD, (tile1, tile2) => varSum_Function(tile1, tile2, target_class,class_mean))

        val summary_2 = varSumRDD._1.map { t =>
          val varTile: Tile = t._2.band(0)
          val pixelSum = varTile.toArrayDouble().sum
          pixelSum
        }.reduce { (a, b) => a + b }

        val fac_var_class = summary_2 / class_num

        //更新facVar_claVar
        val tmp3 = target_class -> fac_var_class
        if (facVar_claVar.contains(facType)) {
          //这里已有当前因子，也就是有当前的key
          //就要先获取当前类别（如‘dem’）下的map，然后再进行+=，否则由于都是同key会被覆盖
          val Var_value: mutable.Map[Double, Double] = facVar_claVar(facType)
          Var_value += tmp3
          facVar_claVar(facType) = Var_value
        } else {
          //如果没有当前因子，也就是没有当前的key，则可以直接添加
          var tmp3_mutable = mutable.Map.empty[Double, Double]
          tmp3_mutable += tmp3
          val newfacVar = facVar_name(fac) -> tmp3_mutable
          facVar_claVar += newfacVar
        }

      }

      //用于对depVar进行分类
      def classifyFunction(depVar_Tile: Tile, facVar_Tile: Tile, tar_fac_Class:Double): Tile= {
        val resultTile = depVar_Tile.combineDouble(facVar_Tile) { (value1, value2) =>
          // 根据 tile1 的类别对 tile2 的像素值进行分类
          if (value2 == tar_fac_Class) {
            value1
          } else {
            0
          }
        }
        resultTile
      }

      //用于计算特定分类的depVar的差方和
      def varSum_Function(depVar_Tile: Tile, facVar_Tile: Tile, tar_fac_Class:Double, fac_Class_mean: Double): Tile = {
        val resultTile = depVar_Tile.combineDouble(facVar_Tile) { (value1, value2) =>
          if (value2 == tar_fac_Class) {
            (value1 - fac_Class_mean)*(value1 - fac_Class_mean)
          } else {
            0
          }
        }
        resultTile
      }
    }

    var q_fac_list = Map.empty[String, Double]
    var p_fac_list = Map.empty[String, Double]

    //遍历每一种因子，计算q值和p值
    for(fac <- facVar.indices){
      val facType = facVar_name(fac) //该影响因子对应的名称
      val facClassNum = facVar_claNum(facType).size //该影响因子对应的类别数量
      val facClass_List = facVar_claNum(facType).keys.toList //该影响因子对应的类别值list

      val facClaNum = facVar_claNum(facType)
      val facClaVar = facVar_claVar(facType)
      val facClaMean = facVar_claMean(facType)
      var ssw:Double = 0
      var lambda1:Double = 0
      var lambda2:Double = 0

      for(farcls <- facClass_List){
        val Nh = facClaNum(farcls)
        val xigemah = facClaVar(farcls)
        val Ymean = facClaMean(farcls)
        var fac_num:Double = 0
        fac_num += Nh
        ssw += Nh * xigemah
        lambda1+=Ymean*Ymean
        lambda2+=math.sqrt(Nh)*Ymean
      }

      val q_fac = 1-ssw/sst
      val F_fac = ((depVar_N-facClassNum)*q_fac)/((facClassNum-1)*(1-q_fac))
      val lambda_fac = (lambda1-lambda2*lambda2/depVar_N)/depVar_Var

      val fDistribution_fac = new FDistribution(facClassNum - 1, depVar_N - facClassNum, lambda_fac)
      val p_fac = 1.0 - fDistribution_fac.cumulativeProbability(F_fac)


      val newPair_q = facType -> q_fac
      q_fac_list += newPair_q

      val newPair_p = facType -> p_fac
      p_fac_list += newPair_p
    }

    val depVar_mean_format = f"%%.4f".format(depVar_mean)
    val depVar_Var_format = f"%%.4f".format(depVar_Var)
    //    var result_mes = "地理探测器的计算结果如下：\n因变量因子的全局平均值为"+depVar_mean_format+",全局方差为"+depVar_Var_format+"\n"
    var result_mes = "因变量因子的全局平均值为"+depVar_mean_format+",全局方差为"+depVar_Var_format+"\n"

    for(index <- facVar_name.indices){
      val facVar_name_inc = facVar_name(index)
      val facVar_claMean_inc = facVar_claMean(facVar_name_inc)
      val facVar_claVar_inc = facVar_claVar(facVar_name_inc)
      val facVar_claNum_inc = facVar_claNum(facVar_name_inc)
      val facVar_clasind =  facVar_claVar_inc.keys.toList.sorted

      val facVar_q = f"%%.4f".format(q_fac_list(facVar_name_inc))

      val tmp_str1 = "影响因子"+facVar_name_inc+"对因变量的空间分异性解释程度为"+facVar_q+"\n其中：\n"
      result_mes+=tmp_str1

      for(facVar_clas <- facVar_clasind){
        val facVar_clas_mean = f"%%.4f".format(facVar_claMean_inc(facVar_clas))
        val facVar_clas_var = f"%%.4f".format(facVar_claVar_inc(facVar_clas))
        val facVar_clas_num = facVar_claNum_inc(facVar_clas)
        val tmp_str2 = "类别"+facVar_clas.toInt.toString+"有"+facVar_clas_num.toInt.toString+"个像素,类内平均值为"+facVar_clas_mean+",类内方差为"+facVar_clas_var+"\n"
        result_mes+=tmp_str2
      }
      result_mes+="\n"
    }

    result_mes
  }


  def reclass(implicit coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
              rules:List[(Double,Double,Double)],
              NaN_value: Double) = {

    def reclassifyPixel(pixelValue: Double,rules:List[(Double,Double,Double)]): Double = {
      val loop = new Breaks
      var new_val:Double = NaN_value
      loop.breakable {
        for (extent <- rules) {
          val extent_sta = extent._1
          val extent_end = extent._2
          val extent_val = extent._3
          if (pixelValue >= extent_sta & pixelValue <= extent_end) {
            new_val = extent_val
            loop.break
          }
        }
      }
      new_val
    }

    val cov_reclass: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) =
      (coverage._1.map(t => {
        (t._1, t._2.mapBands((_, tile) => {
          val tile_reclass: Tile = tile.mapDouble(pixle => {
            reclassifyPixel(pixle, rules)
          })
          tile_reclass
        }
        ))
      }),coverage._2)

    cov_reclass
  }

  //获取两个栅格数据重叠区域的数据，返回coverage2重叠区域的数据
  def rasterUnion(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val coverageUnion = coverageTemplate(coverage1, coverage2, (tile1, tile2) => Subtract(Add(tile1, tile2), tile1))

    coverageUnion
  }

  def area(implicit coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), ValList: List[String], resolution: Double): Double = {

    //先转投影,转到3857计算面积
    val crs = CRS.fromEpsgCode(3857)
    val coverage_pro = reproject(coverage, crs, resolution)

    //再统计栅格数量
    //先map遍历每一个RDD
    val tar_pix_num: Double = (coverage_pro._1.map(t => {

      val tile = t._2.band(0) //由于输入要求一个波段，直接band（0）获取栅格
      var count: Double = 0 //用于统计栅格数量

      //由于没有输出，所以用foreach遍历每一个栅格值，而不是用map
      tile.foreachDouble(pix => {
        val loop = new Breaks
        loop.breakable {
          //循环解析输入的栅格值范围
          if (ValList.isEmpty) {
            throw new Exception("Please input value extent!")
          }
          for (element <- ValList) {
            //分解单个范围如"0-1"，获取起始值entend_start（0）和终止值entend_end（1）
            val extent = element.split(":")
            if (extent.length != 2) {
              throw new Exception("Please input valid value extent like 0:1!")
            }

            val entend_start = extent(0)
            val entend_end = extent(1)
            //如果输入范围内没有空值，也就是没有无穷量
            if (extent(0) != "" & extent(1) != "") {


              val entent_start = extent(0).toDouble
              val entent_end = extent(1).toDouble

              if (entent_start > entent_end) {
                throw new Exception("The starting value of the input range is greater than the ending value!")
              }
              //如果起始值和终止值一样，就统计值为该值的栅格数量
              if (entent_start == entent_end) {
                if (pix == entent_end) {
                  count += 1
                  loop.break
                }
              }
              //如果起始值和终止值不一样，就统计值在开区间内的栅格数量
              else {
                if (pix > entent_start & pix < entent_end) {
                  count += 1
                  loop.break
                }
              }
            }
            //如果输入范围只有终止值，就只统计小于终止值的栅格数量
            if (extent(0) == "" & extent(1) != "") {
              val entent_end = extent(1).toDouble
              if (pix < entent_end) {
                count += 1
                loop.break

              }
            }
            //如果输入范围只有起始值，就只统计大于起始值的栅格数量
            if (extent(0) != "" & extent(1) == "") {
              val entent_start = extent(0).toDouble
              if (pix > entent_start) {
                count += 1
                loop.break

              }
            }
            //如果输入范围起始值和终止值都没有，就统计所有栅格
            if (extent(0) == "" & extent(1) == "") {
              count += 1
              loop.break
            }
          }
        }
      })
      count
    })

      ).reduce((a, b) => a + b)

    //根据数的栅格数量和分辨率大小计算最终面积，单位为m2
    val area_sum_square_m: Double = tar_pix_num * resolution * resolution
    area_sum_square_m
  }

  /**
   * 生成虚拟coverage
   * 测试函数，不进入正式版本中 TODO：在master分支中删除该函数
   * Int类型的默认NoData值是-2147483648，Double类型的默认NoData值是Double.NaN
   * 可以使用isNoData方法来判断一个值是否是NoData值
   */
  def makeFakeCoverage(implicit sc: SparkContext, array: Array[Int], names: mutable.ListBuffer[String], cols: Int, rows: Int):
  (RDD[
    (SpaceTimeBandKey,
      MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    val tile = MultibandTile(ArrayTile(arr = array, cols, rows),
      ArrayTile(arr = array, cols, rows),
      ArrayTile(arr = array, cols, rows))
    val key = SpaceTimeBandKey(SpaceTimeKey(0, 0, ZonedDateTime.now()), names)
    val rdd: RDD[(SpaceTimeBandKey, MultibandTile)] = sc.parallelize(Seq((key, tile)))
    val metadata = TileLayerMetadata[SpaceTimeKey](
      cellType = IntConstantNoDataCellType,
      layout = LayoutDefinition(Extent(0.0, 0.0, 1.0, 1.0), TileLayout(1, 1, 3, 3)),
      extent = Extent(0.0, 0.0, 1.0, 1.0),
      crs = CRS.fromEpsgCode(4326),
      bounds = KeyBounds(SpaceTimeKey(0, 0, 0), SpaceTimeKey(0, 0, 0))
    )
    (rdd, metadata)
  }

  def makeFakeCoverage(implicit sc: SparkContext, array: Array[Double], names: mutable.ListBuffer[String], cols: Int, rows: Int):
  (RDD[
    (SpaceTimeBandKey,
      MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    val tile = MultibandTile(ArrayTile(arr = array, cols, rows),
      ArrayTile(arr = array, cols, rows)
      , ArrayTile(arr = array, cols, rows))
    val key = SpaceTimeBandKey(SpaceTimeKey(0, 0, ZonedDateTime.now()), names)
    val rdd: RDD[(SpaceTimeBandKey, MultibandTile)] = sc.parallelize(Seq((key, tile)))
    val metadata = TileLayerMetadata[SpaceTimeKey](
      cellType = DoubleConstantNoDataCellType,
      layout = LayoutDefinition(Extent(0.0, 0.0, 1.0, 1.0), TileLayout(1, 1, 3, 3)),
      extent = Extent(0.0, 0.0, 1.0, 1.0),
      crs = CRS.fromEpsgCode(4326),
      bounds = KeyBounds(SpaceTimeKey(0, 0, 0), SpaceTimeKey(0, 0, 0))
    )
    (rdd, metadata)
  }

  /**
   * Return the acquisition date of the given coverage.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def date(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = {
    Instant.ofEpochMilli(coverage._1.first()._1.getSpaceTimeKey().time.toInstant.toEpochMilli.*(1000L)).atZone(ZoneId.systemDefault()).toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, add operation is applied between the 2 bands
   * if not, add the first value to the second for each matched pair of bands in coverage1 and coverage2
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def add(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Add(tile1, tile2))
  }

  /**
   * Add the value to the coverage.
   *
   * @param coverage The coverage for operation.
   * @param i        The value to add.
   * @return Coverage
   */
  def addNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             i: Any*): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i.head match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Add(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Add(tile, x))
      case _ => coverageTemplate(coverage, (tile) => Add(tile, i.head.asInstanceOf[Double]))
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, subtarct the second tile from the first tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def subtract(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Subtract(tile1, tile2))
  }

  /**
   * Subtract the corresponding value from the coverage.
   *
   * @param coverage
   * @param i
   * @return
   */
  def subtractNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Subtract(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Subtract(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, divide operation is applied between the 2 bands
   * if not, divide the first tile by the second tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def divide(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Divide(tile1, tile2))
  }

  def divideNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if(i == 0){
      throw new IllegalArgumentException("The dividend cannot be 0.")
    }
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Divide(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Divide(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * if both coverage1 and coverage2 has only 1 band, multiply operation is applied between the 2 bands
   * if not, multipliy the first tile by the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def multiply(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Multiply(tile1, tile2))
  }

  def multiplyNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Multiply(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Multiply(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }


  /**
   * if both coverage1 and coverage2 has only 1 band, subtract operation is applied between the 2 bands
   * if not, mod the second tile from the first tile for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 first coverage rdd
   * @param coverage2 second coverage rdd
   * @return
   */
  def mod(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Mod(tile1, tile2))
  }

  def modNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             i: AnyVal): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    i match {
      case (x: Int) => coverageTemplate(coverage, (tile) => Mod(tile, x))
      case (x: Double) => coverageTemplate(coverage, (tile) => Mod(tile, x))
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * Compute a polynomial at each pixel using the given coefficients.
   *
   * @param coverage The input coverage.
   * @param l        The polynomial coefficients in increasing order of degree starting with the constant term.
   * @return
   */
  def polynomial(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), l: List[Double]):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var resCoverage = multiplyNum(coverage, 0)
    for (i <- 0 until (l.length)) {
      resCoverage = add(resCoverage, multiplyNum(powNum(coverage, i), l(i)))
    }
    resCoverage
  }

  def normalizedDifference(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bandNames: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (bandNames.length != 2) {
      throw new IllegalArgumentException(s"输入的波段数量不为2，输入了${bandNames.length}个波段")
    }
    else {
      val coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverage, List(bandNames.head))
      val coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverage, List(bandNames(1)))
      coverageTemplate(coverage1, coverage2, (tile1, tile2) => {
        Divide(Subtract(tile1, tile2), Add(tile1, tile2))
      })
    }
  }

  /**
   * coverage Binarization
   *
   * @param coverage  coverage rdd for operation
   * @param threshold threshold
   * @return
   */
  // 自动转成uint8数据类型
  def binarization(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), threshold: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val cellType = coverage._1.first()._2.cellType
    (coverage._1.map(t => {
      (t._1, t._2.mapBands((_, tile) => {
        val tileConverted: Tile = tile.convert(DoubleConstantNoDataCellType)
        val tileMap: Tile = tileConverted.mapDouble(pixel => {
          if (pixel > threshold) {
            255.0
          }
          else {
            0.0
          }
        })
        tileMap.convert(cellType)
      }))
    }), TileLayerMetadata(cellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  }

  /**
   * Combines the given coverages into a single coverage which contains all bands from all of the images.
   *
   * @param coverage1 The first coverage for cat.
   * @param coverage2 The second coverage for cat.
   * @return
   */
  def cat(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageCollection1: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map("c1" -> coverage1, "c2" -> coverage2)
    CoverageCollection.mean(coverageCollection1)
  }

  /**
   * if both have only 1 band, the 2 band will match.
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def bitwiseAnd(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                 coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => And(tile1, tile2))
  }

  /**
   * Returns 1 iff both values are non-zero for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match.
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def and(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => CoverageOverloadUtil.And(tile1, tile2))
  }

  /**
   * Calculates the bitwise XOR of the input values for each matched pair of bands in image1 and image2.
   * If both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def bitwiseXor(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                 coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Xor(tile1, tile2))
  }


  /**
   * bitwiseOr
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def bitwiseOr(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Or(tile1, tile2))
  }


  /**
   * Returns 1 iff either values are non-zero for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def or(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => CoverageOverloadUtil.Or(tile1, tile2))
  }


  /**
   * binaryNot
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def bitwiseNot(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    // forDece:
    // 注意: 该内置方法不支持 浮点
    // double 或者 float 数据会被四舍五入为 int
    coverageTemplate(coverage, tile => Not(tile))
  }

  /**
   * Returns 0 if the input is non-zero, and 1 otherwise
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def not(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => CoverageOverloadUtil.Not(tile))
  }


  /**
   * Computes the sine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def sin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sin(tile))
  }

  /**
   * Computes the cosine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def cos(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Cos(tile))
  }

  /**
   * Computes the tangent of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def tan(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Tan(tile))
  }

  /**
   * Computes the hyperbolic sine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def sinh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sinh(tile))
  }

  /**
   * Computes the hyperbolic cosine of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def cosh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Cosh(tile))
  }

  /**
   * Computes the hyperbolic tangent of the input in radians.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def tanh(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Tanh(tile))
  }

  /**
   * Computes the arc sine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def asin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Asin(tile))
  }

  /**
   * Computes the arc cosine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def acos(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Acos(tile))
  }

  /**
   * Computes the atan sine in radians of the input.
   *
   * @param coverage the coverage rdd for operation
   * @return
   */
  def atan(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Atan(tile))
  }

  /**
   * Operation to get the Arc Tangent2 of values. The first raster holds the y-values, and the second holds the x values.
   * The arctan is calculated from y/x.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 First coverage rdd to operate.
   * @param coverage2 Second coverage rdd to operate.
   * @return
   */
  def atan2(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
            coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Atan2(tile1, tile2))
  }

  /**
   * Computes the smallest integer greater than or equal to the input.
   *
   * @param coverage The coverage for operation.
   * @return
   */
  def ceil(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Ceil(tile))
  }

  /**
   * Computes the largest integer less than or equal to the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def floor(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Floor(tile))
  }

  /**
   * Computes the integer nearest to the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def round(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Round(tile))
  }

  /**
   * Computes the natural logarithm of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def log(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Log(tile))
  }

  /**
   * Computes the base-10 logarithm of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def log10(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Log10(tile))
  }

  /**
   * Computes the square root of the input.
   *
   * @param coverage rdd for operation
   * @return
   */
  def sqrt(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Sqrt(tile))
  }

  /**
   * Computes the signum function (sign) of the input; zero if the input is zero, 1 if the input is greater than zero, -1 if the input is less than zero.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def signum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => tile.map(u => {
      if (u == NODATA) NODATA
      else if (u > 0) 1
      else if (u < 0) -1
      else 0
    }))
  }

  /**
   * Computes the absolute value of the input.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def abs(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, tile => Abs(tile))
  }

  /**
   * Returns 1 iff the first value is equal to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def eq(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Equal(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is greater than the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def gt(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Greater(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is equal or greater to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def gte(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => GreaterOrEqual(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is less than the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def lt(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
         coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Less(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is less or equal to the second for each matched pair of bands in coverage1 and coverage2.
   * if both have only 1 band, the 2 band will match
   *
   * @param coverage1 first coverage rdd to operate
   * @param coverage2 second coverage rdd to operate
   * @return
   */
  def lte(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => LessOrEqual(tile1, tile2))
  }

  /**
   * Returns 1 iff the first value is not equal to the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def neq(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Unequal(tile1, tile2))
  }

  /**
   * Returns an coverage containing all bands copied from the first input and selected bands from the second input,
   * optionally overwriting bands in the first coverage with the same name.
   *
   * @param dstCoverage first coverage
   * @param srcCoverage second coverage
   * @param names       the name of selected bands in srcCoverage
   * @param overwrite   if true, overwrite bands in the first coverage with the same name. Otherwise the bands in the first coverage will be kept.
   * @return
   */
  //这里与GEE逻辑存在出入，GEE会在overwrite==false时将重名的band加上后缀，而不是直接忽略
  //先做重投影，如果不overwrite，就直接把二者union，如果overwrite，就把不overwrite的部分给selectBands出来，再union
  def addBands(dstCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               srcCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
               names: List[String] = List.empty, overwrite: Boolean = false): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    var selectedBands = srcCoverage
    if (names.nonEmpty) {
      selectedBands = selectBands(srcCoverage, names)
    }

    if (!overwrite) {
      val (reprojected1, reprojected2) = checkProjResoExtent(selectedBands, dstCoverage)
      (reprojected1._1.union(reprojected2._1), reprojected1._2)
    } else {
      //选出reprojected1中不在覆盖名单里的波段
      val (reprojected1, reprojected2) = checkProjResoExtent(dstCoverage, selectedBands)

      val bandName1 = reprojected1._1.first()._1.measurementName
      val bandName2 = reprojected2._1.first()._1.measurementName

      val a = bandName1.filter(s => {
        !bandName2.contains(s)
      })

      val selectedCoverage = selectBands(reprojected1, a.toList)
      (selectedCoverage._1.union(reprojected2._1), reprojected1._2)
    }
  }

  /**
   * get all the bands in rdd
   *
   * @param coverage rdd for getting bands
   * @return
   */
  def bandNames(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = {
    coverage._1.first()._1.measurementName.toList.toString()
  }

  def bandNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): Int = {
    coverage._1.first()._1.measurementName.length
  }

  /**
   * get the target bands from original coverage
   *
   * @param coverage    the coverage for operation
   * @param targetBands the intersting bands
   * @return
   */
  def selectBands(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bands: List[String])
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandsAlready: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    val duplicates: List[String] = bands.diff(bands.distinct)
    if (duplicates.nonEmpty) {
      val errorMessage = s"Error: Duplicate bands found: ${duplicates.mkString(", ")}"
      throw new IllegalArgumentException(errorMessage)
    }
    val missingElements: List[String] = bands.filterNot(bandsAlready.contains)
    if (missingElements.nonEmpty) {
      //      val errorMessage = s"Error: Bands not found: ${missingElements.mkString(", ")}"
      return (coverage._1.filter(t => false), coverage._2)
    }



    (coverage._1.map(t => {
      val bandNames: mutable.ListBuffer[String] = t._1.measurementName
      val tileBands: Vector[Tile] = t._2.bands
      val newBandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      for (index <- bandNames.indices) {
        if(bands.contains(bandNames(index))){
          newBandNames += bandNames(index)
          newTileBands :+= tileBands(index)
        }
      }
      (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(newTileBands))
    }), coverage._2)

  }

  def selectBandsForMakingRDD(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bands: List[String])
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandsAlready: mutable.ListBuffer[String] = coverage._1.map(t => {
      t._1.measurementName
    }).reduce((a, b) => {
      a.union(b)
    }).distinct
    val duplicates: List[String] = bands.diff(bands.distinct)
    if (duplicates.nonEmpty) {
      val errorMessage = s"Error: Duplicate bands found: ${duplicates.mkString(", ")}"
      throw new IllegalArgumentException(errorMessage)
    }
    val missingElements: List[String] = bands.filterNot(bandsAlready.contains)
    if (missingElements.nonEmpty) {
      //      val errorMessage = s"Error: Bands not found: ${missingElements.mkString(", ")}"
      return (coverage._1.filter(t => false), coverage._2)
    }


    (coverage._1.map(t => {
      val bandNames: mutable.ListBuffer[String] = t._1.measurementName
      val tileBands: Vector[Tile] = t._2.bands
      val newBandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      for (index <- bandNames.indices) {
        if (bands.contains(bandNames(index))) {
          newBandNames += bandNames(index)
          newTileBands :+= tileBands(index)
        }
      }
      if(newBandNames.isEmpty){
        //先赋值，后面filter掉
        newBandNames+="null"
        (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(tileBands.head))
      }else{
        (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(newTileBands))
      }
    }).filter(c =>{
      c._1.measurementName.head != "null"
    }), coverage._2)

  }

  /**
   * Returns a map of the coverage's band types.
   *
   * @param coverage The coverage from which the left operand bands are taken.
   * @return Map[String, String]  key: band name, value: band type
   */
  def bandTypes(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = {
    var res :String = new String()
    var bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    coverage._1.first()._2.bands.foreach(tile => {
      res += s"${bandNames.head} -> ${tile.cellType.toString()} \n"
      bandNames = bandNames.tail
    })
    res
  }


  /**
   * Rename the bands of a coverage.Returns the renamed coverage.
   *
   * @param coverage The coverage to which to apply the operations.
   * @param name     :List[String]     The new names for the bands. Must match the number of bands in the coverage.
   * @return
   */
  def rename(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             name: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName


    if (bandNames.length != name.length) {
      val errorMessage = s"Error: The number of bands in the coverage is ${bandNames.length}, but the number of names is ${name.length}."
      throw new IllegalArgumentException(errorMessage)
    }

    val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {
      val tileBands: Vector[Tile] = t._2.bands
      val newBandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      for (index <- name.indices) {
        newBandNames += name(index)
        newTileBands :+= tileBands(index)
      }
      (SpaceTimeBandKey(t._1.spaceTimeKey, newBandNames), MultibandTile(newTileBands))
    })
    (newCoverageRdd, coverage._2)

  }

  /**
   * Raises the first value to the power of the second for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def pow(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
          coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Pow(tile1, tile2))
  }

  def powNum(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
             i: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, (tile) => geotrellis.raster.mapalgebra.local.Pow(tile, i))
  }

  /**
   * Selects the minimum of the first and second values for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def mini(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
           coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Min(tile1, tile2))
  }

  /**
   * Selects the maximum of the first and second values for each matched pair of bands in coverage1 and coverage2.
   *
   * @param coverage1 The coverage from which the left operand bands are taken.
   * @param coverage2 The coverage from which the right operand bands are taken.
   * @return
   */
  def maxi(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
           coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage1, coverage2, (tile1, tile2) => Max(tile1, tile2))
  }

  /**
   * Applies a morphological mean filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMean(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Mean.apply, radius)
  }

  /**
   * Applies a morphological median filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMedian(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                  kernelType: String,
                  radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Median.apply, radius)
  }

  /**
   * Applies a morphological max filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use. Options include: 'circle', 'square'. It only supports square and
   *                   circle kernels by now.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMax(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Max.apply, radius)
  }

  /**
   * Applies a morphological min filter to each band of an coverage using a named or custom kernel.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use. Options include: 'circle', 'square'. It only supports square and
   *                   circle kernels by now.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMin(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
               radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Min.apply, radius)
  }

  /**
   * Applies a morphological mode filter to each band of an coverage using a named or custom kernel. Mode does not currently support Double raster data. If you use a raster with a Double CellType, the raster will be rounded to integers.
   *
   * @param coverage   The coverage to which to apply the operations.
   * @param kernelType The type of kernel to use.
   * @param radius     The radius of the kernel to use.
   * @return
   */
  def focalMode(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernelType: String,
                radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, kernelType, focal.Mode.apply, radius)
  }

  /**
   * Selects a contiguous group of bands from a coverage by position.
   *
   * @param coverage The coverage from which to select bands.
   * @param start    Where to start the selection.
   * @param end      Where to end the selection.
   */
  def slice(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), start: Int, end: Int):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (start < 0 || start > coverage._1.first()._1.measurementName.length)
      throw new IllegalArgumentException("Start index out of range!")
    if (end < 0 || end > coverage._1.first()._1.measurementName.length)
      throw new IllegalArgumentException("End index out of range!")
    if (end <= start)
      throw new IllegalArgumentException("End index should be greater than the start index!")
    val bandNames: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName.slice(start, end)
    val newBands: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverage, bandNames.toList)
    newBands
  }

  /**
   * Maps from input values to output values, represented by two parallel lists. Any input values not included in the input list are either set to defaultValue if it is given, or masked if it isn't.
   *
   * @param coverage
   * @param from
   * @param to
   * @param defaultValue
   * @return
   */
  def remap(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), from: List[Int],
            to: List[Double], defaultValue: String = null): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    if (to.length != from.length) {
      throw new IllegalArgumentException("The length of two lists not same!")
    }

    if (defaultValue == null) {
      coverageTemplate(coverage, (tile) => RemapWithoutDefaultValue(tile, from.zip(to).toMap))
    } else {
      coverageTemplate(coverage, (tile) => RemapWithDefaultValue(tile, from.zip(to).toMap, defaultValue.toDouble))
    }
  }


  /**
   * Convolve each band of a coverage with the given kernel. Coverages will be padded with Zeroes.
   *
   * @param coverage The coverage to convolve.
   * @param kernel   The kernel to convolve with.
   * @return
   */
  def convolve(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), kernel: focal
  .Kernel): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage1 = coverage.collect().toMap
    val radius = kernel.extent
    val convolvedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage.map(t => {
      val col = t._1.spaceTimeKey.col
      val row = t._1.spaceTimeKey.row
      val time0 = t._1.spaceTimeKey.time

      val cols = t._2.cols
      val rows = t._2.rows

      var arrayBuffer: mutable.ArrayBuffer[Tile] = new mutable.ArrayBuffer[Tile] {}
      for (index <- t._2.bands.indices) {
        val arrBuffer: Array[Double] = Array.ofDim[Double]((cols + 2 * radius) * (rows + 2 * radius))
        //arr转换为Tile，并拷贝原tile数据
        val tilePadded: Tile = ArrayTile(arrBuffer, cols + 2 * radius, rows + 2 * radius).convert(t._2.cellType)
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            tilePadded.mutable.setDouble(i + radius, j + radius, t._2.bands(index).getDouble(i, j))
          }
        }
        //填充八邻域数据及自身数据，使用mutable进行性能优化
        //左上
        var tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row - 1, time0), t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, 0)
            }
          }
        }
        //上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, 0)
            }
          }
        }

        //右上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, 0)
            }
          }
        }
        //左
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, 0)
            }
          }
        }

        //右
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, 0)
            }
          }
        }

        //左下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, 0)
            }
          }
        }

        //下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, 0)
            }
          }
        }
        //右下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, 0)
            }
          }
        }

        //运算
        val tilePaddedRes: Tile = focal.Convolve(tilePadded, kernel, None, TargetCell.All)


        //将tilePaddedRes 中的值转移进tile
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            if (!isNoData(t._2.bands(index).getDouble(i, j)))
              t._2.bands(index).mutable.setDouble(i, j, tilePaddedRes.getDouble(i + radius, j + radius))
            else
              t._2.bands(index).mutable.setDouble(i, j, Double.NaN)
            //            t._2.bands(index).mutable.set(i, j, 1)
          }
        }
      }
      (t._1, t._2)
    })
    (convolvedRDD, coverage._2)

  }

  /**
   * Calculates slope in degrees from a terrain DEM.
   *
   * @param coverage The elevation coverage.
   * @param radius   The radius of the neighbors in computation.
   * @param zFactor  The use of a z-factor is essential for correct slope calculations when the surface z units are expressed in units different from the ground x,y units.
   * @return
   */
  def slope(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), radius: Int, zFactor: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage1 = coverage.collect().toMap
    val neighborhood = focal.Square(radius)
    val convolvedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage.map(t => {
      val col = t._1.spaceTimeKey.col
      val row = t._1.spaceTimeKey.row
      val time0 = t._1.spaceTimeKey.time

      val cols = t._2.cols
      val rows = t._2.rows

      var arrayBuffer: mutable.ArrayBuffer[Tile] = new mutable.ArrayBuffer[Tile] {}
      for (index <- t._2.bands.indices) {
        val arrBuffer: Array[Double] = Array.ofDim[Double]((cols + 2 * radius) * (rows + 2 * radius))
        //arr转换为Tile，并拷贝原tile数据
        val tilePadded: Tile = ArrayTile(arrBuffer, cols + 2 * radius, rows + 2 * radius).convert(t._2.cellType)
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            tilePadded.mutable.setDouble(i + radius, j + radius, t._2.bands(index).getDouble(i, j))
          }
        }
        //填充八邻域数据及自身数据，使用mutable进行性能优化
        //左上
        var tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row - 1, time0), t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, 0)
            }
          }
        }
        //上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, 0)
            }
          }
        }

        //右上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, 0)
            }
          }
        }
        //左
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, 0)
            }
          }
        }

        //右
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, 0)
            }
          }
        }

        //左下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, 0)
            }
          }
        }

        //下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, 0)
            }
          }
        }
        //右下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, 0)
            }
          }
        }

        //运算
        val tilePaddedRes: Tile = focal.Slope(tilePadded, neighborhood, None, coverage._2.cellSize, zFactor, TargetCell.All)


        //将tilePaddedRes 中的值转移进tile
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            if (!isNoData(t._2.bands(index).getDouble(i, j)))
              t._2.bands(index).mutable.setDouble(i, j, tilePaddedRes.getDouble(i + radius, j + radius))
            else
              t._2.bands(index).mutable.setDouble(i, j, Double.NaN)
            //            t._2.bands(index).mutable.set(i, j, 1)
          }
        }
      }
      (t._1, t._2)
    })
    (convolvedRDD, coverage._2)
  }

  def filter(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), min: Double,max: Double):(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) ={
    (coverage._1.map(t =>{
      (t._1,t._2.mapBands((_,tile) =>{
        tile.mapDouble(p =>{
          if(p>max || p<min){
            doubleNODATA
          }else{
            p
          }
        })
      }))
    }),coverage._2)
  }

  /**
   * Calculates the aspect of the coverage.
   *
   * @param coverage The elevation coverage.
   * @param radius   The radius of the neighbors in computation.
   * @return
   */
  def aspect(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), radius: Int): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage1 = coverage.collect().toMap
    val neighborhood = focal.Square(radius)
    val convolvedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage.map(t => {
      val col = t._1.spaceTimeKey.col
      val row = t._1.spaceTimeKey.row
      val time0 = t._1.spaceTimeKey.time

      val cols = t._2.cols
      val rows = t._2.rows

      var arrayBuffer: mutable.ArrayBuffer[Tile] = new mutable.ArrayBuffer[Tile] {}
      for (index <- t._2.bands.indices) {
        val arrBuffer: Array[Double] = Array.ofDim[Double]((cols + 2 * radius) * (rows + 2 * radius))
        //arr转换为Tile，并拷贝原tile数据
        val tilePadded: Tile = ArrayTile(arrBuffer, cols + 2 * radius, rows + 2 * radius).convert(t._2.cellType)
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            tilePadded.mutable.setDouble(i + radius, j + radius, t._2.bands(index).getDouble(i, j))
          }
        }
        //填充八邻域数据及自身数据，使用mutable进行性能优化
        //左上
        var tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row - 1, time0), t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(0 + x, 0 + y, 0)
            }
          }
        }
        //上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, 0 + y, 0)
            }
          }
        }

        //右上
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row - 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, tiles.toList.head.bands(index).getDouble(x, rows - radius + y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, 0 + y, 0)
            }
          }
        }
        //左
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(0 + x, radius + y, 0)
            }
          }
        }

        //右
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (rows)) {
              tilePadded.mutable.setDouble(cols + radius + x, radius + y, 0)
            }
          }
        }

        //左下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col - 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, tiles.toList.head.bands(index).getDouble(cols - radius + x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(x, rows + radius + y, 0)
            }
          }
        }

        //下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (cols)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(radius + x, rows + radius + y, 0)
            }
          }
        }
        //右下
        tiles = getTileFromCoverage(coverage1, SpaceTimeBandKey(SpaceTimeKey(col + 1, row + 1, time0),
          t._1.measurementName))
        if (tiles.nonEmpty) {
          //拷贝
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, tiles.toList.head.bands(index).getDouble(x, y))
            }
          }
        } else {
          //赋0
          for (x <- 0 until (radius)) {
            for (y <- 0 until (radius)) {
              tilePadded.mutable.setDouble(cols + radius + x, rows + radius + y, 0)
            }
          }
        }

        //运算
        val tilePaddedRes: Tile = focal.Aspect(tilePadded, neighborhood, None, coverage._2.cellSize, TargetCell.All)


        //将tilePaddedRes 中的值转移进tile
        for (i <- 0 until (cols)) {
          for (j <- 0 until (rows)) {
            if (!isNoData(t._2.bands(index).getDouble(i, j)))
              t._2.bands(index).mutable.setDouble(i, j, tilePaddedRes.getDouble(i + radius, j + radius))
            else
              t._2.bands(index).mutable.setDouble(i, j, Double.NaN)
            //            t._2.bands(index).mutable.set(i, j, 1)
          }
        }
      }
      (t._1, t._2)
    })
    (convolvedRDD, coverage._2)
  }


  /**
   * Generates a Chart from an image. Computes and plots histograms of the values of the bands in the specified region of the image.
   *
   * @param coverage The coverage to which to apply the operations.
   * @param scale    The pixel scale used when applying the histogram reducer, in meters.
   * @return
   */
  def histogram(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), scale: Double): Map[Int, Long] = {

    val resolution: Double = coverage._2.cellSize.resolution
    val resampledRDD = coverage._1.map(t => {
      val tile = t._2
      val resampledTile = tile.resample((tile.cols * scale / resolution).toInt, (tile.rows * scale / resolution).toInt)
      (t._1, resampledTile)
    })

    val histRDD: RDD[Histogram[Int]] = coverage._1.map(t => {
      t._2.histogram
    }).flatMap(t => {
      t
    })

    histRDD.reduce((a, b) => {
      a.merge(b)
    }).binCounts().toMap[Int, Long]

  }

  /**
   * Returns the projection of an coverage.
   *
   * @param coverage The coverage to which to get the projection.
   * @return
   */
  def projection(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): String = coverage._2.crs.toString()


  /**
   * Force a coverage to be computed in a given projection and resolution.
   *
   * @param coverage The image to reproject.
   * @param crs      The CRS to project the image to.
   * @param scale    resolution
   * @return The reprojected image.
   */
  def reproject(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                crs: CRS, scale: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val band = coverage._1.first()._1.measurementName
    val resampleTime = Instant.now.getEpochSecond
    val coverageTileRDD = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val extent = coverage._2.extent.reproject(coverage._2.crs, crs)
    val tl = TileLayout(math.max(1,((extent.xmax - extent.xmin) / (scale * 256)).toInt), math.max(1,((extent.ymax - extent.ymin) / (scale * 256)).toInt), 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = coverage._2.cellType
    val srcLayout = coverage._2.layout
    val srcExtent = coverage._2.extent
    val srcCrs = coverage._2.crs
    val srcBounds = coverage._2.bounds
    val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2))
    val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val coverageTileLayerRDD = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData);


    //    val coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)
    //    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    //    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    //    val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
    //    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
    //    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
    //      (t._1.spaceTimeKey.spatialKey, t._2)
    //    })
    //    var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)
    //    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
    //      coverageTMS.reproject(tmsCrs, layoutScheme)

    val (_, coverageTileRDDWithMetadata) = coverageTileLayerRDD.reproject(crs, ld, geotrellis.raster.resample.Bilinear)
    val coverageNoband = (coverageTileRDDWithMetadata.mapValues(tile => tile: MultibandTile), coverageTileRDDWithMetadata.metadata)
    val newBound = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(coverageNoband._2.tileLayout.layoutCols - 1, coverageNoband._2.tileLayout.layoutRows - 1, resampleTime))
    val newMetadata = TileLayerMetadata(coverageNoband._2.cellType, coverageNoband._2.layout, coverageNoband._2.extent, coverageNoband._2.crs, newBound)
    val coverageRDD = coverageNoband.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band), t._2)
    })
    (coverageRDD, newMetadata)
  }

  /**
   * 自定义重采样方法，功能有局限性，勿用
   *
   * @param coverage   // The coverage to resample
   * @param targetZoom // The zoom level to which to resample the coverage
   * @param mode       // The resample method
   * @return // The resampled coverage
   */
  def resampleToTargetZoom(
                            coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                            targetZoom: Int,
                            mode: String
                          )
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val resampleMethod: PointResampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }

    // 求解出原始影像的zoom
    //    val myAcc2: LongAccumulator = sc.longAccumulator("myAcc2")
    //    //    println("srcAcc0 = " + myAcc2.value)
    //
    //    reprojected.map(t => {
    //
    //
    //      //          println("srcRows = " + t._2.rows) 256
    //      //          println("srcRows = " + t._2.cols) 256
    //      myAcc2.add(1)
    //      t
    //    }).collect()
    //    println("srcNumOfTiles = " + myAcc2.value) // 234


    //val level: Int = COGUtil.tileDifference
    val level: Int = targetZoom - COGUtil.tmsLevel // The difference between the target level and the zoom level of the front-end TMS

    if (level > 0) {
      val time1 = System.currentTimeMillis()
      val coverageResampled: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {
        (t._1, t._2.resample(t._2.cols * (1 << level), t._2.rows * (1 << level), resampleMethod))
      })
      val time2 = System.currentTimeMillis()
      println("Resample Time is " + (time2 - time1))
      (coverageResampled,
        TileLayerMetadata(coverage._2.cellType, LayoutDefinition(
          coverage._2.extent,
          TileLayout(coverage._2.layoutCols, coverage._2.layoutRows,
            coverage._2.tileCols * (1 << level), coverage._2.tileRows * (1 << level))
        ), coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    }
    else if (level < 0) {
      val time1 = System.currentTimeMillis()
      val coverageResampled: RDD[(SpaceTimeBandKey, MultibandTile)] = coverage._1.map(t => {

        val cols = if ((t._2.cols / (1 << -level)) > 1) Math.ceil(t._2.cols.toDouble / (1 << -level)).toInt else 1
        val rows = if ((t._2.rows / (1 << -level)) > 1) Math.ceil(t._2.rows.toDouble / (1 << -level)).toInt else 1
        val tileResampled: MultibandTile = t._2.resample(cols, rows, resampleMethod)
        (t._1, tileResampled)
      })


      var tileCols = if ((coverage._2.tileCols / (1 << -level)) > 1) Math.ceil(coverage._2.tileCols.toDouble / (1 << -level)).toInt else 1
      var tileRows = if ((coverage._2.tileRows / (1 << -level)) > 1) Math.ceil(coverage._2.tileRows.toDouble / (1 << -level)).toInt else 1


      val time2 = System.currentTimeMillis()
      println("Resample Time is " + (time2 - time1))
      //      (coverageResampled, TileLayerMetadata(coverage._2.cellType, LayoutDefinition(coverage._2.extent, TileLayout(coverage._2.layoutCols, coverage._2.layoutRows, coverage._2.tileCols / (1 << -level),
      //        coverage._2.tileRows / (1 << -level))), coverage._2.extent, coverage._2.crs, coverage._2.bounds))

      println("coverage._2.tileCols.toDouble = " + coverage._2.tileCols.toDouble)
      (coverageResampled, TileLayerMetadata(coverage._2.cellType, LayoutDefinition(
        coverage._2.extent, TileLayout(coverage._2.layoutCols, coverage._2.layoutRows, tileCols, tileRows)),
        coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    }
    else coverage
  }


  /**
   * Resample the coverage
   *
   * @param coverage The coverage to resample
   * @param level    The resampling level. eg:1 for up-sampling and -1 for down-sampling.
   * @param mode     The interpolation mode to use
   * @return
   */

  def resample(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), level: Double, mode: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = { // TODO 重采样
    val resampleMethod = mode match {
      case "Bilinear" => geotrellis.raster.resample.Bilinear
      case "CubicConvolution" => geotrellis.raster.resample.CubicConvolution
      case _ => geotrellis.raster.resample.NearestNeighbor
    }
    val crs = coverage._2.crs
    val band = coverage._1.first()._1.measurementName
    val resampleTime = Instant.now.getEpochSecond
    val coverageTileRDD = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val extent = coverage._2.extent.reproject(coverage._2.crs, crs)
    val tl = TileLayout(math.max(1, (coverage._2.layout.layoutCols*level).toInt), math.max(1, (coverage._2.layout.layoutCols*level).toInt), 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val cellType = coverage._2.cellType
    val srcLayout = coverage._2.layout
    val srcExtent = coverage._2.extent
    val srcCrs = coverage._2.crs
    val srcBounds = coverage._2.bounds
    val newBounds = Bounds(SpatialKey(0, 0), SpatialKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2))
    val rasterMetaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })
    val coverageTileLayerRDD = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData);



    val (_, coverageTileRDDWithMetadata) = coverageTileLayerRDD.reproject(crs, ld, resampleMethod)
    val coverageNoband = (coverageTileRDDWithMetadata.mapValues(tile => tile: MultibandTile), coverageTileRDDWithMetadata.metadata)
    val newBound = Bounds(SpaceTimeKey(0, 0, resampleTime), SpaceTimeKey(coverageNoband._2.tileLayout.layoutCols - 1, coverageNoband._2.tileLayout.layoutRows - 1, resampleTime))
    val newMetadata = TileLayerMetadata(coverageNoband._2.cellType, coverageNoband._2.layout, coverageNoband._2.extent, coverageNoband._2.crs, newBound)
    val coverageRDD = coverageNoband.map(t => {
      (SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, resampleTime), band), t._2)
    })
    (coverageRDD, newMetadata)
  }

  /**
   * Calculates the gradient.
   *
   * @param coverage The coverage to compute the gradient.
   * @return
   */

  def gradient(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val sobelx = geotrellis.raster.mapalgebra.focal.Kernel(IntArrayTile(Array[Int](-1, 0, 1, -2, 0, 2, -1, 0, 1), 3, 3))
    val sobely = geotrellis.raster.mapalgebra.focal.Kernel(IntArrayTile(Array[Int](1, 2, 1, 0, 0, 0, -1, -2, -1), 3, 3))
    val tilex = convolve(coverage, sobelx)
    val tiley = convolve(coverage, sobely)
    sqrt(add(powNum(tilex, 2), powNum(tiley, 2)))
  }


  /**
   * Clip the raster with the geometry (with the same crs).
   *
   * @param coverage The coverage to clip.
   * @param geom     The geometry used to clip.
   * @return
   */

  def clip(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), geom: Geometry
          ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val RDDExtent: Extent = coverage._2.extent
    val reso: Double = coverage._2.cellSize.resolution
    val coverageRDDWithExtent = coverage._1.map(t => {
      val tileExtent = Extent(RDDExtent.xmin + t._1.spaceTimeKey.col * reso * 256, RDDExtent.ymax - (t._1.spaceTimeKey.row + 1) * 256 * reso,
        RDDExtent.xmin + (t._1.spaceTimeKey.col + 1) * reso * 256, RDDExtent.ymax - t._1.spaceTimeKey.row * 256 * reso)
      (t._1, (t._2, tileExtent))
    })
    val tilesIntersectedRDD: RDD[(SpaceTimeBandKey, (MultibandTile, Extent))] = coverageRDDWithExtent.filter(t => {
      t._2._2.intersects(geom)
    })
    val tilesClippedRDD: RDD[(SpaceTimeBandKey, MultibandTile)] = tilesIntersectedRDD.map(t => {
      val tileClipped = t._2._1.mask(t._2._2, geom)
      (t._1, tileClipped)
    })
    val extents = tilesIntersectedRDD
      .map(t => (t._2._2.xmin, t._2._2.ymin, t._2._2.xmax, t._2._2.ymax))
      .reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4)))
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val colRowInstant = tilesIntersectedRDD
      .map(t => (t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, t._1.spaceTimeKey.instant, t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, t._1.spaceTimeKey.instant))
      .reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.min(a._3, b._3), math.max(a._4, b._4), math.max(a._5, b._5), math.max(a._6, b._6)))
    val tl = TileLayout(colRowInstant._4 - colRowInstant._1, colRowInstant._5 - colRowInstant._2, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val newbounds = Bounds(SpaceTimeKey(colRowInstant._1, colRowInstant._2, colRowInstant._3), SpaceTimeKey(colRowInstant._4, colRowInstant._5, colRowInstant._6))
    val newlayerMetaData = TileLayerMetadata(coverage._2.cellType, ld, extent, coverage._2.crs, newbounds)
    (tilesClippedRDD, newlayerMetaData)
  }


  /**
   * Clamp the raster between low and high
   *
   * @param coverage The coverage to clamp.
   * @param low      The low value.
   * @param high     The high value.
   * @return
   */

  def clamp(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), low: Int, high: Int
           ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageRddClamped = coverage._1.map(t =>
      (t._1, t._2.mapBands((t1, t2) => t2.map(t3 => {
        if (t3 > high) {
          high
        }
        else if (t3 < low) {
          low
        }
        else {
          t3
        }
      }
      ))))

    (coverageRddClamped, coverage._2)
  }


  /**
   * Transforms the coverage from the RGB color space to the HSV color space.
   * Expects a 3 band coverage in the range [0, 255],
   * and produces three bands: hue, saturation and value with values in the range [0, 1].
   *
   * @param coverage The coverage with three bands: R, G, B.
   * @return
   */
  def rgbToHsv(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    // TODO: 如何区分 RGB? 我这里默认索引 RGB 顺序

    if (!coverage._1.first()._2.bandCount.equals(3)) {
      throw new
          IllegalArgumentException("Tile' s bandCount must be three!")
    }

    // 保留原来的 SpaceTimeBandKey, 分离三通道
    val coverageRRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(0)))
    val coverageGRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(1)))
    val coverageBRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(2)))

    // 得到三波段 Tile
    val joinedRGBRdd: RDD[(SpaceTimeBandKey, (Tile, Tile, Tile))] =
      coverageRRdd.join(coverageGRdd).join(coverageBRdd)
        .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2)))
    val hsvRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = joinedRGBRdd.map(t => {
      // 得到每个 Tile 对应的 HSV
      val hTile: MutableArrayTile = t._2._1.convert(CellType.fromName("float32")).mutable
      val sTile: MutableArrayTile = t._2._2.convert(CellType.fromName("float32")).mutable
      val vTile: MutableArrayTile = t._2._3.convert(CellType.fromName("float32")).mutable

      val tileRows: Int = t._2._1.rows
      val tileCols: Int = t._2._1.cols
      for (i <- 0 until tileRows; j <- 0 until tileCols) {
        val r: Double = t._2._1.getDouble(i, j) / 255.0
        val g: Double = t._2._2.getDouble(i, j) / 255.0
        val b: Double = t._2._3.getDouble(i, j) / 255.0
        val cMax: Double = math.max(math.max(r, g), b)
        val cMin: Double = math.min(math.min(r, g), b)
        if (cMax.equals(cMin)) {
          hTile.setDouble(i, j, 0.0)
        } else {
          cMax match {
            case r =>
              hTile.setDouble(i, j,
                if (g >= b) {
                  60 * ((g - b) / (cMax - cMin))
                } else {
                  60 * ((g - b) / (cMax - cMin)) + 360
                })

            case g =>
              hTile.setDouble(i, j,
                60 * ((b - r) / (cMax - cMin)) + 120)
            case b =>
              hTile.setDouble(i, j,
                60 * ((r - g) / (cMax - cMin)) + 240)
          }
        }

        if (math.abs(cMax - 0.0) < 1e-7) {
          sTile.setDouble(i, j, 0.0)
        } else {
          sTile.setDouble(i, j,
            (cMax - cMin) / cMax)
        }
        vTile.setDouble(i, j, cMax)

      }


      (t._1, MultibandTile(Array(hTile, sTile, vTile)))

    })

    // 使用原来的 TileLayerMetadata
    (hsvRdd, coverage._2)
  }


  /**
   * Transforms the coverage from the HSV color space to the RGB color space.
   * Expects a 3 band coverage in the range [0, 1],
   * and produces three bands: red, green and blue with values in the range [0, 255].
   *
   * @param coverage The coverage with three bands: H, S, V
   * @return
   */
  def hsvToRgb(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    if (!coverage._1.first()._2.bandCount.equals(3)) {
      throw new
          IllegalArgumentException("Tile' s bandCount must be three!")
    }


    def isCoverageEqual(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                        coverage2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
    : Boolean = {
      // 相减后判断是否全 0
      subtract(coverage1, coverage2).map(multiTile =>
        multiTile._2.bands.filter(
          tile => math.abs(tile.findMinMax._2) < 1e-6)
      ).isEmpty()

    }

    // TODO: 默认索引 hsv
    val coverageHRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(0)))
    val coverageSRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(1)))
    val coverageVRdd: RDD[(SpaceTimeBandKey, Tile)] =
      coverage._1.map(t => (t._1, t._2.band(2)))


    // 得到三波段 Tile (HSV)
    val joinedHSVRdd: RDD[(SpaceTimeBandKey, (Tile, Tile, Tile))] =
      coverageHRdd.join(coverageSRdd).join(coverageVRdd)
        .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2)))

    val rgbRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = joinedHSVRdd.map(t => {
      val rTile: MutableArrayTile = t._2._1.convert(CellType.fromName("uint8")).mutable
      val gTile: MutableArrayTile = t._2._2.convert(CellType.fromName("uint8")).mutable
      val bTile: MutableArrayTile = t._2._3.convert(CellType.fromName("uint8")).mutable

      val tileRows: Int = t._2._1.rows
      val tileCols: Int = t._2._1.cols
      for (i <- 0 until tileRows; j <- 0 until tileCols) {
        val h: Double = t._2._1.getDouble(i, j) * 360.0
        val s: Double = t._2._2.getDouble(i, j)
        val v: Double = t._2._3.getDouble(i, j)

        val f: Double = (h / 60) - ((h / 60).toInt % 6)
        val p: Double = v * (1 - s)
        val q: Double = v * (1 - f * s)
        val u: Double = v * (1 - (1 - f) * s)

        ((h / 60).toInt % 6) match {
          case 0 =>
            rTile.set(i, j, (v * 255).toInt)
            gTile.set(i, j, (u * 255).toInt)
            bTile.set(i, j, (p * 255).toInt)
          case 1 =>
            rTile.set(i, j, (q * 255).toInt)
            gTile.set(i, j, (v * 255).toInt)
            bTile.set(i, j, (p * 255).toInt)
          case 2 =>
            rTile.set(i, j, (p * 255).toInt)
            gTile.set(i, j, (v * 255).toInt)
            bTile.set(i, j, (u * 255).toInt)
          case 3 =>
            rTile.set(i, j, (p * 255).toInt)
            gTile.set(i, j, (q * 255).toInt)
            bTile.set(i, j, (v * 255).toInt)
          case 4 =>
            rTile.set(i, j, (u * 255).toInt)
            gTile.set(i, j, (p * 255).toInt)
            bTile.set(i, j, (v * 255).toInt)
          case 5 =>
            rTile.set(i, j, (v * 255).toInt)
            gTile.set(i, j, (p * 255).toInt)
            bTile.set(i, j, (q * 255).toInt)

          case _ =>
          //            throw new RuntimeException("Error illegal Hue...")
        } // end match
      } // end for


      (t._1, MultibandTile(Array(rTile, gTile, bTile)))
    })
    (rgbRdd, coverage._2)
  }

  /**
   * Computes the windowed entropy using the specified kernel centered on each input pixel.
   * Entropy is computed as
   * -sum(p * log2(p)), where p is the normalized probability
   * of occurrence of the values encountered in each window.
   *
   * @param coverage The coverage to compute the entropy.
   * @param radius   The radius of the square neighborhood to compute the entropy,
   *                 1 for a 3×3 square.
   * @return
   */
  def entropy(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
              kernelType: String,
              radius: Int)
  : (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    focalMethods(coverage, "square", Entropy.apply, radius)
  }


  //Local caculation.
  protected def stack(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), nums: Int):
  (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    if (coverage._1.first()._1.measurementName.length != 1) {
      coverage
    } else {
      val name: String = coverage._1.first()._1.measurementName(0)
      var names = mutable.ListBuffer.empty[String]
      for (i <- 0 until (nums)) {
        names += name
      }
      (coverage._1.map(t => {
        var tiles: Vector[Tile] = Vector[Tile]()
        for (i <- 0 until (nums)) {
          tiles = tiles :+ t._2.bands(0)
        }
        (SpaceTimeBandKey(t._1.spaceTimeKey, names), MultibandTile(tiles))
      }), coverage._2)
    }
  }

  /**
   * Computes the cubic root of the input.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def cbrt(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, (tile) => Cbrt(tile))
  }

  /**
   * Return the metadata of the input coverage.
   *
   * @param coverage The coverage to get the metadata
   * @return
   */
  def metadata(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))
  : String = {
    val crs = coverage._2.crs.toString()
    val extent = coverage._2.extent.toString()
    val cellSize = coverage._2.cellheight

    val string = s"CRS: $crs \nExtent: $extent \nResolution: $cellSize"

    string
  }

  /**
   * Casts the input value to a signed 8-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    (coverage._1.map(t => {
      (t._1, t._2.convert(ByteConstantNoDataCellType))
    }), TileLayerMetadata(ByteConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
  }

  /**
   * Casts the input value to a unsigned 8-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint8(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UByteConstantNoDataCellType))
    }), TileLayerMetadata(UByteConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted

  }


  /**
   * Casts the input value to a signed 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(ShortConstantNoDataCellType))
    }), TileLayerMetadata(ShortConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a unsigned 16-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toUint16(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(UShortConstantNoDataCellType))
    }), TileLayerMetadata(UShortConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs,
      coverage._2.bounds))
    coverageConverted
  }


  /**
   * Casts the input value to a signed 32-bit integer.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toInt32(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(IntConstantNoDataCellType))
    }), TileLayerMetadata(IntConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a 32-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toFloat(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
             ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(FloatConstantNoDataCellType))
    }), TileLayerMetadata(FloatConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  /**
   * Casts the input value to a 64-bit float.
   *
   * @param coverage The coverage to which the operation is applied.
   * @return
   */
  def toDouble(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])
              ): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageConverted = (coverage._1.map(t => {
      (t._1, t._2.convert(DoubleConstantNoDataCellType))
    }), TileLayerMetadata(DoubleConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    coverageConverted
  }

  //去除图像黑边，黑边为值为0的单元 TODO:添加到图像读取函数中，用户在读取图象时要指定读取图像的格式
  def removeZeroFromCoverage(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    coverageTemplate(coverage, (tile) => removeZeroFromTile(tile))
  }

  def removeZeroFromTile(tile: Tile): Tile = {
    if (tile.cellType.isFloatingPoint)
      tile.mapDouble(i => if (i.equals(0.0)) Double.NaN else i)
    else
      tile.map(i => if (i == 0) NODATA else i)
  }

  /**
   * Generate a coverage with the values from the first coverage, but only include cells in which the corresponding
   * cell in the second coverage *are not* set to the "readMask" value. Otherwise, the value of the cell will be set
   * to the "writeMask".
   * Both coverages are required to have the same number of bands, or the coverage2 must have 1 bands.
   * If the coverage2 has a band count of 1, the mask is applied to each band of coverage1.
   *
   * @param coverage1 The input coverage.
   * @param coverage2 The Mask.
   * @param readMask  The number to be masked in the Mask.
   * @param writeMask The number will be set if the values from coverage2 is equal to readMask.
   */
  def mask(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2: (RDD[
    (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), readMask: Int, writeMask: Int): (RDD[(SpaceTimeBandKey, MultibandTile)],
    TileLayerMetadata[SpaceTimeKey]) = {
    def maskTemplate(coverage1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coverage2:
    (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), func: (Tile, Tile, Int, Int) => Tile): (RDD[
      (SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

      def funcMulti(multibandTile1: MultibandTile, multibandTile2: MultibandTile): MultibandTile = {
        val bands1: Vector[Tile] = multibandTile1.bands
        val bands2: Vector[Tile] = multibandTile2.bands
        val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
        for (i <- bands1.indices) {
          bandsFunc.append(func(bands1(i), bands2(i), readMask, writeMask))
        }
        MultibandTile(bandsFunc)
      }

      var cellType: CellType = null
      val resampleTime: Long = Instant.now.getEpochSecond
      val (coverage1Reprojected, coverage2Reprojected) = checkProjResoExtent(coverage1, coverage2)
      val bandList1: mutable.ListBuffer[String] = coverage1Reprojected._1.first()._1.measurementName
      val bandList2: mutable.ListBuffer[String] = coverage2Reprojected._1.first()._1.measurementName
      if (bandList1.length == bandList2.length) {
        val coverage1NoTime: RDD[(SpatialKey, MultibandTile)] = coverage1Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
        val coverage2NoTime: RDD[(SpatialKey, MultibandTile)] = coverage2Reprojected._1.map(t => (t._1.spaceTimeKey.spatialKey, t._2))
        val rdd: RDD[(SpatialKey, (MultibandTile, MultibandTile))] = coverage1NoTime.join(coverage2NoTime)
        if (bandList1.diff(bandList2).isEmpty && bandList2.diff(bandList1).isEmpty) {
          val indexMap: Map[String, Int] = bandList2.zipWithIndex.toMap
          val indices: mutable.ListBuffer[Int] = bandList1.map(indexMap)
          (rdd.map(t => {
            val bands1: Vector[Tile] = t._2._1.bands
            val bands2: Vector[Tile] = t._2._2.bands
            cellType = func(bands1.head, bands2(indices.head), readMask, writeMask).cellType
            val bandsFunc: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer.empty[Tile]
            for (i <- bands1.indices) {
              bandsFunc.append(func(bands1(i), bands2(indices(i)), readMask, writeMask))
            }
            (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), MultibandTile(bandsFunc))
          }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
        }
        else {
          cellType = funcMulti(coverage1Reprojected._1.first()._2, coverage2Reprojected._1.first()._2).cellType
          (rdd.map(t => {
            (SpaceTimeBandKey(SpaceTimeKey(t._1.col, t._1.row, resampleTime), bandList1), funcMulti(t._2._1, t._2._2))
          }), TileLayerMetadata(cellType, coverage1Reprojected._2.layout, coverage1Reprojected._2.extent, coverage1Reprojected._2.crs, coverage1Reprojected._2.bounds))
        }
      }
      else {
        throw new IllegalArgumentException("Error: 波段数量不相等")
      }
    }

    if (coverage2._1.first()._1.measurementName.length == 1) {
      val coverageStacked = stack(coverage2, coverage1._1.first()._1.measurementName.length)
      maskTemplate(coverage1, coverageStacked, (tile1, tile2, readMask, writeMask) => Mask(tile1, tile2, readMask,
        writeMask))
    }
    else if (coverage1._1.first()._1.measurementName.length == coverage2._1.first()._1.measurementName.length) {
      maskTemplate(coverage1, coverage2, (tile1, tile2, readMask, writeMask) => Mask(tile1, tile2, readMask, writeMask))
    } else {
      throw new IllegalArgumentException("输入Coverage与掩膜Coverage波段数不匹配")
    }

  }

  def intoOne(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bands: List[String]): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverageSelected = selectBands(coverage, bands)
    val coverage1 = (coverageSelected._1.map(t => {
      var bandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      bandNames :+= t._1.measurementName(0)
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      newTileBands :+= Mean(t._2.bands)
      (SpaceTimeBandKey(t._1.spaceTimeKey, bandNames), MultibandTile(newTileBands))
    }), coverageSelected._2)
    coverage1
  }

  def intoOne(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val coverage1 = (coverage._1.map(t => {
      var bandNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
      bandNames :+= t._1.measurementName(0)
      var newTileBands: Vector[Tile] = Vector.empty[Tile]
      newTileBands :+= Mean(t._2.bands)
      (SpaceTimeBandKey(t._1.spaceTimeKey, bandNames), MultibandTile(newTileBands))
    }), coverage._2)
    coverage1
  }

  def addStyles(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val coverageVis1: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = (coverage._1.map(t => (t._1, t._2.convert(DoubleConstantNoDataCellType))), TileLayerMetadata(DoubleConstantNoDataCellType, coverage._2.layout, coverage._2.extent, coverage._2.crs, coverage._2.bounds))
    var coverageVis2: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverageVis1
    //定义结果变量
    var resultCoverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverageVis1

    val bands: mutable.ListBuffer[String] = coverage._1.first()._1.measurementName
    //如果用户选了波段
    if (visParam.getBands.nonEmpty) {
      if (visParam.getBands.forall(bands.contains))
        coverageVis2 = selectBands(coverageVis1, visParam.getBands)
      else {
        throw new IllegalArgumentException(s"输入的波段不存在！")
      }
    }
    //获取用户选择的波段数
    val bandNumber: Int = bandNum(coverageVis2)
    //总体分两种情况，选择的波段数/原coverage波段数<=3
    if (bandNumber <= 3) {
      if (bandNumber == 1) {
        resultCoverage = addStyles1Band(coverageVis2, visParam)
      }
      else if (bandNumber == 2) {
        resultCoverage = addStyles2Band(coverageVis2, visParam)
      }
      else {
        resultCoverage = addStyles3Band(coverageVis2, visParam)
      }
    }
    else {
      //如果用户选的波段数>3,用用户选择的前三个波段进行可视化
      if (visParam.getBands.length > 3) {
        val bandsFirst3: List[String] = visParam.getFirstThreeBands
        val coverageVis3: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverageVis2, bandsFirst3)
        resultCoverage = addStyles3Band(coverageVis3, visParam)
      }
      else {
        //用户没选波段,且原coverage波段数>3
        val bandsDefault: List[String] = coverageVis1._1.first()._1.measurementName.slice(0,3).toList
        val coverageVis3: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = selectBands(coverageVis1, bandsDefault)
        resultCoverage = addStyles3Band(coverageVis3, visParam)
      }
    }
    Coverage.toUint8(resultCoverage)
  }

  def addStyles1Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageOneBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage

    // min的数量
    val minNum: Int = visParam.getMin.length
    // max的数量
    val maxNum: Int = visParam.getMax.length
    // gain的数量
    val gainNum: Int = visParam.getGain.length
    // bias的数量
    val biasNum: Int = visParam.getBias.length
    // gamma的数量
    val gammaNum: Int = visParam.getGamma.length

    if (minNum > 1 || maxNum > 1 || gainNum > 1 || biasNum > 1 || gammaNum > 1) {
      throw new IllegalArgumentException("波段数量与参数数量不相符")
    }
    else {
      // 判断是否同时存在最小最大值拉伸和线性拉伸
      if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
        throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
      }
      else {
        // 如果存在线性拉伸
        if (gainNum * biasNum != 0) {
          val gainVis: Double = visParam.getGain.headOption.getOrElse(1.0)
          val biasVis: Double = visParam.getGain.headOption.getOrElse(0.0)
          coverageOneBand = (coverageOneBand._1.map(t => (t._1, t._2.mapBands((_, tile) => {
            Add(Multiply(tile, gainVis), biasVis)
          }))), coverageOneBand._2)
        }
        // 如果存在最小最大值拉伸
        else if (minNum * maxNum != 0) {
          // 获取用户输入的最大最小值参数
          val minVis: Double = visParam.getMin.headOption.getOrElse(0.0)
          val maxVis: Double = visParam.getMax.headOption.getOrElse(1.0)
          //计算最大最小拉伸参数
          val temp = getGainBias(coverageOneBand, 0, minVis, maxVis)
          val gainVis0: Double = temp._1
          val biasVis0: Double = temp._2
          coverageOneBand = (coverageOneBand._1.map(t => (t._1, t._2.mapBands((_, tile) => {
            Add(Multiply(tile, gainVis0), biasVis0)
          }))), coverageOneBand._2)
        }
        // 如果存在gamma值
        if (gammaNum != 0) {
          val a: List[Double] = visParam.getGamma
          coverageOneBand = (coverageOneBand._1.map(t => (t._1, t._2.mapBands((_, tile) => {
            Pow(tile, a.headOption.getOrElse(1.0))
          }))), coverageOneBand._2)
        }
        // 设置默认的调色盘
        //        var paletteVis: List[String] = List[String]("#808080", "#949494", "#a9a9a9", "#bdbebd", "#d3d3d3", "#e9e9e9")
        // 计算最大最小值
        val minMaxBand: (Double, Double) = coverageOneBand._1.map(t => {
          val noNaNArray: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
          if (noNaNArray.nonEmpty) {
            (noNaNArray.min, noNaNArray.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
        if (visParam.getPalette.nonEmpty) {
          val paletteVis = visParam.getPalette
          val colorVis: List[Color] = paletteVis.map(t => {
            try {
              val color: Color = Color.valueOf(t)
              color
            } catch {
              case e: Exception =>
                throw new IllegalArgumentException(s"输入颜色有误，无法识别$t")
            }
          })
          val colorRGB: List[(Double, Double, Double)] = colorVis.map(t => (t.getRed, t.getGreen, t.getBlue))


          val interval: Double = (minMaxBand._2 - minMaxBand._1) / colorRGB.length
          coverageOneBand = (coverageOneBand._1.map(t => {
            val bandR: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * colorRGB(math.min(((d - minMaxBand._1) / interval).toInt, colorRGB.length - 1))._1
            })
            val bandG: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * colorRGB(math.min(((d - minMaxBand._1) / interval).toInt, colorRGB.length - 1))._2
            })
            val bandB: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * colorRGB(math.min(((d - minMaxBand._1) / interval).toInt, colorRGB.length - 1))._3
            })
            (t._1, MultibandTile(bandR, bandG, bandB))
          }), coverageOneBand._2)
        }
        else {
          //没有调色盘的情况,保证值不为0
          val interval: Double = (minMaxBand._2 - minMaxBand._1)
          coverageOneBand = (coverageOneBand._1.map(t => {
            val bandR: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._1) / interval
            })
            val bandG: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._1) / interval
            })
            val bandB: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._1) / interval
            })
            (t._1, MultibandTile(bandR, bandG, bandB))
          }), coverageOneBand._2)
        }
      }
    }
    //    makeTIFF(coverageOneBand,"coverage")
    coverageOneBand

  }

  def addStyles2Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    var coverageTwoBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    // min的数量
    val minNum: Int = visParam.getMin.length
    // max的数量
    val maxNum: Int = visParam.getMax.length
    // gain的数量
    val gainNum: Int = visParam.getGain.length
    // bias的数量
    val biasNum: Int = visParam.getBias.length
    // gamma的数量
    val gammaNum: Int = visParam.getGamma.length
    // 排除palette
    if (visParam.getPalette.nonEmpty) {
      throw new IllegalArgumentException("palette不能应用于2个波段的影像！")
    }
    else {
      // 判断数量是否对应
      if (minNum > 2 || maxNum > 2 || gainNum > 2 || biasNum > 2 || gammaNum > 2) {
        throw new IllegalArgumentException("波段数量与参数数量不相符")
      }
      else if(minNum == 0 && maxNum == 0 && gainNum == 0 && biasNum == 0){
        //默认渲染模式
        // 计算最大最小值，分别为波段1、2的最小值和波段1、2的最大值
        val minMaxBand: (Double, Double, Double, Double) = coverageTwoBand._1.map(t => {
          val noNaNArray1: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
          val noNaNArray2: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
          if (noNaNArray1.nonEmpty && noNaNArray1.nonEmpty) {
            (noNaNArray1.min, noNaNArray2.min, noNaNArray1.max,noNaNArray2.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MaxValue.toDouble, Int.MinValue.toDouble, Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3,b._3), math.max(a._4,b._4)))

        // 拉伸图像
        val interval1: Double = (minMaxBand._3 - minMaxBand._1)
        val interval2: Double = (minMaxBand._4 - minMaxBand._2)

        coverageTwoBand = (coverageTwoBand._1.map(
          t => {
            val bandR: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._1) / interval1
            })
            val bandG: Tile = t._2.bands(1).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._2) / interval2
            })
            val bandB: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                63
            })
            (t._1, MultibandTile(bandR, bandG, bandB))
          }),coverageTwoBand._2)
      }
      else {
        // 判断是否同时存在最小最大值拉伸和线性拉伸
        if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
          throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
        }
        else {
          //如果存在线性拉伸
          if (gainNum * biasNum != 0) {
            //设置线性拉伸参数初始值
            var gainVis0: Double = 1.0
            var gainVis1: Double = 1.0
            var biasVis0: Double = 0.0
            var biasVis1: Double = 0.0
            if (gainNum == 1) {
              gainVis0 = visParam.getGain.head
              gainVis1 = visParam.getGain.head
            }
            else if (gainNum == 2) {
              gainVis0 = visParam.getGain.head
              gainVis1 = visParam.getGain(1)
            }
            if (biasNum == 1) {
              biasVis0 = visParam.getBias.head
              biasVis1 = visParam.getBias.head
            }
            else if (biasNum == 2) {
              biasVis0 = visParam.getBias.head
              biasVis1 = visParam.getBias(1)
            }
            coverageTwoBand = (coverageTwoBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
              if (i == 0) {
                Add(Multiply(tile, gainVis0), biasVis0)
              }
              else {
                Add(Multiply(tile, gainVis1), biasVis1)
              }
            }))), coverageTwoBand._2)
          }
          // 如果存在最小最大值拉伸
          else if (minNum * maxNum != 0) {
            var minVis0: Double = 0.0
            var minVis1: Double = 0.0
            var maxVis0: Double = 0.0
            var maxVis1: Double = 0.0
            // 判断min的数量,获取用户输入的最大最小值参数
            if (minNum == 1) {
              minVis0 = visParam.getMin.head
              minVis1 = visParam.getMin.head
            }
            else if (minNum == 2) {
              minVis0 = visParam.getMin.head
              minVis1 = visParam.getMin(1)
            }
            if (maxNum == 1) {
              maxVis0 = visParam.getMax.head
              maxVis1 = visParam.getMax.head
            }
            else if (maxNum == 2) {
              maxVis0 = visParam.getMax.head
              maxVis1 = visParam.getMax(1)
            }
            //计算最大最小拉伸参数
            val gainVis0: Double = getGainBias(coverageTwoBand, 0, minVis0, maxVis0)._1
            val biasVis0: Double = getGainBias(coverageTwoBand, 0, minVis0, maxVis0)._2
            val gainVis1: Double = getGainBias(coverageTwoBand, 1, minVis1, maxVis1)._1
            val biasVis1: Double = getGainBias(coverageTwoBand, 1, minVis1, maxVis1)._2
            coverageTwoBand = (coverageTwoBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
              if (i == 0) {
                Add(Multiply(tile, gainVis0), biasVis0)
              }
              else {
                Add(Multiply(tile, gainVis1), biasVis1)
              }
            }))), coverageTwoBand._2)
          }
          // 如果存在gamma值
          if (gammaNum != 0) {
            var gammaVis0: Double = 1.0
            var gammaVis1: Double = 1.0
            if (gammaNum == 1) {
              gammaVis0 = visParam.getGamma.head
              gammaVis1 = visParam.getGamma.head
            }
            else if (gammaNum == 2) {
              gammaVis0 = visParam.getGamma.head
              gammaVis1 = visParam.getGamma(1)
            }
            coverageTwoBand = (coverageTwoBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
              if (i == 0) {
                Pow(tile, gammaVis0)
              }
              else {
                Pow(tile, gammaVis1)
              }
            }))), coverageTwoBand._2)
          }
        }
      }
    }
    coverageTwoBand
  }

  def addStyles3Band(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var coverageThreeBand: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    // min的数量
    val minNum: Int = visParam.getMin.length
    // max的数量
    val maxNum: Int = visParam.getMax.length
    // gain的数量
    val gainNum: Int = visParam.getGain.length
    // bias的数量
    val biasNum: Int = visParam.getBias.length
    // gamma的数量
    val gammaNum: Int = visParam.getGamma.length
    // 判断数量是否对应
    if (minNum > 3 || minNum == 2 || maxNum > 3 || maxNum == 2 || gainNum > 3 || gainNum == 2 || biasNum > 3 || biasNum == 2 || gammaNum > 3 || gammaNum == 2) {
      throw new IllegalArgumentException("波段数量与参数数量不相符")
    }
    else {
      // 判断是否同时存在最小最大值拉伸和线性拉伸
      if (minNum * maxNum != 0 && gainNum * biasNum != 0) {
        throw new IllegalArgumentException("不能同时设置min/max和gain/bias")
      }
      else if (minNum == 0 && maxNum == 0 && gainNum == 0 && biasNum == 0) {
        //默认渲染模式
        // 计算最大最小值，分别为波段1、2、3的最小值和波段1、2、3的最大值
        val minMaxBand: (Double, Double, Double, Double, Double, Double) = coverageThreeBand._1.map(t => {
          val noNaNArray1: Array[Double] = t._2.bands(0).toArrayDouble().filter(!_.isNaN)
          val noNaNArray2: Array[Double] = t._2.bands(1).toArrayDouble().filter(!_.isNaN)
          val noNaNArray3: Array[Double] = t._2.bands(2).toArrayDouble().filter(!_.isNaN)
          if (noNaNArray1.nonEmpty && noNaNArray1.nonEmpty) {
            (noNaNArray1.min, noNaNArray2.min, noNaNArray3.min,noNaNArray1.max, noNaNArray2.max, noNaNArray3.max)
          }
          else {
            (Int.MaxValue.toDouble, Int.MaxValue.toDouble,Int.MaxValue.toDouble, Int.MinValue.toDouble, Int.MinValue.toDouble,Int.MinValue.toDouble)
          }
        }).reduce((a, b) => (math.min(a._1, b._1), math.min(a._2, b._2), math.min(a._3, b._3), math.max(a._4, b._4), math.max(a._5, b._5), math.max(a._6, b._6)))

        // 拉伸图像
        val interval1: Double = (minMaxBand._4 - minMaxBand._1)
        val interval2: Double = (minMaxBand._5 - minMaxBand._2)
        val interval3: Double = (minMaxBand._6 - minMaxBand._3)
        coverageThreeBand = (coverageThreeBand._1.map(
          t => {
            val bandR: Tile = t._2.bands(0).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._1) / interval1
            })
            val bandG: Tile = t._2.bands(1).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._2) / interval2
            })
            val bandB: Tile = t._2.bands(2).mapDouble(d => {
              if (d.isNaN)
                d
              else
                1 + 254.0 * (d - minMaxBand._3) / interval3
            })
            (t._1, MultibandTile(bandR, bandG, bandB))
          }), coverageThreeBand._2)
      }
      else {
        // 如果存在线性拉伸,默认为线性拉伸
        if (gainNum * biasNum != 0) {
          var gainVis0: Double = 1.0
          var gainVis1: Double = 1.0
          var gainVis2: Double = 1.0
          var biasVis0: Double = 0.0
          var biasVis1: Double = 0.0
          var biasVis2: Double = 0.0

          if (gainNum == 1) {
            gainVis0 = visParam.getGain.head
            gainVis1 = visParam.getGain.head
            gainVis2 = visParam.getGain.head
          }
          else if (gainNum == 3) {
            gainVis0 = visParam.getGain.head
            gainVis1 = visParam.getGain(1)
            gainVis2 = visParam.getGain(2)
          }
          if (biasNum == 1) {
            biasVis0 = visParam.getBias.head
            biasVis1 = visParam.getBias.head
            biasVis2 = visParam.getBias.head
          }
          else if (biasNum == 3) {
            biasVis0 = visParam.getBias.head
            biasVis1 = visParam.getBias(1)
            biasVis2 = visParam.getBias(2)
          }
          coverageThreeBand = (coverageThreeBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
            if (i == 0) {
              Add(Multiply(tile, gainVis0), biasVis0)
            }
            else if (i == 1) {
              Add(Multiply(tile, gainVis1), biasVis1)
            }
            else {
              Add(Multiply(tile, gainVis2), biasVis2)
            }
          }))), coverageThreeBand._2)
        }
        //如果存在最小最大值拉伸
        else if (minNum * maxNum != 0) {
          var minVis0: Double = 0.0
          var minVis1: Double = 0.0
          var minVis2: Double = 0.0
          var maxVis0: Double = 0.0
          var maxVis1: Double = 0.0
          var maxVis2: Double = 0.0
          // 判断min的数量
          if (minNum == 1) {
            minVis0 = visParam.getMin.head
            minVis1 = visParam.getMin.head
            minVis2 = visParam.getMin.head
          }
          else if (minNum == 3) {
            minVis0 = visParam.getMin.head
            minVis1 = visParam.getMin(1)
            minVis2 = visParam.getMin(2)
          }
          if (maxNum == 1) {
            maxVis0 = visParam.getMax.head
            maxVis1 = visParam.getMax.head
            maxVis2 = visParam.getMax.head
          }
          else if (maxNum == 3) {
            maxVis0 = visParam.getMax.head
            maxVis1 = visParam.getMax(1)
            maxVis2 = visParam.getMax(2)
          }
          //分别对三个波段进行最小最大值拉伸
          val gainVis0: Double = getGainBias(coverageThreeBand, 0, minVis0, maxVis0)._1
          val biasVis0: Double = getGainBias(coverageThreeBand, 0, minVis0, maxVis0)._2
          val gainVis1: Double = getGainBias(coverageThreeBand, 1, minVis1, maxVis1)._1
          val biasVis1: Double = getGainBias(coverageThreeBand, 1, minVis1, maxVis1)._2
          val gainVis2: Double = getGainBias(coverageThreeBand, 2, minVis2, maxVis2)._1
          val biasVis2: Double = getGainBias(coverageThreeBand, 2, minVis2, maxVis2)._2
          coverageThreeBand = (coverageThreeBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
            if (i == 0) {
              Add(Multiply(tile, gainVis0), biasVis0)
            }
            else if (i == 1) {
              Add(Multiply(tile, gainVis1), biasVis1)
            }
            else {
              Add(Multiply(tile, gainVis2), biasVis2)
            }
          }))), coverageThreeBand._2)
        }
        // 如果存在gamma值
        if (gammaNum != 0) {
          var gammaVis0: Double = 1.0
          var gammaVis1: Double = 1.0
          var gammaVis2: Double = 1.0
          if (gammaNum == 1) {
            gammaVis0 = visParam.getGamma.head
            gammaVis1 = visParam.getGamma.head
            gammaVis2 = visParam.getGamma.head
          }
          else if (gammaNum == 3) {
            gammaVis0 = visParam.getGamma.head
            gammaVis1 = visParam.getGamma(1)
            gammaVis2 = visParam.getGamma(2)
          }
          coverageThreeBand = (coverageThreeBand._1.map(t => (t._1, t._2.mapBands((i, tile) => {
            if (i == 0) {
              Pow(tile, gammaVis0)
            }
            else if (i == 1) {
              Pow(tile, gammaVis1)
            }
            else {
              Pow(tile, gammaVis2)
            }
          }))), coverageThreeBand._2)
        }
      }
    }
    coverageThreeBand
  }

  def getGainBias(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), bandOrder: Int, minParam: Double, maxParam: Double)
  : (Double, Double) = {
    val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = coverage
    // 首先找到现有的最小最大值
    val minMaxBand: (Double, Double) = coverageVis._1.map(t => {
      val noNaNArray: Array[Double] = t._2.bands(bandOrder).toArrayDouble().filter(!_.isNaN)
      if (noNaNArray.nonEmpty) {
        (noNaNArray.min, noNaNArray.max)
      }
      else {
        (Int.MaxValue.toDouble, Int.MinValue.toDouble)
      }
    }).reduce((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))

    val gainBand: Double = (maxParam - minParam) / (minMaxBand._2 - minMaxBand._1)
    val biasBand: Double = (minMaxBand._2 * minParam - minMaxBand._1 * maxParam) / (minMaxBand._2 - minMaxBand._1)
    (gainBand, biasBand)
  }


  def makeTIFF(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), name: String, path: String = ""): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    if (path.nonEmpty) {
      val writePath: String = s"$path$name.tif"
      GeoTiff(stitchedTile, coverage._2.crs).write(writePath)
    }
    else {
      val writePath: String = s"$tempFilePath$name.tif"
      GeoTiff(stitchedTile, coverage._2.crs).write(writePath)
    }

  }

  def visualizeOnTheFly(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), visParam: VisualizationParam): Unit = {
    // 教育版额外的判断,保存结果,调用回调接口
    if(Trigger.dagType.equals("edu")){
      makeTIFF(coverage,dagId)
      val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)

      val saveFilePath = s"$tempFilePath$dagId.tif"

      val path = s"$userId/result/${Trigger.outputFile}"
      //上传
      clientUtil.Upload(path, saveFilePath)
    }


    val coverageVis: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = addStyles(if (Trigger.coverageReadFromUploadFile) {
      reproject(coverage, CRS.fromEpsgCode(3857), resolutionTMSArray(Trigger.level))
    } else {
      coverage
    }, visParam)

    val tmsCrs: CRS = CRS.fromEpsgCode(3857)
    val layoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(tmsCrs, tileSize = 256)
    val newBounds: Bounds[SpatialKey] = Bounds(coverageVis._2.bounds.get.minKey.spatialKey, coverageVis._2.bounds.get.maxKey.spatialKey)
    val rasterMetaData: TileLayerMetadata[SpatialKey] = TileLayerMetadata(coverageVis._2.cellType, coverageVis._2.layout, coverageVis._2.extent, coverageVis._2.crs, newBounds)
    val coverageNoTimeBand: RDD[(SpatialKey, MultibandTile)] = coverageVis._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    })

    var coverageTMS: MultibandTileLayerRDD[SpatialKey] = MultibandTileLayerRDD(coverageNoTimeBand, rasterMetaData)

    //     TODO lrx:后面要不要考虑直接从MinIO读出来的数据进行上下采样？
    //     大于0是上采样
    if (COGUtil.tileDifference > 0) {
      // 首先对其进行上采样
      // 上采样必须考虑范围缩小，不然非常占用内存
      val levelUp: Int = COGUtil.tileDifference
      val layoutOrigin: LayoutDefinition = coverageTMS.metadata.layout
      val extentOrigin: Extent = coverageTMS.metadata.layout.extent
      val extentIntersect: Extent = extentOrigin.intersection(COGUtil.extent).orNull
      val layoutCols: Int = math.max(math.ceil((extentIntersect.xmax - extentIntersect.xmin) / 256.0 / layoutOrigin.cellSize.width * (1 << levelUp)).toInt, 1)
      val layoutRows: Int = math.max(math.ceil((extentIntersect.ymax - extentIntersect.ymin) / 256.0 / layoutOrigin.cellSize.height * (1 << levelUp)).toInt, 1)
      val extentNew: Extent = Extent(extentIntersect.xmin, extentIntersect.ymin, extentIntersect.xmin + layoutCols * 256.0 * layoutOrigin.cellSize.width / (1 << levelUp), extentIntersect.ymin + layoutRows * 256.0 * layoutOrigin.cellSize.height / (1 << levelUp))

      val tileLayout: TileLayout = TileLayout(layoutCols, layoutRows, 256, 256)
      val layoutNew: LayoutDefinition = LayoutDefinition(extentNew, tileLayout)
      coverageTMS = coverageTMS.reproject(coverageTMS.metadata.crs, layoutNew)._2
    }

    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      coverageTMS.reproject(tmsCrs, layoutScheme)
    val on_the_fly_path = GlobalConfig.Others.ontheFlyStorage + Trigger.dagId
    val file = new File(on_the_fly_path)
    if (file.exists() && file.isDirectory) {
      println("Delete existed on_the_fly_path")
      val command = s"rm -rf $on_the_fly_path"
      println(command)
      //调用系统命令
      command.!!
    }


    val outputPath: String = GlobalConfig.Others.ontheFlyStorage
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore: FileAttributeStore = FileAttributeStore(outputPath)
    // Create the writer that we will use to store the tiles in the local catalog.
    val writer: FileLayerWriter = FileLayerWriter(attributeStore)


    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      if (Trigger.level - z <= 2 && Trigger.level - z >= 0) {
        val layerId: LayerId = LayerId(Trigger.dagId, z)
        println(layerId)
        // If the layer exists already, delete it out before writing
        if (attributeStore.layerExists(layerId)) {
          //        new FileLayerManager(attributeStore).delete(layerId)
          try {
            writer.overwrite(layerId, rdd)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
        else {
          try{
            writer.write(layerId, rdd, ZCurveKeyIndexMethod)
          } catch {
            case e:Exception =>
              println(e)
              println("Continue writing Layers!")
          }

        }
      }
    }

    // 回调服务
    val jsonObject: JSONObject = new JSONObject
    val rasterJsonObject: JSONObject = new JSONObject
    if (visParam.getFormat == "png") {
      rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + Trigger.dagId + "/{z}/{x}/{y}.png")
    }
    else {
      rasterJsonObject.put(Trigger.layerName, GlobalConfig.Others.tmsPath + Trigger.dagId + "/{z}/{x}/{y}.jpg")
    }

    PostSender.shelvePost("raster",rasterJsonObject)

  }

  def visualizeBatch(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), batchParam: BatchParam, dagId: String): Unit = {
    val coverageArray: Array[(SpatialKey, MultibandTile)] = coverage._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()

    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(coverageArray)
    val stitchedTile: Raster[MultibandTile] = Raster(tile, coverage._2.extent)
    // 先转到EPSG:3857，将单位转为米缩放后再转回指定坐标系
    var reprojectTile: Raster[MultibandTile] = stitchedTile.reproject(coverage._2.crs, CRS.fromName("EPSG:3857"))
    val resample: Raster[MultibandTile] = reprojectTile.resample(math.max((reprojectTile.cellSize.width * reprojectTile.cols / batchParam.getScale).toInt, 1), math.max((reprojectTile.cellSize.height * reprojectTile.rows / batchParam.getScale).toInt, 1))
    reprojectTile = resample.reproject(CRS.fromName("EPSG:3857"), batchParam.getCrs)


    // 上传文件
    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.tiff"
    GeoTiff(reprojectTile, batchParam.getCrs).write(saveFilePath)

    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val path = batchParam.getUserId + "/result/" + batchParam.getFileName + "." + batchParam.getFormat
    val obj: JSONObject = new JSONObject
    obj.put("path",path.toString)
    PostSender.shelvePost("info",obj)
    //    client.putObject(PutObjectArgs.builder().bucket("oge-user").`object`(batchParam.getFileName + "." + batchParam.getFormat).stream(inputStream,inputStream.available(),-1).build)
    clientUtil.Upload(path, saveFilePath)
    //    client.putObject(PutObjectArgs)

  }

  def visualizeBatch_edu(implicit sc: SparkContext, coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), batchParam: BatchParam, dagId: String): Unit = {
    // 上传文件
    val saveFilePath = s"${GlobalConfig.Others.tempFilePath}${dagId}.tiff"
    saveRasterRDDToTif(coverage,saveFilePath)
    val file: File = new File(saveFilePath)
    val path = Trigger.userId + "/result/" + Trigger.ProcessName
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    clientUtil.Upload(path, saveFilePath)

    val rasterJsonObject: JSONObject = new JSONObject
    rasterJsonObject.put("coverage",path)
    PostSender.shelvePost("raster",rasterJsonObject)
  }


  //用来标识读取上传文件的编号的自增标识符
  var file_id:Long = 0
  def loadCoverageFromUpload(implicit sc: SparkContext, coverageId: String, userID: String, dagId: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    var path: String = new String()
    if (coverageId.endsWith(".tiff") || coverageId.endsWith(".tif")) {
      path = s"${userID}/$coverageId"
    } else {
      path = s"$userID/$coverageId.tiff"
    }
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}_${Trigger.file_id}.tiff"
    val inputStream = clientUtil.getObject(USER_BUCKET_NAME ,path)
    val outputPath = Paths.get(filePath)

    tempFileList.append(filePath)
    Trigger.file_id += 1
    java.nio.file.Files.copy(inputStream, outputPath, REPLACE_EXISTING)
    inputStream.close()
    val coverage = RDDTransformerUtil.makeChangedRasterRDDFromTif(sc, filePath)

    coverage
  }

  def loadCoverageFromCaseData(implicit sc: SparkContext, coverageId: String,  dagId: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val path = "/" + coverageId
    val tempPath = GlobalConfig.Others.tempFilePath
    val filePath = s"$tempPath${dagId}_${Trigger.file_id}.tiff"

    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)
    val inputStream = clientUtil.getObject(BOS_BUCKET_NAME, path)
    val outputPath = Paths.get(filePath)
    tempFileList.append(filePath)
    Trigger.file_id += 1
    java.nio.file.Files.copy(inputStream, outputPath, REPLACE_EXISTING)

    val coverage = whu.edu.cn.util.RDDTransformerUtil.makeChangedRasterRDDFromTif(sc, filePath)

    coverage
  }

  var file_idx:Long = 0
  def loadTxtFromUpload(txt: String, userID: String, dagId: String, loadtype: String) = {
    var path: String = s"${userID}/$txt"
    val clientUtil = ClientUtil.createClientUtil(CLIENT_NAME)

    val tempPath = loadtype match {
      case "saga" => GlobalConfig.Others.sagatempFilePath
      case "otb" => GlobalConfig.Others.otbtempFilePath
      case _ => GlobalConfig.Others.tempFilePath
    }
    //    val tempPath = GlobalConfig.Others.sagatempFilePath

    val filePath = s"$tempPath${dagId}_$file_idx.txt"
    val dockerFilePath = loadtype match {
      case "saga" => s"/tmp/saga/${dagId}_$file_idx.txt"
      case "otb" => s"/tmp/otb/${dagId}_$file_idx.txt"
      case _ => filePath
    }

    val tempfile = new File(filePath)
    try {
      clientUtil.Download(path, filePath)
    }
    catch {
      case e: Throwable =>
        println(e)
    }
    Trigger.tempFileList.append(filePath) //加入待删除的临时文件路径下
    file_idx = file_idx + 1
    println(filePath, dockerFilePath)
    dockerFilePath
  }

}

object Relection extends App {


  def reflectCall(functionName: String, params: Any*): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {

    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    // 获取并调用方法
    val moduleSymbol = ru.typeOf[Coverage.type].termSymbol.asModule
    val moduleMirror = mirror.reflectModule(moduleSymbol)

    val methodSymbol = ru.typeOf[Coverage.type].decl(ru.TermName(functionName)).asMethod
    val instanceMirror = mirror.reflect(moduleMirror.instance)
    val methodMirror = instanceMirror.reflectMethod(methodSymbol)
    methodMirror(params: _*).asInstanceOf[(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])]
  }
}
