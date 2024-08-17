package whu.edu.cn.algorithms.MLlib

import java.io._
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.zip.{ZipEntry, ZipFile, ZipInputStream, ZipOutputStream}

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{CellType, DoubleArrayTile, MultibandTile, Tile}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import whu.edu.cn.algorithms.ImageProcess.core.MathTools.findSpatialKeyMinMax
import whu.edu.cn.algorithms.ImageProcess.core.TypeAliases.RDDImage
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import whu.edu.cn.algorithms.ImageProcess.core.MathTools.findSpatialKeyMinMax


object util {
  // DataFrame -> RDD
  //TODO 这个函数及其有关重载，后面得基于标注样本的逻辑重新改，标签和特征放在同一个coverage中不太合理
  def makeRasterDataFrameFromRDD(implicit ss: SparkSession, coverage: RDDImage, features: List[Int], label: Int): DataFrame = {
    //TODO label暂时先改为必须有值，即这个函数只适用于分类（带标签的情况）
    //features从0开始计数，对应波段索引
    val numRows: Int = findSpatialKeyMinMax(coverage)._1 //瓦片行数
    val numCols: Int = findSpatialKeyMinMax(coverage)._2 //瓦片列数
    //    println(coverage._1.first()._1._measurementName)
    val bandCount: Int = coverage._1.first()._2.bandCount
    val cellType: CellType = coverage._1.first()._2.cellType
    val time: Long = coverage._1.first()._1.spaceTimeKey.instant
    // 将瓦片RDD转成像素RDD
    val IntVectorRdd: RDD[(Int, Int, Int, Int, List[Double], Double)] = coverage._1.flatMap(t => {
      val list: ListBuffer[(Int, Int, Int, Int, List[Double], Double)] = ListBuffer.empty[(Int, Int, Int, Int, List[Double], Double)]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for (i <- 0 until 256; j <- 0 until 256) { // (i, j)是像素在该瓦片中的定位
        val featureBands: ListBuffer[Double] = ListBuffer.empty[Double] //TODO 类型先写作Double，后面改成与cellType同
        for (featureIndex <- features) {
          if (featureIndex >= bandCount) throw new IllegalArgumentException("Error: 输入features序号超过波段数量bandCount")
          else featureBands.append(t._2.band(featureIndex).getDouble(j, i))
        }
        val labelBand: Double =
          if (label >= bandCount) throw new IllegalArgumentException("Error: 输入label序号超过波段数量bandCount")
          else t._2.band(label).getDouble(j, i)
        list.append((row, col, i, j, featureBands.toList, labelBand)) //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //转换成Row格式的RDD
    val rowRdd: RDD[Row] = IntVectorRdd.map(t => {
      val list = List(t._1.toDouble, t._2.toDouble, t._3.toDouble, t._4.toDouble) ::: t._5 ::: List(t._6)
      Row(list: _*)
    })
    //各个列的名称设置："label1", "label2", "label3", "label4"，"col1"，"col2"，，，
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to features.length) {
      colNames.append(s"feature$i")
    }
    val labelColNames = List("loc1", "loc2", "loc3", "loc4") ::: colNames.toList ::: List("label") //TODO 暂时label列一定在最后一个波段
    //设置字段类型
    val fieldTypes = List.fill(bandCount + 4)(DoubleType) //TODO 把上面Int类型的label转成Double，这里设为Double，是因为我只会把所有字段设成一种类型
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val df = ss.createDataFrame(rowRdd, schema).toDF(labelColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assemblerDF =assembler.transform(df)
    assemblerDF
  }

  def makeRasterDataFrameFromRDD(implicit ss: SparkSession, coverage: RDDImage, features: List[Int]): DataFrame = {
    //TODO label暂时先改为必须有值，即这个函数只适用于分类（带标签的情况）
    //features从0开始计数，对应波段索引
    val numRows: Int = findSpatialKeyMinMax(coverage)._1 //瓦片行数
    val numCols: Int = findSpatialKeyMinMax(coverage)._2 //瓦片列数
    //    println(coverage._1.first()._1._measurementName)
    val bandCount: Int = coverage._1.first()._2.bandCount
    val cellType: CellType = coverage._1.first()._2.cellType
    val time: Long = coverage._1.first()._1.spaceTimeKey.instant
    // 将瓦片RDD转成像素RDD
    val IntVectorRdd: RDD[(Int, Int, Int, Int, List[Double])] = coverage._1.flatMap(t => {
      val list: ListBuffer[(Int, Int, Int, Int, List[Double])] = ListBuffer.empty[(Int, Int, Int, Int, List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for (i <- 0 until 256; j <- 0 until 256) { // (i, j)是像素在该瓦片中的定位
        val featureBands: ListBuffer[Double] = ListBuffer.empty[Double] //TODO 类型先写作Double，后面改成与cellType同
        for (featureIndex <- features) {
          if (featureIndex >= bandCount) throw new IllegalArgumentException("Error: 输入features序号超过波段数量bandCount")
          else featureBands.append(t._2.band(featureIndex).getDouble(j, i))
        }
        list.append((row, col, i, j, featureBands.toList)) //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //转换成Row格式的RDD
    val rowRdd: RDD[Row] = IntVectorRdd.map(t => {
      val list = List(t._1.toDouble, t._2.toDouble, t._3.toDouble, t._4.toDouble) ::: t._5
      Row(list: _*)
    })
    //各个列的名称设置："label1", "label2", "label3", "label4"，"col1"，"col2"，，，
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to features.length) {
      colNames.append(s"feature$i")
    }
    val labelColNames = List("loc1", "loc2", "loc3", "loc4") ::: colNames.toList
    //设置字段类型
    val fieldTypes = List.fill(features.length + 4)(DoubleType) //TODO 把上面Int类型的label转成Double，这里设为Double，是因为我只会把所有字段设成一种类型
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val df = ss.createDataFrame(rowRdd, schema).toDF(labelColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assemblerDF =assembler.transform(df)
    assemblerDF
  }

  //未指定feature和label所在索引，所有波段均作为特征转成列"features"
  def makeRasterDataFrameFromRDD(implicit ss: SparkSession, coverage: RDDImage): DataFrame = {
    //所有波段都作为特征
    val numRows: Int = findSpatialKeyMinMax(coverage)._1 //瓦片行数
    val numCols: Int = findSpatialKeyMinMax(coverage)._2 //瓦片列数
    //    println(coverage._1.first()._1._measurementName)
    val bandCount: Int = coverage._1.first()._2.bandCount
    val cellType: CellType = coverage._1.first()._2.cellType
    val time: Long = coverage._1.first()._1.spaceTimeKey.instant
    // 将瓦片RDD转成像素RDD
    val IntVectorRdd: RDD[(Int, Int, Int, Int, List[Double])] = coverage._1.flatMap(t => {
      val list: ListBuffer[(Int, Int, Int, Int, List[Double])] = ListBuffer.empty[(Int, Int, Int, Int, List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for (i <- 0 until 256; j <- 0 until 256) { // (i, j)是像素在该瓦片中的定位
        val featureBands: ListBuffer[Double] = ListBuffer.empty[Double] //TODO 类型先写作Double，后面改成与cellType同
        for (bandIndex <- 0 until bandCount) {
          featureBands.append(t._2.band(bandIndex).getDouble(j, i))
        }
        list.append((row, col, i, j, featureBands.toList)) //使用4个label标识像素在哪个瓦片的哪个位置
      }
      list.toList
    })
    //转换成Row格式的RDD
    val rowRdd: RDD[Row] = IntVectorRdd.map(t => {
      val list = List(t._1.toDouble, t._2.toDouble, t._3.toDouble, t._4.toDouble) ::: t._5
      Row(list: _*)
    })
    //各个列的名称设置："label1", "label2", "label3", "label4"，"col1"，"col2"，，，
    val colNames: ListBuffer[String] = ListBuffer.empty[String]
    for (i <- 1 to bandCount) {
      colNames.append(s"feature$i")
    }
    val labelColNames = List("loc1", "loc2", "loc3", "loc4") ::: colNames.toList //TODO 暂时label列一定在最后一个波段
    //设置字段类型
    val fieldTypes = List.fill(bandCount + 4)(DoubleType) //TODO 把上面Int类型的label转成Double，这里设为Double，是因为我只会把所有字段设成一种类型
    val fields = labelColNames.zip(fieldTypes).map { case (name, dataType) =>
      StructField(name, dataType)
    }
    val schema = StructType(fields)
    //转换RDD到DataFrame
    val df = ss.createDataFrame(rowRdd, schema).toDF(labelColNames: _*)
    val assembler = new VectorAssembler()
      .setInputCols(colNames.toArray)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val assemblerDF =assembler.transform(df)
    assemblerDF
  }

  //  def makeRasterDataFrameFormRDDMap(implicit ss: SparkSession, coverageMap: Map[String,RDDImage]): DataFrame = {
  //    val featuresCoverage: RDDImage = coverageMap("features")
  //    val featuresCount: Int = featuresCoverage._1.first()._2.bandCount
  //    val labelCoverage: RDDImage = coverageMap("label")
  //  }

  // RDD -> DataFrame
  def makeRasterRDDFromDataFrame(coverage: DataFrame, featuresCol: Boolean = false, labelCol: Boolean = false, predictionCol: Boolean = false, predictedLabelCol: Boolean = true, rawPredictionCol: Boolean = false, probabilityCol: Boolean = false, varianceCol: Boolean = false, topicDistributionCol: Boolean = false): RDD[(SpaceTimeBandKey, MultibandTile)] ={
    //featureCol表示是否需要特征列；labelCol表示是否需要原始标签列
    //把DataFrame中用户需要的每个特征列都转换成一个波段
    val coverageAddOneColumn: DataFrame =
    if(coverage.columns.contains("predictedLabel")){
      coverage.withColumn("predictedLabelDouble", col("predictedLabel").cast("double"))
    }
    else coverage
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = sdf.parse(now).getTime //TODO 时间暂时这么写，后面肯定要改的
    val colNamesType: Array[(String, String)] = coverageAddOneColumn.dtypes //(列名, 列的数据类型）
    val colNames: Array[String] = colNamesType.map({case (s1, s2) => s1})
    //    val bandCount: Int = labelColNamesType.length - 4 //?

    //基于相同的Key将这些Rdd组合起来
    //    val groupRdd: RDD[Row] =
    val outputCols: ListBuffer[String] = ListBuffer.empty[String]
    if(featuresCol && colNames.contains("features")) outputCols.append("features")
    if (labelCol && colNames.contains("label")) outputCols.append("label")
    if (predictionCol && colNames.contains("prediction")) outputCols.append("prediction")
    if (predictedLabelCol && colNames.contains("predictedLabelDouble")) outputCols.append("predictedLabelDouble")
    if (rawPredictionCol && colNames.contains("rawPrediction")) outputCols.append("rawPrediction")
    if (probabilityCol && colNames.contains("probability")) outputCols.append("probability")
    if (varianceCol && colNames.contains("variance")) outputCols.append("variance")
    if (topicDistributionCol && colNames.contains("topicDistribution")) outputCols.append("topicDistribution")

    val bandGroupCount: Int = outputCols.length //有几组输出列
    val allColsExceptLoc1: List[String] = List("loc2", "loc3", "loc4")::: outputCols.toList
    val colSelect: RDD[Row] = coverageAddOneColumn.select("loc1", allColsExceptLoc1:_*).rdd
    val groupRdd: RDD[((Double, Double), Iterable[((Double, Double), ListBuffer[Double])])] =
      colSelect.map(t => {
        val colValues: ListBuffer[Double] = ListBuffer.empty[Double]
        for (i <- 0 until bandGroupCount){
          //TODO 目前仅实现如下四类，因为MLlib返回的DataFrame数据类型基本都为如下四类
          if (t.get(i+4).isInstanceOf[Double]) colValues.append(t.getDouble(i+4))
          else if (t.get(i+4).isInstanceOf[Int]) colValues.append(t.getInt(i+4))
          else if (t.get(i+4).isInstanceOf[SparseVector]){
            val sv = t.getAs[SparseVector](i+4).toArray
            for(j <- sv) colValues.append(j)
          }
          else{
            val dv = t.getAs[DenseVector](i+4).toArray
            for(j <- dv) colValues.append(j)
          }
        }
        ((t.getDouble(0), t.getDouble(1)), ((t.getDouble(2), t.getDouble(3)), colValues))
      })  //.getAs[List[Double]](4))
        .groupByKey()
    //对每个瓦片进行处理，生成一张新瓦片，格式变换为RDD[(SpaceTimeBandKey, MultibandTile)]
    val newCoverageRdd: RDD[(SpaceTimeBandKey, MultibandTile)] = groupRdd.map(t => {
      val bandCount = t._2.toList(0)._2.size
      val multibandTileArray = Array.ofDim[Tile](bandCount)
      //定位瓦片
      val row = t._1._1.toInt
      val col = t._1._2.toInt
      val newBand: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String] //TODO 需要改成新的波段列表
      val arr: Iterable[((Double, Double), ListBuffer[Double])] = t._2 // 取消 新增.toArray ; java.util.List ; DenseVector[Double] ; Vector
      //分波段计算新像素值
      for (bandIndex <- 0 until bandCount) {
        newBand.append(bandIndex.toString()) //TODO 波段名称用波段序号命，后面是否要改成用特征命名
        val doubleArrayTile: DoubleArrayTile = DoubleArrayTile.empty(256, 256)
        doubleArrayTile.fill(Double.NaN)
        for (list <- arr) {
          doubleArrayTile.setDouble(list._1._2.toInt, list._1._1.toInt, list._2(bandIndex))
        }
        multibandTileArray(bandIndex) = doubleArrayTile
      }
      (SpaceTimeBandKey(SpaceTimeKey(col, row, time), newBand), MultibandTile(multibandTileArray))
    })
    newCoverageRdd
  }

  def joinTwoCoverage(coverage1: RDDImage, coverage2: RDDImage, col1: List[Int], col2: List[Int]): RDD[Row] = {
    val numRows1: Int = findSpatialKeyMinMax(coverage1)._1 //瓦片行数
    val numCols1: Int = findSpatialKeyMinMax(coverage1)._2 //瓦片列数
    val numRows2: Int = findSpatialKeyMinMax(coverage2)._1 //瓦片行数
    val numCols2: Int = findSpatialKeyMinMax(coverage2)._2 //瓦片列数
    if(numRows1 != numRows2 || numCols1 != numCols2) throw new IllegalArgumentException("传入两影像尺寸不同！")
    val bandCount1: Int = coverage1._1.first()._2.bandCount
    val bandCount2: Int = coverage2._1.first()._2.bandCount
    val rdd1: RDD[((Int, Int, Int, Int), List[Double])] = coverage1._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), List[Double])] = ListBuffer.empty[((Int, Int, Int, Int), List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        val bandsList: ListBuffer[Double] = ListBuffer.empty[Double]
        for(index <- col1){
          if(index>=bandCount1) throw new IllegalArgumentException("col1超出最大波段数！")
          else bandsList.append(t._2.band(index).getDouble(j, i))
        }
        list.append(((row, col, i, j), bandsList.toList))
      }
      list.toList
    })
    val rdd2: RDD[((Int, Int, Int, Int), List[Double])] = coverage2._1.flatMap(t=>{
      val list: ListBuffer[((Int, Int, Int, Int), List[Double])] = ListBuffer.empty[((Int, Int, Int, Int), List[Double])]
      //定位瓦片
      val row = t._1.spaceTimeKey.spatialKey.row
      val col = t._1.spaceTimeKey.spatialKey.col
      //定位像素
      for(i <- 0 until 256; j <- 0 until 256){  // (i, j)是像素在该瓦片中的定位
        val bandsList: ListBuffer[Double] = ListBuffer.empty[Double]
        for(index <- col2){
          if(index>=bandCount2) throw new IllegalArgumentException("col2超出最大波段数！")
          else bandsList.append(t._2.band(index).getDouble(j, i))
        }
        list.append(((row, col, i, j), bandsList.toList))
      }
      list.toList
    })

    val joinRdd: RDD[((Int, Int, Int, Int), (List[Double], List[Double]))] = rdd1.join(rdd2)
    val rowRdd: RDD[Row] = joinRdd.map(t => {
      val list = List(t._1._1.toDouble, t._1._2.toDouble, t._1._3.toDouble, t._1._4.toDouble) ::: t._2._1 ::: t._2._2
      Row(list:_*)
    })
    rowRdd
  }
  def compressFile(srcFilePath: String, destZipPath: String): Unit = {
    var zos: ZipOutputStream = null
    try {
      val fos: FileOutputStream = new FileOutputStream(new File(destZipPath))
      zos = new ZipOutputStream(fos)
      val sourceFile = new File(srcFilePath)
      compress(sourceFile, zos, sourceFile.getName, true)
    } catch {
      case e: Exception =>
        throw new RuntimeException("zip error from ZipUtils", e)
    } finally {
      if (zos != null) {
        try {
          zos.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }
  def compress(sourceFile: File, zos: ZipOutputStream, name: String, keepDirStructure: Boolean): Unit = {
    val buf = new Array[Byte](1024) //TODO BufferSize先写1024
    if (sourceFile.isFile) {
      // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
      zos.putNextEntry(new ZipEntry(name))
      val in = new FileInputStream(sourceFile)
      try {
        var len = in.read(buf)
        while (len != -1) {
          zos.write(buf, 0, len)
          len = in.read(buf)
        }
        // 完成一个entry
        zos.closeEntry()
      } finally {
        in.close()
      }
    } else {
      val listFiles = sourceFile.listFiles()
      if (listFiles == null || listFiles.isEmpty) {
        // 需要保留原来的文件结构时,需要对空文件夹进行处理
        if (keepDirStructure) {
          // 空文件夹的处理
          zos.putNextEntry(new ZipEntry(name + "/"))
          zos.closeEntry()
        }
      } else {
        for (file <- listFiles) {
          // 判断是否需要保留原来的文件结构
          if (keepDirStructure) {
            // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
            // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
            compress(file, zos, name + "/" + file.getName, keepDirStructure)
          } else {
            compress(file, zos, file.getName, keepDirStructure)
          }
        }
      }
    }
  }
  def unCompressFile(srcZipPath: String): Unit = {
    // 待解压的zip文件，需要在zip文件上构建输入流，读取数据到Java中
    val file = new File(srcZipPath) // 定义压缩文件名称
    var outFile: File = null // 输出文件的时候要有文件夹的操作
    val zipFile = new ZipFile(file) // 实例化ZipFile对象
    var zipInput: ZipInputStream = null // 定义压缩输入流

    // 定义解压的文件名
    var out: OutputStream = null // 定义输出流，用于输出每一个实体内容
    var input: InputStream = null // 定义输入流，读取每一个ZipEntry
    var entry: ZipEntry = null // 每一个压缩实体
    zipInput = new ZipInputStream(new FileInputStream(file)) // 实例化ZipInputStream

    // 遍历压缩包中的文件
    while ({ entry = zipInput.getNextEntry(); entry } != null) { // 得到一个压缩实体
      //      println(s"解压缩 ${entry.getName} 文件")
      println(entry.getName)
      outFile = new File(Paths.get(srcZipPath).getParent().toString() + s"/${entry.getName}") // 定义输出的文件路径
      if (!outFile.getParentFile.exists()) { // 如果输出文件夹不存在
        outFile.getParentFile.mkdirs() // 创建文件夹
      }
      if (!outFile.exists()) { // 判断输出文件是否存在
        if (entry.isDirectory) {
          outFile.mkdirs()
          //          println("create directory...")
        } else {
          outFile.createNewFile() // 创建文件
          //          println("create file...")
        }
      }
      if (!entry.isDirectory) {
        input = zipFile.getInputStream(entry) // 得到每一个实体的输入流
        out = new FileOutputStream(outFile) // 实例化文件输出流
        var temp = 0
        while ({ temp = input.read(); temp != -1 }) {
          out.write(temp)
        }
        input.close() // 关闭输入流
        out.close() // 关闭输出流
      }
    }
    zipInput.closeEntry()
    zipInput.close()
  }

}

