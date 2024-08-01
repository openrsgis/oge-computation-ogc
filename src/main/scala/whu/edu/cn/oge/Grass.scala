package whu.edu.cn.oge

import com.alibaba.fastjson.JSON
import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{Bounds, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleCellType, MultibandTile, Raster, Tile}
import geotrellis.spark._
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey
import java.io._
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import whu.edu.cn.util.SSHClientUtil._

import scala.util.control.Breaks._
import java.nio.file._
import java.util.concurrent.TimeUnit

import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.util.RDDTransformerUtil._

/**
  * 用于消耗java进程与grass进程之间的输入信息流和错误信息流，避免缓冲区堵塞
  *
  * @param inputStream
  */
class DealProcessSream(var inputStream: InputStream) extends Thread {
  override def run(): Unit = {
    var inputStreamReader:InputStreamReader = null
    var br:BufferedReader = null
    try {
      inputStreamReader = new InputStreamReader(inputStream)
      br = new BufferedReader(inputStreamReader)
      // 不打印信息
      while (br.readLine != null) {}
    } catch {
      case ioe: IOException => ioe.printStackTrace()
    } finally try {
      br.close()
      inputStreamReader.close
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}

object GrassUtil {


  final val startShNamePrefix="startGrass"
  final val execShNamePrefix="execGrass"

//  _out后缀为服务器绝对路径，不带的为docker绝对路径
  //保存sh file的文件目录
  final val shFilePath="grass_sh/"
  final val shFilePath_out="/mnt/storage/grass/grass_sh/"


  //保存输入输出tif文件的位置
  final val tifFilePath="grassdata/"    //docker下相对路径
  final val tifFilePath_out="/mnt/storage/grass/grassdata/"     //绝对路径
  //grass7.8版本的启动脚本
  final val grassRoot="grass"


  //grass数据库的根目录
  final val grassdataPath="grassdataroot/"
  final val grassdataPath_out="/mnt/storage/grass/grassdataroot/"



  //用于oge的mapset
  final val mapset="mapset1"

//  json文件的绝对路径
//  final val grassConf:String="/mnt/storage/grass/grassConf.json"
//
//  final val grassLocation:String="/mnt/storage/grass/grassLocation.json"


  /**
    *用于创建一个启动grass的脚本，并且在启动grass时指定一个交给grass执行的批处理脚本
    *
    * @param grassRoot 用来启动grass
    * @param exeShFile 用于批处理的grass命令脚本，必须在启动grass时指定
    * @param filePath_out 用于存放StartSh脚本目录的绝对路径
    * @param filePath 用于存放StartSh脚本目录的docker相对路径
    * @param tifName 如果要处理的tif影像所具有的坐标系在grass中没有对应的location，则需要根据该tif影像在grass中重新创建一个对应的location
    * @param location 要创建的location的名称
    * @return  返回startSH脚本的docker内路径
    */
  def createStartSh2(grassRoot:String,exeShFile:String,filePath_out:String,filePath:String,tifName:String=null,location:String=null): String ={
    var command:List[String] =List.empty
    command = command:+ ""
    command = command:+"cd /grass"
    command = command:+ grassRoot+" --text -c "+tifName+" "+location+" --exec sh "+exeShFile
    val name=startShNamePrefix+System.currentTimeMillis()+".sh"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath_out+name))
    out.write("#!/bin/bash"+"\n")
    out.write("\n")
    for(com <- command){
      out.write(com+"\n")
    }
    out.close()
    filePath+name
  }

  /**
    * 用于创建一个进入docker容器的脚本，并在进入容器时执行startSh脚本启动grass
    *
    * @param filePath  用于存放StartSh脚本的docker相对路径
    * @param startSh_path  startSH脚本的绝对地址
    * @return  启动进入docker并开始执行的脚本的绝对地址
    */

  def createStartSh_docker(filePath:String,startSh_path:String): String={
    var command:List[String] =List.empty
    command = command:+ "docker start c1c0a9548fe1 "
    command = command:+ "docker exec c1c0a9548fe1  /bin/bash /grass/"+startSh_path
    val name="enterdocker"+System.currentTimeMillis()+".sh"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath+name))
    out.write("#!/bin/bash"+"\n")
    out.write("\n")
    for(com <- command) {
      out.write(com + "\n")
    }
    out.close()
    filePath+name
  }

  /**
    *
    * @param commandList 命令列表
    * @param filePath_out 用于存放Exec脚本的目录的绝对路径
    * @param filePath  用于存放Exec脚本的目录的docker内路径
    * @return 返回Exec脚本的docker内路径
    */

  def createExecSh2(commandList:List[String],filePath_out:String,filePath:String): String ={
    val name=execShNamePrefix+System.currentTimeMillis()+".sh"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath_out+name))
    out.write("\n")
    for(command <- commandList){
      out.write(command+"\n")
    }
    out.close()
    filePath+name
  }

  /**
    * 启动外部进程来执行grass任务
    *
    * @param startShFile 待执行的脚本
    */
  def runGrassSh2(startShFile:String): Unit ={
    val host = GlobalConfig.QGISConf.QGIS_HOST
    val userName = GlobalConfig.QGISConf.QGIS_USERNAME
    val password = GlobalConfig.QGISConf.QGIS_PASSWORD
    val port = GlobalConfig.QGISConf.QGIS_PORT

    try {
      versouSshUtil(host, userName, password, port)
      val st =
        raw"sh "+startShFile
      //        raw"""conda activate qgis;d /home/geocube/oge/oge-server/dag-boot/qgis;python algorithmCodeByQGIS/gdal_onesidebuffer.py --input "$outputShpPath" --distance $distance --explodecollections $explodeCollections --field $field --bufferSide $bufferSideInput --dissolve $dissolve --geometry $geometry --options $options --output "$writePath"""".stripMargin

      println(s"st = $st")
      runCmd(st, "UTF-8")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  /**
    *
    * @param input 需要转换的RDD数据
    * @param outputTiffPath  tif文件落地后的绝对路径
    */
  def saveRDDToTif(input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),outputTiffPath:String):Unit={
    val tileLayerArray = input._1.map(t => {
      (t._1.spaceTimeKey.spatialKey, t._2)
    }).collect()
    val layout = input._2.layout
    val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(tileLayerArray)
    val stitchedTile = Raster(tile, layout.extent)
    GeoTiff(stitchedTile, input._2.crs).write(outputTiffPath)
    println("成功落地tif")
  }




  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    ////    选取多幅影像组成map    注意部分算子输入map部分算子逐次输入RDD
    //    val img=Coverage.load(sc,"LE07_L1TP_125039_20130110_20161126_01_T1",level = 0)
    //    val BandList1=List("B2")
    //    val BandList2=List("B3")
    //    val BandList3=List("B4")
    //    val img1=Coverage.selectBands(img,BandList1)
    //    val img2=Coverage.selectBands(img,BandList2)
    //    val img3=Coverage.selectBands(img,BandList3)
    //    var img_input:Map[String,
    //      (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])] = Map()
    //    img_input += ("1" -> img1)
    //    img_input += ("2" -> img2)
    //    img_input += ("3" -> img3)

    //    val img_output=r_cross(sc,img_input)
    //    val img_output=r_blend(sc,img1,img2)
    //    val img_output=r_shade(sc,img1,img2)
    //    val band_output=Coverage.bandNames(img_output)
    //    println(band_output)
    //    saveRDDToTif(img_output,tifFilePath+"grass_result"+".tif")

    //    选取一个RDD波段

    //        val img=Coverage.load(sc,"LC08_L1TP_124039_20180109_20180119_01_T1",level = 1)
    //            val img=Coverage.load(sc,"LE07_L1TP_125039_20130110_20161126_01_T1",level = 1)
//    val img=Coverage.load(sc,"LE07_L1TP_123038_20161206_20170127_01_T1","LE07_L1TP_C01_T1",level = 1)
//    val BandList=List("B2")
//    val img1=Coverage.selectBands(img,BandList)


    //    测试返回单个影像算子
    //        val img2=r_neighbors(sc,img1)
    //        val img2=r_rescale(sc,img1,"0,128")
//    val img2=r_sunmask(sc,img1,"2000","1","1","0","0","0","-5")
//    saveRDDToTif(img2,tifFilePath+"grass_result"+".tif")


    //    测试返回string类算子
    //        val str = r_surf_area(sc,img1)
    //    val str = r_stats(sc,img1,"a")
    //    val str = r_coin(sc,img1,img2,"p")
    //    val str = r_volume(sc,img1,img2)
    //        println(str)

    println("******************测试完毕*********************！")
  }


  def r_neighbors(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String = "3", method:String = "average" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.neighbors"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_buffer(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), distances:String, unit:String = "meters" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.buffer"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" distances="+distances+" unit="+unit
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }


  def r_cross(sc: SparkContext, input: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])])={
    var outputTiffPath_1: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_1_out: ListBuffer[String] = ListBuffer.empty
    for(img <-input) {
      val time_1 = System.currentTimeMillis()
      Thread.sleep(10)
      val path_1=tifFilePath+"grassinput_"+time_1+".tif"
      val path_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
      saveRDDToTif(img._2, path_1_out)
      outputTiffPath_1=outputTiffPath_1:+path_1
      outputTiffPath_1_out=outputTiffPath_1_out:+path_1_out
    }
    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    var grassInputDataName_1:ListBuffer[String]=ListBuffer.empty
    for(path <- outputTiffPath_1){
      val dataName_1="javainput"+System.currentTimeMillis()
      Thread.sleep(10)
      grassInputDataName_1=grassInputDataName_1:+dataName_1
      commandList=commandList:+"r.in.gdal "+"input="+path +" output="+dataName_1
    }
    var grassInputDataName_s=""

    breakable{
      for( a <- 0 until grassInputDataName_1.length){
        grassInputDataName_s = grassInputDataName_s+grassInputDataName_1(a)
        if( a == grassInputDataName_1.length-1){
          break
        }
        grassInputDataName_s += ","
      }
    }

    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.cross"+" input="+grassInputDataName_s+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1.head,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)
    tif
  }

  def r_patch(sc: SparkContext, input: Map[String, (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])])={
    var outputTiffPath_1: ListBuffer[String] = ListBuffer.empty
    var outputTiffPath_1_out: ListBuffer[String] = ListBuffer.empty
    for(img <-input) {
      val time_1 = System.currentTimeMillis()
      Thread.sleep(10)
      val path_1=tifFilePath+"grassinput_"+time_1+".tif"
      val path_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
      saveRDDToTif(img._2, path_1_out)
      outputTiffPath_1=outputTiffPath_1:+path_1
      outputTiffPath_1_out=outputTiffPath_1_out:+path_1_out
    }
    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    var grassInputDataName_1:ListBuffer[String]=ListBuffer.empty
    for(path <- outputTiffPath_1){
      val dataName_1="javainput"+System.currentTimeMillis()
      Thread.sleep(10)
      grassInputDataName_1=grassInputDataName_1:+dataName_1
      commandList=commandList:+"r.in.gdal "+"input="+path +" output="+dataName_1
    }
    var grassInputDataName_s=""

    breakable{
      for( a <- 0 until grassInputDataName_1.length){
        grassInputDataName_s = grassInputDataName_s+grassInputDataName_1(a)
        if( a == grassInputDataName_1.length-1){
          break
        }
        grassInputDataName_s += ","
      }
    }
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.patch"+" input="+grassInputDataName_s+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1.head,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_latlong(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.latlong"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_blend(sc: SparkContext, first: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), second: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), percent:String = "50")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(first,outputTiffPath_1_out)
    Thread.sleep(10)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2_out=tifFilePath_out+"grassinput_"+time_2+".tif"
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(second,outputTiffPath_2_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    Thread.sleep(10)
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.blend"+" first="+grassInputDataName_1+"@"+mapset+" second="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" percent="+percent
    commandList=commandList:+"d.mon wx0"
    commandList=commandList:+"d. rgb blue="+grassOutPutDataName+".b green="+grassOutPutDataName+".g red="+grassOutPutDataName+".r"
    commandList=commandList:+"r.composite"+" red="+grassOutPutDataName+".r@"+mapset+" green="+grassOutPutDataName+".g@"+mapset+" blue="+grassOutPutDataName+".b@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_composite(sc: SparkContext, red: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), green: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), blue: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), levels:String = "32" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(red,outputTiffPath_1_out)
    Thread.sleep(10)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2_out=tifFilePath_out+"grassinput_"+time_2+".tif"
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(green,outputTiffPath_2_out)
    Thread.sleep(10)
    val time_3=System.currentTimeMillis()
    val outputTiffPath_3_out=tifFilePath_out+"grassinput_"+time_3+".tif"
    val outputTiffPath_3=tifFilePath+"grassinput_"+time_3+".tif"
    saveRDDToTif(blue,outputTiffPath_3_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    Thread.sleep(10)
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    Thread.sleep(10)
    val grassInputDataName_3="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_3 +" output="+grassInputDataName_3
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.composite"+" red="+grassInputDataName_1+"@"+mapset+" green="+grassInputDataName_2+"@"+mapset+" blue="+grassInputDataName_3+"@"+mapset+" output="+grassOutPutDataName +" levels="+levels
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_sunmask(sc: SparkContext, elevation: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), year:String, month:String, day:String, hour:String, minute:String, second:String="0", timezone:String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(elevation,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.sunmask"+" elevation="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" year="+year+" month="+month+" day="+day+" hour="+hour+" minute="+minute+" second="+second+" timezone="+timezone
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }


  def r_resamp_stats(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), res:String, method:String = "average", quantile:String = "0.5" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"g.region raster="+grassInputDataName_1+" -p"
    commandList=commandList:+"g.region res="+res+" -ap"
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.stats"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method+" quantile="+quantile
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_resamp_interp(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),  res:String ,method:String = "bilinear" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"g.region raster="+grassInputDataName_1+" -p"
    commandList=commandList:+"g.region res="+res+" -ap"
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.interp"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_resamp_bspline(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),res:String, method:String = "bicubic" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"g.region raster="+grassInputDataName_1+" -p"
    commandList=commandList:+"g.region res="+res+" -ap"
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.bspline"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_resamp_filter(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),res:String, filter:String, radius:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"g.region raster="+grassInputDataName_1+" -p"
    commandList=commandList:+"g.region res="+res+" -ap"
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.filter"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" filter="+filter+" radius="+radius
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_resample(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),res:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"g.region raster="+grassInputDataName_1+" -p"
    commandList=commandList:+"g.region res="+res+" -ap"
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resample"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_texture(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String = "3", distance:String = "1", method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.texture"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" distance="+distance+" method="+method
    val method_upper=method.toUpperCase
    commandList=commandList:+"g.region raster="+grassOutPutDataName+"_"+method_upper+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"_"+method_upper+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_viewshed(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coordinates:String, observer_elevation:String = "1.75", target_elevation:String = "0.0", max_distance:String = "-1", refraction_coeff:String = "0.14286")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.viewshed"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" coordinates="+coordinates+" observer_elevation="+observer_elevation+" target_elevation="+target_elevation+" max_distance="+max_distance+" refraction_coeff="+refraction_coeff
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_shade(sc: SparkContext, shade: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), color: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), brighten:String = "0" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(shade,outputTiffPath_1_out)
    Thread.sleep(10)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2_out=tifFilePath_out+"grassinput_"+time_2+".tif"
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(color,outputTiffPath_2_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    Thread.sleep(10)
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.shade"+" shade="+grassInputDataName_1+"@"+mapset+" color="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" brighten="+brighten
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }


  def r_surf_idw(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),npoints: String = "12")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.surf.idw"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" npoints="+npoints
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }


  def r_rescale(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),to: String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.rescale"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" to="+to
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_surf_area(sc: SparkContext, map: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),vscale: String = "1.0" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(map,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"r.surf.area"+" map="+grassInputDataName_1+"@"+mapset +" vscale="+vscale+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str

  }

  def r_stats(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),flags: String,separator: String = "space" ,null_value: String = "*",nsteps: String = "255")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"r.stats -"+flags+" input="+grassInputDataName_1+"@"+mapset +" separator="+separator+" null_value="+null_value+" nsteps="+nsteps+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str

  }

  def r_coin(sc: SparkContext, first: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), second: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),units:String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(first,outputTiffPath_1_out)
    Thread.sleep(10)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2_out=tifFilePath_out+"grassinput_"+time_2+".tif"
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(second,outputTiffPath_2_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    Thread.sleep(10)
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    commandList=commandList:+"r.coin"+" first="+grassInputDataName_1+"@"+mapset +" second="+grassInputDataName_2+"@"+mapset+" units="+units+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str

  }

  def r_volume(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),clump:(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)
    Thread.sleep(10)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2_out=tifFilePath_out+"grassinput_"+time_2+".tif"
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(clump,outputTiffPath_2_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    Thread.sleep(10)
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_2
    commandList=commandList:+"r.volume"+" input="+grassInputDataName_1+"@"+mapset +" clump="+grassInputDataName_2+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str

  }


  def r_out_png(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),compression: String = "6")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".png"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".png"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.png"+" input="+grassInputDataName_1+"@"+mapset+" output="+sourceTiffpath +" compression="+compression+" -w"
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)

    sourceTiffpath
  }


  def reclassByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), rules:String = "-")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.reclass"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" rules="+rules
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def reportByGrass(sc: SparkContext, map: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(map,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"r.report"+" map="+grassInputDataName_1+"@"+mapset+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str
  }


  def supportStatsByGrass(sc: SparkContext, map: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) )={

    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(map,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".txt"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    commandList=commandList:+"r.support.stats"+" map="+grassInputDataName_1+"@"+mapset+" >> "+sourceTiffpath

    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(file.length()==0 && duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    Thread.sleep(1000)
    val Str = Source.fromFile(sourceTiffpath_out).mkString

    Str

  }

  def outGdalByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), format:String = "GTiff")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.out.gdal"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" format="+format
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()){}
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  //26
  def outBinByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),nu_ll:String = "0",bytes:String ,order:String = "native")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".bin"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".bin"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.out.bin"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" null="+nu_ll+" bytes="+bytes+" order="+order
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()){}

    sourceTiffpath
  }

  def inGdalByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), band:String, location:String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" band="+band+" location="+location
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()){}
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }
  def r_grow(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.surf.idw"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_fillnulls(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.surf.idw"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_random(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),npoints: String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.surf.idw"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" npoints="+npoints
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

  def r_univar(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.surf.idw"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val start = System.nanoTime()
    var end = System.nanoTime()
    var duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()&&duration<60000){
      end = System.nanoTime()
      duration = TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    tif
  }

}
