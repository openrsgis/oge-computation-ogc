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
import scala.io.Source

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

//  final val startShName="startGrass.sh"
//  final val execShName="execGrass.sh"

  final val startShNamePrefix="startGrass"
  final val execShNamePrefix="execGrass"

  //保存sh file的文件目录
  final val shFilePath="/usr/oge/"

  final val shFilePathForTest="D:\\Apersonal\\"

  //保存输入输出tif文件的位置
  final val tifFilePath="/usr/oge/data/"

  //grass7.8版本的启动脚本
//  final val grassRoot="/usr/bin/grass78"
  final val grassRoot="/usr/local/bin/grass"

  //grass数据库的根目录
  final val grassdataPath="/usr/oge/grassdata/"

  //用于oge的mapset
  final val mapset="mapset1"

  final val grassConf:String="D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage3\\code\\oge-computation-ogc\\src\\main\\scala\\whu\\edu\\cn\\application\\oge\\grassConf.json"

  final val grassLocation:String="D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage3\\code\\oge-computation-ogc\\src\\main\\scala\\whu\\edu\\cn\\application\\oge\\grassLocation.json"

//  final val grassConf:String="/usr/oge/data/grassConf.json"
//
//  final val grassLocation:String="/usr/oge/data/grassLocation.json"


  /**
    * 用于创建一个启动grass的脚本，并且在启动grass时指定一个交给grass执行的批处理脚本
    *
    * @param grassRoot 用来启动grass，随着grass版本的不同会有不同，比如grass7.8版本为grass78
    * @param exeShFile 用于批处理的grass命令脚本，必须在启动grass时指定
    * @param filePath 用于存放StartSh脚本的地址
    * @param tifName 如果要处理的tif影像所具有的坐标系在grass中没有对应的location，则需要根据该tif影像在grass中重新创建一个对应的location
    * @param location 要创建的location的名称
    * @return
    */
  def createStartSh(grassRoot:String,exeShFile:String,filePath:String,tifName:String=null,location:String=null): String ={
    var command:String=grassRoot+" --text -c "+tifName+" "+location+" --exec sh "+exeShFile
//    var command:String=grassRoot+" --text "+"--exec sh "+exeShFile
    val name=startShNamePrefix+System.currentTimeMillis()+".sh"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath+name))
    out.write(command)
    out.close()
    filePath+name
  }

  /**
    * 用于创建交给grass执行的批处理脚本
    *
    * @param commandList 命令列表
    * @param filePath 用于存放Exec脚本的地址
    * @return
    */
  def createExecSh(commandList:List[String],filePath:String): String ={
    val name=execShNamePrefix+System.currentTimeMillis()+".sh"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath+name))
    out.write("#!/bin/bash"+"\n")
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
  def runGrassSh(startShFile:String): Unit ={

    //授予执行脚本755权限（读取、写入、执行）
    val builder:ProcessBuilder = new ProcessBuilder("/bin/chmod", "755",startShFile);
    val process:Process = builder.start()
    process.waitFor()

    var ps:Process=null
    try{
      println("开始执行Runtime.getRuntime.exec(cmd)")
      ps = Runtime.getRuntime.exec(startShFile)
      new DealProcessSream(ps.getInputStream).start()
      new DealProcessSream(ps.getErrorStream).start()
      ps.waitFor()
    }catch {
      case ex:Exception => ex.printStackTrace()
      case ex:InterruptedException=>ex.printStackTrace()
    }finally {
      ps.destroy()
    }

  }

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

  def makeRDDFromTif(sc: SparkContext,input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                      sourceTiffpath:String):(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])={
//    val layout = input._2.layout
//    val inputRdd = HadoopGeoTiffRDD.spatial(new Path(sourceTiffpath))(sc)
//    val tiled = inputRdd.tileToLayout(DoubleCellType, layout, Bilinear)
//    val srcLayout = input._2.layout
//    val srcExtent = input._2.extent
//    val srcCrs = input._2.crs
//    val srcBounds = input._2.bounds
//    val now = "1000-01-01 00:00:00"
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = sdf.parse(now).getTime
//    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
//    val metaData = TileLayerMetadata(DoubleCellType, srcLayout, srcExtent, srcCrs, newBounds)
//    val tiledOut = tiled.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), mutable.ListBuffer("Grass")), t._2)
//    })
//    println("成功读取tif")
//    (tiledOut, metaData)
    input
  }

  //TODO:从数据库中查询对应坐标系的location
  def getLocationOfCRS(epsg:String):String={
//    val grassLocation:String="D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage3\\code\\oge-computation_ogc\\src\\main\\scala\\whu\\edu\\cn\\application\\oge\\grassLocation.json"
    val line: String = Source.fromFile(grassLocation).mkString
    val locationName = JSON.parseObject(line).getString(epsg)
    locationName
  }

  def createFuctionFromJson(grassFunctionName:String)={
//    val grassConf:String="D:\\Apersonal\\PostStu\\Project\\luojiaEE\\stage2\\code2\\oge-computation_ogc-master\\src\\main\\scala\\whu\\edu\\cn\\application\\oge\\grassConf.json"
    var content=""

    val line: String = Source.fromFile(grassConf).mkString
    val jsonObject = JSON.parseObject(line).getJSONObject(grassFunctionName)

    val functionName=jsonObject.getString("name")
    val inputsJsonArray=jsonObject.getJSONArray("inputArgs")
    val argsJsonArray=jsonObject.getJSONArray("otherArgs")
    var inputs:List[String]=List.empty
    var args:List[String]=List.empty
    var a=0
    while(a<inputsJsonArray.size()){
      inputs=inputs:+inputsJsonArray.getString(a)
      a=a+1
    }
    var b=0
    while(b<argsJsonArray.size()){
      args=args:+argsJsonArray.getString(b)
      b=b+1
    }

    val functionDefination=writeFuctionDefination(functionName,inputs,args)
    content=content+functionDefination+"={\n"

    //println(functionDefination)

    val saveTif=writeSaveTif(inputs)
    content=content+saveTif+"\n"
    //println(saveTif)

    val createExecsh=writeCreateExecSh(grassFunctionName,inputs,args)
    content=content+createExecsh+"\n"
    //println(createExecsh)

    val createStartSh=writeCreateStartSh()
    content=content+createStartSh+"\n"

    val runSh=writeRunSh()
    content=content+runSh+"\n"

    content=content+"    val file:File=new File(sourceTiffpath)\n"
    content=content+"    while(!file.exists()){}\n"

    val getTif=writeGetTif(inputs)
    content=content+getTif+"\n"

    content=content+"    tif\n"

    content=content+"  }"

    println(content)

  }

  def writeFuctionDefination(functionName:String,inputs:List[String],args:List[String]): String ={
    var functionDefination="  def "+functionName+"(sc: SparkContext, "
    val argsNum:Int=inputs.length+args.length
    var i:Int=0
    for(input <- inputs){
      i=i+1
      println(input)
      if(i==argsNum){
        functionDefination=functionDefination+input+": (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) )"
      }
      else{
        functionDefination=functionDefination+input+": (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), "
      }
    }
    for(arg <- args){
      i=i+1
      println(arg)
      if(i==argsNum){
        functionDefination=functionDefination+arg+":String )"
      }
      else{
        functionDefination=functionDefination+arg+":String, "
      }
    }
    functionDefination
  }

  def writeSaveTif(inputs:List[String]): String ={
    var saveTif=""
    var a=1
    for(input <- inputs){
      saveTif=saveTif+"    val time_"+a+"=System.currentTimeMillis()\n"
      saveTif=saveTif+"    val outputTiffPath_"+a+"=tifFilePath+\"grassinput_\"+"+"time_"+a+"+\".tif\"\n"
      saveTif=saveTif+"    saveRDDToTif("+input+",outputTiffPath_"+a+")\n"
      a=a+1
    }
    saveTif
  }

  def writeGetTif(inputs:List[String]): String ={
    var getTif="    val tif=makeRDDFromTif(sc,"+inputs.head+",sourceTiffpath)\n"
    getTif
  }

  def writeCreateExecSh(grassFuctionName:String,inputs:List[String],args:List[String]): String ={
    var createExecsh=""
    createExecsh=createExecsh+"    val sourceTiffpath=tifFilePath+\"grassoutput_\"+System.currentTimeMillis()+\".tif\"\n"
    createExecsh=createExecsh+"    val epsg="+inputs.head+"._2.crs.toString\n"
//    createExecsh=createExecsh+"    val locationName=getLocationOfCRS(epsg)\n"
    createExecsh=createExecsh+"    val locationName=\"location\"+System.currentTimeMillis()\n"
    createExecsh=createExecsh+"    val location=grassdataPath+locationName\n"
    createExecsh=createExecsh+"    var commandList:List[String] =List.empty\n"
    createExecsh=createExecsh+"    commandList=commandList:+\"g.mapset -c mapset=\"+mapset+\" location=\"+locationName\n"
    var a=1
    for(input <- inputs){
      createExecsh=createExecsh+"    val grassInputDataName_"+a+"=\"javainput\"+System.currentTimeMillis()\n"
//      createExecsh=createExecsh+"    commandList=commandList:+\"r.in.gdal \"+\""+input+"=\"+outputTiffPath_"+a+" +\" output=\"+grassInputDataName_"+a+"\n"
      createExecsh=createExecsh+"    commandList=commandList:+\"r.in.gdal input=\"+outputTiffPath_"+a+" +\" output=\"+grassInputDataName_"+a+"\n"
      a=a+1
    }
    createExecsh=createExecsh+"    val grassOutPutDataName=\"grassoutput\"+System.currentTimeMillis()\n"
    createExecsh=createExecsh+"    commandList=commandList:+\""+grassFuctionName+"\"+"
    a=1
    for(input <- inputs){
      createExecsh=createExecsh+"\" "+input+"=\"+grassInputDataName_"+a+"+\"@\"+mapset+"
      a=a+1
    }
    createExecsh=createExecsh+"\" output=\"+grassOutPutDataName +"
    var b=1
    for(arg <- args){
      if(b==args.length){
        createExecsh=createExecsh+"\" "+arg+"=\"+"+arg+"\n"
      }
      else{
        createExecsh=createExecsh+"\" "+arg+"=\"+"+arg+"+"
      }
      b=b+1
    }
    createExecsh=createExecsh+"    commandList=commandList:+\"g.region raster=\"+grassOutPutDataName+\" -p\"\n"
    createExecsh=createExecsh+"    commandList=commandList:+\"r.out.gdal input=\"+grassOutPutDataName+\"@\"+mapset+\" output=\"+sourceTiffpath\n"

    createExecsh=createExecsh+"    val execShFile=createExecSh(commandList,shFilePath)\n"


    createExecsh
  }

  def writeCreateStartSh(): String ={
    var createStartSh="    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)\n"
    createStartSh
  }

  def writeRunSh():String={
    val runsh="    runGrassSh(startShFile)\n"
    runsh
  }



  def main(args: Array[String]): Unit = {
//    createFuctionFromJson("r.neighbors")
//    createFuctionFromJson("r.buffer")
//    createFuctionFromJson("r.cross")
//    createFuctionFromJson("r.patch")
//    createFuctionFromJson("r.latlong")
//    createFuctionFromJson("r.test")
//    r_neighborsFortest("9","maximum")
//    createFuctionFromJson("r.blend")
//    createFuctionFromJson("r.composite")
//    createFuctionFromJson("r.sunmask")
//    createFuctionFromJson("r.resamp.stats")
//    createFuctionFromJson("r.resamp.interp")
//    createFuctionFromJson("r.resamp.bspline")
//    createFuctionFromJson("r.resamp.filter")
    createFuctionFromJson("r.texture")
    createFuctionFromJson("r.viewshed")
    createFuctionFromJson("r.shade")


//    val conf = new SparkConf()
//      //        .setMaster("spark://gisweb1:7077")
//      .setMaster("local[*]")
//      .setAppName("query")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//    val sc = new SparkContext(conf)
//
//    val img=Image.load(sc,"LE07_L1TP_C01_T1",measurementName = "Near-Infrared",crs="EPSG:32649",dateTime ="[2016-07-01 00:00:00,2016-08-01 00:00:00]",geom = "[112.054,28.8,115.588,31.774]",level = 11)
//
//    val epsg=img._1._2.crs.toString
//    println("EPSG:"+epsg)
//    println("LocationName:"+getLocationOfCRS(epsg))
//
//    val img2=r_neighbors(sc,img._1,"9","maximum")
//    println("******************测试完毕*********************！")
//    val bands=Image.bandNames(img2)
//    println(bands)

  }

//  def r_neighbors(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String, method:String )={
//    val time_1=System.currentTimeMillis()
//    //将imageRDD落地为tif
//    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
//    saveRDDToTif(input,outputTiffPath_1)
//    println("******************落地*********************！")
//    println("落地地址为："+outputTiffPath_1)
//
//    //grass输出tif的地址
//    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
//
//    val epsg=input._2.crs.toString
////    val locationName=getLocationOfCRS(epsg)
//    val locationName="location"+System.currentTimeMillis()
//
//    val location=grassdataPath+locationName
//
//    println("location为："+location)
//
//    println("******************构建sh*********************！")
//
//    //构建exeSh
//    var commandList:List[String] =List.empty
//    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
//    val grassInputDataName_1="javainput"+System.currentTimeMillis()
//    commandList=commandList:+"r.in.gdal "+"input="+outputTiffPath_1 +" output="+grassInputDataName_1
//    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
//    commandList=commandList:+"r.neighbors"+" "+"input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" method="+method
//    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
//    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
//    val execShFile=createExecSh(commandList,shFilePath)
//
//    //构建startSh
//    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)
//
//    println("execSh为："+execShFile)
//    println("startSh为："+startShFile)
//
//    println("******************执行sh*********************！")
//
//    //执行startSh
//    runGrassSh(startShFile)
//
//    println("******************等待结果输出*********************！")
//
//    val file:File=new File(sourceTiffpath)
//    while(!file.exists()){}
//
//    println("grass结果输出的地址为："+sourceTiffpath)
//
//    //把grass输出的tif文件转为imageRDD
//    val tif=makeRDDFromTif(sc,input,sourceTiffpath)
//    println("******************tif成功转为RDD*********************！")
//
//    tif
//  }

  def r_neighbors(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String, method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
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
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_buffer(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), distances:String, unit:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
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
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_cross(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), distances:String, unit:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.cross"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" distances="+distances+" unit="+unit
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_patch(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.patch"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_latlong(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), isLongitude:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.latlong"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" isLongitude="+isLongitude
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_blend(sc: SparkContext, first: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), second: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), percent:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(first,outputTiffPath_1)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(second,outputTiffPath_2)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=first._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.blend"+" first="+grassInputDataName_1+"@"+mapset+" second="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" percent="+percent
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,first,sourceTiffpath)

    tif
  }

  def r_composite(sc: SparkContext, red: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), green: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), blue: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), levels:String, level_red:String, level_blue:String, level_green:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(red,outputTiffPath_1)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(green,outputTiffPath_2)
    val time_3=System.currentTimeMillis()
    val outputTiffPath_3=tifFilePath+"grassinput_"+time_3+".tif"
    saveRDDToTif(blue,outputTiffPath_3)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=red._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassInputDataName_3="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_3 +" output="+grassInputDataName_3
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.composite"+" red="+grassInputDataName_1+"@"+mapset+" green="+grassInputDataName_2+"@"+mapset+" blue="+grassInputDataName_3+"@"+mapset+" output="+grassOutPutDataName +" levels="+levels+" level_red="+level_red+" level_blue="+level_blue+" level_green="+level_green
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,red,sourceTiffpath)

    tif
  }

  def r_sunmask(sc: SparkContext, elevation: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), altitude:String, azimuth:String, year:String, month:String, day:String, hour:String, minute:String, second:String, timezone:String, east:String, north:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(elevation,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=elevation._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.sunmask"+" elevation="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" altitude="+altitude+" azimuth="+azimuth+" year="+year+" month="+month+" day="+day+" hour="+hour+" minute="+minute+" second="+second+" timezone="+timezone+" east="+east+" north="+north
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,elevation,sourceTiffpath)

    tif
  }

  def r_resamp_stats(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), method:String, quantile:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.stats"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method+" quantile="+quantile
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_resamp_interp(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.interp"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_resamp_bspline(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.bspline"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_resamp_filter(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), filter:String, radius:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.resamp.filter"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" filter="+filter+" radius="+radius
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_texture(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String, distance:String, method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.texture"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" distance="+distance+" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_viewshed(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), coordinates:String, observer_elevation:String, target_elevation:String, max_distance:String, direction_range:String, refraction_coeff:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.viewshed"+" input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" coordinates="+coordinates+" observer_elevation="+observer_elevation+" target_elevation="+target_elevation+" max_distance="+max_distance+" direction_range="+direction_range+" refraction_coeff="+refraction_coeff
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    tif
  }

  def r_shade(sc: SparkContext, shade: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), color: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), brighten:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(shade,outputTiffPath_1)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(color,outputTiffPath_2)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=shade._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.shade"+" shade="+grassInputDataName_1+"@"+mapset+" color="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" brighten="+brighten
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,shade,sourceTiffpath)

    tif
  }


  def r_test(sc: SparkContext, first: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), second: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), args1:String, args2:String, args3:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(first,outputTiffPath_1)
    val time_2=System.currentTimeMillis()
    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
    saveRDDToTif(second,outputTiffPath_2)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val epsg=first._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassInputDataName_2="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.test"+" first="+grassInputDataName_1+"@"+mapset+" second="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" args1="+args1+" args2="+args2+" args3="+args3
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePath)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    runGrassSh(startShFile)

    val file:File=new File(sourceTiffpath)
    while(!file.exists()){}
    val tif=makeRDDFromTif(sc,first,sourceTiffpath)

    tif
  }

  def r_neighborsFortest(size:String, method:String )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"


    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"

    val locationName="location4"
    val location=grassdataPath+locationName
    var commandList:List[String] =List.empty
    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    commandList=commandList:+"r.in.gdal "+"input="+outputTiffPath_1 +" output="+grassInputDataName_1
    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
    commandList=commandList:+"r.neighbors"+" "+"input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" method="+method
    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh(commandList,shFilePathForTest)

    val startShFile=createStartSh(grassRoot,execShFile,shFilePathForTest)
  }

}
