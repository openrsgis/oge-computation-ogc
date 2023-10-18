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

  //  final val startShName="startGrass.sh"
  //  final val execShName="execGrass.sh"

  final val startShNamePrefix="startGrass"
  final val execShNamePrefix="execGrass"

  //  新增_up目录用于删除本地清理存储空间

  //保存sh file的文件目录
  //  final val shFilePath="/usr/oge/"
  //  final val shFilePath="F:\\user\\oge\\"
  //  final val shFilePath_up="F:\\user\\oge"
  //  final val shFilePathForTest="F:\\oge_project\\"
  final val shFilePath="grass_sh/"
  final val shFilePath_out="/mnt/storage/grass/grass_sh/"


  //保存输入输出tif文件的位置
  //  final val tifFilePath="/usr/oge/data/"
  //  final val tifFilePath="F:\\user\\oge\\data\\"
  //  final val tifFilePath_up="F:\\user\\oge\\data"
  final val tifFilePath="grassdata/"    //docker下相对路径
  final val tifFilePath_out="/mnt/storage/grass/grassdata/"     //绝对路径
  //grass7.8版本的启动脚本
  //  final val grassRoot="/usr/bin/grass78"
  //  final val grassRoot="/usr/local/bin/grass"
  //  final val grassRoot="D:\\user\\local\\bin\\grass82"
  //  final val grassRoot="F:\\GRASS-GIS-8.2\\grass82"
  final val grassRoot="grass"


  //grass数据库的根目录
  //  final val grassdataPath="/usr/oge/grassdata/"
  //  final val grassdataPath="F:\\grassdataRoot\\"
  //  final val grassdataPath_up="F:\\grassdataRoot"
  final val grassdataPath="grassdataroot/"
  final val grassdataPath_out="/mnt/storage/grass/grassdataroot/"



  //用于oge的mapset
  final val mapset="mapset1"

  //TODO:迁移到服务器上如何修改相应路径
  final val grassConf:String="/mnt/storage/grass/grassConf.json"

  final val grassLocation:String="/mnt/storage/grass/grassLocation.json"
  //  弄清本地各个地址  路径不能留空格   输入参数也不能出现空格！    已在本机成功运行！
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
    //    docker是否需要单独一行
    //    var command1:String="docker start bc1b4cb0a9db;"+"docker exec -it bc1b4cb0a9db /bin/bash;"
    //    var command2:String="cd /grass;"
    var command:List[String] =List.empty
    //    command = command:+ "docker start bc1b4cb0a9db"
    //    command = command:+ "docker exec -it bc1b4cb0a9db /bin/bash"
    command = command:+ ""
    //    command = command:+  "cd /grass"
    command = command:+ grassRoot+" --text -c "+tifName+" "+location+" --exec sh "+exeShFile    //不读取最后一行？
    //    val command:String=grassRoot+" --text -c "+tifName+" "+location+" --exec "+exeShFile

    //    var command:String=grassRoot+" --text "+"--exec sh "+exeShFile
    val name=startShNamePrefix+System.currentTimeMillis()+".sh"
    //    val name=startShNamePrefix+System.currentTimeMillis()+".bat"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath+name))
    out.write("#!/bin/bash"+"\n")
    out.write("\n")
    for(com <- command){
      out.write(com+"\n")
    }
    //    out.write(command1+"\n")
    //    out.write(command2+"\n")
    out.close()
    filePath+name
  }

  def createStartSh2(grassRoot:String,exeShFile:String,filePath_out:String,filePath:String,tifName:String=null,location:String=null): String ={
    var command:List[String] =List.empty
    command = command:+ ""
    command = command:+"cd /grass"
    command = command:+ grassRoot+" --text -c "+tifName+" "+location+" --exec sh "+exeShFile
    command = command:+ "pwd"
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

  def createStartSh_docker(filePath:String,startSh_path:String): String={
    var command:List[String] =List.empty
    command = command:+ "docker start bc1b4cb0a9db"
    command = command:+ "docker exec  bc1b4cb0a9db /bin/bash /grass/"+startSh_path
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
    * 用于创建交给grass执行的批处理脚本
    *
    * @param commandList 命令列表
    * @param filePath 用于存放Exec脚本的地址
    * @return
    */
  def createExecSh(commandList:List[String],filePath:String): String ={
    val name=execShNamePrefix+System.currentTimeMillis()+".sh"
    //    val name=execShNamePrefix+System.currentTimeMillis()+".bat"
    val out:BufferedWriter=new BufferedWriter(new FileWriter(filePath+name))
    out.write("\n")
    for(command <- commandList){
      out.write(command+"\n")
    }
    out.close()
    filePath+name
  }

  def createExecSh2(commandList:List[String],filePath_out:String,filePath:String): String ={
    val name=execShNamePrefix+System.currentTimeMillis()+".sh"
    //    val name=execShNamePrefix+System.currentTimeMillis()+".bat"
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

    try {
      versouSshUtil("10.101.240.10", "root", "ypfamily", 22)
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

  def runGrassSh(startShFile:String): Unit = {

    //授予执行脚本755权限（读取、写入、执行）
    val builder: ProcessBuilder = new ProcessBuilder("/bin/chmod", "755", startShFile)
    val process: Process = builder.start()
    process.waitFor()
    process.destroy()
    var ps: Process = null
    try {
      println("开始执行Runtime.getRuntime.exec(cmd)")
      ps = Runtime.getRuntime.exec(startShFile)
      //      ps = Runtime.getRuntime.exec("cmd /c start "+startShFile)

      new DealProcessSream(ps.getInputStream).start()
      new DealProcessSream(ps.getErrorStream).start()
      ps.waitFor()
      println("执行完成Runtime.getRuntime.exec(cmd)")
    } catch {
      case ex: Exception => ex.printStackTrace()
      case ex: InterruptedException => ex.printStackTrace()
    } finally {
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

  def makeRDDFromTif(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
                     sourceTiffpath: String): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
    val layout = input._2.layout
    val inputRdd = HadoopGeoTiffRDD.spatialMultiband(new Path(sourceTiffpath))(sc)
    val tiled = inputRdd.tileToLayout(input._2.cellType, layout, Bilinear)
    val srcLayout = input._2.layout
    val srcExtent = input._2.extent
    val srcCrs = input._2.crs
    val srcBounds = input._2.bounds
    val cellType = input._2.cellType
    val now = "1000-01-01 00:00:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(now).getTime
    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
    val tiledOut = tiled.map(t => {
      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), mutable.ListBuffer("Grass")), t._2)
    })
    println("成功读取tif")
    (tiledOut, metaData)
  }

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

  //  及时删除落地的tif与释放存储空间
  def DeleteFile(filePath:String):Unit={
    val file = new File(filePath)
    val fileList = file.listFiles()
    for(i <- fileList) {
      if (i.isFile)
      {
        i.delete()
        println("delete file : " + i)
      }
    }
  }

  //  连同目录一同删除，用于删除GRASS数据库中的location
  def deleteFileMethod(filePath:String): Unit ={
    val file = new File(filePath)
    if(file.isDirectory){
      val fileList = file.listFiles()

      for(i <- fileList){
        if(i.isDirectory){
          deleteFileMethod(i.toString)
        }else{
          i.delete()
        }

      }
      file.delete()
    }else{
      file.delete()
    }

  }

  //    生成最终RDD需要参照初始RDD的元数据，这会使得GRASS结果与最终算子返回RDD产生出入，对于输入多景影像算子依照输入第一个生成的尤其明显！ 已替换！
  def main(args: Array[String]): Unit = {
    //    createFuctionFromJson("r.neighbors")


    val conf = new SparkConf()
      //        .setMaster("spark://gisweb1:7077")
      .setMaster("local[*]")
      .setAppName("query")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)

    //    清空存储
    //    DeleteFile(tifFilePath_up)
    //    DeleteFile(shFilePath_up)
    //    deleteFileMethod(grassdataPath_up)

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
    val img=Coverage.load(sc,"LE07_L1TP_123038_20161206_20170127_01_T1","LE07_L1TP_C01_T1",level = 1)
    val BandList=List("B2")
    val img1=Coverage.selectBands(img,BandList)


    //    测试返回单个影像算子
    //        val img2=r_neighbors(sc,img1)
    //        val img2=r_rescale(sc,img1,"0,128")
    val img2=r_sunmask(sc,img1,"2000","1","1","0","0","0","-5")
    saveRDDToTif(img2,tifFilePath+"grass_result"+".tif")


    //    测试返回string类算子
    //        val str = r_surf_area(sc,img1)
    //    val str = r_stats(sc,img1,"a")
    //    val str = r_coin(sc,img1,img2,"p")
    //    val str = r_volume(sc,img1,img2)
    //        println(str)

    println("******************测试完毕*********************！")

    //    val path1=tifFilePath+"test1"+".tif"
    //    val path2=tifFilePath+"test2"+".tif"
    //    val path3=tifFilePath+"test3"+".tif"
    //    val path4=tifFilePath+"test4"+".tif"
    //
    //    saveRDDToTif(img1,path1)
    //
    //    val img2=makeRDDFromTif(sc,img1,path1)
    //    saveRDDToTif(img2,path2)
    //
    //    val img3=makeRDDFromTif(sc,img2,path2)
    //    saveRDDToTif(img3,path3)
    //
    //    val img4=makeRDDFromTif(sc,img3,path3)
    //    saveRDDToTif(img4,path4)
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
  //  未给定Default值的非必要参数怎么处理？根据GRASS的例子修改！   JSON的修改？不使用自动生成，不修改！   不需要拿到坐标系，随机生成location
  //  WINDOWS系统似乎不支持转换current location?似乎已解决      输入多波段TIF后GRASS按各个波段成图如何处理？？先选取参数影像的波段后再输入GRASS进行处理

  //  测试用例
  def r_neighborsForTest(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String = "9", method:String = "maximum" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    //    val epsg=input._2.crs.toString
    val locationName="location"+System.currentTimeMillis()
    //    val locationName=getLocationOfCRS(epsg)
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
    val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath)

    tif
  }

  def r_neighbors(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), size:String = "3", method:String = "average" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    val locationName="location"+System.currentTimeMillis()
    //    val locationName=getLocationOfCRS(epsg)
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

  //  输入多幅RDD影像 cross & patch    已修改  待测试    执行命令行语句需用逗号隔开多幅影像，已通过循环实现！

  def r_cross(sc: SparkContext, input: Map[String,
    (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])])={
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
    //    val epsg=input.head._2._2.crs.toString
    //    val locationName="location"+System.currentTimeMillis()
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


    //    val grassInputDataName_1="javainput"+System.currentTimeMillis()
    //    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
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
    //    val tif=makeRDDFromTif(sc,input.head._2,sourceTiffpath)

    tif
  }

  def r_patch(sc: SparkContext, input: Map[String,
    (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])])={
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
    //    val epsg=input.head._2._2.crs.toString
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

  //  TODO:执行blend操作后自动跳出如何解决？    shade同样问题
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
    //    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
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
    //    val epsg=red._2.crs.toString
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

  //  多参数有点难绷 sunmask
  def r_sunmask(sc: SparkContext, elevation: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), year:String, month:String, day:String, hour:String, minute:String, second:String="0", timezone:String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(elevation,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    //    val epsg=elevation._2.crs.toString
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
  //  重采样函数需要考虑引入采样后的分辨率    已修改
  //  可能会改变元数据？？？已优化TIF转RDD
  //  g.region res=20 -ap

  def r_resamp_stats(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), res:String, method:String = "average", quantile:String = "0.5" )={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    //    val epsg=input._2.crs.toString
    //    val locationName=getLocationOfCRS(epsg)
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
    //    val epsg=input._2.crs.toString
    //    val locationName=getLocationOfCRS(epsg)
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

  //  计算后名称发生变化，已解决！
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

//    val file:File=new File(sourceTiffpath_out)
//    while(!file.exists()){}
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

  //// 这是什么东西？讲解用例！
  //  def r_test(sc: SparkContext, first: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), second: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), args1:String, args2:String, args3:String )={
  //    val time_1=System.currentTimeMillis()
  //    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
  //    saveRDDToTif(first,outputTiffPath_1)
  //    val time_2=System.currentTimeMillis()
  //    val outputTiffPath_2=tifFilePath+"grassinput_"+time_2+".tif"
  //    saveRDDToTif(second,outputTiffPath_2)
  //
  //    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
  //    val epsg=first._2.crs.toString
  //    val locationName="location"+System.currentTimeMillis()
  //    val location=grassdataPath+locationName
  //    var commandList:List[String] =List.empty
  //    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
  //    val grassInputDataName_1="javainput"+System.currentTimeMillis()
  //    commandList=commandList:+"r.in.gdal input="+outputTiffPath_1 +" output="+grassInputDataName_1
  //    val grassInputDataName_2="javainput"+System.currentTimeMillis()
  //    commandList=commandList:+"r.in.gdal input="+outputTiffPath_2 +" output="+grassInputDataName_2
  //    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
  //    commandList=commandList:+"r.test"+" first="+grassInputDataName_1+"@"+mapset+" second="+grassInputDataName_2+"@"+mapset+" output="+grassOutPutDataName +" args1="+args1+" args2="+args2+" args3="+args3
  //    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
  //    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
  //    val execShFile=createExecSh(commandList,shFilePath)
  //
  //    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)
  //
  //    runGrassSh(startShFile)
  //
  //    val file:File=new File(sourceTiffpath)
  //    while(!file.exists()){}
  //    val tif=makeRDDFromTif(sc,first,sourceTiffpath)
  //
  //    tif
  //  }

  ////  本地测试？对！
  //  def r_neighborsFortest(size:String, method:String )={
  //    val time_1=System.currentTimeMillis()
  //    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
  //
  //
  //    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
  //
  //    val locationName="location4"
  //    val location=grassdataPath+locationName
  //    var commandList:List[String] =List.empty
  //    commandList=commandList:+"g.mapset -c mapset="+mapset+" location="+locationName
  //    val grassInputDataName_1="javainput"+System.currentTimeMillis()
  //    commandList=commandList:+"r.in.gdal "+"input="+outputTiffPath_1 +" output="+grassInputDataName_1
  //    val grassOutPutDataName="grassoutput"+System.currentTimeMillis()
  //    commandList=commandList:+"r.neighbors"+" "+"input="+grassInputDataName_1+"@"+mapset+" output="+grassOutPutDataName +" size="+size+" method="+method
  //    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
  //    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
  //    val execShFile=createExecSh(commandList,shFilePathForTest)
  //
  //    val startShFile=createStartSh(grassRoot,execShFile,shFilePathForTest)
  //  }

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

    //    runGrassSh(startShFile)

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
  //  统计分析类算子返回值为list如何解决？  1.直接将命令结果导出为txt后再读取txt返回  已测试，需等待txt有输出结果后返回+等待txt完全生成！

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

    //    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    //    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
    val execShFile=createExecSh2(commandList,shFilePath_out,shFilePath)

    val startShFile=createStartSh2(grassRoot,execShFile,shFilePath_out,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

//    val file:File=new File(sourceTiffpath_out)
//    while(file.length()==0){}
//    Thread.sleep(1000)
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
    //    val tif=makeRDDFromTif(sc,map,sourceTiffpath)

    //    tif
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

  //  输入输出算子返回值是什么形式 如何返回？直接落地成对应格式！
  //  单独的GRASS算子输入和输出感觉意义不大

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
    //    commandList=commandList:+"g.region raster="+grassOutPutDataName+" -p"
    //    commandList=commandList:+"r.out.gdal input="+grassOutPutDataName+"@"+mapset+" output="+sourceTiffpath
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
    //    val tif=makeRDDFromTif(sc,input,sourceTiffpath)

    sourceTiffpath
  }


  //剩余算子
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

  //26
  def reportByGrass(sc: SparkContext, map: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]))={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(map,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    // val epsg=map._2.crs.toString
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


  //26
  def supportStatsByGrass(sc: SparkContext, map: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) )={
    /* val time_1=System.currentTimeMillis()
     val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
     saveRDDToTif(map,outputTiffPath_1)*/

    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(map,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".txt"
    // val epsg=map._2.crs.toString
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

//  ？？  前端无法执行
  def outGdalByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), format:String = "GTiff")={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    val sourceTiffpath_out=tifFilePath_out+"grassoutput_"+System.currentTimeMillis()+".tif"
    // val epsg=input._2.crs.toString
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
    //  val epsg=input._2.crs.toString
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

    val startShFile=createStartSh(grassRoot,execShFile,shFilePath,outputTiffPath_1,location)

    val startShFile_docker=createStartSh_docker(shFilePath_out,startShFile)

    runGrassSh2(startShFile_docker)

    val file:File=new File(sourceTiffpath_out)
    while(!file.exists()){}
    // val tif=makeChangedRasterRDDFromTif(sc,sourceTiffpath_out)

    sourceTiffpath
  }

  //26
  def inGdalByGrass(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]), band:String, location:String)={
    val time_1=System.currentTimeMillis()
    val outputTiffPath_1_out=tifFilePath_out+"grassinput_"+time_1+".tif"
    val outputTiffPath_1=tifFilePath+"grassinput_"+time_1+".tif"
    saveRDDToTif(input,outputTiffPath_1_out)

    val sourceTiffpath=tifFilePath+"grassoutput_"+System.currentTimeMillis()+".tif"
    //val epsg=input._2.crs.toString
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

}
