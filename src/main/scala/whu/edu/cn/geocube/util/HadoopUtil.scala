package whu.edu.cn.geocube.util

import java.io.ByteArrayInputStream

import geotrellis.spark.store.hadoop.HadoopLayerWriter
import geotrellis.spark.store.hadoop.geotiff.AttributeStore
import geotrellis.store.hadoop.HadoopAttributeStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
 * @author czp
 * @date 2022/3/7 20:52
 */
object HadoopUtil {
  //1 创建连接
  val conf = new Configuration();
  //2 连接端口
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  conf.set("fs.default.name","hdfs://192.168.112.128:9000")
  def putHDFS(value:Array[Byte],filePath:String): Unit ={
    //3 获取连接对象
    val fs = FileSystem.get(conf);
//    //本地文件上传到 hdfs
//    fs.copyFromLocalFile(new Path(filePath), new Path("/data/custom"));
//    fs.close();
    //使用HDFS的工具类，来简化上传的过程
    //创建输出流 ------> HDFS
    val out:FSDataOutputStream= fs.create(new Path(filePath))
    val in=new FSDataInputStream(new ByteArrayInputStream(value))
    IOUtils.copyBytes(in, out, 1024)
//    val attributeStore = HadoopAttributeStore(filePath)
//    val writer = HadoopLayerWriter(filePath, attributeStore)
//    writer.
  }
}
