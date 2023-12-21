package whu.edu.cn.geocube.util

import java.io.IOException
import java.util
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, TableName}
import whu.edu.cn.config.GlobalConfig.Others.hbaseHost

import scala.collection.mutable.ListBuffer

/**
 * Some basic CRUD and extended operations in HBase.
 *
 */
object HbaseUtil {
  System.setProperty("hadoop.home.dir", "/root/hadoop")

  //configuration for hbase
  val configuration = HBaseConfiguration.create()

  //set zookeeper cluster
  configuration.set(HConstants.ZOOKEEPER_QUORUM, hbaseHost)

  //set RPC timeout
  configuration.set("hbase.rpc.timeout", "200000")

  //set scanner cache
  configuration.set("hbase.client.scanner.caching", "200000")

  //set scanner timeout
  configuration.set("hbase.client.scanner.timeout.period", "200000")

  //set mapreduce task timeout
  configuration.setInt("mapreduce.task.timeout", 60000)

  //  configuration.setInt("hbase.client.retries.number",3)

  //set maximum allowed size of a KeyValue instance, default 10485760
  configuration.set("hbase.client.keyvalue.maxsize", "104857600")

  //set maximum allowed size of an individual cell, inclusive of value and all key components, default 10485760
  configuration.set("hbase.server.keyvalue.maxsize", "104857600")

  configuration.set("hbase.client.retries.number", "3")

  configuration.setInt("hbase.client.operation.timeout",200000)

  //establish connection and adimin
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin

  /**
   * Create a table in HBase
   *
   * @param tableName
   * @param columnFamilies
   */
  def createTable(tableName: String, columnFamilies: Array[String]) = {
    //table name
    val tName = TableName.valueOf(tableName)
    //create table if not exits
    if (!admin.tableExists(tName)) {
      val descriptor = new HTableDescriptor(tName)
      for (columnFamily <- columnFamilies) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }
      admin.createTable(descriptor)
      println("table " + tableName + " is created")
    }
  }

  /**
   * Drop a table.
   *
   * @param tableName
   */
  def dropTable(tableName: String): Unit = {
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
    println("table " + tableName + " is dropped")
  }

  /**
   * insert a row with one columnFamily and column.
   *
   * @param tableName
   * @param rowKey
   * @param columnFamily
   * @param column
   * @param value
   */
  def insertData(tableName: String, rowKey: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    table.put(put)
    println(rowKey + "insert successfully!")
  }

  /**
   * Delete a row.
   *
   * @param tableName
   * @param rowKey
   */
  def deleteRow(tableName: String, rowKey: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val delete = new Delete(Bytes.toBytes(rowKey))
    table.delete(delete)
    println(rowKey + "delete successfully!")
  }

  /**
   * Query a row data using rowkey.
   *
   * @param tableName
   * @param rowKey
   * @return a row data
   */
  def getRow(tableName: String, rowKey: String): Result = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)
    /*for (rowKv <- result.rawCells()) {
      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
      println("TimeStamp:" + rowKv.getTimestamp)
      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      println("Value:" + new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
      println("Value:" + Bytes.toInt(rowKv.getValueArray, 3))
      println("rowKv.getValueArray.length:" + rowKv.getValueArray.length)
      println("rowKv.getValueLength:" + rowKv.getValueLength)
      println("rowKv.getValueOffset:" + rowKv.getValueOffset)
    }*/
    result
  }

  /**
   * Query a row data by fuzzy matching with a fuzzy rowkey.
   *
   * If the input fuzzy rowkey string is contained in
   * a rowkey string in the table, print the row data.
   *
   * @param tableName
   * @param fuzzyRowKey
   * @return
   */
  def getFuzzyRow(tableName: String, fuzzyRowKey: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()

    //create a row filter
    val rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(fuzzyRowKey))
    scan.setFilter(rowFilter)
    val scanner = table.getScanner(scan)
    val scannerIter = scanner.iterator()
    var count = 0
    while (scannerIter.hasNext) {
      println(scannerIter.next())
      count += 1
    }
    println("features sum: " + count)
  }

  /**
   * Scan specific column family on all rows.
   *
   * @param tableName
   * @param columnFamily
   */
  def scanDataFromHTable(tableName: String, columnFamily: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //initiate scan object
    val scan = new Scan()

    //add column family
    scan.addFamily(columnFamily.getBytes())

    //scan table
    val scanner = table.getScanner(scan)
    var result = scanner.next()

    // check results
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)}")
      result = scanner.next()
    }

    //close scanner
    scanner.close()
  }

  /**
   * Close connection.
   */
  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
        println("closed successfully!")
      } catch {
        case e: IOException => println("failed to close!")
      }
    }
  }

  /**
   * Get specific column values of a list of rows
   *
   * @param tableName
   * @param columnFamily
   * @param column
   * @param rowKeyList
   * @return a list of column values
   */
  def getRowColumnValue(tableName: String, columnFamily: String, column: String, rowKeyList: util.List[String]): util.List[String] = {
    val getList: util.List[Get] = new util.LinkedList[Get]()
    val haveRowkeys: util.List[String] = new util.LinkedList[String]()

    //query specific column values of a list of rows
    val table = connection.getTable(TableName.valueOf(tableName))
    for (i <- 0 until rowKeyList.size()) {
      val rowkey = rowKeyList.get(i)
      val get: Get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
      getList.add(get)
    }
    val results: Array[Result] = table.get(getList)

    //transform queries values into UTF-8 format
    for (result <- results) {
      for (kv <- result.rawCells()) {
        val value = new String(kv.getRowArray, kv.getRowOffset, kv.getRowLength, "UTF-8")
        haveRowkeys.add(value)
      }
    }
    haveRowkeys
  }

  /**
   * Get raster tile meta.
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @return a string of raster tile meta
   */
  def getTileMeta(tableName: String, rowKey: String, family: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if (!get.isCheckExistenceOnly) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result: Result = table.get(get)
      val res = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
      res
    } else {
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }

  /**
   * Get raster tile data.
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @return a byte array of tile
   */
  def getTileCell(tableName: String, rowKey: String, family: String, column: String): Array[Byte] = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if (!get.isCheckExistenceOnly) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result: Result = table.get(get)
      val res = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))
      res
    } else {
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }

  /**
   * Get vector data meta.
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @return a string of vector data meta
   */
  def getVectorMeta(tableName: String, rowKey: String, family: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if (!get.isCheckExistenceOnly) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result: Result = table.get(get)
      val rowKv = result.rawCells().last
      val res = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      res
    } else {
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }

  /**
   * Get vector data .
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param column
   * @return a string of vector data
   */
  def getVectorCell(tableName: String, rowKey: String, family: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if (!get.isCheckExistenceOnly) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result: Result = table.get(get)
      val rowKv = result.rawCells().last
      val res = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      res
    } else {
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  /**
   *
   *
   * @param tableName tableName for query
   * @param prefix prefix used for filter
   * @return
   */
  def getVectorWithPrefix(tableName:String,prefix:String):ListBuffer[(String,(String,String,String))]={
    var resList=ListBuffer.empty[(String,(String,String,String))]
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan:Scan=new Scan()
    val filter=new PrefixFilter(Bytes.toBytes(prefix))
    scan.setFilter(filter)
    val scanner=table.getScanner(scan)
    val it: util.Iterator[Result] = scanner.iterator()
    while(it.hasNext){
      val result: Result = it.next()
      val cells=result.rawCells()
      val rowkey=CellUtil.cloneRow(result.rawCells().last)
      var geom=""
      var meta=""
      var userData=""
      for(cell <- cells){
        val family = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        if("geom".equals(qualifier)){
          geom=(value)
        }
        else if("metaData".equals(qualifier)){
          meta=(value)
        }
        else if("customExtension".equals(family)){
          userData=(value)
        }
      }
      val kv=(Bytes.toString(rowkey),(geom,meta,userData))
      resList.append(kv)
    }
    resList
  }
}
