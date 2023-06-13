package whu.edu.cn.modelbuilder

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable

class DAGObject {
  //成员变量,存储所有的算子
  private var DAGMap: List[(String, String, mutable.Map[String, String])] = List.empty[(String, String, mutable.Map[String, String])]
  private var temp: (String, String) = Tuple2("", "")
  private var tmpMap = mutable.Map.empty[String, String]

  def getDAGMap: List[(String, String, mutable.Map[String, String])] = DAGMap

  //成员方法
  def saveOperatorHead(uid: String, operatorName: String): Unit = {
    this.temp = (uid, operatorName)
  }

  //添加算子
  def addOperator(operatorObject: JSONObject): Unit = {
    val id = operatorObject.getString("id")
    val number = operatorObject.getString("number")
    if (!number.equals("")) {
      this.tmpMap.put(id, number)
    }
  }

  def addOperator(id: String, number: String): Unit = {
    if (!number.equals("")) {
      this.tmpMap.put(id, number)
    }
  }

  // close
  def clearTemp(): Unit = {
    val tuple3Operator = Tuple3(temp._1, temp._2, tmpMap)
    this.DAGMap = this.DAGMap :+ tuple3Operator
    this.temp = new Tuple2("", "")
    this.tmpMap = mutable.Map.empty[String, String]
  }

  // print
  def printDAG(): Unit = {
    println(this.DAGMap)
  }
}
