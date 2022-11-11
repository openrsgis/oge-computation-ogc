package whu.edu.cn.jsonparser

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.Map
import scala.io.Source
import scala.util.control.Breaks

object JsonToArgLocal {
//  val jsonAlgorithms: String = "C:/Users/dell/Desktop/cube_algorithms.json"
//  val scalaWriteFile: String = "C:/Users/dell/Desktop/cube_runMain.scala"
  val jsonAlgorithms: String = "src/main/scala/whu/edu/cn/jsonparser/algorithms_ogc.json"
//    val jsonAlgorithms: String = "/home/geocube/jupyter/lge_python/algorithm_data/algorithms.json"
//    val scalaWriteFile: String = "/home/geocube/lge/testRemote/src/main/scala/cn/edu/whu/runMain.scala"
  var arg:List[Tuple3[String, String, Map[String,String]]] = List.empty[Tuple3[String, String, Map[String,String]]]

  def numberOfArgs(functionName: String): Int = {
    val line: String = Source.fromFile(jsonAlgorithms).mkString
    val jsonObject = JSON.parseObject(line)
    val num = jsonObject.getJSONObject(functionName).getJSONArray("args").size()
    num
  }

  def getArgNameByIndex(functionName: String, index: Int): String = {
    val line: String = Source.fromFile(jsonAlgorithms).mkString
    val jsonObject = JSON.parseObject(line)
    val argName = jsonObject.getJSONObject(functionName).getJSONArray("args").getJSONObject(index).getString("name")
    argName
  }

  def getValueReference(valueReference: String, jsonObject: JSONObject): JSONObject = {
    jsonObject.getJSONObject(valueReference)
  }

  def BackAndOn(node: Node, depth: Int, jsonObject:JSONObject): Node = {
    for (i <- 0 until numberOfArgs(node.getFunctionName())) {
      val keys = node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i))
      if (node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)) != null) {
        if (keys.containsKey("functionInvocationValue")) {
          val nodeChildren: Node = new Node(keys.getJSONObject("functionInvocationValue").getString("functionName"),
            keys.getJSONObject("functionInvocationValue").getJSONObject("arguments"), node, null, 0, i + 1, node.getUUID() + i.toString)
          node.addChildren(nodeChildren)
          BackAndOn(nodeChildren, depth + 1, jsonObject)
        }
        if (keys.containsKey("valueReference")) {
          val value = getValueReference(keys.getString("valueReference"), jsonObject)
          if (value.getJSONObject("functionInvocationValue") != null) {
            val nodeChildren: Node = new Node(value.getJSONObject("functionInvocationValue").getString("functionName"),
              value.getJSONObject("functionInvocationValue").getJSONObject("arguments"), node, null, 0, i + 1, node.getUUID() + i.toString)
            node.addChildren(nodeChildren)
            BackAndOn(nodeChildren, depth + 1, jsonObject)
          }
          if (value.getString("constantValue") != null) {
            node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).remove("valueReference")
            if (value.get("constantValue").isInstanceOf[JSONArray]) {
              node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).put("constantValue", value.getString("constantValue").replace("\"", ""))
            }
            else {
              node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).put("constantValue", value.getString("constantValue"))
            }
          }
        }
        if (keys.containsKey("arrayValue")) {
          val nodeArray = keys.getJSONObject("arrayValue").getJSONArray("values")
          for (i <- (0 until nodeArray.size()).reverse) {
            val key = nodeArray.getJSONObject(i)
            if (key.containsKey("valueReference")) {
              val value = getValueReference(key.getString("valueReference"), jsonObject)
              if (value.getJSONObject("functionInvocationValue") != null) {
                val nodeChildren: Node = new Node(value.getJSONObject("functionInvocationValue").getString("functionName"),
                  value.getJSONObject("functionInvocationValue").getJSONObject("arguments"), node, null, 0, i + 1, node.getUUID() + i.toString)
                node.addChildren(nodeChildren)
                BackAndOn(nodeChildren, depth + 1, jsonObject)
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject()
                jsonObjectAdd.put("constantValue", nodeChildren.getUUID())
                nodeArray.add(i, jsonObjectAdd)
              }
              if (value.getString("constantValue") != null) {
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject()
                jsonObjectAdd.put("constantValue", value.getString("constantValue"))
                nodeArray.add(i, jsonObjectAdd)
              }
            }
            if (key.containsKey("functionInvocationValue")) {
              if (key.getJSONObject("functionInvocationValue") != null) {
                val nodeChildren: Node = new Node(key.getJSONObject("functionInvocationValue").getString("functionName"),
                  key.getJSONObject("functionInvocationValue").getJSONObject("arguments"), node, null, 0, i + 1, node.getUUID() + i.toString)
                node.addChildren(nodeChildren)
                BackAndOn(nodeChildren, depth + 1, jsonObject)
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject()
                jsonObjectAdd.put("constantValue", nodeChildren.getUUID())
                nodeArray.add(i, jsonObjectAdd)
              }
              if (key.getString("constantValue") != null) {
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject()
                jsonObjectAdd.put("constantValue", key.getString("constantValue"))
                nodeArray.add(i, jsonObjectAdd)
              }
            }
          }
        }
      }
    }
    node.setDepth(node.getDepth() + depth + 1)
    node
  }

  def DFS(nodeList: List[Node]): List[Node] = {
    val node = nodeList.iterator
    while (node.hasNext) {
      val nodeNow = node.next()
      if (nodeNow.getChildren() != null) {
        DFS(nodeNow.getChildren())
      }
      writeAsTuple(nodeNow)
    }
    nodeList
  }

  def writeAsTuple(node: Node):Unit = {
    val UUID = node.getUUID()
    val name = node.getFunctionName()
    var map = Map.empty[String, String]
    arg = arg :+ Tuple3(UUID, name, map)
    val num = numberOfArgs(node.getFunctionName())
    val size = node.getArguments().size()
    var sizeCount = 1
    val loop = new Breaks
    loop.breakable {
      for (i <- 0 until num) {
        if (node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)) != null) {
          val keys = node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i))
          if (keys.containsKey("functionInvocationValue") || keys.containsKey("valueReference")) {
            if (sizeCount <= size) {
              map += (getArgNameByIndex(node.getFunctionName(), i) -> (node.getUUID() + i.toString))
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break()
            }
          }
          else if (keys.containsKey("arrayValue")) {
            val getArray = node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).getJSONObject("arrayValue").getJSONArray("values")
            if (sizeCount <= size) {
              var st: String = "["
              for (i <- 0 until getArray.size() - 1) {
                val get = getArray.getJSONObject(i).get("constantValue")
                st = st + get + ","
              }
              val get = getArray.getJSONObject(getArray.size() - 1).get("constantValue")
              st = st + get + "]"
              map += (getArgNameByIndex(node.getFunctionName(), i) -> st)
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break()
            }
          }
          else {
            val get = node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).get("constantValue")
            val getString = node.getArguments().getJSONObject(getArgNameByIndex(node.getFunctionName(), i)).getString("constantValue")
            if (sizeCount <= size) {
              if (get.isInstanceOf[JSONArray]) {
                map += (getArgNameByIndex(node.getFunctionName(), i) -> getString.replace("\"", ""))
              }
              else {
                map += (getArgNameByIndex(node.getFunctionName(), i) -> getString)
              }
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break()
            }
          }
        }
      }
    }
  }


  def trans(jsonObject:JSONObject): List[Tuple3[String, String, Map[String,String]]] = {
    val node = new Node(jsonObject.getJSONObject("0").getJSONObject("functionInvocationValue").getString("functionName"),
      jsonObject.getJSONObject("0").getJSONObject("functionInvocationValue").getJSONObject("arguments"), null, null, 0, 1, "0")
    BackAndOn(node, 0, jsonObject)
    DFS(List(node))
    arg
  }
}