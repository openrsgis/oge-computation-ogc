package whu.edu.cn.jsonparser

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSONArray
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.entity.Node

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.control.Breaks

// TODO lrx: 解析的时候加上数据类型？
object JsonToArg {
  var jsonAlgorithms: String = GlobalConfig.Others.jsonAlgorithms
  var thirdJson: String = GlobalConfig.Others.thirdJson
  var dagMap: mutable.Map[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]] = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]]

  def numberOfArgs(functionName: String): Int = {
    val source: BufferedSource = Source.fromFile(jsonAlgorithms)
    val line: String = source.mkString
    val jsonObject: JSONObject = JSON.parseObject(line)
    val num: Int = jsonObject.getJSONObject(functionName).getJSONArray("args").size
    num
  }

  def getArgNameByIndex(functionName: String, index: Int): String = {
    val source: BufferedSource = Source.fromFile(jsonAlgorithms)
    val line: String = source.mkString
    val jsonObject: JSONObject = JSON.parseObject(line)
    val argName: String = jsonObject.getJSONObject(functionName).getJSONArray("args").getJSONObject(index).getString("name")
    argName
  }

  def getValueReference(valueReference: String, jsonObject: JSONObject): JSONObject = {
    jsonObject.getJSONObject(valueReference)
  }

  def BackAndOn(node: Node, depth: Int, jsonObject: JSONObject): Node = {
    for (i <- 0 until numberOfArgs(node.getFunctionName)) {
      val keys: JSONObject = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i))
      if (node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)) != null) {
        if (keys.containsKey("functionDefinitionValue")) {
          val body: String = keys.getJSONObject("functionDefinitionValue").getString("body")
          trans(jsonObject, body)
        }
        if (keys.containsKey("functionInvocationValue")) {
          val nodeChildren: Node = new Node
          nodeChildren.setFunctionName(keys.getJSONObject("functionInvocationValue").getString("functionName"))
          nodeChildren.setArguments(keys.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
          nodeChildren.setParent(node)
          nodeChildren.setDepth(0)
          nodeChildren.setWidth(i + 1)
          nodeChildren.setUUID(node.getUUID + i.toString)
          node.addChildren(nodeChildren)
          BackAndOn(nodeChildren, depth + 1, jsonObject)
        }
        if (keys.containsKey("valueReference")) {
          val value: JSONObject = getValueReference(keys.getString("valueReference"), jsonObject)
          if (value.getJSONObject("functionInvocationValue") != null) {
            val nodeChildren: Node = new Node
            nodeChildren.setFunctionName(value.getJSONObject("functionInvocationValue").getString("functionName"))
            nodeChildren.setArguments(value.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
            nodeChildren.setParent(node)
            nodeChildren.setDepth(0)
            nodeChildren.setWidth(i + 1)
            nodeChildren.setUUID(node.getUUID + i.toString)
            node.addChildren(nodeChildren)
            BackAndOn(nodeChildren, depth + 1, jsonObject)
          }
          if (value.getString("constantValue") != null) {
            node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).remove("valueReference")
            if (value.get("constantValue").isInstanceOf[JSONArray]) {
              node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).put("constantValue", value.getString("constantValue").replace("\"", ""))
            }
            else {
              node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).put("constantValue", value.getString("constantValue"))
            }
          }
        }
        if (keys.containsKey("arrayValue")) {
          val nodeArray: JSONArray = keys.getJSONObject("arrayValue").getJSONArray("values")
          for (i <- (0 until nodeArray.size).reverse) {
            val key: JSONObject = nodeArray.getJSONObject(i)
            if (key.containsKey("valueReference")) {
              val value: JSONObject = getValueReference(key.getString("valueReference"), jsonObject)
              if (value.getJSONObject("functionInvocationValue") != null) {
                val nodeChildren: Node = new Node
                nodeChildren.setFunctionName(value.getJSONObject("functionInvocationValue").getString("functionName"))
                nodeChildren.setArguments(value.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
                nodeChildren.setParent(node)
                nodeChildren.setDepth(0)
                nodeChildren.setWidth(i + 1)
                nodeChildren.setUUID(node.getUUID + i.toString)
                node.addChildren(nodeChildren)
                BackAndOn(nodeChildren, depth + 1, jsonObject)
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject
                jsonObjectAdd.put("constantValue", nodeChildren.getUUID)
                nodeArray.add(i, jsonObjectAdd)
              }
              if (value.getString("constantValue") != null) {
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject
                jsonObjectAdd.put("constantValue", value.getString("constantValue"))
                nodeArray.add(i, jsonObjectAdd)
              }
            }
            if (key.containsKey("functionInvocationValue")) {
              if (key.getJSONObject("functionInvocationValue") != null) {
                val nodeChildren: Node = new Node
                nodeChildren.setFunctionName(key.getJSONObject("functionInvocationValue").getString("functionName"))
                nodeChildren.setArguments(key.getJSONObject("functionInvocationValue").getJSONObject("arguments"))
                nodeChildren.setParent(node)
                nodeChildren.setDepth(0)
                nodeChildren.setWidth(i + 1)
                nodeChildren.setUUID(node.getUUID + i.toString)
                node.addChildren(nodeChildren)
                BackAndOn(nodeChildren, depth + 1, jsonObject)
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject
                jsonObjectAdd.put("constantValue", nodeChildren.getUUID)
                nodeArray.add(i, jsonObjectAdd)
              }
              if (key.getString("constantValue") != null) {
                nodeArray.remove(i)
                val jsonObjectAdd = new JSONObject
                jsonObjectAdd.put("constantValue", key.getString("constantValue"))
                nodeArray.add(i, jsonObjectAdd)
              }
            }
          }
        }
      }
    }
    node.setDepth(node.getDepth + depth + 1)
    node
  }

  def DFS(nodeList: List[Node], arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): List[Node] = {
    val node: Iterator[Node] = nodeList.iterator
    while (node.hasNext) {
      val nodeNow: Node = node.next
      if (nodeNow.getChildren != null) {
        DFS(nodeNow.getChildren, arg)
      }
      writeAsTuple(nodeNow, arg)
    }
    nodeList
  }

  def writeAsTuple(node: Node, arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])]): Unit = {
    val UUID: String = node.getUUID
    val name: String = node.getFunctionName
    val map = mutable.Map.empty[String, String]
    arg.append(Tuple3(UUID, name, map))
    val num: Int = numberOfArgs(node.getFunctionName)
    val size: Int = node.getArguments.size
    var sizeCount = 1
    val loop = new Breaks
    loop.breakable {
      for (i <- 0 until num) {
        if (node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)) != null) {
          val keys: JSONObject = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i))
          if (keys.containsKey("functionInvocationValue") || keys.containsKey("valueReference")) {
            if (sizeCount <= size) {
              map += (getArgNameByIndex(node.getFunctionName, i) -> (node.getUUID + i.toString))
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
          else if (keys.containsKey("arrayValue")) {
            val getArray: JSONArray = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).getJSONObject("arrayValue").getJSONArray("values")
            if (sizeCount <= size) {
              var st: String = "["
              for (i <- 0 until getArray.size - 1) {
                val get: AnyRef = getArray.getJSONObject(i).get("constantValue")
                st = st + get + ","
              }
              val get: AnyRef = getArray.getJSONObject(getArray.size - 1).get("constantValue")
              st = st + get + "]"
              map += (getArgNameByIndex(node.getFunctionName, i) -> st)
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
          else if (keys.containsKey("constantValue")) {
            val get: AnyRef = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).get("constantValue")
            val getString: String = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).getString("constantValue")
            if (sizeCount <= size) {
              if (get.isInstanceOf[JSONArray]) {
                map += (getArgNameByIndex(node.getFunctionName, i) -> getString.replace("\"", ""))
              }
              else {
                map += (getArgNameByIndex(node.getFunctionName, i) -> getString)
              }
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
          else if (keys.containsKey("argumentReference")) {
            val get: AnyRef = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).get("argumentReference")
            val getString: String = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).getString("argumentReference")
            if (sizeCount <= size) {
              if (get.isInstanceOf[JSONArray]) {
                map += (getArgNameByIndex(node.getFunctionName, i) -> getString.replace("\"", ""))
              }
              else {
                map += (getArgNameByIndex(node.getFunctionName, i) -> getString)
              }
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
          else if (keys.containsKey("functionDefinitionValue")) {
            val getString: String = node.getArguments.getJSONObject(getArgNameByIndex(node.getFunctionName, i)).getJSONObject("functionDefinitionValue").getString("body")
            if (sizeCount <= size) {
              map += (getArgNameByIndex(node.getFunctionName, i) -> getString)
              sizeCount = sizeCount + 1
            }
            else if (sizeCount > size) {
              loop.break
            }
          }
        }
      }
    }
  }


  def trans(jsonObject: JSONObject, UUID: String): Unit = {
    val node: Node = new Node
    node.setFunctionName(jsonObject.getJSONObject(UUID).getJSONObject("functionInvocationValue").getString("functionName"))
    node.setArguments(jsonObject.getJSONObject(UUID).getJSONObject("functionInvocationValue").getJSONObject("arguments"))
    node.setDepth(0)
    node.setWidth(1)
    node.setUUID(UUID)

    BackAndOn(node, 0, jsonObject)
    val arg: mutable.ArrayBuffer[(String, String, mutable.Map[String, String])] = mutable.ArrayBuffer.empty[(String, String, mutable.Map[String, String])]
    DFS(List(node), arg)
    dagMap += (UUID -> arg)
  }
}
