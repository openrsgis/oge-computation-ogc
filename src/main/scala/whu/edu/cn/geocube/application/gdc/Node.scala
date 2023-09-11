package whu.edu.cn.geocube.application.gdc

import com.alibaba.fastjson.JSONObject


class Node {
  var functionName: String = _
  var arguments: JSONObject = _
  var parent: Node = _
  var children: List[Node] = List.empty[Node]
  var depth: Int = _
  var width: Int = _
  var UUID: String = _

  def getFunctionName: String = {
    this.functionName
  }

  def setFunctionName(functionName: String): Unit = {
    this.functionName = functionName
  }

  def getArguments: JSONObject = {
    this.arguments
  }

  def setArguments(arguments: JSONObject): Unit = {
    this.arguments = arguments
  }

  def getParent: Node = {
    this.parent
  }

  def setParent(parent: Node): Unit = {
    this.parent = parent
  }

  def getChildren: List[Node] = {
    this.children
  }

  def addChildren(node: Node): Unit = {
    if (this.children == null) {
      this.children = List(node)
    }
    else if (this.children != null) {
      this.children = this.children :+ node
    }
  }

  def getDepth: Int = {
    this.depth
  }

  def setDepth(depth: Int): Unit = {
    this.depth = depth
  }

  def getWidth: Int = {
    this.width
  }

  def setWidth(width: Int): Unit = {
    this.width = width
  }

  def getUUID: String = {
    this.UUID
  }

  def setUUID(UUID: String): Unit = {
    this.UUID = UUID
  }
}
