package whu.edu.cn.jsonparser

import com.alibaba.fastjson.JSONObject


class Node(var functionName : String, var arguments : JSONObject, var parent: Node, var children: List[Node], var depth:Int, var width:Int, var UUID:String) {
  def getFunctionName(): String ={
    functionName
  }
  def setFunctionName(functionName:String) = {
    this.functionName = functionName
  }
  def getArguments():JSONObject ={
    arguments
  }
  def setArguments(arguments:JSONObject) = {
    this.arguments = arguments
  }
  def getParent():Node = {
    parent
  }
  def setParent(parent:Node) = {
    this.parent = parent
  }
  def getChildren():List[Node] = {
    children
  }
  def addChildren(node:Node) = {
    if(this.children==null){
      this.children = List(node)
    }
    else if(this.children!=null) {
      this.children = this.children :+ node
    }
  }
  def getDepth():Int = {
    this.depth
  }
  def setDepth(depth:Int) = {
    this.depth = depth
  }
  def getWidth():Int = {
    this.width
  }
  def setWidth(width:Int) = {
    this.width = width
  }
  def getUUID():String = {
    this.UUID
  }
  def setUUID(UUID:String) = {
    this.UUID = UUID
  }
}
