package whu.edu.cn.util

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/***
 * 单例类，仅用于发送结果返回值
 */
object PostSender {
  private val messages = mutable.Map.empty[String,mutable.ListBuffer[JSONObject]]
  private val stringMessages = mutable.Map.empty[String,String]
  def shelvePost(name:String, jsonObject:JSONObject): Unit = {
    if(messages.contains(name)){
      messages(name).append(jsonObject)
    }else{
      messages += (name -> mutable.ListBuffer(jsonObject))
    }
  }

  def shelvePost(name: String, jsonObject: Array[JSONObject]): Unit = {
    if (messages.contains(name)) {
      messages(name).union(jsonObject.toList)
    } else {
      messages += (name -> jsonObject.toList.to[ListBuffer])
    }
  }

  def shelvePost(name: String, value:String): Unit = {
    stringMessages += (name -> value)
  }

  def sendShelvedPost() : Unit ={
    val url = DAG_ROOT_URL + "/deliverUrl"
    val JsonObject_1 = new JSONObject()
    for((name,json) <- messages){
      JsonObject_1.put(name,json.toArray)
    }

    val outPutJson = new JSONObject()
    outPutJson.put("workID", Trigger.dagId)
    outPutJson.put("json", JsonObject_1)
    for ((name, value) <- stringMessages) {
      outPutJson.put(name, value)
    }
    println(outPutJson.toJSONString)
    sendPost(url,outPutJson.toJSONString)
  }

  def sendShelvedPost(URL: String): Unit = {
    val url = URL
    val JsonObject_1 = new JSONObject()
    for ((name, json) <- messages) {
      JsonObject_1.put(name, json.toArray)
    }

    val outPutJson = new JSONObject()
    outPutJson.put("workID", Trigger.dagId)
    outPutJson.put("json", JsonObject_1)
    for((name, value) <- stringMessages){
      outPutJson.put(name,value)
    }
    println(outPutJson.toJSONString)
    sendPost(url, outPutJson.toJSONString)
  }

  def clearShelvedMessages():Unit={
    messages.clear()
    stringMessages.clear()
  }
}
