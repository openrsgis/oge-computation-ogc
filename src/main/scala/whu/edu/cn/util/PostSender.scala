package whu.edu.cn.util

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.config.GlobalConfig.Others.educationDataRoot
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/***
 * 单例类，仅用于发送结果返回值
 */
object PostSender {
  private val messages = mutable.Map.empty[String,mutable.ListBuffer[JSONObject]]

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

  def print() : Unit={
    val JsonObject_1 = new JSONObject()
    for((name,json) <- messages){
      JsonObject_1.put(name,json.toArray)
    }

    val outPutJson = new JSONObject()
    outPutJson.put("workID", Trigger.dagId)
    outPutJson.put("json", JsonObject_1)
    println(outPutJson.toJSONString)
    println(outPutJson.toJSONString)

  }

  def sendShelvedPost() : Unit ={
    val url = DAG_ROOT_URL + "/deliverUrl"
    val eduUrl = educationDataRoot
    val JsonObject_1 = new JSONObject()
    for((name,json) <- messages){
      JsonObject_1.put(name,json.toArray)
    }

    val outPutJson = new JSONObject()
    outPutJson.put("workID", Trigger.dagId)
    outPutJson.put("json", JsonObject_1)
    println(outPutJson.toJSONString)
    sendPost(url,outPutJson.toJSONString)
    sendPost(eduUrl, outPutJson.toJSONString)
  }

  def clearShelvedMessages():Unit={
    messages.clear()
  }
}
