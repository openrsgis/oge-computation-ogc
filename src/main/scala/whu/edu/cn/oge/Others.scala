package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost

package object Others {
  def sendNotice(notice: JSONObject): Unit = {

    val noticeJson = new JSONObject
    noticeJson.put("workID", Trigger.dagId)
    noticeJson.put("notice", notice.toJSONString)

    sendPost(DAG_ROOT_URL + "/deliverUrl", noticeJson.toJSONString)
  }
  def printNotice(name:String, res: String):Unit={
    val noticeJson = new JSONObject
    noticeJson.put(name,res)
    sendNotice(noticeJson)
  }
}
