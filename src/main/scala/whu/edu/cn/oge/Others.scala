package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.GlobalConstantUtil
import whu.edu.cn.util.HttpRequestUtil.sendPost

package object Others {
  def sendNotice(notice: JSONObject): Unit = {

    val noticeJson = new JSONObject
    noticeJson.put("workID", Trigger.dagId)
    noticeJson.put("notice", notice.toJSONString)

    sendPost(GlobalConstantUtil.DAG_ROOT_URL + "/deliverUrl", noticeJson.toJSONString)
  }
  def printNotice(name:String, res: String):Unit={
    val noticeJson = new JSONObject
    noticeJson.put(name,res)
    sendNotice(noticeJson)
  }
}
