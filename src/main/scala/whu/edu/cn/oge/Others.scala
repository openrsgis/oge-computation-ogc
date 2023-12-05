package whu.edu.cn.oge

import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostSender

import java.math.BigInteger
import java.security.MessageDigest

package object Others {
  def sendNotice(notice: JSONObject): Unit = {
    PostSender.shelvePost("notice", notice)
  }

  def printNotice(name: String, res: String): Unit = {
    val noticeJson = new JSONObject
    noticeJson.put(name, res)
    sendNotice(noticeJson)
  }

  // returns a 32-character MD5 hash version of the input string
  def md5HashPassword(usPassword: String): String = {
    val md = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(usPassword.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedPassword = bigInt.toString(16).trim
    hashedPassword
  }

  def main(args: Array[String]): Unit = {
    println(md5HashPassword("aaa"))
  }
}
