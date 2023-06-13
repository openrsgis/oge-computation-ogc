package whu.edu.cn.geocube.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

object HttpUtil {

  def postResponse(url: String, params: String = null, header: String = null): String = {
    val httpClient: CloseableHttpClient = HttpClients.createDefault() // 创建 client 实例
    val post = new HttpPost(url) // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json: JSONObject = JSON.parseObject(header)
      // 根据 json的数据设置请求头
      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
    }

    // 填充请求体的参数
    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response: CloseableHttpResponse = httpClient.execute(post) // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8") // 获取返回结果
  }


}
