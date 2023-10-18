package whu.edu.cn.util;

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import whu.edu.cn.config.GlobalConfig.RedisConf.{JEDIS_HOST, JEDIS_PORT, JEDIS_PWD}

import java.time.Duration

class JedisUtil {
  // 连接池
  val poolConfig = new JedisPoolConfig
  // 最大连接数
  poolConfig.setMaxTotal(8)
  // 最大空闲
  poolConfig.setMaxIdle(8)
  // 最小空闲
  poolConfig.setMinIdle(0)
  // 设置连接池中的最小可驱逐空闲时间（秒）
  poolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60))


  private val JEDIS_POOL: JedisPool = new JedisPool(poolConfig, JEDIS_HOST, JEDIS_PORT, 1000, JEDIS_PWD)

  def getJedis: Jedis = JEDIS_POOL.getResource
}
