package whu.edu.cn.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static whu.edu.cn.util.SystemConstants.*;

public class JedisConnectionFactory {
    private static JedisPool jedisPool;
    static {
        // 连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8); // 最大连接数
        poolConfig.setMaxIdle(8); // 最大空闲
        poolConfig.setMinIdle(0); // 最小空闲
        poolConfig.setMaxWaitMillis(1000); // 最大等待时间

        jedisPool = new JedisPool(poolConfig, JEDIS_HOST,
                JEDIS_PORT, 1000);
    }

    public static Jedis getJedis(){
        return jedisPool.getResource();
    }
}
