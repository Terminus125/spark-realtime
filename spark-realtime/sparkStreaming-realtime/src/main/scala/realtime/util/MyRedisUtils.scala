package realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis 工具类
 * 用于获取 Jedis 连接，操作 Redis
 *
 * @author Akaza
 */

object MyRedisUtils {

  var jedisPool: JedisPool = _

  def getJedisFromPool: Jedis = {

    if (jedisPool == null) {
      // 连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(5000)
      jedisPoolConfig.setTestOnBorrow(true)
      val host: String = MyPropsUtils(MyConfig.REDIS_HOST)
      val port: String = MyPropsUtils(MyConfig.REDIS_PORT)

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
}
