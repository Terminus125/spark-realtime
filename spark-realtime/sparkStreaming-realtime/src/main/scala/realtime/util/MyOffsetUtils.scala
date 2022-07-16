package realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

/**
 * Offset 管理工具类
 * 用于向 Redis 中储存和读取 offset
 *
 * @author Akaza
 */

object MyOffsetUtils {
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      // 提取 offset 数据
      val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val endOffset: Long = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      println("提交 offset: " + offsets)
      // 往 Redis 中存
      val jedis: Jedis = MyRedisUtils.getJedisFromPool
      val redisKey: String = s"offset:$topic:$groupId"
      jedis.hset(redisKey, offsets)
      jedis.close()
    }
  }

  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] ={
    val jedis: Jedis = MyRedisUtils.getJedisFromPool
    val redisKey: String = s"offset:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到 offset: " + offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // 将 java 的 map 转换成 scala 的 map 进行迭代
    for ((partition, offset) <- offsets.asScala) {
      val tp: TopicPartition = new TopicPartition(topic, partition.toInt)
      results.put(tp, offset.toLong)
    }
    jedis.close()
    results.toMap
  }
}
