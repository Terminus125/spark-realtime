package realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import redis.clients.jedis.Jedis

import java.util

/**
 * 业务数据消费分流
 *
 * @author Akaza
 */

object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {

    // 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB"
    val groupId: String = "ODS_BASE_DB_GROUP"

    // 从 Redis 中读取上次消费的偏移量作为此次的起始偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    // 从 Kafka 中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 按指定 offset 进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      // 按默认 offset 进行消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 从当前消费到的数据中提取 offset
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      (rdd: RDD[ConsumerRecord[String, String]]) => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      (consumerRecord: ConsumerRecord[String, String]) => {
        // 获取 ConsumerRecord 中的 value，即日志数据
        val log: String = consumerRecord.value()
        // 转换成 Json 对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        jsonObj
      }
    )

    // 分流数据
    jsonObjDStream.foreachRDD(
      (rdd: RDD[JSONObject]) => {
        // 获取表清单
        val jedis: Jedis = MyRedisUtils.getJedisFromPool
        val redisFactKeys: String = "FACT:TABLES"
        val redisDimKeys: String = "DIM:TABLES"
        val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
        println("factTables: " + factTables)
        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        println("dimTables: " + dimTables)
        // 做成广播变量，增加传输效率
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        jedis.close()

        rdd.foreachPartition(
          (jsonObjIter: Iterator[JSONObject]) => {
            val jedis: Jedis = MyRedisUtils.getJedisFromPool
            for (jsonObj <- jsonObjIter) {
              // 提取操作类型，后续按操作类型分流
              val operaType: String = jsonObj.getString("type")
              val operaValue: String = operaType match {
                case "bootstrap-insert" => "I" // 全量导入的历史数据
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              // 判断操作类型
              if (operaValue != null) {
                // 提取表名
                val tableName: String = jsonObj.getString("table")

                // 事实数据，写入 Kafka
                if (factTablesBC.value.contains(tableName)) {
                  // 提取数据
                  val data: String = jsonObj.getString("data")
                  // 发送到 Kafka
                  val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_$operaValue"
                  MyKafkaUtils.send(dwdTopicName, data)
                }

                // 模拟数据延迟
//                if (tableName.equals("order_detail")) {
//                  Thread.sleep(200)
//                }

                // 维度数据，写入 Redis
                if (dimTablesBC.value.contains(tableName)) {
                  // 提取 ID
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  // 发送到 Redis
                  val redisKey : String = s"DIM:${tableName.toUpperCase}:$id"

                  jedis.set(redisKey, dataObj.toJSONString)
                }
              }
            }
            jedis.close()
            // 刷写 Kafka 缓冲区
            MyKafkaUtils.flush()
          }
        )
        // 提交 offset
        MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
