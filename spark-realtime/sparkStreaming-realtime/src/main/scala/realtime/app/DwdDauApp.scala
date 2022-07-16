package realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import realtime.bean.{DauInfo, PageLog}
import realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.{lang, util}
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表任务
 *
 * @author Akaza
 */

object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 状态还原
    revertState()

    // 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "DWD_PAGE_LOG_TOPIC"
    val groupId: String = "DWD_DAU_GROUP"

    // 从 Redis 中读取起始偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    // 从 Kafka 消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 提取 offset 结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      (rdd: RDD[ConsumerRecord[String, String]]) => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      (consumerRecord: ConsumerRecord[String, String]) => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )

    // 去重
    // 自我审查
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      (pageLog: PageLog) => pageLog.last_page_id == null
    )
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      (pageLogIter: Iterator[PageLog]) => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool

        // 存储第三方审查后的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()

        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        // 第三方审查
        for (pageLog <- pageLogIter) {
          // 提取每条数据中的 mid
          val mid: String = pageLog.mid
          // 获取访问日期
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"
          // 判断 Redis 中是否有此 mid
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          jedis.expire(redisDauKey, 24 * 3600)
          // 仅当 set 成功写入时才将 pageLog 加入 pageLogs 中
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        pageLogs.iterator
      }
    )

    // 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      (pageLogIter: Iterator[PageLog]) => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool

        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        for (pageLog <- pageLogIter) {
          // 将 pageLog 中的字段拷贝到 DauInfo 中
          val dauInfo: DauInfo = new DauInfo()
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          // 补充维度
          // 用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          // 提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          // 提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          // 换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears
          // 补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          // 地区信息维度
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          // 补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          // 日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          // 补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )

    // 写入 OLAP
    dauInfoDStream.foreachRDD(
      (rdd: RDD[DauInfo]) => {
        rdd.foreachPartition(
          (dauInfoIter: Iterator[DauInfo]) => {
            // 转换结构
            val docs: List[(String, DauInfo)] =
              dauInfoIter.map((dauInfo: DauInfo) => (dauInfo.mid, dauInfo)).toList
            // 写入 ES
            if (docs.nonEmpty) {
              // 从第一条数据获取日期
              val ts: Long = docs.head._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dataStr: String = sdf.format(new Date(ts))
              val indexName = s"gmall_dau_info_$dataStr"
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        // 提交 offset
        MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  // 状态还原
  def revertState(): Unit = {
    // 从 ES 中查询当天所有的 mid
    val date: LocalDate = LocalDate.now()
    val indexName: String = s"gmall_dau_info_$date"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)

    // 删除 redis 中当天记录的所有 mid
    val jedis: Jedis = MyRedisUtils.getJedisFromPool
    val redisDauKey: String = s"DAU:$date"
    jedis.del(redisDauKey)

    // 将查询到的 mid 覆盖到 Redis 中
    if (mids != null && mids.nonEmpty) {
      // 建立一个 Redis 管道批量处理
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey, mid)
      }
      pipeline.sync()
    }
    jedis.close()
  }
}
