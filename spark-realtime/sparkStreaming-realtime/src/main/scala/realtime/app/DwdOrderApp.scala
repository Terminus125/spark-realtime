package realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 *
 * @author Akaza
 */

object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 读取 offset
    // order_info
    val orderInfoTopicName: String = "DWD_ORDER_INFO_I"
    val orderInfoGroup: String = "DWD_ORDER_INFO_GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderInfoTopicName, orderInfoGroup)
    // order_detail
    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup: String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    // 从 Kafka 中消费数据
    // order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    } else {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }
    // order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsets != null && orderDetailOffsets.nonEmpty) {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    } else {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    // 提取偏移量结束点
    // order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      (rdd: RDD[ConsumerRecord[String, String]]) => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      (rdd: RDD[ConsumerRecord[String, String]]) => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 转换结构
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      (consumerRecord: ConsumerRecord[String, String]) => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      (consumerRecord: ConsumerRecord[String, String]) => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )

    // 维度关联
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      (orderInfoIter: Iterator[OrderInfo]) => {
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPool
        for (orderInfo <- orderInfos) {

          // 关联用户维度
          val uid: String = orderInfo.user_id
          val redisUserKey = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
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
          orderInfo.user_gender = gender
          orderInfo.user_age = age.toString

          // 关联地区维度
          val provinceID: String = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          // 补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_iso_code = provinceIsoCode
          orderInfo.province_3166_2 = province3166
          orderInfo.province_area_code = provinceAreaCode

          // 处理日期字段
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)
          // 补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr
        }
        jedis.close()
        orderInfos.iterator
      }
    )

    // 双流 join
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] =
      orderInfoDimDStream.map((orderInfo: OrderInfo) => (orderInfo.id, orderInfo))
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] =
      orderDetailDStream.map((orderDetail: OrderDetail) => (orderDetail.id, orderDetail))
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)
    // 进行处理延迟数据和后续封装
    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      (orderJoinIter: Iterator[(Long, (Option[OrderInfo], Option[OrderDetail]))]) => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          if (orderInfoOp.isDefined) {
            val orderInfo: OrderInfo = orderInfoOp.get
            // orderInfo 有，orderDetail 有，则直接封装
            if (orderDetailOp.isDefined) {
              // 取出 orderDetail
              val orderDetail: OrderDetail = orderDetailOp.get
              // 组装 orderWide (join)
              val orderWide = new OrderWide(orderInfo, orderDetail)
              // 放入结果集
              orderWides.append(orderWide)
            }

            // orderInfo 写缓存
            val redisOrderInfoKey: String = s"ORDER_JOIN:ORDER_INFO:${orderInfo.id}"
            jedis.setex(redisOrderInfoKey, 24 * 3600,
              JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            // orderInfo 有，orderDetail 无，则读缓存试图 join
            // orderInfo 读缓存
            val redisOrderDetailKey: String = s"ORDER_JOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            // 如果读到相同 id 的 orderDetail 缓存，则 join 并放入结果集
            if (orderDetails != null && orderDetails.size() > 0) {
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                val orderWide = new OrderWide(orderInfo, orderDetail)
                orderWides.append(orderWide)
              }
            }
          } else {
            // orderInfo 无，orderDetail 有，则读缓存试图 join
            val orderDetail: OrderDetail = orderDetailOp.get
            // orderDetail 读缓存
            val redisOrderInfoKey: String = s"ORDER_JOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if ( orderInfoJson != null && orderInfoJson.nonEmpty) {
              // 读到了缓存，直接 join
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            } else {
              // 没读到缓存，将本条数据写入缓存
              val redisOrderDetailKey: String = s"ORDER_JOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )

    // 写入 OLAP
    orderWideDStream.foreachRDD(
      (rdd: RDD[OrderWide]) => {
        rdd.foreachPartition(
          (orderWideIter: Iterator[OrderWide]) => {
            // 转换结构
            val orderWides: List[(String, OrderWide)] =
              orderWideIter.map((orderWide: OrderWide) => (orderWide.detail_id.toString, orderWide)).toList
            // 写入 ES
            if (orderWides.nonEmpty) {
              val time: String = orderWides.head._2.create_time
              val date: String = time.split(" ")(0)
              val indexName = s"gmall_order_wide_$date"
              MyEsUtils.bulkSave(indexName, orderWides)
            }
          }
        )
        // 提交 offset
        MyOffsetUtils.saveOffset(orderInfoTopicName, orderInfoGroup, orderInfoOffsetRanges)
        MyOffsetUtils.saveOffset(orderDetailTopicName, orderDetailGroup, orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
