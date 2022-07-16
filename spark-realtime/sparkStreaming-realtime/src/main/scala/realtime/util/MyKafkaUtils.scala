package realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * Kafka 工具类
 * 用于生成和消费数据
 *
 * @author Akaza
 */

object MyKafkaUtils {

  // 消费者配置
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // Kafka 集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    // KV 反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_KEY_DESERIALIZER_CLASS),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_VALUE_DESERIALIZER_CLASS),

    // Offset 提交模式
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> MyPropsUtils(MyConfig.KAFKA_AUTO_COMMIT),

    // Offset 重置模式
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> MyPropsUtils(MyConfig.KAFKA_AUTO_OFFSET_RESET)
  )

  // 基于 SparkStreaming 消费，获取到 KafkaDStream，使用默认的 offset
  def getKafkaDStream(ssc: StreamingContext, topic:String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    kafkaDStream
  }

  // 基于 SparkStreaming 消费，获取到 KafkaDStream，使用指定的 offset
  def getKafkaDStream(ssc: StreamingContext, topic:String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets)
    )
    kafkaDStream
  }

  // 获取生产者对象
  val producer: KafkaProducer[String, String] = createProducer()

  // 创建生产者对象
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]

    // Kafka 集群位置
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))

    // KV 反序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfig.KAFKA_KEY_SERIALIZER_CLASS))
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfig.KAFKA_VALUE_SERIALIZER_CLASS))

    // acks 级别
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, MyPropsUtils(MyConfig.KAFKA_ACKS))

    // 幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, MyPropsUtils(MyConfig.KAFKA_IDEMPOTENCE))

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  // 生产方法
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }
  // 按照 key 分区
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  // 将缓冲区的数据刷写到磁盘
  def flush(): Unit = {
    producer.flush()
  }

  //关闭生产者对象
  def close(): Unit = {
    if (producer != null) producer.close()
  }

}
