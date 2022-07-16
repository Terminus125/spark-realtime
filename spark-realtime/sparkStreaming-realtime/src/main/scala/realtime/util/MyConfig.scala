package realtime.util

/**
 * 配置类
 * 统一管理配置项的名称，实现解耦
 *
 * @author Akaza
 */

object MyConfig {

  // Kafka 配置名称
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap-servers"
  val KAFKA_KEY_DESERIALIZER_CLASS: String = "kafka.key-deserializer-class"
  val KAFKA_KEY_SERIALIZER_CLASS: String = "kafka.key-serializer-class"
  val KAFKA_VALUE_DESERIALIZER_CLASS: String = "kafka.value-deserializer-class"
  val KAFKA_VALUE_SERIALIZER_CLASS: String = "kafka.value-serializer-class"
  val KAFKA_AUTO_COMMIT: String = "kafka.auto-commit"
  val KAFKA_AUTO_COMMIT_INTERVAL: String = "kafka.auto-commit-interval"
  val KAFKA_AUTO_OFFSET_RESET: String = "kafka.auto-offset-reset"

  val KAFKA_ACKS: String = "kafka.acks"
  val KAFKA_BATCH_SIZE: String = "kafka.batch-size"
  val KAFKA_LINGER_MS: String = "kafka.linger-ms"
  val KAFKA_RETRIES: String = "kafka.retries"
  val KAFKA_IDEMPOTENCE: String = "kafka.idempotence"

  // Redis 配置名称
  val REDIS_HOST: String = "redis.host"
  val REDIS_PORT: String = "redis.port"

  // ES 配置名称
  val ES_HOST: String = "es.host"
  val ES_PORT: String = "es.port"
}
