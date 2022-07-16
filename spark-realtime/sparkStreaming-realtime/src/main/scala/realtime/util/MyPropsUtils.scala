package realtime.util

import com.thoughtworks.paranamer.ParameterNamesNotFoundException

import java.util.ResourceBundle

/**
 * 配置文件解析工具类
 *
 * @author Akaza
 */

object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String = {
    try {
      bundle.getString(propsKey)
    } catch {
      case e: Exception =>
        propsKey match {
          case "kafka.key-deserializer-class" => "org.apache.kafka.common.serialization.StringDeserializer"
          case "kafka.key-serializer-class" => "org.apache.kafka.common.serialization.StringSerializer"
          case "kafka.value-deserializer-class" => "org.apache.kafka.common.serialization.StringDeserializer"
          case "kafka.value-serializer-class" => "org.apache.kafka.common.serialization.StringSerializer"
          case "kafka.auto-commit" => "true"
          case "kafka.auto-offset-reset" => "latest"
          case "kafka.acks" => "all"
          case "kafka.idempotence" => "true"
          case _ => throw new ParameterNamesNotFoundException("未配置此参数")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("kafka.acks"))
  }
}
