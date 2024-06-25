package omkar

import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ser.std.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

class KafkaService(bootstrapServers: String, topic: String) {
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[String, String](props)

  def send(metric: Metric): Unit = {
    val json = MetricGenerator.toJson(metric)
    val record = new ProducerRecord[String, String](topic, json)
    producer.send(record)
  }
}
