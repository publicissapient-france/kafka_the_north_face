package fr.xebia.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object ScalaBatchConsumer {

  def main(args: Array[String]) {
    val consumer = createKafkaConsumer()

    assignPartitions(consumer)

    seek(consumer)

    process(consumer)
  }

  private def createKafkaConsumer(): KafkaConsumer[String, String] = {
    // TODO 3_1
    import scala.collection.JavaConversions._
    val props = Map(
      "bootstrap.servers" -> "localhost:9092,localhost:9093",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "enable.auto.commit" -> "false",
      "group.id" -> "batch"
    )
    new KafkaConsumer[String, String](props)
  }

  private def assignPartitions(consumer: KafkaConsumer[String, String]) {
    // TODO 3_2
    import scala.collection.JavaConversions._
    val partitionInfos = consumer.partitionsFor("winterfell")
    val topicPartitions = partitionInfos.map(partitionInfo => new TopicPartition("winterfell", partitionInfo.partition()))
    System.out.println(topicPartitions)
    consumer.assign(topicPartitions)
  }

  private def seek(consumer: KafkaConsumer[String, String]) {
    // TODO 3_3
    ???
  }

  private def process(consumer: KafkaConsumer[String, String]) {
    // TODO 3_4
    import scala.collection.JavaConversions._
    ???
  }

}
