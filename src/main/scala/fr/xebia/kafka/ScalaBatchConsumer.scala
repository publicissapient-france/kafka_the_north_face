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
    ???
  }

  private def assignPartitions(consumer: KafkaConsumer[String, String]) {
    // TODO 3_2
    import scala.collection.JavaConversions._
    ???
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
