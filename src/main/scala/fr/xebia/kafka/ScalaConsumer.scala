package fr.xebia.kafka

import java.util
import java.util.Collections

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

object ScalaConsumer {

  def main(args: Array[String]) {
    import scala.collection.JavaConversions._
    //configuration d'un consumer
    val consumer: KafkaConsumer[String, String] = createKafkaConsumer()

    // TODO 2_7 : shutdownhook
    sys addShutdownHook {
      println("Starting exit...")
      consumer.wakeup()
    }

    // TODO 2_2
    consumer.subscribe(Collections.singletonList("winterfell"), new HandleRebalance())
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(1000)
        for (record <- records) {
          display(record)
        }
        manualCommit(consumer)
      }
    } finally {
      consumer.close()
    }
  }

  private def display(record: ConsumerRecord[String, String]) {
    println(s"topic = ${record.topic}, partition: ${record.partition}, offset: ${record.offset}: ${record.value}")
  }

  private def createKafkaConsumer(): KafkaConsumer[String, String] = {
    import scala.collection.JavaConversions._
    // TODO 2_1
    val props = Map(
      "bootstrap.servers" -> "localhost:9092,localhost:9093",
      "group.id" -> "whatever",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      // TODO 2_3
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "5000",
      "session.timeout.ms" -> "30000",
      "heartbeat.interval.ms" -> "3000",
      "fetch.min.bytes" -> "1",
      "fetch.max.wait.ms" -> "500",
      "max.partition.fetch.bytes" -> "1048576",
      "auto.offset.reset" -> "latest",
      "client.id" -> ""
    )
    new KafkaConsumer[String, String](props)
  }

  private def manualCommit(consumer: KafkaConsumer[String, String]) {
    // TODO 2_4
    try {
      ???
    } catch {
      case e: CommitFailedException =>
        e.printStackTrace()
    }
  }

  private def manualAsynchronousCommit(consumer: KafkaConsumer[String, String]) {
    // TODO 2_5
    import scala.collection.JavaConversions._
    ???
  }

}

// TODO 2_6
class HandleRebalance extends ConsumerRebalanceListener {
  import scala.collection.JavaConversions._
  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
    ???
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
    ???
  }
}


