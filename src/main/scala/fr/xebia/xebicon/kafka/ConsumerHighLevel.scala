package fr.xebia.xebicon.kafka

import java.util.Properties

import kafka.consumer.{KafkaStream, ConsumerConnector}
import kafka.message.MessageAndMetadata

import scala.concurrent.Future

//TODO STEP_2
object ConsumerHighLevel {

  def main(args: Array[String]): Unit = {

    //configuration d'un connector
    val consumer: ConsumerConnector = createConsumer()

    //demande de la création d'un stream pour notre topic
    val streams: List[KafkaStream[Array[Byte], Array[Byte]]] = createStream(consumer)

    //récupération + itération sur les messages
    streams.foreach(partition => consumeStreamFrom(partition))
  }

  def createConsumer(): ConsumerConnector = {
    import kafka.consumer.{ConsumerConfig, Consumer}

    def groupId = "xebicon_printer"
    def zookeeper = "127.0.0.1:2181"

    val props = new Properties()

    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    Consumer.create(new ConsumerConfig(props))
  }

  def createStream(consumer: ConsumerConnector): List[KafkaStream[Array[Byte], Array[Byte]]] = {

    def topic = "xebicon"
    def numberOfPartitions = 4

    consumer.createMessageStreams(Map(topic -> numberOfPartitions))(topic)
  }

  def consumeStreamFrom(partitionStream: KafkaStream[Array[Byte], Array[Byte]]): Unit = {

    def display(message: MessageAndMetadata[Array[Byte], Array[Byte]]): Unit = {
      def payload: String = new String(message.message(), "UTF-8")
      def partition: Int = message.partition
      def offset: Long = message.offset

      println(s"partition: $partition, offset: $offset: $payload")
    }

    //TODO STEP_2_3
  }
}
