package fr.xebia.kafka

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KeyValueMapper}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

object ScalaKStream {

  def main(args: Array[String]) {
    System.out.println("GO")

    // TODO 5_1 : Create a KStreamBuilder
    val kStreamBuilder: KStreamBuilder = new KStreamBuilder

    // TODO 5_2 : Create a source KStream : the stream of messages from topic winterfell connect
    val source: KStream[String, String] = kStreamBuilder.stream("winterfell-connect")

    // TODO 5_3 : Create a new sink KStream from the source KStream with the map method : send new KeyValue message, prepend "STREAM : " to the value of the message
    val sink: KStream[String, String] = source.map {
      new KeyValueMapper[String, String, KeyValue[String, String]] {
        override def apply(key: String, value: String): KeyValue[String, String] = new KeyValue[String, String](key, "STREAM : " + value)
      }
    }

    // TODO 5_4 : Send the message from the sink KStream to the Kafka topic winterfell-streams-out
    sink.to("winterfell-streams-out")

    // TODO 5_5 : Create a KafkaStreams object from this KStreamBuilder and a Properties object
    ???

    // TODO 5_6 : start the KafkaStreams
    ???
  }

}
