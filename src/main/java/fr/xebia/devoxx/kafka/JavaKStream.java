package fr.xebia.devoxx.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class JavaKStream {

    public static void main(String[] args) throws Exception {
        System.out.println("GO");

        // TODO 5_1 : Create a KStreamBuilder
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        // TODO 5_2 : Create a source KStream : the stream of messages from topic devoxx connect
        KStream<String, String> source = kStreamBuilder.stream("devoxx-connect");

        // TODO 5_3 : Create a new sink KStream from the source KStream with the map method : send new KeyValue message, prepend "STREAM : " to the value of the message
        KStream<String, String> sink = source.map((key, value) -> new KeyValue<>(key, "STREAM : " + value));

        // TODO 5_4 : Send the message from the sink KStream to the Kafka topic devoxx-streams-out
        sink.to("devoxx-streams-out");

        // TODO 5_5 : Create a KafkaStreams object from this KStreamBuilder and a Properties object
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, props);

        // TODO 5_6 : start the KafkaStreams
        kafkaStreams.start();
    }
}
