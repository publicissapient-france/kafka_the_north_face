package fr.xebia.devoxx.kafka;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "devoxx-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStream<String, String> source = builder.stream("devoxx-wordcount");

        KTable<String, Long> wordCounts = source
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
                // `map` to ensure the words are available as message keys, too.
                .map((key, value) -> new KeyValue<>(value, value))
                // Count the occurrences of each word (message key).
                //
                // This will change the stream type from `KStream<String, String>` to
                // `KTable<String, Long>` (word -> count), hence we must provide serdes for `String`
                // and `Long`.
                //
                .groupByKey().count();

        wordCounts.to("devoxx-wordcount-out");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        System.out.println("START");
    }
}