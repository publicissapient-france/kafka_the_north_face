package fr.xebia.xebicon.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JavaConsumerHighLevel {

    public static void main(String[] args) {
        //configuration d'un connector
        ConsumerConnector consumer = createConsumer();

        //demande de la création d'un stream pour notre topic
        List<KafkaStream<byte[], byte[]>> streams = createStream(consumer);

        //récupération + itération sur les messages
        streams.parallelStream().forEach((partitionStream) -> {
            JavaConsumerHighLevel.consumeStreamFrom(partitionStream);
        });
    }

    private static ConsumerConnector createConsumer() {
        String groupId = "xebicon_printer";
        String zookeeper = "127.0.0.1:2181";

        Properties props = new Properties();

        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        //TODO STEP_2_1
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }


    private static List<KafkaStream<byte[], byte[]>> createStream(ConsumerConnector consumer) {
        String topic = "xebicon";
        Integer numberOfPartitions = 4;

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, numberOfPartitions);

        //TODO STEP_2_2
        return consumer.createMessageStreams(topicCountMap).get(topic);
    }

    private static void display(MessageAndMetadata<byte[], byte[]> message) {
        String payload = new String(message.message());
        int partition = message.partition();
        long offset = message.offset();

        System.out.println(String.format("partition: %d, offset: %d: %s", partition, offset, payload));
    }

    private static void consumeStreamFrom(KafkaStream<byte[], byte[]> partitionStream) {
        //TODO STEP_2_3
        ConsumerIterator<byte[], byte[]> iterator = partitionStream.iterator();
        while (iterator.hasNext()) {
            display(iterator.next());
        }
    }

}
