package fr.xebia.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaBatchConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        assignPartitions(consumer);

        seek(consumer);

        process(consumer);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        // TODO 3_1
        return null;
    }

    private static void assignPartitions(KafkaConsumer<String, String> consumer) {
        // TODO 3_2

    }

    private static void seek(KafkaConsumer<String, String> consumer) {
        // TODO 3_3

    }



    private static void process(KafkaConsumer<String, String> consumer) {
        // TODO 3_4

    }


}
