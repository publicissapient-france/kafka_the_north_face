package fr.xebia.xebicon.kafka;


import kafka.admin.AdminUtils;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaLowLevelConsumer {

    public static void main(String[] args) {
        //créer un client Zookeeper
        ZkClient zkClient = connectToZookeeper();

        //récupération des métadonnées
        TopicMetadata topicMetadata = fetchTopicMetadata(zkClient);


        //recherche du broker leader pour chaque partition
        Map<Integer, Option<Broker>> partitionsLeaders = findPartitionLeader(topicMetadata);

        partitionsLeaders.entrySet().stream().forEach(integerOptionEntry -> {
            consumePartition(integerOptionEntry.getKey(), integerOptionEntry.getValue().get());
        });
    }

    private static ZkClient connectToZookeeper() {
        //TODO STEP_3_1
        return new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer$.MODULE$);
    }

    private static TopicMetadata
    fetchTopicMetadata(ZkClient zkClient) {
        // TODO STEP_3_2
        return AdminUtils.fetchTopicMetadataFromZk("xebicon", zkClient);
    }

    private static Map<Integer, Option<Broker>> findPartitionLeader(TopicMetadata topicMetadata) {
        // TODO STEP_3_3
        Seq<PartitionMetadata> partitionMetadataSeq = topicMetadata.partitionsMetadata();
        java.util.Map<Integer, List<PartitionMetadata>> map = JavaConversions
                .asJavaList(partitionMetadataSeq)
                .stream()
                .collect(Collectors.groupingBy(PartitionMetadata::partitionId));
        return map.entrySet().stream().collect(Collectors.toMap(java.util.Map.Entry::getKey, e -> e.getValue().get(0).leader()));
    }

    private static kafka.javaapi.consumer.SimpleConsumer consumer(Broker leader) {
        // TODO STEP_3_4
        return new kafka.javaapi.consumer.SimpleConsumer(leader.host(), leader.port(), 10000, 64000, "xebicon-printer");
    }

    private static long earliestOffset(kafka.javaapi.consumer.SimpleConsumer consumer, String topic, int partition) {
        // TODO STEP_3_5
        return earliestOrLatestOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime());
    }

    private static long latestOffset(kafka.javaapi.consumer.SimpleConsumer consumer, String topic, int partition) {
        // TODO STEP_3_5
        return earliestOrLatestOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime());
    }

    private static long earliestOrLatestOffset(kafka.javaapi.consumer.SimpleConsumer consumer, String topic, int partition,
                                     long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        kafka.javaapi.OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private static void consumePartition(Integer partition, Broker leader) {
        kafka.javaapi.consumer.SimpleConsumer consumer = consumer(leader);
        long earliestOffset = earliestOffset(consumer, "xebicon", partition);
        long latestOffset = latestOffset(consumer, "xebicon", partition);

        for (long offset = latestOffset; offset >= earliestOffset; offset--) {
            FetchResponse fetchReply = requestData(partition, consumer, offset);
            consume(partition, fetchReply);
        }
    }

    private static FetchResponse requestData(Integer partition, kafka.javaapi.consumer.SimpleConsumer consumer, Long offset) {
        // TODO STEP_3_6
        FetchRequest req = new FetchRequestBuilder()
                .clientId(consumer.clientId())
                .addFetch("xebicon", partition, offset, 1 * 1000)
                .build();
        return consumer.fetch(req);
    }

    private static void consume(Integer partition, FetchResponse fetchResponse) {
        //TODO STEP_3_7
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet("xebicon", partition)) {
            long currentOffset = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(String.format("partition: %d, offset: %d. %s", partition, currentOffset, new String(bytes)));
            break;
        }
    }
}
