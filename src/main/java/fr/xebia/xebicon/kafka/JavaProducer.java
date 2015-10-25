package fr.xebia.xebicon.kafka;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class JavaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //créer un client Zookeeper
        ZkClient zkClient = connectToZookeeper();

        //récupérer les addresses des noeuds Kafka
        String connectionString = createBrokerListConnection(zkClient);

        //instanciation du KafkaProducer
        KafkaProducer producer = createKafkaProducer(connectionString);

        //écriture
        produceData(producer);
    }

    private static ZkClient connectToZookeeper() {
        // TODO STEP_1_1
        return new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer$.MODULE$);
    }

    private static String createBrokerListConnection(ZkClient zkClient) {
        // TODO STEP_1_2
        Seq<Broker> allBrokersInCluster1 = ZkUtils.getAllBrokersInCluster(zkClient);
        Collection<Broker> allBrokersInCluster = JavaConversions.asJavaCollection(allBrokersInCluster1);

        String connectionString = allBrokersInCluster.stream().map(broker -> broker.connectionString())
                .collect(Collectors.joining(","))
                .toString();

        return connectionString;
    }

    private static KafkaProducer<String, String> createKafkaProducer(String connectionString) {
        Map<String, Object> props = new HashMap();
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("max.request.size", "10000");
        props.put("bootstrap.servers", connectionString);
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("retry.backoff.ms", "500");

        // TODO STEP_1_3
        return new KafkaProducer<String, String>(props);
    }

    private static void produceData(KafkaProducer producer) throws ExecutionException, InterruptedException {

        while (true) {
            Date now = Calendar.getInstance().getTime();
            double averageSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

            //TODO STEP_1_4
            ProducerRecord<String, String> data = new ProducerRecord<String, String>("xebicon", String.format("%s: avg_load: %f", now.toString(), averageSystemLoad));
            producer.send(data).get();
            Thread.sleep(1000);
        }

    }

}
