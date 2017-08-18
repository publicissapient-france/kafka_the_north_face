package fr.xebia.devoxx.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaSparkStreaming {

    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext context = createStreamContext();
        JavaInputDStream<ConsumerRecord<String, String>> stream = createStream(context);

        // TODO Step 6_3
        stream.map(v1 -> extractValueFromRecord(v1.value()))
            .foreachRDD(JavaSparkStreaming::displayAvg);

        context.start();
        context.awaitTermination();
    }

    public static JavaStreamingContext createStreamContext() {
        // TODO Step 6_1
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("streaming-client");

        return new JavaStreamingContext(conf, Durations.seconds(5));
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> createStream(JavaStreamingContext context) {
        // TODO Step 6_2
        Collection<String> topics = Collections.singletonList("devoxx");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,localhost:9093");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming-client");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        return KafkaUtils.createDirectStream(
                context,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }

    public static double extractValueFromRecord(String line) {
        final Pattern pattern = Pattern.compile(".{24}: avg_load: (.*)");
        final Matcher matcher = pattern.matcher(line);
        if(matcher.find()) {
            return Double.valueOf(matcher.group(1).replace(",", "."));
        }
        return 0;
    }

    public static void displayAvg(JavaRDD<Double> rdd) {
        Double sum = rdd.fold(0d, (v1, v2) -> v1 + v2);
        long count = rdd.count();

        Double avg = count == 0 ? 0 : sum/count;

        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");

        System.out.println(timeFormatter.format(new Date()) + " : last 5 seconds average load => " + avg);
    }
}
