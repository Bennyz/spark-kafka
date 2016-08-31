package io.benny.kafka.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.benny.kafka.spark.resolution.RedisConnection;
import io.benny.kafka.spark.resolution.RedisDNSResolver;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.IOException;
import java.util.*;

/**
 * Created by benny on 8/29/16.
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("KafkaStreaming");
        conf.set("es.index.auto.create", "true");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        Set<String> topics = new HashSet<>();
        topics.add("logstash");

        Map<String, String> brokers = new HashMap<>();
        brokers.put("metadata.broker.list", "localhost:9092");

        JavaPairDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                brokers,
                topics);

        JavaDStream<String> messages = kafkaStream.map(v1 -> extractField("message", v1._2));

        messages.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                RedisDNSResolver redisDNSResolver =
                        new RedisDNSResolver(RedisConnection.INSTANCE.getConnection());

                String currentRecord = rdd.first();
                String message = redisDNSResolver.resolve(currentRecord);
                System.out.println(message);

                Map<String, String> map = new HashMap<String, String>();
                map.put("kaka", message);
                List<Map<String, String>> arr = new ArrayList<>();
                arr.add(map);
                JavaRDD<Map<String, String>> parallelize = jssc.sparkContext().parallelize(arr);
                JavaEsSpark.saveToEs(parallelize, "kafka/ip");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }



    private static String extractField(String field, String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;

        try {
            jsonNode = objectMapper.readTree(json);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return StringUtils.deleteWhitespace(jsonNode.get(field).asText());
    }
}
