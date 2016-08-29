package io.benny.kafka.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by benny on 8/29/16.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Set<String> topics = new HashSet<String>() {{
           add("logstash");
        }};

        Map<String, String> brokers = new HashMap<String, String>() {{
            put("metadata.broker.list", "localhost:9092");
        }};

        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                brokers,
                topics);

        JavaDStream<String> map = messages.map(v1 -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(v1._2);
            return jsonNode.get("message").toString();
        });

        map.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
