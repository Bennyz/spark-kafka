package io.benny.kafka.spark.resolution;

/**
 * Created by benny on 8/29/16.
 */
public interface DNSResolver {
    String resolve(String hostName);
}
