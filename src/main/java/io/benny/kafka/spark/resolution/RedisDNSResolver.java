package io.benny.kafka.spark.resolution;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by benny on 8/29/16.
 */
public class RedisDNSResolver implements DNSResolver, Serializable {
    private static final Logger logger = LogManager.getLogger(RedisDNSResolver.class);
    private Jedis jedis;

    public RedisDNSResolver(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public String resolve(String hostName) {
        String ip = jedis.get(hostName);

        try {
            if (ip == null) {
                ip = resolveHost(hostName);
                jedis.set(ip, hostName);
            }
        } finally {
            jedis.close();
        }

        return ip;
    }

    private String resolveHost(String host) {
        String address = null;

        try {
            address = InetAddress.getByName(host).getHostAddress();
        } catch (UnknownHostException e) {
            logger.error(String.format("Failed to resolve %s", address), e);

            // TODO: handle unresolved hosts
        }

        return address;
    }
}
