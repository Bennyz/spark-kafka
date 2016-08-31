package io.benny.kafka.spark.resolution;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by benny on 8/31/16.
 */
public enum RedisConnection {
    INSTANCE;

    private JedisPool pool;

    public Jedis getConnection() {

        if (pool == null) {
            pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);
        }

        return pool.getResource();
    }
}
