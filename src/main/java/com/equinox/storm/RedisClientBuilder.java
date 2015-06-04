package com.equinox.storm;

import com.lambdaworks.redis.RedisClient;

import java.io.Serializable;

public class RedisClientBuilder implements Serializable {

    private final String hostname;
    private final int port;

    public RedisClientBuilder(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public RedisClient build() {
        return new RedisClient(hostname, port);
    }
}
