package com.equinox.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisConnection;

import java.util.Map;

public class RedisReporter extends BaseRichBolt {

    private final RedisClientBuilder clientBuilder;
    private final String topologyId;

    private RedisConnection connection;

    public RedisReporter(RedisClientBuilder clientBuilder, String topologyId) {
        this.clientBuilder = clientBuilder;
        this.topologyId = topologyId;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        connection = clientBuilder.build().connect();
    }

    @Override
    public void execute(Tuple tuple) {
        String hashTag = tuple.getStringByField(Counter.HASHTAG_FIELD);
        Integer count = tuple.getIntegerByField(Counter.COUNT_FIELD);

        connection.publish(topologyId, hashTag + "|" + Long.toString(count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
