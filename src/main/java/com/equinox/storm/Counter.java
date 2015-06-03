package com.equinox.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class Counter extends BaseRichBolt {

    public static final String HASHTAG_FIELD = "hashtag";
    public static final String COUNT_FIELD = "count";

    private OutputCollector collector;
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String hashtag = tuple.getString(0);

        if (!counts.containsKey(hashtag)) {
            counts.put(hashtag, 0);
        }

        Integer newCount = counts.get(hashtag) + 1;
        counts.put(hashtag, newCount);

        collector.emit(new Values(hashtag, newCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(HASHTAG_FIELD, COUNT_FIELD));
    }
}
