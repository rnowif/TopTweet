package com.equinox.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterTweetsWithHashTag extends BaseRichBolt {

    public static final String HASHTAG_FIELD = "hashtag";

    private static final Pattern PATTERN = Pattern.compile("#[^\\s]*");

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        Matcher matcher = PATTERN.matcher(tuple.getString(0));

        while (matcher.find()) {
            collector.emit(new Values(matcher.group()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(HASHTAG_FIELD));
    }
}
