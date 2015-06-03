package com.equinox.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {

    public static final String TWEET_FIELD = "tweet";

    private static final int BUFFER_SIZE = 1000;

    private final String consumerKey;
    private final String consumerSecretKey;
    private final String accessToken;
    private final String accessTokenSecret;

    private LinkedBlockingQueue<String> queue;
    private SpoutOutputCollector collector;
    private TwitterStream twitterStream;

    public TweetSpout(String consumerKey, String consumerSecretKey, String accessToken, String accessTokenSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecretKey = consumerSecretKey;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TWEET_FIELD));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        collector = spoutOutputCollector;

        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(consumerKey)
                        .setOAuthConsumerSecret(consumerSecretKey)
                        .setOAuthAccessToken(accessToken)
                        .setOAuthAccessTokenSecret(accessTokenSecret)
                        .build()
        ).getInstance();

        queue = new LinkedBlockingQueue<>(BUFFER_SIZE);

        twitterStream.addListener(new TweeterListener());
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {

        if (queue.isEmpty()) {
            Utils.sleep(50);
            return;
        }

        collector.emit(new Values(queue.poll()));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    private class TweeterListener implements StatusListener {
        @Override
        public void onStatus(Status status) {
            queue.offer(status.getText());
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

        }

        @Override
        public void onTrackLimitationNotice(int i) {

        }

        @Override
        public void onScrubGeo(long l, long l1) {

        }

        @Override
        public void onStallWarning(StallWarning stallWarning) {

        }

        @Override
        public void onException(Exception e) {

        }
    }
}
