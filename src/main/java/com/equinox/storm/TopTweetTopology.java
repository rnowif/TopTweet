package com.equinox.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.ArrayUtils;

public class TopTweetTopology {

    public static final String TOP_TWEET_TOPOLOGY_ID = "top-tweet-topology";
    public static final String TWEET_SPOUT_ID = "tweet-spout";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = buildTopology();

        submitTopology(args, builder);
    }

    private static TopologyBuilder buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        TweetSpout tweetSpout = new TweetSpout(
                "XXX",
                "XXX",
                "XXX",
                "XXX"
        );

        builder.setSpout(TWEET_SPOUT_ID, tweetSpout, 1);

        builder.setBolt("filter-hashtags", new FilterTweetsWithHashTag(), 10).shuffleGrouping(TWEET_SPOUT_ID);

        return builder;
    }

    private static void submitTopology(String[] args, TopologyBuilder builder) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();

        conf.setDebug(true);

        if (ArrayUtils.isEmpty(args)) {

            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("top-tweet-topology", conf, builder.createTopology());

            Utils.sleep(30000000);

            cluster.killTopology(TOP_TWEET_TOPOLOGY_ID);
            cluster.shutdown();

        } else {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
