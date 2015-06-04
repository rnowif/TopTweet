package com.equinox.storm;

import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class RedisReporterTest {

    private Tuple tuple;
    private RedisReporter reporter;
    private RedisConnection connection;

    @Before
    public void setUp() {
        tuple = mock(Tuple.class);
        connection = mock(RedisConnection.class);

        RedisClient client = mock(RedisClient.class);
        when(client.connect()).thenReturn(connection);

        RedisClientBuilder builder = mock(RedisClientBuilder.class);
        when(builder.build()).thenReturn(client);

        reporter = new RedisReporter(builder, "TopTweetTopology");
        reporter.prepare(null, null, null);
    }

    @Test
    public void should_push_to_redis() {

        when(tuple.getStringByField("hashtag")).thenReturn("#tag");
        when(tuple.getIntegerByField("count")).thenReturn(4);

        reporter.execute(tuple);

        verify(connection).publish("TopTweetTopology", "#tag|4");
    }


}