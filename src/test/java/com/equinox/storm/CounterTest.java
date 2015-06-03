package com.equinox.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CounterTest {

    private OutputCollector collector;
    private Tuple tuple;
    private Counter counter;

    @Before
    public void setUp() {
        collector = mock(OutputCollector.class);
        tuple = mock(Tuple.class);

        counter = new Counter();
        counter.prepare(null, null, collector);
    }

    @Test
    public void should_emit_one_if_first_time() {
        when(tuple.getString(0)).thenReturn("#firsthashtag");

        counter.execute(tuple);

        verify(collector).emit(new Values("#firsthashtag", 1));
    }

    @Test
    public void should_emit_two_if_second_time() {
        when(tuple.getString(0)).thenReturn("#hashtag");

        counter.execute(tuple);
        counter.execute(tuple);

        verify(collector).emit(new Values("#hashtag", 1));
        verify(collector).emit(new Values("#hashtag", 2));
    }
}