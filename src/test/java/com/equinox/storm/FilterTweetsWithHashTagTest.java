package com.equinox.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FilterTweetsWithHashTagTest {

    private OutputCollector collector;
    private Tuple tuple;
    private FilterTweetsWithHashTag filter;

    @Before
    public void setUp() {
        collector = mock(OutputCollector.class);
        tuple = mock(Tuple.class);

        filter = new FilterTweetsWithHashTag();
        filter.prepare(null, null, collector);
    }

    @Test
    public void should_not_emit_when_no_hashtag() {
        when(tuple.getString(0)).thenReturn("Super tweet sans hashtag");

        filter.execute(tuple);

        verifyZeroInteractions(collector);
    }

    @Test
    public void should_emit_once_when_one_hashtag() {
        when(tuple.getString(0)).thenReturn("Super tweet avec #hashtag");

        filter.execute(tuple);

        verify(collector).emit(new Values("#hashtag"));
    }

    @Test
    public void should_emit_twice_when_two_hashtags() {
        when(tuple.getString(0)).thenReturn("Super #tweet avec #hashtag");

        filter.execute(tuple);

        verify(collector).emit(new Values("#tweet"));
        verify(collector).emit(new Values("#hashtag"));
    }

}