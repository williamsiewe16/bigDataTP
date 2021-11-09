package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortByHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SortByHeightReducer sortByHeightReducer;

    @Before
    public void setup() {
        this.sortByHeightReducer = new SortByHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        FloatWritable i = new FloatWritable(11.0F);
        FloatWritable i1 = new FloatWritable(9.0F);
        FloatWritable i2 = new FloatWritable(8.0F);
        Iterable<FloatWritable> values = Arrays.asList(i,i1,i2);
        this.sortByHeightReducer.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(i2, NullWritable.get());
        verify(this.context).write(i1, NullWritable.get());
        verify(this.context).write(i, NullWritable.get());
    }
}
