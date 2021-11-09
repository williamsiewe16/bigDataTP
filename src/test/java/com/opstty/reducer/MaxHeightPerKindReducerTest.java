package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
public class MaxHeightPerKindReducerTest {
    @Mock
    private Reducer.Context context;
    private MaxHeightPerKindReducer maxHeightPerKindReducer;

    @Before
    public void setup() {
        this.maxHeightPerKindReducer = new MaxHeightPerKindReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "sephora";
        FloatWritable max = new FloatWritable(15);
        FloatWritable value1 = new FloatWritable(12);
        FloatWritable value2 = new FloatWritable(10);
        Iterable<FloatWritable> values = Arrays.asList(value1, max, value2);
        this.maxHeightPerKindReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), max);
    }
}
