package com.opstty.reducer;

import com.opstty.IntTupleWritable;
import com.opstty.mapper.MostTreesDistrictMapper2;
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
public class MostTreesDistrictReducerTest1 {
    @Mock
    private Reducer.Context context;
    private MostTreesDistrictReducer1 mostTreesDistrictReducer1;

    @Before
    public void setup() {
        this.mostTreesDistrictReducer1 = new MostTreesDistrictReducer1();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.mostTreesDistrictReducer1.reduce(value, values, this.context);
        verify(this.context).write(value, new IntWritable(3));
    }
}
