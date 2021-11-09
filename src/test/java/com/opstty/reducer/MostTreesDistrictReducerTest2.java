package com.opstty.reducer;

import com.opstty.IntTupleWritable;
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
public class MostTreesDistrictReducerTest2 {
    @Mock
    private Reducer.Context context;
    private MostTreesDistrictReducer2 mostTreesDistrictReducer2;

    @Before
    public void setup() {
        this.mostTreesDistrictReducer2 = new MostTreesDistrictReducer2();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntTupleWritable i = new IntTupleWritable(17,5);
        IntTupleWritable max = new IntTupleWritable(8,12);
        IntTupleWritable i1 = new IntTupleWritable(5,8);
        Iterable<IntTupleWritable> values = Arrays.asList(i,i1,max);
        this.mostTreesDistrictReducer2.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(new IntWritable(max.get()[0]), NullWritable.get());
    }
}
