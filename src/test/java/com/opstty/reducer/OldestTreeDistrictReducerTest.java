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
public class OldestTreeDistrictReducerTest {
    @Mock
    private Reducer.Context context;
    private OldestTreeDistrictReducer oldestTreeDistrictReducer;

    @Before
    public void setup() {
        this.oldestTreeDistrictReducer = new OldestTreeDistrictReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntTupleWritable i = new IntTupleWritable(17,1715);
        IntTupleWritable i1 = new IntTupleWritable(8,1718);
        IntTupleWritable min = new IntTupleWritable(5,1710);
        Iterable<IntTupleWritable> values = Arrays.asList(i,i1,min);
        this.oldestTreeDistrictReducer.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(new IntWritable(min.get()[0]), NullWritable.get());
    }
}
