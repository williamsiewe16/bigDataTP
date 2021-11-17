package com.opstty.reducer;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
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
        MapWritable map1 = new MapWritable();
        map1.put(new Text("arrondissement"),new IntWritable(6));
        map1.put(new Text("annee"),new IntWritable(1605));
        MapWritable map2 = new MapWritable();
        map2.put(new Text("arrondissement"),new IntWritable(5));
        map2.put(new Text("annee"),new IntWritable(1492));
        MapWritable map3 = new MapWritable();
        map3.put(new Text("arrondissement"),new IntWritable(4));
        map3.put(new Text("annee"),new IntWritable(1952));
        Iterable<MapWritable> values = Arrays.asList(map1,map3,map2);
        this.oldestTreeDistrictReducer.reduce(new IntWritable(1), values, this.context);
        verify(this.context).write(new IntWritable(5), NullWritable.get());
    }
}
