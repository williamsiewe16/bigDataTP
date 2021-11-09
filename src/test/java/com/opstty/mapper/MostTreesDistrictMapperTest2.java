package com.opstty.mapper;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MostTreesDistrictMapperTest2 {
    @Mock
    private Mapper.Context context;
    private MostTreesDistrictMapper2 mostTreesDistrictMapper2;

    @Before
    public void setup() {
        this.mostTreesDistrictMapper2 = new MostTreesDistrictMapper2();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "1\t4";
        this.mostTreesDistrictMapper2.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new IntWritable(1), new IntTupleWritable(1,4));
    }
}