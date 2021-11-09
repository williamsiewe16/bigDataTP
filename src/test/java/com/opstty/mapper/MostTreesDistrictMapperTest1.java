package com.opstty.mapper;

import com.opstty.IntTupleWritable;
import com.opstty.job.MostTreesDistrict;
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
public class MostTreesDistrictMapperTest1 {
    @Mock
    private Mapper.Context context;
    private MostTreesDistrictMapper1 mostTreesDistrict;

    @Before
    public void setup() {
        this.mostTreesDistrict = new MostTreesDistrictMapper1();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;17;sephora;tom;1605;12.0;4";
        this.mostTreesDistrict.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new IntWritable(1), new IntWritable(1));
    }
}