package com.opstty.mapper;

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
public class MaxHeightPerKindMapperTest {
    @Mock
    private Mapper.Context context;
    private MaxHeightPerKindMapper maxHeightPerKindMapper;
    private DistrictMapper districtMapper;

    @Before
    public void setup() {
        this.maxHeightPerKindMapper = new MaxHeightPerKindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora;tom;pa;12.0;4";
        this.maxHeightPerKindMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("tux"), new FloatWritable(12.0F));
    }
}