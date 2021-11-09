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
public class SortByHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private SortByHeightMapper sortByHeightMapper;

    @Before
    public void setup() {
        this.sortByHeightMapper = new SortByHeightMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora;tom;pa;fo;12.0;4";
        this.sortByHeightMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new IntWritable(1), new FloatWritable(12.0F));
    }
}