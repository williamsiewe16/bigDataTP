package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class DistrictMapperTest {
    @Mock
    private Mapper.Context context;
    private DistrictMapper districtMapper;

    @Before
    public void setup() {
        this.districtMapper = new DistrictMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux";
        this.districtMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("1"), NullWritable.get());
    }
}