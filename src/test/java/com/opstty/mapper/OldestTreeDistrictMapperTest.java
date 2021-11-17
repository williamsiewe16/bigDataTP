package com.opstty.mapper;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
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
public class OldestTreeDistrictMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeDistrictMapper oldestTreeDistrictMapper;

    @Before
    public void setup() {
        this.oldestTreeDistrictMapper = new OldestTreeDistrictMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        MapWritable map = new MapWritable();
        map.put(new Text("arrondissement"),new IntWritable(6));
        map.put(new Text("annee"),new IntWritable(1605));
        String value = "foo;6;17;sephora;tom;1605;12.0;4";
        this.oldestTreeDistrictMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new IntWritable(1), map);
    }
}