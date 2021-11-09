package com.opstty.mapper;

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
public class ExistingSpeciesMapperTest {
    @Mock
    private Mapper.Context context;
    private ExistingSpeciesMapper existingSpeciesMapper;

    @Before
    public void setup() {
        this.existingSpeciesMapper = new ExistingSpeciesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;1;tux;sephora";
        this.existingSpeciesMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("sephora"), NullWritable.get());
    }
}