package com.opstty.mapper;
import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreesDistrictMapper2 extends Mapper<Object, Text, IntWritable, IntTupleWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String [] values = value.toString().split("\t");
        System.out.println(values[1]);
        int district = Integer.parseInt(values[0]);
        int count = Integer.parseInt(values[1]);
        context.write(new IntWritable(1), new IntTupleWritable(district,count));
    }
}
