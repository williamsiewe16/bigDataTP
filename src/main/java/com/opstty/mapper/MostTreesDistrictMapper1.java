package com.opstty.mapper;
import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreesDistrictMapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            int district = Integer.parseInt(tokens[1]);
            context.write(new IntWritable(district), new IntWritable(1));
        }
    }
}
