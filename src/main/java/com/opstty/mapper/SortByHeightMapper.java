package com.opstty.mapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByHeightMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            float height = tokens[6].isEmpty() ? 0  : Float.parseFloat(tokens[6]);
            context.write(new IntWritable(1), new FloatWritable(height));
        }
    }
}
