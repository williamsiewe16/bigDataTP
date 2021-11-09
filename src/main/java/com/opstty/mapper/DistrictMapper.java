package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[1]);
            context.write(word, NullWritable.get());
        }
    }
}
