package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountTreesByKindsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[2]);
            context.write(word, new IntWritable(1));
        }
    }
}
