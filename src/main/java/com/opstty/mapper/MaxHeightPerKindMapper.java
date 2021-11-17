package com.opstty.mapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightPerKindMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");
            word.set(tokens[2]);
            float height = tokens[6].isEmpty() ? 0  : Float.parseFloat(tokens[6]);
            context.write(word, new FloatWritable(height));
        }
    }
}
