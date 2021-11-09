package com.opstty.reducer;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SortByHeightReducer extends Reducer<IntWritable, FloatWritable, FloatWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<FloatWritable> list = Lists.newArrayList(values);
        Collections.sort(list);
        for(FloatWritable i : list){
            context.write(i, NullWritable.get());
        }
    }
}
