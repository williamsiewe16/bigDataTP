package com.opstty.reducer;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeDistrictReducer extends Reducer<IntWritable, IntTupleWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<IntTupleWritable> tuples, Context context) throws IOException, InterruptedException {
        int minYear=9999; int arrondissement=0;
        for(IntTupleWritable tuple: tuples){
            int [] tupleValues = tuple.get();
            if(minYear >= tupleValues[1]){
                minYear = tupleValues[1];
                arrondissement = tupleValues[0];
            }
        }
        context.write(new IntWritable(arrondissement), NullWritable.get());
    }
}
