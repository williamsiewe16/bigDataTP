package com.opstty.reducer;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreesDistrictReducer2 extends Reducer<IntWritable, IntTupleWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<IntTupleWritable> tuples, Context context) throws IOException, InterruptedException {
        int max=0; int arrondissement=0;
        for(IntTupleWritable tuple: tuples){
            int [] tupleValues = tuple.get();
            if(max <= tupleValues[1]){
                max = tupleValues[1];
                arrondissement = tupleValues[0];
            }
        }
        context.write(new IntWritable(arrondissement), NullWritable.get());
    }
}
