package com.opstty.reducer;

import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeDistrictReducer extends Reducer<IntWritable, MapWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<MapWritable> tuples, Context context) throws IOException, InterruptedException {
        int minYear=9999; int arrondissement_=0;
        for(MapWritable tuple: tuples){
            int arrondissement = ((IntWritable)tuple.get(new Text("arrondissement"))).get();
            int annee = ((IntWritable)tuple.get(new Text("annee"))).get();
            System.out.println("no");
            if(minYear >= annee){
                System.out.println("good");
                minYear = annee;
                arrondissement_ = arrondissement;
            }
        }
        context.write(new IntWritable(arrondissement_), NullWritable.get());
    }
}
