package com.opstty.mapper;
import com.opstty.IntTupleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeDistrictMapper extends Mapper<Object, Text, IntWritable, IntTupleWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");

            if(!tokens[1].isEmpty() & !tokens[5].isEmpty()){
                int arrondissement = Integer.parseInt(tokens[1]);
                int annee = Integer.parseInt(tokens[5]);

                context.write(new IntWritable(1), new IntTupleWritable(arrondissement,annee));
            }
        }
    }
}
