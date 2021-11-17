package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeDistrictMapper extends Mapper<Object, Text, IntWritable, MapWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.startsWith("GEOPOINT")){
            String [] tokens = line.split(";");

            if(!tokens[1].isEmpty() & !tokens[5].isEmpty()){
                int arrondissement = Integer.parseInt(tokens[1]);
                int annee = Integer.parseInt(tokens[5]);
                MapWritable result = new MapWritable();
                result.put(new Text("arrondissement"),new IntWritable(arrondissement));
                result.put(new Text("annee"),new IntWritable(annee));

                context.write(new IntWritable(1), result);
            }
        }
    }
}
