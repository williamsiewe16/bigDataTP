package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.mapper.ExistingSpeciesMapper;
import com.opstty.reducer.DistrictReducer;
import com.opstty.reducer.ExistingSpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ListExistingSpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: listExistingSpecies <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "listExistingSpecies");
        job.setJarByClass(ListExistingSpecies.class);
        job.setMapperClass(ExistingSpeciesMapper.class);
        job.setCombinerClass(ExistingSpeciesReducer.class);
        job.setReducerClass(ExistingSpeciesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
