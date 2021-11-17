package com.opstty.job;

import com.opstty.IntTupleWritable;
import com.opstty.mapper.MostTreesDistrictMapper1;
import com.opstty.mapper.MostTreesDistrictMapper2;
import com.opstty.mapper.OldestTreeDistrictMapper;
import com.opstty.mapper.SortByHeightMapper;
import com.opstty.reducer.MostTreesDistrictReducer1;
import com.opstty.reducer.MostTreesDistrictReducer2;
import com.opstty.reducer.OldestTreeDistrictReducer;
import com.opstty.reducer.SortByHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MostTreesDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: mostTreesDistrict <in> [<in>...] <out>");
            System.exit(2);
        }

        /* JOB 1 configuration */
        Job job1 = Job.getInstance(conf, "mostTreesDistrict");
        job1.setJarByClass(MostTreesDistrict.class);
        job1.setMapperClass(MostTreesDistrictMapper1.class);
        job1.setCombinerClass(MostTreesDistrictReducer1.class);
        job1.setReducerClass(MostTreesDistrictReducer1.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

       /* JOB 2 configuration */
       // Configuration conf2 = new Configuration();
       /* Job job2 = Job.getInstance(conf, "mostTreesDistrict");
        job2.setJarByClass(MostTreesDistrict.class);
        job2.setMapperClass(MostTreesDistrictMapper2.class);
        //job2.setCombinerClass(MostTreesDistrictReducer2.class);
        job2.setReducerClass(MostTreesDistrictReducer2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntTupleWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));*/

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
