package com.mapreduce.ELM;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class elmDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"D:\\VMware\\hadoop\\资料\\11_input\\inputlog", "D:\\VMware\\hadoop\\资料\\11_input\\inputlog\\output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(elmDriver.class);

        job.setMapperClass(elmMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
