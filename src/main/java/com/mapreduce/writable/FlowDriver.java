package com.mapreduce.writable;

//1. 获取job
//2. 设置jar包lujing
//3. 关联mapper、reducer
//4. 设置map输出的kv类型
//5.设置最终输出的kv类型
//6.设置输入路径和输出路径
//7.提交job

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2. 设置jar包路径
        job.setJarByClass(FlowDriver.class);
        // 3. 关联mapper、reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 4. 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 5. 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\VMware\\hadoop\\资料\\11_input\\inputflow\\phone_data"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\VMware\\hadoop\\资料\\11_input\\inputflow\\outputflow"));
        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
