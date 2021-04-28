package com.atguigu.mapreduce.reduceJoin;

import com.atguigu.mapreduce.partitioner.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//1. 获取job
//2. 设置jar包lujing
//3. 关联mapper、reducer
//4. 设置map输出的kv类型
//5.设置最终输出的kv类型
//6.设置输入路径和输出路径
//7.提交job
public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2.
        job.setJarByClass(TableDriver.class);
        // 3. 关联mapper、reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce2.class);
        // 4. 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        // 5. 设置最终输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\VMware\\hadoop\\资料\\11_input\\inputtable\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\VMware\\hadoop\\资料\\11_input\\inputtable\\output"));
        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
