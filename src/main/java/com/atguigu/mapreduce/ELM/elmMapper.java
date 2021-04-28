package com.atguigu.mapreduce.ELM;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class elmMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 导入一行
        String line = value.toString();

        //切割
        boolean result = parselong(line,context);
        // 判断
        if (!result){
            return;
        }

        context.write(value,NullWritable.get());
    }

    private boolean parselong(String line, Context context) {
        String[] s = line.split(" ");
        return s.length > 11;
    }
}
