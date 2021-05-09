package com.mapreduce.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String, String> pdMap= new HashMap<>();;
    private Text outK = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//      pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

//       从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
//
        String line;
        while (StringUtils.isNotEmpty(line=reader.readLine())){
//       切割
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]);
        }
//       关流
        IOUtils.closeStream(fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//      order.txt
        String line = value.toString();
        String[] fields = line.split("\t");

        String pname = pdMap.get(fields[1]);

        outK.set(fields[0]+"\t"+pname+"\t"+fields[2]);

        context.write(outK,NullWritable.get());

    }
}
