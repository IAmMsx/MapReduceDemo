package com.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //
        String line = value.toString();
        String[] split = line.split("\t");
        if (filename.contains("order")) {// order.txt
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setFlag("order");
            outV.setPname("");
            outV.setPid(split[1]);
        } else {//pd.txt

            outK.set(split[0]);
            outV.setPname(split[1]);
            outV.setAmount(0);
            outV.setFlag("pd");
            outV.setId("");
            outV.setPid(split[0]);
        }

        context.write(outK, outV);
    }
}
