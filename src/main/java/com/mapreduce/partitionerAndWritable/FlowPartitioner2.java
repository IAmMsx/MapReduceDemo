package com.mapreduce.partitionerAndWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 泛型为mapper输出的key与value
public class FlowPartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        String prephone = phone.substring(0, 3);

        int partitioners;
        if ("136".equals(prephone)) {
            partitioners = 0;
        } else if ("137".equals(prephone)) {
            partitioners = 1;
        } else if ("138".equals(prephone)) {
            partitioners = 2;
        } else if ("139".equals(prephone)) {
            partitioners = 3;
        } else {
            partitioners = 4;
        }
        return partitioners;
    }
}
