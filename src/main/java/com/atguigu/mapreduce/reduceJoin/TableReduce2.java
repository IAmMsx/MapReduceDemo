package com.atguigu.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReduce2 extends Reducer<Text, TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> tableBeans = new ArrayList<>();
        TableBean pdbean = new TableBean();
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())){//order.txt
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                tableBeans.add(tmpBean);
            }else { //pd.txt
                try {
                    BeanUtils.copyProperties(pdbean,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

            }
        for (TableBean tableBean : tableBeans) {
            tableBean.setPname(pdbean.getPname());
            context.write(tableBean,NullWritable.get());
        }

    }
}
