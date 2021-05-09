//package com.atguigu.mapreduce.reduceJoin;
//
//import javafx.scene.control.Tab;
//import org.apache.commons.beanutils.BeanUtils;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import com.atguigu.mapreduce.reduceJoin.TableBean;
//
//import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;
//import java.util.ArrayList;
//
//public class TableReducer extends Reducer<Text, TableBean,TableBean, NullWritable> {
//    @Override
//    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
//
////        ArrayList<TableBean> orderBeans = new ArrayList<>();
//        TableBean pdBean = new TableBean();
//        ArrayList<Object> orderBeans = new ArrayList<>();
//        for (TableBean value : values) {
//            if ("order".equals(value.getFlag())){//order
//                TableBean tmpTableBean = new TableBean();
//                try {
//                    BeanUtils.copyProperties(tmpTableBean,value);
//                } catch (IllegalAccessException | InvocationTargetException e) {
//                    e.printStackTrace();
//                }
//                orderBeans.add(tmpTableBean);
////                orderBeans.add(tmpTableBean);
//            }else {//pd
//                try {
//                    BeanUtils.copyProperties(pdBean,value);
//                } catch (IllegalAccessException | InvocationTargetException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        for (TableBean orderBean : orderBeans) {
//            orderBean.setPname(pdBean.getPname());
//            context.write(orderBean,NullWritable.get());
//        }
//
//    }
//}
