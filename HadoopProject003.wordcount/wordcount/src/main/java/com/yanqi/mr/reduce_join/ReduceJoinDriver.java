package com.yanqi.mr.reduce_join;

import com.yanqi.mr.wc.WordCountCombiner;
import com.yanqi.mr.wc.WordCountDriver;
import com.yanqi.mr.wc.WordCountMapper;
import com.yanqi.mr.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "ReduceJoinDriver");
//        2. 指定程序jar的本地路径
        job.setJarByClass(ReduceJoinDriver.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeliverBean.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(DeliverBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0])); //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}