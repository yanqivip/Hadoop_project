package com.yanqi.mr.comment.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CombineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "CombineDriver");
//        2. 指定程序jar的本地路径
        job.setJarByClass(CombineDriver.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(CombineMapper.class);
//        job.setReducerClass(MergeReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //指定使用CombineTextinputformat读取数据
        job.setInputFormatClass(CombineTextInputFormat.class);
        //指定分片大小
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 4); //4M

        FileInputFormat.setInputPaths(job, new Path("E:\\merge\\input")); //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("E:\\merge\\merge-out")); //指定结果数据输出路径
        job.setNumReduceTasks(3);
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
