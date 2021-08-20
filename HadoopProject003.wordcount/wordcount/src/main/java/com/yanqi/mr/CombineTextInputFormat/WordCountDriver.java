package com.yanqi.mr.CombineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//封装任务并提交运行
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*
        1. 获取配置文件对象，获取job对象实例
        2. 指定程序jar的本地路径
        3. 指定Mapper/Reducer类
        4. 指定Mapper输出的kv数据类型
        5. 指定最终输出的kv数据类型
        6. 指定job处理的原始数据路径
        7. 指定job输出结果路径
        8. 提交作业
         */
//        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();
        //针对reduce端输出使用snappy压缩
//        conf.set("mapreduce.output.fileoutputformat.compress", "true");
//        conf.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
//        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        final Job job = Job.getInstance(conf, "WordCountDriver");
//        2. 指定程序jar的本地路径
        job.setJarByClass( WordCountDriver.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass( WordCountMapper.class);
        job.setReducerClass( WordCountReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置使用CombinerTextInputFormat读取数据
        job.setInputFormatClass( CombineTextInputFormat.class);
        //设置虚拟存储切片的最大值，单位为byte
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        6. 指定job读取数据路径
        FileInputFormat.setInputPaths(job, new Path("E:\\data\\小文件\\wc-combine\\input")); //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("E:\\data\\小文件\\wc-combine\\output")); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
