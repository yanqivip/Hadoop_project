package com.yanqi.mr.WcCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

        // 5.1.设置使用combiner组件
//        job.setCombinerClass(WordCountCombiner.class);
        job.setCombinerClass(WordCountReducer.class);           //可以使用reducer作为combiner组件来使用是可以的

//        6. 指定job读取数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));  //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
