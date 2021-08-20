package com.yanqi.mr.speak;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "speakDriver");
        //设置jar包本地路径
        job.setJarByClass(SpeakDriver.class);
        //使用的mapper和reducer
        job.setMapperClass(SpeakMapper.class);
        job.setReducerClass(SpeakReducer.class);
        //map的输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        //设置reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        //读取的数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交任务
        final boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
