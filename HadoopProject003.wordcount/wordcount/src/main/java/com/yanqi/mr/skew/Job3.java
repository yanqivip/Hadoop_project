package com.yanqi.mr.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class Job3 {
    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //提升为全局变量，避免每次执行map方法都执行此操作
        final Text word = new Text();
        final IntWritable num = new IntWritable();
        Random random = new Random();

        // LongWritable, Text-->文本偏移量，一行文本内容，map方法的输入参数，一行文本就调用一次map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        1 接收到文本内容，转为String类型
            final String str = value.toString();
//        2 按照空格进行切分
            final String[] words = str.split("\t");
//        3 输出<单词，数量>
            String[] fields = words[0].split("&");
            //遍历数据
            word.set(fields[0]);
            num.set(Integer.parseInt(words[1]));
            context.write(word, num);


        }
    }


    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //2 遍历key对应的values，然后累加结果
            int sum = 0;
            for (IntWritable value : values) {
                int i = value.get();
                sum += i;
            }
            // 3 直接输出当前key对应的sum值，结果就是单词出现的总次数
            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();
        //针对reduce端输出使用snappy压缩
        final Job job = Job.getInstance(conf, "Job3");
//        2. 指定程序jar的本地路径
        job.setJarByClass(Job3.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0])); //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
