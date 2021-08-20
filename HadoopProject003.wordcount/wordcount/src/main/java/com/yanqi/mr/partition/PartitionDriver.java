package com.yanqi.mr.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取配置文件
        final Configuration conf = new Configuration();
        //2 获取job实例
        final Job job = Job.getInstance(conf);
        //3 设置任务相关参数
        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        // 4 设置使用自定义分区器
        job.setPartitionerClass(CustomPartitioner.class);
        //5 指定reducetask的数量与分区数量保持一致,分区数量是3
        job.setNumReduceTasks(3); //reducetask不设置默认是1个
//        job.setNumReduceTasks(5);
//        job.setNumReduceTasks(2);
        // 6 指定输入和输出数据路径
        FileInputFormat.setInputPaths(job, new Path("e:/speak.data"));
        FileOutputFormat.setOutputPath(job, new Path("e:/parition/out"));
        // 7 提交任务
        final boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);

    }
}
