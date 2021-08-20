package com.yanqi.mr.comment.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommentDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CommentDriver");
        job.setJarByClass(CommentDriver.class);

        job.setMapperClass(CommentMapper.class);
        job.setReducerClass(CommentReducer.class);

        job.setMapOutputKeyClass(CommentBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(CommentBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CommentPartitioner.class);
        //指定inputformat类型
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //指定输出outputformat类型
        job.setOutputFormatClass(CommentOutputFormat.class);
        //指定输入，输出路径
        FileInputFormat.setInputPaths(job,
                new Path("E:\\merge\\merge-output"));
        FileOutputFormat.setOutputPath(job,
                new Path("E:\\merge\\outmulti-out"));
        //指定reducetask的数量
        job.setNumReduceTasks(3);
        boolean b = job.waitForCompletion(true);
        if (b) {
            System.exit(0);
        }
    }
}
