package com.yanqi.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {
    //写出数据的对象
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //定义写出数据的路径信息，并获取到输出流传入writer对象中
        final Configuration conf = context.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        //定义输出的路径
        final FSDataOutputStream yanqiOut = fs.create(new Path("e:/yanqi.log"));
        final FSDataOutputStream otherOut = fs.create(new Path("e:/other.log"));
        CustomWriter customWriter = new CustomWriter(yanqiOut, otherOut);
        return customWriter;
    }
}
