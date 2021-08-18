package com.yanqi.mr.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class CustomWriter extends RecordWriter<Text, NullWritable> {
    //定义成员变量
    private FSDataOutputStream yanqiOut;
    private FSDataOutputStream otherOut;

    //定义构造方法接收两个输出流


    public CustomWriter(FSDataOutputStream yanqiOut, FSDataOutputStream otherOut) {
        this.yanqiOut = yanqiOut;
        this.otherOut = otherOut;
    }

    //写出数据的逻辑,控制写出的路径
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //写出数据需要输出流
        final String line = key.toString();
        if (line.contains("yanqi")) {
            yanqiOut.write(line.getBytes());
            yanqiOut.write("\r\n".getBytes());
        } else {
            otherOut.write(line.getBytes());
            otherOut.write("\r\n".getBytes());
        }
    }

    //关闭，释放资源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(yanqiOut);
        IOUtils.closeStream(otherOut);
    }
}
