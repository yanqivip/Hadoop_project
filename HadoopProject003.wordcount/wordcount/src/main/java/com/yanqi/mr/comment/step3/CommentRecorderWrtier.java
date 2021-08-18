package com.yanqi.mr.comment.step3;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CommentRecorderWrtier extends RecordWriter<CommentBean, NullWritable> {
    //定义写出数据的流
    private FSDataOutputStream goodOut;
    private FSDataOutputStream commonOut;
    private FSDataOutputStream badOut;

    public CommentRecorderWrtier(FSDataOutputStream goodOut, FSDataOutputStream commonOut, FSDataOutputStream badOut) {
        this.goodOut = goodOut;
        this.commonOut = commonOut;
        this.badOut = badOut;
    }

    //实现把数据根据不同的评论类型输出到不同的目录下
    //写出数据的逻辑
    @Override
    public void write(CommentBean key, NullWritable value) throws IOException, InterruptedException {
        int commentStatus = key.getCommentStatus();
        String beanStr = key.toString();
        if (commentStatus == 0) {
            goodOut.write(beanStr.getBytes());
            goodOut.write("\n".getBytes());
            goodOut.flush();
        } else if (commentStatus == 1) {
            commonOut.write(beanStr.getBytes());
            commonOut.write("\n".getBytes());
            commonOut.flush();
        } else {
            badOut.write(beanStr.getBytes());
            badOut.write("\n".getBytes());
            badOut.flush();
        }
    }

    //释放资源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(goodOut);
        IOUtils.closeStream(commonOut);
        IOUtils.closeStream(badOut);
    }
}
