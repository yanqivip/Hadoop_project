package com.yanqi.mr.commenttest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommentOutputFormat extends FileOutputFormat<CommentBean, NullWritable> {


    @Override
    public RecordWriter<CommentBean, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        //获取文件系统对象
        Configuration conf = job.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        //指定输出数据的文件
         Path goodPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir")+"\\good\\goodComments.log");
        Path badPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir")+"\\bad\\badComments.log");
        Path commonPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir")+"\\common\\commonComments.log");
        //获取输出流
        final FSDataOutputStream lagouOut = fs.create(goodPath);
        final FSDataOutputStream otherOut = fs.create(badPath);
        FSDataOutputStream commonOut = fs.create(commonPath);
        return new CustomWriter(lagouOut, otherOut,commonOut);

    }


    static class CustomWriter extends RecordWriter<CommentBean, NullWritable> {
        private FSDataOutputStream goodOut;
        private FSDataOutputStream commonOut;
        private FSDataOutputStream badOut;

        public CustomWriter(FSDataOutputStream goodOut, FSDataOutputStream commonOut, FSDataOutputStream badOut) {
            this.goodOut = goodOut;
            this.commonOut = commonOut;
            this.badOut = badOut;
        }

        @Override
        public void write(CommentBean key, NullWritable value) throws IOException, InterruptedException {
            // 判断是否包含“lagou”输出到不同文件
            if (key.getCommentStatus() == 0) {
                goodOut.write(key.toString().getBytes());
                goodOut.write("\r\n".getBytes());
            } else if (key.getCommentStatus() == 1) {
                commonOut.write(key.toString().getBytes());
                commonOut.write("\r\n".getBytes());
            } else {
                badOut.write(key.toString().getBytes());
                badOut.write("\r\n".getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            IOUtils.closeStream(goodOut);
            IOUtils.closeStream(commonOut);
            IOUtils.closeStream(badOut);
        }
    }
}
