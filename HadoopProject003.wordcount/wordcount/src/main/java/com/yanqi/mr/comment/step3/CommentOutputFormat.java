package com.yanqi.mr.comment.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//最终输出的kv类型
public class CommentOutputFormat extends FileOutputFormat<CommentBean, NullWritable> {
    //负责写出数据的对象
    @Override
    public RecordWriter<CommentBean, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        //当前reducetask处理的分区编号来创建文件获取输出流
        //获取到在Driver指定的输出路径;0是好评，1是中评，2是差评
        String outputDir = conf.get("mapreduce.output.fileoutputformat.outputdir");
        FSDataOutputStream goodOut=null;
        FSDataOutputStream commonOut=null;
        FSDataOutputStream badOut=null;
        int id = job.getTaskAttemptID().getTaskID().getId();//当前reducetask处理的分区编号
        if(id==0){
            //好评数据
            goodOut  =fs.create(new Path(outputDir + "\\good\\good.log"));
        }else if(id ==1){
            //中评数据
            commonOut = fs.create(new Path(outputDir + "\\common\\common.log"));
        }else{
             badOut = fs.create(new Path(outputDir + "\\bad\\bad.log"));
        }


       return new CommentRecorderWrtier(goodOut,commonOut,badOut);

    }
}
