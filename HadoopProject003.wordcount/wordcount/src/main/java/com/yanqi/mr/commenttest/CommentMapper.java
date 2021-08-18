package com.yanqi.mr.commenttest;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommentMapper extends Mapper<LongWritable, Text, CommentBean,NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        boolean jobSetupCleanupNeeded = context.getJobSetupCleanupNeeded();
        super.setup(context);
    }



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        CommentBean commentBean = CommentBean.parseStrToCommentBean(value.toString());
        if(null !=commentBean){
            context.write(commentBean, NullWritable.get());
        }else{
            System.out.println("数据转换异常，原始数据："+value.toString());
        }

    }
}
