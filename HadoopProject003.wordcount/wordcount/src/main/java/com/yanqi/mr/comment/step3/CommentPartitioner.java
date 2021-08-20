package com.yanqi.mr.comment.step3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CommentPartitioner extends Partitioner<CommentBean, NullWritable> {
    @Override
    public int getPartition(CommentBean commentBean, NullWritable nullWritable, int numPartitions) {
//        return (commentBean.getCommentStatus() & Integer.MAX_VALUE) % numPartitions;
      return commentBean.getCommentStatus();//0,1,2 -->对应分区编号的
    }
}
