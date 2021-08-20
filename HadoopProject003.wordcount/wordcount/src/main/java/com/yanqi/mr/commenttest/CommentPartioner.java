package com.yanqi.mr.commenttest;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CommentPartioner extends Partitioner<CommentBean, NullWritable> {
    @Override
    public int getPartition(CommentBean commentBean, NullWritable nullWritable, int numPartitions) {
        return (commentBean.getCommentStatus() & Integer.MAX_VALUE) % numPartitions;
    }
}
