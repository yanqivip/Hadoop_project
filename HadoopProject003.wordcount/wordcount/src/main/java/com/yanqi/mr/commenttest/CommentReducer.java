package com.yanqi.mr.commenttest;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommentReducer extends Reducer<CommentBean, NullWritable, CommentBean, NullWritable> {
    @Override
    protected void reduce(CommentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //遍历输出key
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
