package com.yanqi.mr.comment.step3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommentReducer extends Reducer<CommentBean, NullWritable, CommentBean, NullWritable> {
    @Override
    protected void reduce(CommentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //遍历values，输出的是key；key：是一个引用地址，底层获取value同时，key的值也发生了变化
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }
}
