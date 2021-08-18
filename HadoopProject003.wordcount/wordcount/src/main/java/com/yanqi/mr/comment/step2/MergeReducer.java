package com.yanqi.mr.comment.step2;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //输出value值（文件内容），只获取其中第一个即可（只有一个）
        context.write(key, values.iterator().next());
    }
}
