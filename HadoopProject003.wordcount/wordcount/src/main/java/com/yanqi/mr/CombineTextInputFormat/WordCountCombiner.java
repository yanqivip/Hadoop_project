package com.yanqi.mr.CombineTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//combiner组件的输入和输出类型与map()方法保持一致
public class WordCountCombiner extends Reducer<Text, IntWritable, NullWritable, IntWritable> {
    final IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        //进行局部汇总，逻辑是与reduce方法保持一致
        for (IntWritable value : values) {
            final int i = value.get();
            num += i;
        }
        total.set(num);
        //输出单词，累加结果
        context.write(NullWritable.get(), total);
    }
}
