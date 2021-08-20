package com.yanqi.mr.CombineTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//需求：单词计数
//1 继承Mapper类
//2 Mapper类的泛型参数：共4个，2对kv
//2.1 第一对kv:map输入参数类型
//2.2 第二队kv：map输出参数类型
// LongWritable, Text-->文本偏移量（后面不会用到），一行文本内容
//Text, IntWritable-->单词，1
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //3 重写Mapper类的map方法
    /*
    1 接收到文本内容，转为String类型
    2 按照空格进行切分
    3 输出<单词，1>
     */
    //提升为全局变量，避免每次执行map方法都执行此操作
    final Text word = new Text();
    final IntWritable one = new IntWritable(1);

    // LongWritable, Text-->文本偏移量，一行文本内容，map方法的输入参数，一行文本就调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        1 接收到文本内容，转为String类型
        final String str = value.toString();
//        2 按照空格进行切分
        final String[] words = str.split(" ");
//        3 输出<单词，1>
        //遍历数据
        for (String s : words) {
            word.set(s);
            context.write(word, one);
        }

    }
}
