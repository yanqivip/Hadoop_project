package com.yanqi.mr.WcCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//继承的Reducer类有四个泛型参数,2对kv
//第一对kv:类型要与Mapper输出类型一致：Text, IntWritable
//第二队kv:自己设计决定输出的结果数据是什么类型：Text, IntWritable
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //1 重写reduce方法

    //Text key:map方法输出的key,本案例中就是单词,
    // Iterable<IntWritable> values：一组key相同的kv的value组成的集合

    IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //2 遍历key对应的values，然后累加结果
        int sum = 0;
        for (IntWritable value : values) {
            int i = value.get();
            sum += 1;
        }
        // 3 直接输出当前key对应的sum值，结果就是单词出现的总次数
        total.set(sum);
        context.write(key, total);
        /*
    假设map方法：hello 1;hello 1;hello 1
    reduce的key和value是什么？
    key：hello,
    values:<1,1,1>

    假设map方法输出：hello 1;hello 1;hello 1，hadoop 1， mapreduce 1,hadoop 1
    reduce的key和value是什么？
    reduce方法何时调用：一组key相同的kv中的value组成集合然后调用一次reduce方法
    第一次：key:hello ,values:<1,1,1>
    第二次：key:hadoop ,values<1,1>
    第三次：key:mapreduce ,values<1>
     */
    }
}
