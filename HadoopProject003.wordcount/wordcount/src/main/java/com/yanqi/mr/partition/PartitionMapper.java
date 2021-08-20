package com.yanqi.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
1. 读取一行文本，按照制表符切分

2. 解析出appkey字段，其余数据封装为PartitionBean对象（实现序列化Writable接口）

3. 设计map()输出的kv,key-->appkey(依靠该字段完成分区)，PartitionBean对象作为Value输出
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {
    final PartitionBean bean = new PartitionBean();
    final Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");
        String appkey = fields[2];

        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);

        k.set(appkey);
        context.write(k, bean); //shuffle开始时会根据k的hashcode值进行分区，但是结合我们自己的业务，默认hash分区方式不能满足需求
    }
}
