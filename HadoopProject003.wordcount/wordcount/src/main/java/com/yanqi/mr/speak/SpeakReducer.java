package com.yanqi.mr.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        //定义时长累加的初始值
        Long self_duration = 0L;
        Long third_part_duration = 0L;

        //reduce方法的key:map输出的某一个key
        //reduce方法的value:map输出的kv对中相同key的value组成的一个集合
        //reduce 逻辑：遍历迭代器累加时长即可
        for (SpeakBean bean : values) {
            final Long selfDuration = bean.getSelfDuration();
            final Long thirdPartDuration = bean.getThirdPartDuration();
            self_duration += selfDuration;
            third_part_duration += thirdPartDuration;
        }
        //输出，封装成一个bean对象输出
        final SpeakBean bean = new SpeakBean(self_duration, third_part_duration, key.toString());
        context.write(key, bean);
    }
}
