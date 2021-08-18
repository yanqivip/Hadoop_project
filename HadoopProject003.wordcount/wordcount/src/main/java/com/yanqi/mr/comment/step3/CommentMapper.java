package com.yanqi.mr.comment.step3;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//第一对kv:使用SequenceFileinputformat读取，所以key:Text,Value:BytesWritable(原因是生成sequencefile文件指定就是这种类型)
public class CommentMapper extends Mapper<Text, BytesWritable, CommentBean, NullWritable> {
    //key就是文件名
    //value:一个文件的完整内容
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //且分区每一行
        String str = new String(value.getBytes());
        String[] lines = str.split("\n");
        for (String line : lines) {
            CommentBean commentBean = parseStrToCommentBean(line);
            if (null != commentBean) {
                context.write(commentBean, NullWritable.get());
            }

        }

    }

    //切分字符串封装成commentbean对象
    public CommentBean parseStrToCommentBean(String line) {
        if (StringUtils.isNotBlank(line)) {
            //每一行进行切分
            String[] fields = line.split("\t");
            if (fields.length >= 9) {
                return new CommentBean(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), fields[4], fields[5], fields[6], Integer.parseInt(fields[7]),
                        fields[8]);
            }
            {
                return null;
            }
        }

        return null;
    }
}
