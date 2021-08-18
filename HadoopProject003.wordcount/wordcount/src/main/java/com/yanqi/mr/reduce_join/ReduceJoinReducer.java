package com.yanqi.mr.reduce_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, DeliverBean, DeliverBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<DeliverBean> values, Context context) throws IOException,
            InterruptedException {
        //相同positionid的 bean对象放到一起(1个职位数据，n个投递行为数据)
        ArrayList<DeliverBean> deBeans = new ArrayList<>();
        DeliverBean positionBean = new DeliverBean();
        for (DeliverBean bean : values) {
            String flag = bean.getFlag();
            if (flag.equalsIgnoreCase("deliver")) {
                //投递行为数据
                //此处不能直接把bean对象添加到debeans中，需要深度拷贝才行
                DeliverBean newBean = new DeliverBean();
                try {
                    BeanUtils.copyProperties(newBean, bean);
                    deBeans.add(newBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                //职位
                try {
                    BeanUtils.copyProperties(positionBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //遍历投递行为数据拼接positionname
        for (DeliverBean bean : deBeans) {
            bean.setPositionName(positionBean.getPositionName());
            context.write(bean, NullWritable.get());
        }
    }
}
