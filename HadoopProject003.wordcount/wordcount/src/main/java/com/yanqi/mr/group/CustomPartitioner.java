package com.yanqi.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        //希望订单id相同的数据进入同个分区

        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
