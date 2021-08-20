package com.yanqi.mr.group;

import com.sun.corba.se.impl.orb.ParserTable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupingComparator extends WritableComparator {

    public CustomGroupingComparator() {
        super(OrderBean.class, true); //注册自定义的GroupingComparator接受OrderBean对象
    }

    //重写其中的compare方法，通过这个方法来让mr接受orderid相等则两个对象相等的规则，key相等

    @Override
    public int compare(WritableComparable a, WritableComparable b) { //a 和b是orderbean的对象
        //比较两个对象的orderid
        final OrderBean o1 = (OrderBean) a;
        final OrderBean o2 = (OrderBean) b;
        final int i = o1.getOrderId().compareTo(o2.getOrderId());
        return i; // 0 1 -1
    }
}
