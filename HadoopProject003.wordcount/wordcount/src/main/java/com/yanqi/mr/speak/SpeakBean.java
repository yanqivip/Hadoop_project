package com.yanqi.mr.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//这个类型是map输出kv中value的类型，需要实现writable序列化接口
public class SpeakBean implements Writable {

    //定义属性
    private Long selfDuration;//自有内容时长
    private Long thirdPartDuration;//第三方内容时长
    private String deviceId;//设备id
    private Long sumDuration;//总时长

    //准备一个空参构造

    public SpeakBean() {
    }


    //序列化方法:就是把内容输出到网络或者文本中
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDuration);
    }

    //有参构造

    public SpeakBean(Long selfDuration, Long thirdPartDuration, String deviceId) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = this.selfDuration + this.thirdPartDuration;
    }

    //反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration = in.readLong();//自由时长
        this.thirdPartDuration = in.readLong();//第三方时长
        this.deviceId = in.readUTF();//设备id
        this.sumDuration = in.readLong();//总时长
    }


    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }


    //为了方便观察数据，重写toString()方法

    @Override
    public String toString() {
        return
                selfDuration +
                        "\t" + thirdPartDuration +
                        "\t" + deviceId + "\t" + sumDuration;
    }
}
