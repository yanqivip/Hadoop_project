package com.yanqi.mr.reduce_join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeliverBean implements Writable {

    private String userId;
    private String positionId;
    private String date;
    private String positionName;
    //判断是投递数据还是职位数据标识
    private String flag;

    public DeliverBean() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(positionId);
        out.writeUTF(date);
        out.writeUTF(positionName);
        out.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.positionId = in.readUTF();
        this.date = in.readUTF();
        this.positionName = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return "DeliverBean{" +
                "userId='" + userId + '\'' +
                ", positionId='" + positionId + '\'' +
                ", date='" + date + '\'' +
                ", positionName='" + positionName + '\'' +
                ", flag='" + flag + '\'' +
                '}';
    }
}
