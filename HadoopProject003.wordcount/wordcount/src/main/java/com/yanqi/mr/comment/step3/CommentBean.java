package com.yanqi.mr.comment.step3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommentBean implements WritableComparable<CommentBean> {
    private String orderId;
    private String comment;
    private String commentExt;
    private int goodsNum;
    private String phoneNum;
    private String userName;
    private String address;
    private int commentStatus;
    private String commentTime;

    @Override
    public String toString() {
        return orderId+"\t"+comment+"\t"+commentExt+"\t"+goodsNum+"\t"+phoneNum+"\t"+userName+"\t"+address+"\t"+commentStatus+"\t"+commentTime;
    }
//无参构造

    public CommentBean() {
    }

    public CommentBean(String orderId, String comment, String commentExt, int goodsNum, String phoneNum, String userName, String address, int commentStatus, String commentTime) {
        this.orderId = orderId;
        this.comment = comment;
        this.commentExt = commentExt;
        this.goodsNum = goodsNum;
        this.phoneNum = phoneNum;
        this.userName = userName;
        this.address = address;
        this.commentStatus = commentStatus;
        this.commentTime = commentTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getCommentExt() {
        return commentExt;
    }

    public void setCommentExt(String commentExt) {
        this.commentExt = commentExt;
    }

    public int getGoodsNum() {
        return goodsNum;
    }

    public void setGoodsNum(int goodsNum) {
        this.goodsNum = goodsNum;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getCommentStatus() {
        return commentStatus;
    }

    public void setCommentStatus(int commentStatus) {
        this.commentStatus = commentStatus;
    }

    public String getCommentTime() {
        return commentTime;
    }

    public void setCommentTime(String commentTime) {
        this.commentTime = commentTime;
    }

    //定义排序规则,按照时间降序;0,1,-1
    @Override
    public int compareTo(CommentBean o) {
        return o.getCommentTime().compareTo(this.commentTime);
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(comment);
        out.writeUTF(commentExt);
        out.writeInt(goodsNum);
        out.writeUTF(phoneNum);
        out.writeUTF(userName);
        out.writeUTF(address);
        out.writeInt(commentStatus);
        out.writeUTF(commentTime);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.comment = in.readUTF();
        this.commentExt = in.readUTF();
        this.goodsNum = in.readInt();
        this.phoneNum = in.readUTF();
        this.userName = in.readUTF();
        this.address = in.readUTF();
        this.commentStatus = in.readInt();
        this.commentTime = in.readUTF();
    }
}
