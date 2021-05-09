package com.mapreduce.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {

    private String id;
    private String pid;
    private int amount;
    private String pname;
    private String flag;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    // 无参构造
    public TableBean() {
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.pid);
        dataOutput.writeUTF(this.pname);
        dataOutput.writeInt(this.amount);
        dataOutput.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.pname = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.flag = dataInput.readUTF();

    }
}
