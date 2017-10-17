package com.paner.dp.numberPattern.minMaxCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * 存储3个值的Writable对象，被用作mapper端的输出值
 *
 * @User: paner
 * @Email: panyiwen2009@gmail.com
 * @Date: 17/10/15 下午9:16
 */
public class MinMaxCountTuple implements Writable{

    private Date min = new Date();
    private Date max = new Date();
    private long count = 0;

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(min.getTime());
        dataOutput.writeLong(max.getTime());
        dataOutput.writeLong(count);

    }

    public void readFields(DataInput dataInput) throws IOException {

        min = new Date(dataInput.readLong());
        max = new Date(dataInput.readLong());
        count = dataInput.readLong();
    }

    public String toString(){
        return frmt.format(min)+"\t"+frmt.format(max)+"\t"+count;
    }
}
