package com.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/***
 * 必须实现Writable 才可以在reduce的时候能被操作
 */
public class Result implements Writable {

    private long upload;
    private long download;
    private long total;

    public Result(){}

    public Result(long u, long d, long t) {
        upload = u;
        download=d;
        total = t;
    }


    public long getUpload() {
        return upload;
    }

    public void setUpload(long upload) {
        this.upload = upload;
    }

    public long getDownload() {
        return download;
    }

    public void setDownload(long download) {
        this.download = download;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeLong(upload);
       dataOutput.writeLong(download);
       dataOutput.writeLong(total);
   }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upload = dataInput.readLong();
        this.download = dataInput.readLong();
        this.total = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "Result{" +
                "upload=" + this.upload +
                ", download=" + this.download +
                ", total=" + this.total +
                '}';
    }
}
