package com.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Text 数据类型(对应MAP阶段输出的KEY)
 * Result reduce阶段输入类型(对应MAP阶段输出的value)
 * Text  reduce阶段输出类型
 * Result reduce阶段输出最后封装结果
 */
public class TelReducer extends Reducer<Text, Result,Text, Result> {


    /**
     *
     * @param key
     * @param values  需要是可以被序列化的实体对象
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Text key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        System.out.print("reduce...");
        int totalup=0;
        int totalDown=0;
        int totalSum=0;

        for(Result r:values){
            totalup += r.getUpload();
            totalDown += r.getDownload();
            totalSum += r.getTotal();
            System.out.print(totalup);
        }
        context.write(key,new Result(totalup,totalDown,totalSum));
    }
}
