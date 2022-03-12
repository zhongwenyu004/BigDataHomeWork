package com.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/***
 * 执行map任务类
 * LongWritable 偏移量 表示该行在文中的位置
 * Text MAP阶段的输入数据，一行文本信息 String类型
 * Text map阶段数据字符类型
 * Result map阶段输出value类型 Result
 *
 *
 * map阶段 将每行数据分割后 用InfoEntity封装 电话号 上下行 数据量并写入context
 * <k,v> ==> <tel, Result>
 */
public class TelMapper extends Mapper<LongWritable, Text,Text, Result> {//4个参数分别为输入key 类型，输入value类型;输出key类型，输出value类型


    /**
     *
     * @param key 输入的键
     * @param value 输入的值
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{

        //时间戳、电话号码、基站的物理地址、访问网址的 ip、网站域名、数据包、接包数、上行 / 传流量、下行 / 载流量、响应码
        String[] line = value.toString().split("\t");//读取一行数据  比如：1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200

        if(line.length<10){
            return;
        }
        String tel = line[1];
        int upload = Integer.parseInt(line[8]);
        int download = Integer.parseInt(line[9]);
        context.write(new Text(tel),new Result(upload,download,upload+download));
    }
}
