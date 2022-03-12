package com.hadoop;


import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;



public class HomeWork {

    private static Logger logger =Logger.getLogger(HomeWork.class);

    /**
     * 执行时 增加参数 args[0] = input 文件夹地址  ；args[1] = output文件夹地址 程序执行前不能存在由程序自动生成
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("start task...");

        Job job = Job.getInstance(conf, "TelCount");
        //指定程序路径
        job.setJarByClass(HomeWork.class);

        job.setMapperClass(TelMapper.class);
        job.setReducerClass(TelReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Result.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }

}
