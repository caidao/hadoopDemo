package com.paner.dp.numberPattern.minMaxCount;

import com.paner.common.HDFS_File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @User: paner
 * @Date: 17/10/16 下午11:00
 */
public class MinMaxCountMain {
    public static void main(String[] args) {

        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output";

            Configuration conf = new Configuration();
            conf.set("fs.default.name", "local");
            conf.set("mapred.job.tracker", "local");


            Job job = Job.getInstance(conf);
            job.setJarByClass(MinMaxCountMain.class);
            job.setJobName("minMaxCount");


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MinMaxCountTuple.class);

            job.setMapperClass(MinMaxCountMapper.class);
            //优化的点
            job.setCombinerClass(MinMaxCountReducer.class);
            job.setReducerClass(MinMaxCountReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(dstFile));
            FileOutputFormat.setOutputPath(job, new Path(srcFile));

            job.waitForCompletion(true);


        }catch (Exception ex){
          ex.printStackTrace();
        }
    }
}
