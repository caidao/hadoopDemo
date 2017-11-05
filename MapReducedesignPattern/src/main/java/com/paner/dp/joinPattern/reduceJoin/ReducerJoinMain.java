package com.paner.dp.joinPattern.reduceJoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @User: paner
 * @Date: 17/11/5 上午11:00
 */
public class ReducerJoinMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String userInputPath = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/users";
        String commentInputPath ="/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/comments";
        String outputPath ="/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "file:///");


        Job job = Job.getInstance(conf);
        job.setJarByClass(ReducerJoinMain.class);
        job.setJobName("reducerJoin");


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,new Path(userInputPath),
                TextInputFormat.class,UserJoinMapper.class);
        MultipleInputs.addInputPath(job,new Path(commentInputPath),
                TextInputFormat.class,CommentJoinMapper.class);

        job.getConfiguration().set("join.type","rightouter");

        job.setReducerClass(UserJoinReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));


        job.waitForCompletion(true);

    }
}
