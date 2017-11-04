package com.paner.dp.dataOrganizingPattern.totalOrderSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


import java.io.IOException;


/**
 * @User: paner
 * @Date: 17/11/2 上午9:48
 */
public class TotalOrderSortMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String inputPath = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/users";
        String partitionFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/"+"partition.lst";
        String outputStage = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/"+"_staging";
        String outputOrder = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output1";

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "file:///");


        Job job = Job.getInstance(conf);
        job.setJarByClass(TotalOrderSortMain.class);
        job.setJobName("TotalOrderSortingStage");


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(LastAccessDateMapper.class);
        //优化的点
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputStage));

        int code = job.waitForCompletion(true)?0:1;

        if (code == 0){


            Job orderJob = Job.getInstance(conf);
            orderJob.setJarByClass(TotalOrderSortMain.class);
            orderJob.setJobName("TotalOrderSortingStage");


            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);


            orderJob.setMapperClass(Mapper.class);
            //优化的点
            orderJob.setReducerClass(ValueReducer.class);
            orderJob.setNumReduceTasks(10);

            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),new Path(partitionFile));

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.addInputPath(orderJob,new Path(outputStage));


            orderJob.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(orderJob, new Path(outputOrder));

            orderJob.getConfiguration().set("mapreduce.output.textoutputformat.separator","");

            InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler(.001,1000));

            code = orderJob.waitForCompletion(true)?0:2;

        }
    }
}
