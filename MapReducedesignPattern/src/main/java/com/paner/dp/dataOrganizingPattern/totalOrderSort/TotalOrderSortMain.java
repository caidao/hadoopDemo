package com.paner.dp.dataOrganizingPattern.totalOrderSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @User: paner
 * @Date: 17/11/2 上午9:48
 */
public class TotalOrderSortMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {


        String inputPath = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/users";
        String partitionFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/partition/"+"partition.lst";
        String outputStage = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/"+"_staging";
        String outputOrder = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output1";

        FileSystem.get(new Configuration()).delete(new Path(partitionFile),true);
        FileSystem.get(new Configuration()).delete(new Path(outputStage),true);
        FileSystem.get(new Configuration()).delete(new Path(outputOrder),true);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapred.job.tracker", "file:///");


//        Job job = Job.getInstance(conf);
//        job.setJarByClass(TotalOrderSortMain.class);
//        job.setJobName("TotalOrderSortingStage1");
//
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//
//        job.setMapperClass(LastAccessDateMapper.class);
//        //优化的点
//        job.setNumReduceTasks(0);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        SequenceFileOutputFormat.setOutputPath(job, new Path(outputStage));

        int code = 0;//job.waitForCompletion(true)?0:1;

        if (code == 0){


            Job orderJob = Job.getInstance(conf);
            orderJob.setJarByClass(TotalOrderSortMain.class);
            orderJob.setJobName("TotalOrderSortingStage2");


            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);


            orderJob.setMapperClass(LastAccessDateMapper.class);
            //优化的点
            orderJob.setReducerClass(ValueReducer.class);
            orderJob.setNumReduceTasks(10);

            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),new Path(partitionFile));

            orderJob.setInputFormatClass(KeyValueTextInputFormat.class);
            SequenceFileInputFormat.addInputPath(orderJob,new Path(inputPath));


            orderJob.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(orderJob, new Path(outputOrder));

          //  orderJob.getConfiguration().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");

            InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler(0.1, 1000, 10);
            InputSampler.writePartitionFile(orderJob,sampler);



            code = orderJob.waitForCompletion(true)?0:2;

        }
    }
}
