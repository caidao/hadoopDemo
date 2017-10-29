package com.paner.dp.dataOrganizingPattern.partition;

import com.paner.dp.dataOrganizingPattern.structuredToHierarchical.CommentMapper;
import com.paner.dp.dataOrganizingPattern.structuredToHierarchical.PostCommentHierarchyReducer;
import com.paner.dp.dataOrganizingPattern.structuredToHierarchical.PostMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @User: paner
 * @Date: 17/10/29 下午12:52
 */
public class PartitionerMain {

    public static void main(String[] args) {
        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output";

            Configuration conf = new Configuration();
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.job.tracker", "file:///");


            Job job = Job.getInstance(conf);
            job.setJarByClass(PartitionerMain.class);
            job.setJobName("partitioner");


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);


           job.setMapperClass(CreationDateMapper.class);
            //优化的点
            job.setPartitionerClass(CreationDatePartitioner.class);
            job.setReducerClass(ValueReducer.class);

            //作业需要配置成使用自定义分区器，同时分区器也需要配置
            job.setNumReduceTasks(4);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(job, new Path(dstFile));
            FileOutputFormat.setOutputPath(job, new Path(srcFile));

            job.waitForCompletion(true);


        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
