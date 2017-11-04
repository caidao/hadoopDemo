package com.paner.dp.filterPattern.distinctUser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class DistinctUserMain {
    public static void main(String[] args) {

        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/comments";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output";

            Configuration conf = new Configuration();
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.job.tracker", "file:///");


            Job job = Job.getInstance(conf);
            job.setJarByClass(DistinctUserMain.class);
            job.setJobName("distinctUser");


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);


            job.setMapperClass(DistinctUserMapper.class);
            //优化的点
            //job.setCombinerClass(DistinctUserReducer.class);
            job.setReducerClass(DistinctUserReducer.class);

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
