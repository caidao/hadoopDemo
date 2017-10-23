package com.paner.dp.filterPattern.topTen;

import com.paner.dp.numberPattern.countNumUserForId.CountNumUsersByIdMapper;
import com.paner.dp.numberPattern.minMaxCount.MinMaxCountMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @User: paner
 * @Date: 17/10/21 下午6:18
 */
public class TopTenMain {


    public static void main(String[] args) {

        try {
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output1";


            Configuration conf = new Configuration();
            conf.set("fs.default.name", "local");
            conf.set("mapred.job.tracker", "local");


            Job job = Job.getInstance(conf);
            job.setJarByClass(TopTenMain.class);
            job.setJobName("TopTen");


            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(TopTenMapper.class);
            job.setReducerClass(TopTenReducer.class);


            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(srcFile));
            FileOutputFormat.setOutputPath(job, new Path(dstFile));

            job.waitForCompletion(true);



        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
