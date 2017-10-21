package com.paner.dp.filterPattern.grep;

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
 * @Date: 17/10/21 下午10:59
 */
public class GrepMain {

    public static void main(String[] args) {

        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output";


            Configuration conf = new Configuration();
            conf.set("fs.default.name", "local");
            conf.set("mapred.job.tracker", "local");

            conf.set("mapregex","python");

            Job job = Job.getInstance(conf);
            job.setJarByClass(MinMaxCountMain.class);
            job.setJobName("grep");


            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(GrepMapper.class);


            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(dstFile));
            FileOutputFormat.setOutputPath(job, new Path(srcFile));

            int code =  job.waitForCompletion(true)?0:1;





        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
