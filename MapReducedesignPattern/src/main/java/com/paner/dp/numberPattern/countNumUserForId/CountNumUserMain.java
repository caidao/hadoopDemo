package com.paner.dp.numberPattern.countNumUserForId;

import com.paner.dp.numberPattern.minMaxCount.MinMaxCountMain;
import com.paner.dp.numberPattern.minMaxCount.MinMaxCountMapper;
import com.paner.dp.numberPattern.minMaxCount.MinMaxCountReducer;
import com.paner.dp.numberPattern.minMaxCount.MinMaxCountTuple;
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
public class CountNumUserMain {


    public static void main(String[] args) {

        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/input";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output1";


            Configuration conf = new Configuration();
            conf.set("fs.default.name", "local");
            conf.set("mapred.job.tracker", "local");


            Job job = Job.getInstance(conf);
            job.setJarByClass(MinMaxCountMain.class);
            job.setJobName("CountNumUser");


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setMapperClass(CountNumUsersByIdMapper.class);


            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(dstFile));
            FileOutputFormat.setOutputPath(job, new Path(srcFile));

          int code =  job.waitForCompletion(true)?0:1;

          if (code==0){
              //输出计数
              for (Counter counter : job.getCounters().getGroup(CountNumUsersByIdMapper.MODE_COUNTER_GROUP)){
                  System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
              }
          }



        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
