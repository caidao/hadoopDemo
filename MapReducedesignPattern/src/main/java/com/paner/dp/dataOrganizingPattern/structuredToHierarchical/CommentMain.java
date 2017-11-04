package com.paner.dp.dataOrganizingPattern.structuredToHierarchical;

import com.paner.dp.filterPattern.distinctUser.DistinctUserMain;
import com.paner.dp.filterPattern.distinctUser.DistinctUserMapper;
import com.paner.dp.filterPattern.distinctUser.DistinctUserReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @User: paner
 * @Date: 17/10/29 下午12:52
 */
public class CommentMain {

    public static void main(String[] args) {
        try {
            String dstFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/comments";
            String srcFile = "/Users/pan/code/hadoopDemo/MapReducedesignPattern/src/main/resources/output";

            Configuration conf = new Configuration();
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.job.tracker", "file:///");


            Job job = Job.getInstance(conf);
            job.setJarByClass(CommentMain.class);
            job.setJobName("commentTransform");


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


//            job.setMapperClass(CommentMapper.class);
//            job.setMapperClass(PostMapper.class);
            //优化的点
            //job.setCombinerClass(DistinctUserReducer.class);
            job.setReducerClass(PostCommentHierarchyReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            MultipleInputs.addInputPath(job, new Path(dstFile),TextInputFormat.class,PostMapper.class);
            MultipleInputs.addInputPath(job,new Path(dstFile),TextInputFormat.class,CommentMapper.class);
            FileOutputFormat.setOutputPath(job, new Path(srcFile));

            job.waitForCompletion(true);


        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
