import com.paner.common.HDFS_File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by pan on 16/3/30.
 */
public class WordCount {

    public static class WordCountMap extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        //首先定义两个临时文件夹，这里可以使用随机函数+文件名，这样重名的几率就很小。
        String dstFile = "temp_src";
        String srcFile = "temp_dst";

        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://122.114.47.171:9000");
        conf.set("mapred.job.tracker","122.114.47.171:9001");

        HDFS_File file = new HDFS_File();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        //从本地上传文件到HDFS,可以是文件也可以是目录
        file.PutFile(conf, args[0], dstFile);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(dstFile));
        FileOutputFormat.setOutputPath(job, new Path(srcFile));

        job.waitForCompletion(true);

        //从HDFS取回文件保存至本地
        file.GetFile(conf, srcFile, args[1]);

        //删除临时文件或目录
        file.DelFile(conf, dstFile, true);
        file.DelFile(conf, srcFile, true);
    }
}
