package com.paner.dp.metaPattern.jobChain;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/13 下午11:25
 */
public class UserIdBinningMapper extends Mapper<Object,Text,Text,Text>{

    public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";

    public static void setAveragePostsPerUser(Job job,double avg){
        job.getConfiguration().set(AVERAGE_POSTS_PER_USER,Double.toString(avg));
    }

    public static double getAveragePostPerUser(Configuration conf){
        return Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
    }

    private double average = 0.0;
    private MultipleOutputs<Text,Text> mos = null;
    private Text outkey = new Text();
    private Text outValue = new Text();
    private HashMap<String,String> userIdToReputaion = new HashMap<String,String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        average = getAveragePostPerUser(context.getConfiguration());
        mos = new MultipleOutputs<Text, Text>(context);

        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for (Path p:files){

            BufferedReader rdr = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(new File(p.toString()))));
            String line;
            while ((line = rdr.readLine())!=null){
                Map<String,String> parsed = CommonUtil.transformXmlToMap(line);
                userIdToReputaion.put(parsed.get("Id"),parsed.get("Reputation"));
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[]  tokens = value.toString().split("\t");

        String userId = tokens[0];

        int posts = Integer.parseInt(tokens[1]);

        outkey.set(userId);
        outValue.set(posts+"\t"+userIdToReputaion.get(userId));

        if (posts < average){
            mos.write(ConstantMapper.MULTIPLE_OUTPUTS_BELOW_NAME,outkey,outValue,
                    ConstantMapper.MULTIPLE_OUTPUTS_BELOW_NAME+"/part");
        }else {
            mos.write(ConstantMapper.MULTIPLE_OUTPUTS_ABOVE_NAME,outkey,outValue,
                    ConstantMapper.MULTIPLE_OUTPUTS_ABOVE_NAME+"/part");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
