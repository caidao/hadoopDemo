package com.paner.dp.dataOrganizingPattern.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @User: paner
 * @Date: 17/10/29 下午9:31
 */
public class CreationDatePartitioner extends Partitioner<IntWritable,Text>{

    private static final String MIN_LAST_MONTH = "min.last.month";

    private Configuration conf = null;
    private int minLastMonth = 0;

    public int getPartition(IntWritable intWritable, Text text, int i) {
        return intWritable.get()%4-minLastMonth;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        minLastMonth = conf.getInt(MIN_LAST_MONTH,0);
    }

    public static void setMinLastMonth(Job job,int minLastMonth){
        job.getConfiguration().setInt(MIN_LAST_MONTH,minLastMonth);

    }
}
