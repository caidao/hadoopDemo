package com.paner.dp.metaPattern.jobChain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @User: paner
 * @Date: 17/11/13 下午11:18
 */
public class UserIdSumReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

    public static final String USER_COUNTER_NAME = "Users";
    private LongWritable outValue = new LongWritable(1);

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        context.getCounter(ConstantMapper.AVERAGE_CALC_GROUP,USER_COUNTER_NAME).increment(1);

        int sum = 0 ;
        for (LongWritable value:values){
            sum += value.get();
        }
        outValue.set(sum);
        context.write(key,outValue);
    }
}
