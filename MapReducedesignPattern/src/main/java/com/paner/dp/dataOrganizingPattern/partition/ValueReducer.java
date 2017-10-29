package com.paner.dp.dataOrganizingPattern.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @User: paner
 * @Date: 17/10/29 下午9:34
 */
public class ValueReducer extends Reducer<IntWritable,Text,Text,NullWritable>{

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t:values){
            context.write(t,NullWritable.get());
        }
    }
}
