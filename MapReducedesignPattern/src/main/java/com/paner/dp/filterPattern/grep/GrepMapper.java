package com.paner.dp.filterPattern.grep;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @User: paner
 * @Date: 17/10/21 下午10:53
 */
public class GrepMapper extends Mapper<Object,Text,NullWritable,Text>{

    private String mapRegex = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        mapRegex = context.getConfiguration().get("mapregex");
        System.out.println("regex = [" + mapRegex + "]");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().contains(mapRegex)){
            System.out.println(value.toString());
            context.write(NullWritable.get(),value);
        }
    }
}
