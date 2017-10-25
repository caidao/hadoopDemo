package com.paner.dp.filterPattern.distinctUser;

import jdk.nashorn.internal.objects.NativeUint8Array;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 每一个reducer都接受到要给唯一的键和一系列的null值，这些值都会被忽略
 * @User: paner
 * @Date: 17/10/24 下午10:30
 */
public class DistinctUserReducer extends Reducer<Text,NullWritable,Text,Text> {



    private Text outV = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        int count=0;
        for (NullWritable writable:values){
            count++;
        }

        outV.set(String.valueOf(count));
        context.write(key, outV);
    }
}
