package com.paner.dp.dataOrganizingPattern.totalOrderSort;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/2 上午9:46
 */
public class ValueReducer extends Reducer<Text,Text,Text,NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t:values){
            context.write(t,NullWritable.get());
        }
    }
}
