package com.paner.dp.numberPattern.wikiped;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @User: paner
 * @Date: 17/10/19 下午10:05
 */
public class Concatenator extends Reducer<Text,Text,Text,Text>{

    private Text result = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        boolean first = true;

        for (Text id: values){
            if (first){
                first =false;
            }else {
                builder.append(" ");
            }
            builder.append(id.toString());
        }

        result.set(builder.toString());
        context.write(key,result);
    }
}
