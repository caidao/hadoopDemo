package com.paner.dp.filterPattern.topTen;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 *  reduce函数遍历所有的记录并将其存储在一个TreeMap结构中，
 *  当TreeMap结构中的记录超过10条记录时，第一个元素将会从map中移除
 * @User: paner
 * @Date: 17/10/23 下午9:51
 */
public class TopTenReducer extends Reducer<NullWritable,Text,NullWritable,Text>{

    private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer, Text>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value :values){
            Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());

            String score = parsed.get("Score");
            if (score==null){
                return;
            }


            repToRecordMap.put(Integer.parseInt(score),new Text(value));


            if (repToRecordMap.size()>10){
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }


        for (Text t: repToRecordMap.values()){
            context.write(NullWritable.get(),t);
        }
    }
}
