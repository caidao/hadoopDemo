package com.paner.dp.filterPattern.topTen;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 *  处理所有的输入记录并将其存储在TreeMap的结构中。TreeMap是Map基于键排序的子类。Integers的默认顺序是增序
 * @User: paner
 * @Date: 17/10/23 下午9:42
 */
public class TopTenMapper extends Mapper<Object,Text,NullWritable,Text>{

    private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer, Text>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
       for (Text t: repToRecordMap.values()){
           context.write(NullWritable.get(),t);
       }
    }
}
