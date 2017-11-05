package com.paner.dp.joinPattern.reduceJoin;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/5 上午10:43
 */
public class CommentJoinMapper extends Mapper<Object,Text,Text,Text>{

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());

        String userId = parsed.get("UserId");
        if (userId==null){
            return;
        }
        outKey.set(userId);
        outValue.set("B"+value.toString());

        context.write(outKey,outValue);

    }
}
