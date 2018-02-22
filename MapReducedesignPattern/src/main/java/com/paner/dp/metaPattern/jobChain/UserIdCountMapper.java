package com.paner.dp.metaPattern.jobChain;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/13 下午11:04
 */
public class UserIdCountMapper extends Mapper<Object,Text,Text,LongWritable>{

    public static final String RECORDS_COUNTER_NAME ="Records";

    private static final LongWritable ONE = new LongWritable(1);
    private Text outKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());
        String userId = parsed.get("OwnerUserId");
        if (userId!=null){
            outKey.set(userId);
            context.write(outKey,ONE);
            context.getCounter(ConstantMapper.AVERAGE_CALC_GROUP,
                    RECORDS_COUNTER_NAME).increment(1);
        }


    }
}
