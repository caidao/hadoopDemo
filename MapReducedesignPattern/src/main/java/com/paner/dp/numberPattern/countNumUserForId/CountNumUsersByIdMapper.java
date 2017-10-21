package com.paner.dp.numberPattern.countNumUserForId;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * 获取用户id,按照个位数统计用户个数
 * @User: paner
 * @Date: 17/10/21 下午3:34
 */
public class CountNumUsersByIdMapper extends Mapper<Object,Text,NullWritable,NullWritable> {


    public static final String MODE_COUNTER_GROUP  ="MODE";

    private Integer[] modeArray = new Integer[]{0,1,2,3,4,5,6,7,8,9};
    private HashSet<Integer> modes = new HashSet<Integer>(Arrays.asList(modeArray));

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String,String> parse = CommonUtil.transformXmlToMap(value.toString());

        String userId = parse.get("UserId");

        if (userId!=null){
            long userIdL = Long.valueOf(userId);
            Integer md = Integer.valueOf((int) (userIdL%10));

            if (modes.contains(md)){
                //按照mode累加
                context.getCounter(MODE_COUNTER_GROUP,String.valueOf(md)).increment(1);
            }
        }

    }
}
