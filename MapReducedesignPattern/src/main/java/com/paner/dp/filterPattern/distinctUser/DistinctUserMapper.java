package com.paner.dp.filterPattern.distinctUser;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * 从每一个输入记录中得到用户的id，这个用户id作为键并以null作为输出到reducer
 * @User: paner
 * @Date: 17/10/24 下午10:27
 */
public class DistinctUserMapper extends Mapper<Object,Text,Text,NullWritable> {

    private Text outUserId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parser = CommonUtil.transformXmlToMap(value.toString());

        String userid = parser.get("UserId");
        if (userid==null){
            return;
        }

        outUserId.set(userid);

        context.write(outUserId,NullWritable.get());
    }
}
