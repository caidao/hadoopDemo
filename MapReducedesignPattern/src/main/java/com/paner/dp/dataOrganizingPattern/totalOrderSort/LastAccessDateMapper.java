package com.paner.dp.dataOrganizingPattern.totalOrderSort;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/2 上午9:29
 */
public class LastAccessDateMapper extends Mapper<Object,Text,Text,Text>{

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());
        String lastAccessDate = parsed.get("LastAccessDate");
        if (lastAccessDate==null){
            return;
        }
        outKey.set(lastAccessDate);
        outValue.set(parsed.get("LastAccessDate")+","+parsed.get("DisplayName"));
        context.write(outKey,outValue);
    }
}
