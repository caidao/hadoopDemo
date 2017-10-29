package com.paner.dp.dataOrganizingPattern.structuredToHierarchical;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;


/**
 * @User: paner
 * @Date: 17/10/28 下午11:24
 */
public class CommentMapper extends Mapper<Object,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parse = CommonUtil.transformXmlToMap(value.toString());

        if (parse.get("PostId")==null){
            return;
        }
        outKey.set(parse.get("PostId"));
        //
        outValue.set("C"+value.toString());
        context.write(outKey,outValue);

    }

}
