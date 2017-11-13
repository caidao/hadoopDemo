package com.paner.dp.joinPattern.copyJoin;


import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/11/5 下午5:51
 */
public class ReplicatedJoinMapper extends Mapper<Object,Text,Text,Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private HashMap<String,String> userIdToInfo = new HashMap<String,String>();

    private Text outValue = new Text();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();

        for (URI file:files){
            BufferedReader reader = new BufferedReader(new FileReader(new File(file.getPath())));
            String line = null;
            while ((line=reader.readLine())!=null){
                Map<String,String> parsed = CommonUtil.transformXmlToMap(line);
                String userid = parsed.get("Id");
                if (userid!=null){
                    userIdToInfo.put(userid,line);
                }

            }
        }

        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());

        String userid = parsed.get("UserId");
        if (userid==null){
            return;
        }

        String userInfo = userIdToInfo.get(userid);
        if (userInfo!=null){
            outValue.set(userInfo);
            context.write(value,outValue);
        }
        if ("leftouter".equalsIgnoreCase(joinType)){
            context.write(value,EMPTY_TEXT);
        }
    }
}
