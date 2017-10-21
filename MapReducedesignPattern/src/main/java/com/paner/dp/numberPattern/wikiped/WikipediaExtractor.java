package com.paner.dp.numberPattern.wikiped;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;


import java.io.IOException;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/10/18 下午11:34
 */
public class WikipediaExtractor extends Mapper<Object,Text,Text,Text> {

    private Text link = new Text();
    private Text outkey = new Text();

    public WikipediaExtractor() {
        super();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());

        String txt = parsed.get("Body");
        String posttype = parsed.get("PostTypeId");
        String rowid = parsed.get("id");

        //
        if (txt==null || (posttype != null && posttype.equals("1"))){
            return;
        }

        txt = StringUtils.unEscapeString(txt.toLowerCase()) ;
        link.set(getWikipediaUrl(txt));
        outkey.set(rowid);

        context.write(link,outkey);
       // link.set();
    }

    private String getWikipediaUrl(String txt){
        return "";
    }
}
