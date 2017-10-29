package com.paner.dp.dataOrganizingPattern.partition;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * @User: paner
 * @Date: 17/10/29 下午9:22
 */
public class CreationDateMapper extends Mapper<Object,Text,IntWritable,Text>{

    private final static SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private IntWritable outKey = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String,String> parse = CommonUtil.transformXmlToMap(value.toString());
        String date = parse.get("CreationDate");
        if (date==null){
            return;
        }

        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(dft.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        outKey.set(cal.get(Calendar.HOUR));

        context.write(outKey,value);
    }
}
