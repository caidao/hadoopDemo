package com.paner.dp.numberPattern.minMaxCount;

import com.paner.utils.CommonUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 通过抽取每一个输入记录中的xml属性来重新处理我们的输出值：
 * 评论的创建数据和用户id；
 * mapper的输出键是用户ID,值是我们将来要输出的三个列：最小日期，最大日期和用户评论的总数
 *
 * @User: paner
 * @Email: panyiwen2009@gmail.com
 * @Date: 17/10/15 下午9:30
 */
public class MinMaxCountMapper extends Mapper<Object,Text,Text,MinMaxCountTuple> {

    private Text  outUserId = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String,String> parsed = CommonUtil.transformXmlToMap(value.toString());
        if (parsed==null || parsed.size()==0){
            return;
        }

        String startDate = parsed.get("CreationDate");
        String userId = parsed.get("UserId");

        Date creationDate=null;
        try {
             creationDate= frmt.parse(startDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        outTuple.setMin(creationDate);
        outTuple.setMax(creationDate);

        outTuple.setCount(1);

        outUserId.set(userId);

        context.write(outUserId,outTuple);
    }

}
