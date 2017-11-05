package com.paner.dp.joinPattern.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @User: paner
 * @Date: 17/11/5 上午10:49
 */
public class UserJoinReducer extends Reducer<Text,Text,Text,Text>{

    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp =new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();

        for (Text tmp:values){
            if ('A'==tmp.charAt(0)){
                listA.add(new Text(tmp.toString().substring(1)));
            }else if ('B'==tmp.charAt(0)){
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }

        //执行join逻辑
        excuteJoinLogic(context);
    }


    private void excuteJoinLogic(Context context) throws IOException, InterruptedException {
        if (joinType.equalsIgnoreCase("inner")){
            if (!listA.isEmpty() && !listB.isEmpty()){
                for (Text A:listA){
                    for (Text B:listB){
                        context.write(A,B);
                    }
                }
            }
        }else if ("leftouter".equalsIgnoreCase(joinType)){
            for(Text A:listA){
                if (!listB.isEmpty()){
                    for (Text B:listB){
                        context.write(A,B);
                    }
                }else {
                    context.write(A,EMPTY_TEXT);
                }
            }
        }else if ("rightouter".equalsIgnoreCase(joinType)){
            for (Text B:listB){
                if (!listA.isEmpty()){
                    for (Text A:listA){
                        context.write(A,B);
                    }
                }else {
                    context.write(EMPTY_TEXT,B);
                }
            }
        }
    }
}
