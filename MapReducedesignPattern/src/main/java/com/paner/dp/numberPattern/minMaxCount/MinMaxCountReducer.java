package com.paner.dp.numberPattern.minMaxCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * 遍历所有值得到其最小日期，最大日期并计算次数的总和。
 * 在确定了整个输入数据的最小日期和最大日期之后，最终计数会被设置成我们的输出值，并写入文件系统
 *
 * @User: paner
 * @Email: panyiwen2009@gmail.com
 * @Date: 17/10/15 下午9:55
 */
public class MinMaxCountReducer extends Reducer<Text,MinMaxCountTuple,Text,MinMaxCountTuple>{

    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {

        result.setMin(null);
        result.setMax(null);
        result.setCount(0);

        int sum = 0;

        for (MinMaxCountTuple val : values){
            if (val.getMin() == null || val.getMin().compareTo(result.getMin())<0){
                result.setMin(val.getMin());
            }

            if (val.getMax() == null || val.getMax().compareTo(result.getMax())>0){
                result.setMax(result.getMax());
            }
            sum += val.getCount();
        }
        result.setCount(sum);
        System.out.println("key = [" + key + "], values = [" + values + "]");
        context.write(key,result);
    }
}
