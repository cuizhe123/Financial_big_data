package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class job3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final Map<String, Integer> userActivityMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int activeDays = 0;

        // 累加活跃天数
        for (IntWritable val : values) {
            activeDays += val.get();
        }

        // 将结果保存到 Map 中
        userActivityMap.put(key.toString(), activeDays);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 将 Map 转换为 List 并按活跃天数降序排序
        List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(userActivityMap.entrySet());
        sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // 输出排序后的结果
        for (Map.Entry<String, Integer> entry : sortedList) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}
