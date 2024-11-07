package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class job4_Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalrate = 0;
        double totalrate7 = 0;
        int count = 0;

        for (Text value : values) {
            String[] amounts = value.toString().split(",");
            if (amounts.length == 2) {
                try {
                    // 使用Double解析流入和流出量
                    totalrate += Double.parseDouble(amounts[0]);
                    totalrate7 += Double.parseDouble(amounts[1]);
                    count++;
                } catch (NumberFormatException e) {
                    // 忽略不合法的输入
                    continue;
                }
            }
        }

        // 计算平均流入和流出量
        if (count > 0) {
            double avgrate = totalrate / count;
            double avgrate7 = totalrate7 / count;
            context.write(key, new Text(avgrate + "," + avgrate7));
        }
    }
}
