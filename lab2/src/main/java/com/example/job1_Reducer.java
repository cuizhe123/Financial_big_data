package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class job1_Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalInflow = 0;
        double totalOutflow = 0;

        for (Text value : values) {
            String[] amounts = value.toString().split(",");
            // 确保 amounts 数组长度为2
            if (amounts.length == 2) {
                try {
                    double inflow = Double.parseDouble(amounts[0]);
                    double outflow = Double.parseDouble(amounts[1]);
                    totalInflow += inflow;
                    totalOutflow += outflow;
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in input: " + value.toString());
                }
            } else {
                System.err.println("Invalid input format: " + value.toString());
            }
        }

        // 输出格式：<日期, "流入总量,流出总量">
        context.write(key, new Text((int) totalInflow + "," + (int) totalOutflow));
    }
}
