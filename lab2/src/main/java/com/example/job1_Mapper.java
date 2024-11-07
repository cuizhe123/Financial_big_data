package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class job1_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        if (columns.length < 5) {
            return;  // 跳过无效行
        }

        String reportDate = columns[1];
        String totalPurchaseAmt = columns[4].isEmpty() ? "0" : columns[4];
        String totalRedeemAmt = columns[8].isEmpty() ? "0" : columns[8];

        context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
    }
}
