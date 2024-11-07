package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class job3_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text userId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        // 检查数据格式，确保字段足够多
        if (fields.length >= 9) {
            String user_id = fields[0];

            // 跳过表头行或无效数据行
            try {
                Double.parseDouble(user_id); // 尝试将 user_id 转为数字
            } catch (NumberFormatException e) {
                // 如果无法解析为数字，表明这是表头行或无效数据行，跳过
                return;
            }

            double direct_purchase_amt = Double.parseDouble(fields[5]);
            double total_redeem_amt = Double.parseDouble(fields[8]);

            // 检查是否有直接购买或赎回行为
            if (direct_purchase_amt > 0 || total_redeem_amt > 0) {
                userId.set(user_id);
                context.write(userId, one);
            }
        }
    }
}
